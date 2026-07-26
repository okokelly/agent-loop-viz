#!/usr/bin/env python3
"""
Agent Manager (v3) — Solo dev's AI-agent management layer.

Two levels of visibility for one person running many agents in parallel:

  * Top level   — Dashboard / Kanban / Table / Inbox across ALL tasks.
  * Drill-down  — click any task to see its loop flowchart (orchestrator →
                  subagents → verifier) and exactly which step is stuck.

Single file, zero dependencies (Python stdlib). Agents push state over a small
REST API; the browser updates live over SSE. Built for a single machine / one
user — no accounts, no multi-person collaboration.

Reuses the SSE / persistence / cancellable-simulation machinery from v2.
"""
import copy
import json
import math
import os
import queue
import random
import socket
import sys
import threading
import time
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import unquote, urlparse


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


try:
    PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8768
except (TypeError, ValueError):
    raise SystemExit("PORT must be an integer")
if not 1 <= PORT <= 65535:
    raise SystemExit("PORT must be between 1 and 65535")

MAX_CONTENT_LENGTH = 64 * 1024
MAX_SSE_CLIENTS = 32
REQUEST_TIMEOUT = 10
SSE_HEARTBEAT_INTERVAL = 15
SSE_WRITE_TIMEOUT = 10
STATE_FILE = os.path.join(os.getcwd(), "agent-manager-state.json")
STATE_MAX_AGE_SECONDS = 24 * 60 * 60

TASK_STATUSES = ("todo", "running", "review", "blocked", "done")
NODE_STATUSES = ("pending", "queued", "running", "done", "blocked")
TOKEN_COST_PER_M = 0.435
MAX_ACTIVITY = 40
MAX_LEARNINGS = 30
AUTO_START_DEMO = os.environ.get("AGENT_MGR_AUTOSTART", "1") != "0"


def normalize_path_prefix(value):
    value = (value or "").strip().strip("/")
    if not value:
        return ""
    return "/" + value


PATH_PREFIX = normalize_path_prefix(os.environ.get("AGENT_MGR_PATH_PREFIX", ""))


# ── Data model ─────────────────────────────────────────────────
# A *task* is owned by an agent and carries management-level fields (for the
# Kanban / Inbox / Table) plus a *graph* — the loop it runs internally, used
# for the drill-down flowchart.

def build_graph(orch_task, workers, ver_task):
    """Lay out a plan → workers → verify loop for the drill-down view."""
    nodes = [{"id": "orch", "label": "Plan", "x": 320, "y": 48,
              "status": "pending", "tokens": 0, "task": orch_task}]
    edges = []
    count = len(workers)
    for index, (label, task) in enumerate(workers):
        wid = f"w{index + 1}"
        x = 320 + (index - (count - 1) / 2) * 200
        nodes.append({"id": wid, "label": label, "x": x, "y": 180,
                      "status": "pending", "tokens": 0, "task": task})
        edges.append({"from": "orch", "to": wid, "label": ""})
        edges.append({"from": wid, "to": "ver", "label": ""})
    nodes.append({"id": "ver", "label": "Verify", "x": 320, "y": 312,
                  "status": "pending", "tokens": 0, "task": ver_task})
    return {"viewBox": "0 0 640 380", "nodes": nodes, "edges": edges}


def make_task(tid, title, agent, avatar, status, todo, graph, tags=None):
    return {
        "id": tid,
        "title": title,
        "agent": agent,
        "avatar": avatar,
        "status": status,
        "todo": todo,
        "tags": tags or [],
        "needs_attention": status in ("review", "blocked"),
        "attention_reason": "",
        "tokens": 0,
        "created_at": time.time(),
        "updated_at": time.time(),
        "graph": graph,
    }


def initial_tasks():
    return [
        make_task(
            "t1", "Refactor auth → JWT", "auth-agent", "🔐", "todo",
            "Waiting for dispatch",
            build_graph(
                "Map session-cookie flow, list risks",
                [("Implement", "Swap to JWT issue/verify"),
                 ("Migrate", "Backfill existing sessions"),
                 ("Tests", "Run auth + e2e suite")],
                "Independent security re-check"),
            ["backend", "security"]),
        make_task(
            "t2", "Write REST API docs", "docs-agent", "📝", "todo",
            "Waiting for dispatch",
            build_graph(
                "Enumerate endpoints from routes",
                [("Draft", "Write per-endpoint reference"),
                 ("Examples", "Add curl + response samples")],
                "Lint links, check completeness"),
            ["docs"]),
        make_task(
            "t3", "Fix flaky CI on macOS", "ci-agent", "🧪", "todo",
            "Waiting for dispatch",
            build_graph(
                "Reproduce flake, bisect commits",
                [("Diagnose", "Trace race in test harness"),
                 ("Patch", "Add retry + fix timer")],
                "Re-run 50× to confirm stable"),
            ["infra"]),
        make_task(
            "t4", "Add dark mode toggle", "ui-agent", "🎨", "todo",
            "Waiting for dispatch",
            build_graph(
                "Audit hardcoded colors",
                [("Tokens", "Extract CSS variables"),
                 ("Toggle", "Wire persisted switch")],
                "Visual diff light vs dark"),
            ["frontend"]),
        make_task(
            "t5", "Migrate DB schema v4", "db-agent", "🗄️", "todo",
            "Waiting for dispatch",
            build_graph(
                "Draft migration + rollback plan",
                [("Write", "Author up/down migrations"),
                 ("Dry-run", "Apply to shadow DB")],
                "Verify row counts + constraints"),
            ["backend", "data"]),
    ]


STATE = {
    "project": "agent-loop-viz  ·  solo dev",
    "elapsed": 0,
    "total_tokens": 0,
    "total_cost": 0.0,
    "tasks": initial_tasks(),
    "learnings": [],
    "activity": [],
}

STATE_LOCK = threading.RLock()
PERSISTENCE_LOCK = threading.Lock()
SSE_CLIENTS = []
CONTROL = {"paused": False, "speed": 1.0, "running": False}


# ── Cancellable simulation machinery (ported from v2) ──────────
class RunCancelled(Exception):
    """Raised when a simulation loses ownership of the active run."""


class RunContext:
    def __init__(self, generation):
        self.generation = generation
        self.cancel = threading.Event()
        self.thread = None
        self.started_at = 0.0
        self.paused_since = None
        self.paused_total = 0.0


_thread_context = threading.local()
_run_lifecycle_lock = threading.Lock()
_active_context = None
_run_generation = 0
_sse_event_id = 0


def current_context():
    return getattr(_thread_context, "run", None)


def _is_current_locked(context):
    return (
        context is None
        or (
            context is _active_context
            and context.generation == _run_generation
            and CONTROL["running"]
            and not context.cancel.is_set()
        )
    )


def ensure_current(context=None):
    context = current_context() if context is None else context
    if context is None:
        return
    with STATE_LOCK:
        if not _is_current_locked(context):
            raise RunCancelled


def state_snapshot():
    with STATE_LOCK:
        return copy.deepcopy(STATE)


def _elapsed_for_locked(context, now=None):
    if not context.started_at:
        return 0
    now = time.monotonic() if now is None else now
    paused = context.paused_total
    if context.paused_since is not None:
        paused += now - context.paused_since
    return max(0, int(now - context.started_at - paused))


def update_elapsed(context):
    with STATE_LOCK:
        if not _is_current_locked(context):
            raise RunCancelled
        STATE["elapsed"] = _elapsed_for_locked(context)


def _sse_payload(snapshot, event_id):
    data = json.dumps(snapshot, separators=(",", ":"))
    return f"id: {event_id}\ndata: {data}\n\n"


def persist_state(snapshot):
    temporary_path = STATE_FILE + ".tmp"
    try:
        with PERSISTENCE_LOCK:
            with open(temporary_path, "w", encoding="utf-8") as state_file:
                json.dump(snapshot, state_file, ensure_ascii=False, indent=2)
                state_file.write("\n")
            os.replace(temporary_path, STATE_FILE)
    except OSError as error:
        print(f"Warning: could not persist state: {error}", file=sys.stderr)


def load_persisted_state():
    try:
        age = time.time() - os.path.getmtime(STATE_FILE)
    except FileNotFoundError:
        return False
    except OSError as error:
        print(f"Warning: could not inspect persisted state: {error}", file=sys.stderr)
        return False
    if age >= STATE_MAX_AGE_SECONDS:
        return False
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as state_file:
            persisted = json.load(state_file)
    except (OSError, json.JSONDecodeError) as error:
        print(f"Warning: could not load persisted state: {error}", file=sys.stderr)
        return False
    if not isinstance(persisted, dict):
        print("Warning: persisted state must be a JSON object", file=sys.stderr)
        return False
    with STATE_LOCK:
        STATE.update(copy.deepcopy(persisted))
    return True


def broadcast():
    global _sse_event_id
    with STATE_LOCK:
        snapshot = copy.deepcopy(STATE)
        _sse_event_id += 1
        event_id = _sse_event_id
        clients = tuple(SSE_CLIENTS)
    persist_state(snapshot)
    payload = _sse_payload(snapshot, event_id)
    dead = []
    for client_queue in clients:
        try:
            client_queue.put_nowait(payload)
        except queue.Full:
            try:
                client_queue.get_nowait()
            except queue.Empty:
                pass
            try:
                client_queue.put_nowait(payload)
            except queue.Full:
                dead.append(client_queue)
        except (RuntimeError, OSError):
            dead.append(client_queue)
    if dead:
        with STATE_LOCK:
            for client_queue in dead:
                if client_queue in SSE_CLIENTS:
                    SSE_CLIENTS.remove(client_queue)


def current_sse_snapshot(last_event_id=None):
    global _sse_event_id
    with STATE_LOCK:
        if last_event_id is not None:
            _sse_event_id = max(_sse_event_id, last_event_id)
        _sse_event_id += 1
        event_id = _sse_event_id
        snapshot = copy.deepcopy(STATE)
    return _sse_payload(snapshot, event_id)


def add_sse_client(client_queue):
    with STATE_LOCK:
        if len(SSE_CLIENTS) >= MAX_SSE_CLIENTS:
            return False
        SSE_CLIENTS.append(client_queue)
        return True


def remove_sse_client(client_queue):
    with STATE_LOCK:
        if client_queue in SSE_CLIENTS:
            SSE_CLIENTS.remove(client_queue)


def set_paused(paused):
    now = time.monotonic()
    with STATE_LOCK:
        context = _active_context
        if paused and not CONTROL["paused"]:
            CONTROL["paused"] = True
            if context is not None and context.started_at and context.paused_since is None:
                context.paused_since = now
        elif not paused and CONTROL["paused"]:
            CONTROL["paused"] = False
            if context is not None and context.paused_since is not None:
                context.paused_total += now - context.paused_since
                context.paused_since = None
    broadcast()


def set_speed(speed):
    with STATE_LOCK:
        CONTROL["speed"] = speed
    broadcast()


def wait_tick(seconds, context=None):
    context = current_context() if context is None else context
    remaining = float(seconds)
    while remaining > 0:
        ensure_current(context)
        with STATE_LOCK:
            paused = CONTROL["paused"]
            speed = CONTROL["speed"]
        if paused:
            if context.cancel.wait(0.1):
                raise RunCancelled
            continue
        chunk = min(0.1, remaining / speed)
        if context.cancel.wait(chunk):
            raise RunCancelled
        remaining -= chunk * speed
    update_elapsed(context)
    broadcast()


# ── Task-level mutations ───────────────────────────────────────
def _find_task_locked(tid):
    for task in STATE["tasks"]:
        if task["id"] == tid:
            return task
    return None


def _recompute_attention(task, explicit=None, reason=None):
    if explicit is not None:
        task["needs_attention"] = bool(explicit)
    else:
        task["needs_attention"] = task["status"] in ("review", "blocked")
    if reason is not None:
        task["attention_reason"] = reason
    elif not task["needs_attention"]:
        task["attention_reason"] = ""


def set_task(tid, _bypass_context=False, reason=None, needs_attention=None, **fields):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        task = _find_task_locked(tid)
        if task is None:
            return False
        for key, value in fields.items():
            if key in ("id", "graph"):
                continue
            task[key] = value
        _recompute_attention(task, explicit=needs_attention, reason=reason)
        task["updated_at"] = time.time()
        return True


def set_node(tid, nid, _bypass_context=False, **fields):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        task = _find_task_locked(tid)
        if task is None:
            return False
        for node in task["graph"]["nodes"]:
            if node["id"] == nid:
                node.update(fields)
                task["updated_at"] = time.time()
                return True
        return False


def burn(tid, nid, amount):
    context = current_context()
    ensure_current(context)
    with STATE_LOCK:
        if not _is_current_locked(context):
            raise RunCancelled
        STATE["total_tokens"] += amount
        STATE["total_cost"] = round(STATE["total_tokens"] / 1_000_000 * TOKEN_COST_PER_M, 4)
        task = _find_task_locked(tid)
        if task is None:
            return
        task["tokens"] += amount
        for node in task["graph"]["nodes"]:
            if node["id"] == nid:
                node["tokens"] += amount
                break


def add_activity(msg, _bypass_context=False):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    stamp = time.strftime("%H:%M:%S")
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        STATE["activity"].append(f"[{stamp}] {msg}")
        if len(STATE["activity"]) > MAX_ACTIVITY:
            STATE["activity"] = STATE["activity"][-MAX_ACTIVITY:]


def add_learning(text, task_title="", agent="", _bypass_context=False):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    stamp = time.strftime("%H:%M")
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        STATE["learnings"].insert(0, {
            "time": stamp, "task": task_title, "agent": agent, "text": text,
        })
        if len(STATE["learnings"]) > MAX_LEARNINGS:
            STATE["learnings"] = STATE["learnings"][:MAX_LEARNINGS]


def reset_state(_bypass_context=False):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        STATE["elapsed"] = 0
        STATE["total_tokens"] = 0
        STATE["total_cost"] = 0.0
        STATE["tasks"] = initial_tasks()
        STATE["learnings"] = []
        STATE["activity"] = []


# ── Solo-dev demo simulation ───────────────────────────────────
def _dispatch(tid, todo):
    set_task(tid, status="running", todo=todo)


def simulate_loop(context):
    with STATE_LOCK:
        if not _is_current_locked(context):
            raise RunCancelled
        context.started_at = time.monotonic()
        if CONTROL["paused"]:
            context.paused_since = context.started_at
    _thread_context.run = context

    reset_state()
    add_activity("Solo dev online — 5 agents standing by")
    broadcast()
    wait_tick(0.6)

    # ── t1: Refactor auth → JWT (runs a full loop, finishes) ──────
    _dispatch("t1", "Planning JWT refactor")
    set_node("t1", "orch", status="running")
    add_activity("auth-agent 🔐 dispatched: Refactor auth → JWT")
    wait_tick(0.8)
    burn("t1", "orch", 3200)
    add_learning("Auth uses express-session + Redis, NOT Passport. JWT must "
                 "coexist during migration.", "Refactor auth → JWT", "auth-agent")
    set_node("t1", "orch", status="done")
    set_node("t1", "w1", status="running")
    set_task("t1", todo="Implementing JWT issue/verify")
    add_activity("auth-agent 🔐 plan done → implementing")
    wait_tick(0.9)
    burn("t1", "w1", 5400)
    set_node("t1", "w1", status="done")
    set_node("t1", "w2", status="running")
    set_task("t1", todo="Backfilling existing sessions")
    wait_tick(0.8)
    burn("t1", "w2", 4100)
    set_node("t1", "w2", status="done")
    set_node("t1", "w3", status="running")
    set_task("t1", todo="Running auth + e2e suite")
    wait_tick(0.7)
    burn("t1", "w3", 3600)
    set_node("t1", "w3", status="done")
    set_node("t1", "ver", status="running")
    wait_tick(0.6)
    burn("t1", "ver", 2200)
    set_node("t1", "ver", status="done")
    set_task("t1", status="done", todo="Merged to feature/jwt-auth")
    add_activity("auth-agent 🔐 ✅ done — all 42 auth tests green")

    # ── t2: API docs (dispatch, mid-flight) ──────────────────────
    _dispatch("t2", "Enumerating endpoints from routes")
    set_node("t2", "orch", status="running")
    add_activity("docs-agent 📝 dispatched: Write REST API docs")
    wait_tick(0.8)
    burn("t2", "orch", 2400)
    set_node("t2", "orch", status="done")
    set_node("t2", "w1", status="running")
    set_task("t2", todo="Drafting per-endpoint reference (11 routes)")
    wait_tick(0.7)
    burn("t2", "w1", 4800)

    # ── t3: Fix flaky CI (dispatch → BLOCKED, needs attention) ────
    _dispatch("t3", "Reproducing the flake")
    set_node("t3", "orch", status="running")
    add_activity("ci-agent 🧪 dispatched: Fix flaky CI on macOS")
    wait_tick(0.8)
    burn("t3", "orch", 2600)
    set_node("t3", "orch", status="done")
    set_node("t3", "w1", status="running")
    set_task("t3", todo="Tracing race in test harness")
    wait_tick(0.7)
    burn("t3", "w1", 3300)
    set_node("t3", "w1", status="blocked")
    set_task("t3", status="blocked",
             todo="Needs macOS runner secret to reproduce",
             reason="Blocked: missing CI secret MACOS_RUNNER_TOKEN — needs you")
    add_activity("ci-agent 🧪 ⛔ BLOCKED — missing MACOS_RUNNER_TOKEN")
    add_learning("Flake only repros on macOS runners; Linux is clean. Root "
                 "cause is a timer race, not the test itself.", "Fix flaky CI on macOS",
                 "ci-agent")

    # ── t4: Dark mode (dispatch → REVIEW, needs attention) ───────
    _dispatch("t4", "Auditing hardcoded colors")
    set_node("t4", "orch", status="running")
    add_activity("ui-agent 🎨 dispatched: Add dark mode toggle")
    wait_tick(0.8)
    burn("t4", "orch", 2100)
    set_node("t4", "orch", status="done")
    set_node("t4", "w1", status="running")
    set_task("t4", todo="Extracting CSS variables")
    wait_tick(0.7)
    burn("t4", "w1", 3900)
    set_node("t4", "w1", status="done")
    set_node("t4", "w2", status="running")
    set_task("t4", todo="Wiring persisted toggle")
    wait_tick(0.7)
    burn("t4", "w2", 2800)
    set_node("t4", "w2", status="done")
    set_node("t4", "ver", status="running")
    wait_tick(0.5)
    burn("t4", "ver", 1500)
    set_node("t4", "ver", status="done")
    set_task("t4", status="review",
             todo="PR #128 open — needs your review",
             reason="Ready for review: 6 files, +214 −58 · visual diff attached")
    add_activity("ui-agent 🎨 🔍 ready for review — PR #128")

    # ── t5 stays in Todo; final summary ──────────────────────────
    add_activity("Board: 1 done · 1 in-progress · 1 review · 1 blocked · 1 todo")
    broadcast()


def run_simulation(context):
    global _active_context
    _thread_context.run = context
    try:
        simulate_loop(context)
    except RunCancelled:
        pass
    finally:
        with STATE_LOCK:
            if _active_context is context:
                CONTROL["running"] = False
                CONTROL["paused"] = False
                _active_context = None
        _thread_context.run = None


def restart_simulation():
    global _active_context, _run_generation
    with _run_lifecycle_lock:
        with STATE_LOCK:
            old_context = _active_context
            if old_context is not None:
                old_context.cancel.set()
            _run_generation += 1
            context = RunContext(_run_generation)
            _active_context = context
            CONTROL["running"] = True
            CONTROL["paused"] = False
            old_thread = old_context.thread if old_context is not None else None
        if old_thread is not None and old_thread is not threading.current_thread():
            old_thread.join()
        thread = threading.Thread(
            target=run_simulation, args=(context,),
            name=f"simulate-loop-{context.generation}", daemon=True)
        context.thread = thread
        thread.start()
    return context.generation


def stop_simulation():
    with _run_lifecycle_lock:
        with STATE_LOCK:
            context = _active_context
            if context is not None:
                context.cancel.set()
            thread = context.thread if context is not None else None
        if thread is not None and thread is not threading.current_thread():
            thread.join()


HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Agent Manager</title>
<style>
:root{
  --bg:#f4f6fa;--bg2:#eef1f7;--card:#ffffff;--card2:#fafbfd;--text:#161b22;--text2:#3d444d;
  --border:#e2e6ec;--border2:#d3d9e0;--muted:#6b7480;--hover:#f0f3f8;--col-bg:#eaeef4;
  --accent:#5b5bf0;--accent2:#8a5cf6;--accent-soft:#ecebfe;--ring:rgba(91,91,240,.35);
  --shadow-sm:0 1px 2px rgba(20,25,35,.06);--shadow:0 4px 16px rgba(20,25,35,.09);
  --shadow-lg:0 20px 60px rgba(20,25,35,.24);--glass:rgba(255,255,255,.72);
  --todo:#7a8593;--todo-bg:#eef1f5;--run:#c07a00;--run-bg:#fff5e0;
  --review:#7c4dff;--review-bg:#efeaff;--block:#e5484d;--block-bg:#ffeceb;
  --done:#1f9c54;--done-bg:#e6f7ec;
  --n-pending:#eef1f5;--n-queued:#fff2cf;--n-running:#fff6dd;--n-done:#dcf5e4;--n-blocked:#ffdedc;
  --d-pending:#9aa4b1;--d-queued:#d9a01a;--d-running:#efc200;--d-done:#22a85a;--d-blocked:#e5484d;
  --edge:#d3d9e0;--edge-run:#efc200;--edge-done:#9be0b6;
}
[data-theme="dark"]{
  --bg:#0b0e14;--bg2:#0e121a;--card:#151a23;--card2:#11161e;--text:#e6edf3;--text2:#adb7c2;
  --border:#242c38;--border2:#2e3745;--muted:#8b95a3;--hover:#1b212c;--col-bg:#0f141c;
  --accent:#7b7bff;--accent2:#a780ff;--accent-soft:#211f3d;--ring:rgba(123,123,255,.4);
  --shadow-sm:0 1px 2px rgba(0,0,0,.4);--shadow:0 6px 22px rgba(0,0,0,.5);
  --shadow-lg:0 24px 70px rgba(0,0,0,.7);--glass:rgba(21,26,35,.72);
  --todo:#8b95a3;--todo-bg:#1a2027;--run:#e3b341;--run-bg:#2c2410;
  --review:#b18bff;--review-bg:#241d3d;--block:#ff6b6b;--block-bg:#3a1a1c;
  --done:#3fce74;--done-bg:#0f2a1a;
  --n-pending:#1a212b;--n-queued:#38300f;--n-running:#3d3512;--n-done:#123021;--n-blocked:#411a1d;
  --d-pending:#8b95a3;--d-queued:#d9a422;--d-running:#e8c33a;--d-done:#3fce74;--d-blocked:#ff6b6b;
  --edge:#2e3745;--edge-run:#d9a422;--edge-done:#1f7a45;
}
*{margin:0;padding:0;box-sizing:border-box}
html,body{height:100%}
body{background:linear-gradient(180deg,var(--bg),var(--bg2));color:var(--text);
  font:13.5px/1.55 -apple-system,BlinkMacSystemFont,"Segoe UI",Inter,sans-serif;
  display:flex;flex-direction:column;height:100vh;overflow:hidden;-webkit-font-smoothing:antialiased}
::selection{background:var(--accent-soft)}
.mono{font-family:"SF Mono",ui-monospace,"JetBrains Mono",monospace;font-variant-numeric:tabular-nums}
button{font-family:inherit}
/* header */
header{background:var(--glass);backdrop-filter:blur(14px);-webkit-backdrop-filter:blur(14px);
  border-bottom:1px solid var(--border);padding:9px 16px;display:flex;align-items:center;gap:14px;
  flex-shrink:0;min-height:52px;z-index:20}
.brand{display:flex;align-items:center;gap:8px;font-size:15px;font-weight:700;letter-spacing:-.01em;white-space:nowrap}
.brand .logo{width:26px;height:26px;border-radius:8px;display:grid;place-items:center;font-size:15px;
  background:linear-gradient(135deg,var(--accent),var(--accent2));box-shadow:0 2px 8px var(--ring)}
.brand .nm b{background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;background-clip:text;-webkit-text-fill-color:transparent}
.project{font-size:12px;color:var(--muted);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;max-width:200px;padding-left:2px}
.spacer{flex:1}
.search{display:flex;align-items:center;gap:6px;background:var(--card);border:1px solid var(--border);
  border-radius:9px;padding:5px 10px;min-width:150px;transition:border-color .15s,box-shadow .15s}
.search:focus-within{border-color:var(--accent);box-shadow:0 0 0 3px var(--ring)}
.search input{border:none;background:none;outline:none;color:var(--text);font-size:12.5px;width:100%}
.search .k{color:var(--muted);font-size:11px}
.stats{display:flex;gap:16px;font-size:11.5px;color:var(--muted)}
.stats .v{color:var(--text);font-weight:600}
.stats .v.acc{color:var(--accent)}
.ctrl{display:flex;gap:6px;align-items:center}
.iconbtn{background:var(--card);border:1px solid var(--border);color:var(--text2);width:32px;height:32px;
  border-radius:9px;cursor:pointer;font-size:13px;display:grid;place-items:center;transition:all .15s}
.iconbtn:hover{background:var(--hover);color:var(--text);border-color:var(--border2)}
.bell{position:relative}
.bell .dot{position:absolute;top:-5px;right:-5px;background:var(--block);color:#fff;font-size:10px;font-weight:700;
  min-width:16px;height:16px;border-radius:8px;display:grid;place-items:center;padding:0 4px;box-shadow:0 0 0 2px var(--card)}
/* nav */
nav{background:var(--card);border-bottom:1px solid var(--border);display:flex;align-items:center;gap:2px;
  padding:0 14px;flex-shrink:0}
nav button{background:none;border:none;color:var(--muted);padding:11px 13px;font-size:13px;font-weight:500;
  cursor:pointer;border-bottom:2px solid transparent;display:flex;align-items:center;gap:7px;transition:color .15s}
nav button:hover{color:var(--text)}
nav button.active{color:var(--text);border-bottom-color:var(--accent);font-weight:600}
nav .cnt{background:var(--col-bg);color:var(--muted);border-radius:20px;font-size:11px;font-weight:600;
  padding:1px 7px;min-width:20px;text-align:center}
nav button.active .cnt{background:var(--accent);color:#fff}
nav .cnt.alert{background:var(--block-bg);color:var(--block)}
nav button.active .cnt.alert{background:var(--block);color:#fff}
main{flex:1;overflow:auto;padding:18px}
.wrap{max-width:1180px;margin:0 auto}
/* dashboard */
.tiles{display:grid;grid-template-columns:repeat(5,1fr);gap:12px;margin-bottom:18px}
.tile{background:var(--card);border:1px solid var(--border);border-radius:14px;padding:14px 16px;cursor:pointer;
  position:relative;overflow:hidden;transition:transform .12s,box-shadow .18s,border-color .18s;box-shadow:var(--shadow-sm)}
.tile::before{content:"";position:absolute;left:0;top:0;bottom:0;width:3px;background:var(--tc)}
.tile:hover{transform:translateY(-2px);box-shadow:var(--shadow);border-color:var(--border2)}
.tile .n{font-size:30px;font-weight:750;line-height:1;letter-spacing:-.02em}
.tile .l{font-size:12px;color:var(--muted);margin-top:6px;display:flex;align-items:center;gap:6px;font-weight:500}
.tile .l .sw{width:8px;height:8px;border-radius:50%;background:var(--tc)}
.dash-grid{display:grid;grid-template-columns:1.1fr .9fr;gap:16px}
.panel{background:var(--card);border:1px solid var(--border);border-radius:14px;overflow:hidden;box-shadow:var(--shadow-sm)}
.panel h3{font-size:11.5px;font-weight:700;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;
  padding:13px 16px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:8px}
.panel .body{max-height:calc(100vh - 350px);overflow:auto}
.learn{padding:12px 16px;border-bottom:1px solid var(--border);display:flex;gap:10px}
.learn:last-child{border-bottom:none}
.learn .bulb{font-size:15px;line-height:1.4;flex-shrink:0}
.learn .meta{font-size:11px;color:var(--muted);margin-bottom:2px}
.learn .txt{font-size:13px;color:var(--text2)}
.act{padding:8px 16px;font-size:12px;color:var(--muted);border-bottom:1px solid var(--border);
  word-break:break-word;display:flex;gap:8px}
.act:last-child{border-bottom:none}
.act .t{color:var(--muted);opacity:.7;flex-shrink:0}
.act.ok .m{color:var(--done)}.act.bl .m{color:var(--block);font-weight:600}.act.rv .m{color:var(--review)}
.empty{padding:34px 16px;text-align:center;color:var(--muted);font-size:13px}
.empty .big{font-size:30px;display:block;margin-bottom:8px;opacity:.85}
/* kanban */
.board{display:grid;grid-template-columns:repeat(5,minmax(216px,1fr));gap:14px;align-items:start}
.col{background:var(--col-bg);border-radius:14px;padding:10px;display:flex;flex-direction:column;min-width:0;
  transition:background .15s,box-shadow .15s;border:1.5px solid transparent}
.col.drop{background:var(--accent-soft);border-color:var(--accent);box-shadow:0 0 0 3px var(--ring)}
.col h4{font-size:11.5px;font-weight:700;padding:5px 7px 10px;display:flex;align-items:center;gap:7px;
  color:var(--muted);text-transform:uppercase;letter-spacing:.04em}
.col h4 .sw{width:9px;height:9px;border-radius:50%;box-shadow:0 0 0 3px var(--swg)}
.col h4 .cnt{margin-left:auto;background:var(--card);border-radius:20px;padding:1px 8px;font-size:11px;color:var(--text2)}
.cards{display:flex;flex-direction:column;gap:9px;min-height:12px}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:11px 12px;cursor:pointer;
  position:relative;transition:box-shadow .16s,transform .1s,border-color .16s;box-shadow:var(--shadow-sm);
  border-left:3px solid var(--cc,var(--border))}
.card:hover{box-shadow:var(--shadow);transform:translateY(-1px);border-color:var(--border2);border-left-color:var(--cc)}
.card.dragging{opacity:.45;transform:rotate(1.5deg) scale(.98)}
.card .ttl{font-size:13px;font-weight:650;margin-bottom:7px;line-height:1.35;padding-right:16px}
.card .todo{font-size:12px;color:var(--muted);margin-bottom:9px;line-height:1.42;
  display:-webkit-box;-webkit-line-clamp:2;-webkit-box-orient:vertical;overflow:hidden}
.loopbar{display:flex;gap:3px;margin-bottom:9px}
.loopbar i{height:5px;flex:1;border-radius:3px;background:var(--n-pending);transition:background .3s}
.loopbar i.done{background:var(--d-done)}.loopbar i.running{background:var(--d-running)}
.loopbar i.queued{background:var(--d-queued)}.loopbar i.blocked{background:var(--d-blocked)}
.card .foot{display:flex;align-items:center;gap:7px;font-size:11px;color:var(--muted)}
.card .agent{display:flex;align-items:center;gap:5px;font-weight:500;color:var(--text2);min-width:0}
.card .agent .em{font-size:13px}
.card .agent .nm{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.sdot{width:7px;height:7px;border-radius:50%;flex-shrink:0}
.card .meta-r{margin-left:auto;display:flex;align-items:center;gap:8px;flex-shrink:0}
.card .tok{color:var(--accent)}
.card .ago{opacity:.75}
.attn-flag{position:absolute;top:10px;right:11px;font-size:12px}
.tag{font-size:10px;background:var(--col-bg);color:var(--muted);border-radius:5px;padding:1px 6px;font-weight:500}
/* status pill */
.st{display:inline-flex;align-items:center;gap:5px;font-size:11px;font-weight:650;border-radius:20px;
  padding:2px 9px;white-space:nowrap}
.st .d{width:6px;height:6px;border-radius:50%;background:currentColor}
/* table */
.tbl-wrap{background:var(--card);border:1px solid var(--border);border-radius:14px;overflow:hidden;box-shadow:var(--shadow-sm)}
table{width:100%;border-collapse:collapse}
th{text-align:left;font-size:11px;text-transform:uppercase;letter-spacing:.04em;color:var(--muted);
  padding:11px 14px;border-bottom:1px solid var(--border);font-weight:700;cursor:pointer;user-select:none;white-space:nowrap}
th:hover{color:var(--text)}
th .ar{opacity:.5;font-size:9px}
td{padding:11px 14px;border-bottom:1px solid var(--border);font-size:13px;vertical-align:middle}
tr:last-child td{border-bottom:none}
tbody tr{cursor:pointer;transition:background .12s}
tbody tr:hover{background:var(--hover)}
.tprog{display:flex;align-items:center;gap:8px}
.tprog .bar{width:54px;height:5px;border-radius:3px;background:var(--col-bg);overflow:hidden}
.tprog .bar i{display:block;height:100%;background:var(--d-done);border-radius:3px}
/* inbox */
.inbox{display:flex;flex-direction:column;gap:11px;max-width:800px;margin:0 auto}
.ibx{background:var(--card);border:1px solid var(--border);border-left:3px solid var(--cc);border-radius:12px;
  padding:14px 16px;cursor:pointer;display:flex;gap:13px;align-items:center;transition:box-shadow .16s,transform .1s;box-shadow:var(--shadow-sm)}
.ibx:hover{box-shadow:var(--shadow);transform:translateY(-1px)}
.ibx .ic{font-size:20px;flex-shrink:0}
.ibx .mid{flex:1;min-width:0}
.ibx .ttl{font-size:14px;font-weight:650}
.ibx .reason{font-size:12px;color:var(--muted);margin-top:2px}
.ibx .acts{display:flex;gap:7px;flex-shrink:0}
.qbtn{border:1px solid var(--border);background:var(--card2);color:var(--text2);border-radius:8px;padding:5px 11px;
  font-size:12px;font-weight:600;cursor:pointer;transition:all .14s;white-space:nowrap}
.qbtn:hover{background:var(--hover);border-color:var(--border2)}
.qbtn.primary{background:var(--accent);border-color:var(--accent);color:#fff}
.qbtn.primary:hover{filter:brightness(1.08)}
.qbtn.warn{border-color:var(--block);color:var(--block)}
.qbtn.warn:hover{background:var(--block-bg)}
/* modal */
.overlay{position:fixed;inset:0;background:rgba(10,13,20,.5);backdrop-filter:blur(4px);
  display:flex;align-items:center;justify-content:center;z-index:100;padding:22px;opacity:0;pointer-events:none;transition:opacity .2s}
.overlay.show{opacity:1;pointer-events:auto}
.modal{background:var(--card);border:1px solid var(--border);border-radius:18px;width:min(780px,96vw);
  max-height:92vh;overflow:auto;box-shadow:var(--shadow-lg);transform:scale(.97);transition:transform .2s}
.overlay.show .modal{transform:scale(1)}
.mh{padding:18px 20px;border-bottom:1px solid var(--border);display:flex;align-items:flex-start;gap:13px;
  position:sticky;top:0;background:var(--card);z-index:2}
.mh .av{width:42px;height:42px;border-radius:11px;background:var(--accent-soft);display:grid;place-items:center;font-size:22px;flex-shrink:0}
.mh .ttl{font-size:17px;font-weight:750;letter-spacing:-.01em}
.mh .sub{font-size:12px;color:var(--muted);margin-top:6px;display:flex;gap:9px;flex-wrap:wrap;align-items:center}
.mh .x{margin-left:auto;cursor:pointer;color:var(--muted);font-size:18px;width:30px;height:30px;border-radius:8px;
  display:grid;place-items:center;transition:all .14s;flex-shrink:0}
.mh .x:hover{background:var(--hover);color:var(--block)}
.sec{padding:16px 20px;border-bottom:1px solid var(--border)}
.sec:last-child{border-bottom:none}
.sec h5{font-size:11px;text-transform:uppercase;letter-spacing:.05em;color:var(--muted);margin-bottom:11px;font-weight:700}
.attn-card{border-radius:12px;padding:13px 15px;display:flex;align-items:center;gap:12px;background:var(--block-bg)}
.attn-card.review{background:var(--review-bg)}
.attn-card .em{font-size:20px}
.attn-card .msg{flex:1;font-size:13px;font-weight:600;color:var(--block)}
.attn-card.review .msg{color:var(--review)}
.attn-card .acts{display:flex;gap:8px}
.graph-wrap{background:radial-gradient(ellipse at 50% 40%,var(--card2),var(--bg));border:1px solid var(--border);
  border-radius:12px;padding:6px}
.graph-wrap svg{width:100%;height:auto;display:block}
.cur{font-size:12px;color:var(--muted);margin-top:10px;display:flex;gap:7px;align-items:center}
.cur b{color:var(--text2)}
.mlearn{font-size:13px;padding:7px 0;border-bottom:1px dashed var(--border);color:var(--text2);display:flex;gap:9px}
.mlearn:last-child{border-bottom:none}
.tl{display:flex;gap:10px;padding:5px 0;font-size:12px;color:var(--muted)}
.tl .dot{width:7px;height:7px;border-radius:50%;background:var(--accent);margin-top:5px;flex-shrink:0}
@media (max-width:920px){
  .tiles{grid-template-columns:repeat(2,1fr)}
  .dash-grid{grid-template-columns:1fr}
  .board{grid-auto-flow:column;grid-template-columns:none;grid-auto-columns:80vw;overflow-x:auto;padding-bottom:8px}
  .project,.search{display:none}
}
</style>
</head>
<body>
<header>
  <span class="brand"><span class="logo">🗂</span><span class="nm">Agent<b>Manager</b></span></span>
  <span class="project" id="project"></span>
  <span class="spacer"></span>
  <label class="search"><span>🔎</span><input id="search" placeholder="Filter tasks…" autocomplete="off"><span class="k">/</span></label>
  <div class="stats mono">
    <span>🗂 <span class="v" id="s-tasks">0</span></span>
    <span>🔥 <span class="v" id="s-tok">0</span></span>
    <span>💰 <span class="v acc" id="s-cost">$0</span></span>
    <span id="clock">--:--:--</span>
  </div>
  <div class="ctrl">
    <button class="iconbtn bell" id="bell" onclick="view='inbox';render(lastState)" title="Needs your attention">🔔<span class="dot" id="bell-dot" style="display:none">0</span></button>
    <button class="iconbtn" id="btn-play" onclick="togglePlay()" title="Play / pause demo">⏸</button>
    <button class="iconbtn" onclick="doRestart()" title="Restart demo">⟳</button>
    <button class="iconbtn" id="theme" onclick="toggleTheme()" title="Toggle theme">🌙</button>
  </div>
</header>
<nav id="tabs"></nav>
<main><div class="wrap" id="view"></div></main>

<div class="overlay" id="overlay" onclick="if(event.target===this)closeModal()">
  <div class="modal" id="modal"></div>
</div>

<script>
const BASE='__PATH_PREFIX__';
const NODE_W=168,NODE_H=58;
const STATUS=[
  {k:'todo',label:'Todo',emoji:'○'},
  {k:'running',label:'In Progress',emoji:'◐'},
  {k:'review',label:'Review',emoji:'◔'},
  {k:'blocked',label:'Blocked',emoji:'⊘'},
  {k:'done',label:'Done',emoji:'●'},
];
const SMETA=Object.fromEntries(STATUS.map(s=>[s.k,s]));
const CVAR={todo:'todo',running:'run',review:'review',blocked:'block',done:'done'};
let lastState=null,view='dashboard',openTask=null,paused=false,filter='',dragging=false;
let sortKey='status',sortDir=1;

/* theme */
function applyTheme(t,persist=true){
  const sel=t==='dark'?'dark':'light';
  document.documentElement.dataset.theme=sel;
  document.getElementById('theme').textContent=sel==='dark'?'☀️':'🌙';
  if(persist){try{localStorage.setItem('agent-mgr-theme',sel);}catch(e){}}
  if(lastState)render(lastState);
}
function toggleTheme(){applyTheme(document.documentElement.dataset.theme==='dark'?'light':'dark');}
let it='light';try{it=localStorage.getItem('agent-mgr-theme')||'light';}catch(e){}
applyTheme(it,false);

/* helpers */
function fmtNum(n){return n>=1000?(n/1000).toFixed(n>=10000?0:1)+'K':String(n);}
function css(v){return getComputedStyle(document.documentElement).getPropertyValue(v).trim();}
function el(tag,cls,txt){const e=document.createElement(tag);if(cls)e.className=cls;if(txt!=null)e.textContent=txt;return e;}
function ago(ts){const s=Math.max(0,Math.floor(Date.now()/1000-ts));if(s<5)return 'just now';
  if(s<60)return s+'s ago';const m=Math.floor(s/60);if(m<60)return m+'m ago';const h=Math.floor(m/60);
  if(h<24)return h+'h ago';return Math.floor(h/24)+'d ago';}
function loopDone(t){return t.graph.nodes.filter(n=>n.status==='done').length;}
function progressStr(t){return loopDone(t)+'/'+t.graph.nodes.length;}
function needsAttn(t){return t.status==='blocked'||t.status==='review'||t.needs_attention;}
function inbox(state){return state.tasks.filter(needsAttn);}
function matchFilter(t){if(!filter)return true;const q=filter.toLowerCase();
  return (t.title+' '+t.agent+' '+t.todo+' '+(t.tags||[]).join(' ')).toLowerCase().includes(q);}

/* SSE */
const evt=new EventSource(BASE+'/stream');
evt.onmessage=e=>render(JSON.parse(e.data));
function updateClock(){document.getElementById('clock').textContent=new Date().toTimeString().split(' ')[0];}
updateClock();setInterval(updateClock,500);
/* keep "x ago" fresh + refresh relative times when idle */
setInterval(()=>{if(lastState&&!dragging&&!openTask&&(view==='kanban'||view==='table'))render(lastState);},20000);

/* pushes */
function pushTask(id,body){return fetch(BASE+'/api/task/'+id,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});}
function pushNode(id,nid,body){return fetch(BASE+'/api/task/'+id+'/node/'+nid,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});}
async function moveTask(id,status){await pushTask(id,{status});}
async function unblock(id){const t=lastState.tasks.find(x=>x.id===id);if(t){const n=t.graph.nodes.find(x=>x.status==='blocked');if(n)await pushNode(id,n.id,{status:'running'});}await pushTask(id,{status:'running',needs_attention:false});}
async function approve(id){await pushTask(id,{status:'done',needs_attention:false});}
async function sendBack(id){await pushTask(id,{status:'running',needs_attention:false});}

/* top-level render */
function render(state){
  lastState=state;
  if(dragging)return;
  document.getElementById('project').textContent=state.project;
  document.getElementById('s-tasks').textContent=state.tasks.length;
  document.getElementById('s-tok').textContent=fmtNum(state.total_tokens);
  document.getElementById('s-cost').textContent='$'+state.total_cost.toFixed(3);
  const alerts=inbox(state).length;
  const bd=document.getElementById('bell-dot');
  bd.style.display=alerts?'grid':'none';bd.textContent=alerts;
  renderTabs(state);
  const v=document.getElementById('view');v.replaceChildren();
  if(view==='dashboard')v.appendChild(viewDashboard(state));
  else if(view==='kanban')v.appendChild(viewKanban(state));
  else if(view==='table')v.appendChild(viewTable(state));
  else if(view==='inbox')v.appendChild(viewInbox(state));
  if(openTask){const t=state.tasks.find(x=>x.id===openTask);if(t)fillModal(t);else closeModal();}
}

function renderTabs(state){
  const nav=document.getElementById('tabs');nav.replaceChildren();
  const alerts=inbox(state).length;
  [{k:'dashboard',label:'Dashboard',icon:'📊'},
   {k:'kanban',label:'Kanban',icon:'🗂',count:state.tasks.length},
   {k:'table',label:'Table',icon:'▦',count:state.tasks.length},
   {k:'inbox',label:'Inbox',icon:'📥',count:alerts,alert:alerts>0}
  ].forEach(t=>{
    const b=el('button',view===t.k?'active':'');
    b.append(el('span',null,t.icon),el('span',null,t.label));
    if(t.count!=null)b.appendChild(el('span','cnt'+(t.alert?' alert':''),String(t.count)));
    b.onclick=()=>{view=t.k;render(lastState);};
    nav.appendChild(b);
  });
}

function statusPill(k){
  const s=SMETA[k];const p=el('span','st');
  p.append(el('span','d'),el('span',null,s.label));
  p.style.color=css('--'+CVAR[k]);p.style.background=css('--'+CVAR[k]+'-bg');
  return p;
}

/* ── Dashboard ── */
function viewDashboard(state){
  const wrap=el('div');
  const counts={};STATUS.forEach(s=>counts[s.k]=0);
  state.tasks.forEach(t=>counts[t.status]=(counts[t.status]||0)+1);
  const tiles=el('div','tiles');
  STATUS.forEach(s=>{
    const tile=el('div','tile');tile.style.setProperty('--tc',css('--'+CVAR[s.k]));
    tile.appendChild(el('div','n',String(counts[s.k]||0)));
    const l=el('div','l');l.append(el('span','sw'),el('span',null,s.label));
    tile.appendChild(l);
    tile.onclick=()=>{view='kanban';render(lastState);};
    tiles.appendChild(tile);
  });
  wrap.appendChild(tiles);

  const grid=el('div','dash-grid');
  // learnings
  const lp=el('div','panel');lp.appendChild(el('h3','','🧠 Shared memory · learnings'));
  const lb=el('div','body');
  if(!state.learnings.length){const e=el('div','empty');e.append(el('span','big','🧠'),document.createTextNode('No learnings yet — agents leave notes here for each other.'));lb.appendChild(e);}
  state.learnings.forEach(x=>{
    const d=el('div','learn');d.appendChild(el('span','bulb','💡'));
    const mid=el('div');
    mid.appendChild(el('div','meta',`${x.time} · ${x.agent||'agent'}${x.task?' · '+x.task:''}`));
    mid.appendChild(el('div','txt',x.text));
    d.appendChild(mid);lb.appendChild(d);
  });
  lp.appendChild(lb);
  // activity
  const ap=el('div','panel');ap.appendChild(el('h3','','⚡ Recent activity'));
  const ab=el('div','body');
  if(!state.activity.length){const e=el('div','empty');e.append(el('span','big','⚡'),document.createTextNode('Quiet. Press ▶ / ⟳ to run the demo.'));ab.appendChild(e);}
  [...state.activity].reverse().forEach(line=>{
    const m=line.match(/^\[(.*?)\]\s*(.*)$/);
    const d=el('div','act');
    if(line.includes('✅'))d.classList.add('ok');
    if(line.includes('⛔')||line.includes('BLOCKED'))d.classList.add('bl');
    if(line.includes('🔍'))d.classList.add('rv');
    d.append(el('span','t mono',m?m[1]:''),el('span','m',m?m[2]:line));
    ab.appendChild(d);
  });
  ap.appendChild(ab);
  grid.append(lp,ap);wrap.appendChild(grid);
  return wrap;
}

/* ── Kanban (drag & drop) ── */
function viewKanban(state){
  const board=el('div','board');
  STATUS.forEach(s=>{
    const col=el('div','col');col.dataset.status=s.k;
    const items=state.tasks.filter(t=>t.status===s.k&&matchFilter(t));
    const h=el('h4');const sw=el('span','sw');
    sw.style.background=css('--'+CVAR[s.k]);sw.style.setProperty('--swg',css('--'+CVAR[s.k]+'-bg'));
    h.append(sw,el('span',null,s.label),el('span','cnt',String(items.length)));
    col.appendChild(h);
    const cards=el('div','cards');
    items.forEach(t=>cards.appendChild(taskCard(t)));
    col.appendChild(cards);
    // drop handling
    col.addEventListener('dragover',e=>{e.preventDefault();col.classList.add('drop');});
    col.addEventListener('dragleave',e=>{if(!col.contains(e.relatedTarget))col.classList.remove('drop');});
    col.addEventListener('drop',e=>{
      e.preventDefault();col.classList.remove('drop');
      const id=e.dataTransfer.getData('text/plain');
      const task=lastState.tasks.find(x=>x.id===id);
      if(task&&task.status!==s.k)moveTask(id,s.k);
    });
    board.appendChild(col);
  });
  return board;
}
function taskCard(t){
  const c=el('div','card');c.style.setProperty('--cc',css('--'+CVAR[t.status]));
  c.draggable=true;
  c.addEventListener('dragstart',e=>{dragging=true;e.dataTransfer.setData('text/plain',t.id);e.dataTransfer.effectAllowed='move';c.classList.add('dragging');});
  c.addEventListener('dragend',()=>{dragging=false;c.classList.remove('dragging');render(lastState);});
  if(needsAttn(t))c.appendChild(el('div','attn-flag',t.status==='blocked'?'⛔':'🔍'));
  c.appendChild(el('div','ttl',t.title));
  c.appendChild(el('div','todo',t.todo));
  const bar=el('div','loopbar');
  t.graph.nodes.forEach(n=>{const i=el('i');if(n.status!=='pending')i.classList.add(n.status);bar.appendChild(i);});
  c.appendChild(bar);
  const foot=el('div','foot');
  const ag=el('div','agent');
  const dot=el('span','sdot');dot.style.background=css('--'+CVAR[t.status]);
  ag.append(dot,el('span','em',t.avatar),el('span','nm',t.agent));
  foot.appendChild(ag);
  const r=el('div','meta-r mono');
  if(t.tokens)r.appendChild(el('span','tok','🔥'+fmtNum(t.tokens)));
  r.appendChild(el('span','ago',ago(t.updated_at)));
  foot.appendChild(r);
  c.appendChild(foot);
  c.onclick=e=>{if(!dragging)showModal(t.id);};
  return c;
}

/* ── Table (sortable) ── */
function viewTable(state){
  const cols=[{k:'attn',l:''},{k:'title',l:'Task'},{k:'agent',l:'Agent'},{k:'status',l:'Status'},
    {k:'todo',l:'Current step'},{k:'loop',l:'Loop'},{k:'tokens',l:'Tokens'},{k:'updated_at',l:'Updated'}];
  const order={todo:0,running:1,review:2,blocked:3,done:4};
  let rows=state.tasks.filter(matchFilter).slice();
  rows.sort((a,b)=>{
    let x,y;
    if(sortKey==='status'){x=order[a.status];y=order[b.status];}
    else if(sortKey==='loop'){x=loopDone(a)/a.graph.nodes.length;y=loopDone(b)/b.graph.nodes.length;}
    else if(sortKey==='tokens'||sortKey==='updated_at'){x=a[sortKey];y=b[sortKey];}
    else{x=(a[sortKey]||'').toString().toLowerCase();y=(b[sortKey]||'').toString().toLowerCase();}
    return (x<y?-1:x>y?1:0)*sortDir;
  });
  const wrapEl=el('div','tbl-wrap');
  const tbl=el('table');const thead=el('thead');const tr=el('tr');
  cols.forEach(c=>{
    const th=el('th');th.append(document.createTextNode(c.l+' '));
    if(c.k===sortKey)th.appendChild(el('span','ar',sortDir>0?'▲':'▼'));
    if(c.k!=='attn')th.onclick=()=>{if(sortKey===c.k)sortDir*=-1;else{sortKey=c.k;sortDir=1;}render(lastState);};
    tr.appendChild(th);
  });
  thead.appendChild(tr);tbl.appendChild(thead);
  const tb=el('tbody');
  rows.forEach(t=>{
    const row=el('tr');
    row.appendChild(el('td',null,needsAttn(t)?(t.status==='blocked'?'⛔':'🔍'):''));
    const td1=el('td');td1.appendChild(el('b',null,t.title));row.appendChild(td1);
    const td2=el('td');td2.append(el('span',null,t.avatar+' '),el('span',null,t.agent));row.appendChild(td2);
    const td3=el('td');td3.appendChild(statusPill(t.status));row.appendChild(td3);
    const td4=el('td',null,t.todo);td4.style.color=css('--muted');row.appendChild(td4);
    const td5=el('td');const pr=el('div','tprog');const bar=el('div','bar');const fill=el('i');
    fill.style.width=(loopDone(t)/t.graph.nodes.length*100)+'%';bar.appendChild(fill);
    pr.append(bar,el('span','mono',progressStr(t)));td5.appendChild(pr);row.appendChild(td5);
    const tk=el('td','mono',fmtNum(t.tokens));row.appendChild(tk);
    const tu=el('td','mono',ago(t.updated_at));tu.style.color=css('--muted');row.appendChild(tu);
    row.onclick=()=>showModal(t.id);
    tb.appendChild(row);
  });
  tbl.appendChild(tb);wrapEl.appendChild(tbl);
  return wrapEl;
}

/* ── Inbox (with quick actions) ── */
function viewInbox(state){
  const wrap=el('div','inbox');
  const items=inbox(state).filter(matchFilter);
  if(!items.length){const e=el('div','empty');e.append(el('span','big','📭'),document.createTextNode('Inbox zero — nothing needs you right now.'));wrap.appendChild(e);return wrap;}
  items.forEach(t=>{
    const row=el('div','ibx');row.style.setProperty('--cc',css('--'+CVAR[t.status]));
    row.appendChild(el('div','ic',t.status==='blocked'?'⛔':'🔍'));
    const mid=el('div','mid');
    mid.appendChild(el('div','ttl',t.title));
    mid.appendChild(el('div','reason',t.attention_reason||t.todo));
    row.appendChild(mid);
    const acts=el('div','acts');
    if(t.status==='blocked'){
      const b=el('button','qbtn primary','Unblock');b.onclick=e=>{e.stopPropagation();unblock(t.id);};acts.appendChild(b);
    }else if(t.status==='review'){
      const a=el('button','qbtn primary','Approve');a.onclick=e=>{e.stopPropagation();approve(t.id);};
      const s=el('button','qbtn','Send back');s.onclick=e=>{e.stopPropagation();sendBack(t.id);};
      acts.append(a,s);
    }
    row.appendChild(acts);
    row.onclick=()=>showModal(t.id);
    wrap.appendChild(row);
  });
  return wrap;
}

/* ── Modal / drill-down ── */
function showModal(id){openTask=id;document.getElementById('overlay').classList.add('show');render(lastState);}
function closeModal(){openTask=null;document.getElementById('overlay').classList.remove('show');}
document.addEventListener('keydown',e=>{
  if(e.key==='Escape'){if(openTask)closeModal();return;}
  if(e.target.tagName==='INPUT')return;
  if(e.key==='/'){e.preventDefault();document.getElementById('search').focus();}
  const map={'1':'dashboard','2':'kanban','3':'table','4':'inbox'};
  if(map[e.key]){view=map[e.key];render(lastState);}
});

function fillModal(t){
  const m=document.getElementById('modal');m.replaceChildren();
  const mh=el('div','mh');
  mh.appendChild(el('div','av',t.avatar));
  const head=el('div');head.style.flex='1';
  head.appendChild(el('div','ttl',t.title));
  const sub=el('div','sub');
  sub.appendChild(statusPill(t.status));
  sub.appendChild(el('span',null,t.agent));
  sub.appendChild(el('span','mono','🔥 '+fmtNum(t.tokens)+' tokens'));
  sub.appendChild(el('span','mono','loop '+progressStr(t)));
  sub.appendChild(el('span',null,'updated '+ago(t.updated_at)));
  (t.tags||[]).forEach(tag=>sub.appendChild(el('span','tag',tag)));
  head.appendChild(sub);mh.appendChild(head);
  const x=el('div','x','✕');x.onclick=closeModal;mh.appendChild(x);
  m.appendChild(mh);

  if(needsAttn(t)){
    const s=el('div','sec');
    const card=el('div','attn-card'+(t.status==='review'?' review':''));
    card.appendChild(el('span','em',t.status==='blocked'?'⛔':'🔍'));
    card.appendChild(el('span','msg',t.attention_reason||t.todo));
    const acts=el('div','acts');
    if(t.status==='blocked'){
      const b=el('button','qbtn primary','Mark unblocked');b.onclick=()=>unblock(t.id);acts.appendChild(b);
    }else if(t.status==='review'){
      const a=el('button','qbtn primary','Approve → Done');a.onclick=()=>approve(t.id);
      const sb=el('button','qbtn warn','Send back');sb.onclick=()=>sendBack(t.id);
      acts.append(a,sb);
    }
    card.appendChild(acts);s.appendChild(card);m.appendChild(s);
  }

  const gs=el('div','sec');
  gs.appendChild(el('h5','','Loop · where it is right now'));
  const gw=el('div','graph-wrap');gw.appendChild(renderGraph(t.graph));gs.appendChild(gw);
  const cur=el('div','cur');cur.append(el('b',null,'Current:'),document.createTextNode(' '+t.todo));
  gs.appendChild(cur);m.appendChild(gs);

  const tl=lastState.learnings.filter(l=>l.task===t.title);
  if(tl.length){
    const ls=el('div','sec');ls.appendChild(el('h5','','Learnings from this task'));
    tl.forEach(l=>{const d=el('div','mlearn');d.append(el('span',null,'💡'),el('span',null,l.text));ls.appendChild(d);});
    m.appendChild(ls);
  }
}

/* reuse of v2's SVG flowchart, scoped to one task */
function renderGraph(graph){
  const ns='http://www.w3.org/2000/svg';
  const svg=document.createElementNS(ns,'svg');
  svg.setAttribute('viewBox',graph.viewBox||'0 0 640 380');
  svg.setAttribute('preserveAspectRatio','xMidYMid meet');
  const nodeC={pending:css('--n-pending'),queued:css('--n-queued'),running:css('--n-running'),done:css('--n-done'),blocked:css('--n-blocked')};
  const dotC={pending:css('--d-pending'),queued:css('--d-queued'),running:css('--d-running'),done:css('--d-done'),blocked:css('--d-blocked')};
  const edgeC=css('--edge'),edgeRun=css('--edge-run'),edgeDone=css('--edge-done');
  const textC=css('--text'),mutedC=css('--muted'),accentC=css('--accent'),blockC=css('--block');
  const byId=id=>graph.nodes.find(n=>n.id===id);
  graph.edges.forEach(e=>{
    const f=byId(e.from),t=byId(e.to);if(!f||!t)return;
    const line=document.createElementNS(ns,'line');
    line.setAttribute('x1',f.x);line.setAttribute('y1',f.y+NODE_H/2);
    line.setAttribute('x2',t.x);line.setAttribute('y2',t.y-NODE_H/2);
    line.setAttribute('stroke-width','1.5');
    if(f.status==='running'){line.setAttribute('stroke',edgeRun);line.style.animation='dash 1s linear infinite';line.setAttribute('stroke-dasharray','5 4');}
    else if(f.status==='done'&&t.status!=='pending')line.setAttribute('stroke',edgeDone);
    else line.setAttribute('stroke',edgeC);
    svg.appendChild(line);
  });
  graph.nodes.forEach(n=>{
    const g=document.createElementNS(ns,'g');
    g.setAttribute('transform',`translate(${n.x-NODE_W/2},${n.y-NODE_H/2})`);
    if(n.status==='running'||n.status==='blocked')g.style.animation='pulse 1.5s ease-in-out infinite';
    const rect=document.createElementNS(ns,'rect');
    rect.setAttribute('width',NODE_W);rect.setAttribute('height',NODE_H);rect.setAttribute('rx','10');
    rect.setAttribute('fill',nodeC[n.status]||nodeC.pending);
    rect.setAttribute('stroke',n.status==='blocked'?blockC:(n.status==='running'?css('--run'):edgeC));
    rect.setAttribute('stroke-width',n.status==='running'||n.status==='blocked'?'2':'1.25');
    g.appendChild(rect);
    const dot=document.createElementNS(ns,'circle');
    dot.setAttribute('cx',14);dot.setAttribute('cy',15);dot.setAttribute('r',5);
    dot.setAttribute('fill',dotC[n.status]||dotC.pending);g.appendChild(dot);
    const label=document.createElementNS(ns,'text');
    label.setAttribute('x',27);label.setAttribute('y',19);label.setAttribute('fill',textC);
    label.setAttribute('font-size','12.5');label.setAttribute('font-weight','700');label.textContent=n.label;
    g.appendChild(label);
    const task=document.createElementNS(ns,'text');
    task.setAttribute('x',13);task.setAttribute('y',37);task.setAttribute('fill',mutedC);task.setAttribute('font-size','10.5');
    task.textContent=n.task.length>29?n.task.slice(0,28)+'…':n.task;g.appendChild(task);
    const tok=document.createElementNS(ns,'text');
    tok.setAttribute('x',13);tok.setAttribute('y',51);tok.setAttribute('fill',accentC);tok.setAttribute('font-size','10');
    tok.setAttribute('font-family','SF Mono,monospace');tok.textContent=n.tokens?('🔥 '+fmtNum(n.tokens)):'';g.appendChild(tok);
    if(n.status==='blocked'){
      const bt=document.createElementNS(ns,'text');
      bt.setAttribute('x',NODE_W-13);bt.setAttribute('y',19);bt.setAttribute('text-anchor','end');
      bt.setAttribute('fill',blockC);bt.setAttribute('font-size','8.5');bt.setAttribute('font-weight','800');
      bt.setAttribute('letter-spacing','.05em');bt.textContent='BLOCKED';g.appendChild(bt);
    }
    svg.appendChild(g);
  });
  return svg;
}

/* controls + search */
function postControl(action,extra={}){fetch(BASE+'/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({action,...extra})});}
function togglePlay(){paused=!paused;postControl(paused?'pause':'resume');document.getElementById('btn-play').textContent=paused?'▶':'⏸';}
function doRestart(){postControl('restart');paused=false;document.getElementById('btn-play').textContent='⏸';}
document.getElementById('search').addEventListener('input',e=>{filter=e.target.value;if(lastState)render(lastState);});

const style=document.createElement('style');
style.textContent='@keyframes pulse{0%,100%{opacity:1}50%{opacity:.5}}@keyframes dash{to{stroke-dashoffset:-9}}';
document.head.appendChild(style);
</script>
</body>
</html>"""


class RequestError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status = status
        self.message = message


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        self.connection.settimeout(REQUEST_TIMEOUT)

    def handle(self):
        try:
            super().handle()
        except (BrokenPipeError, ConnectionResetError, socket.timeout, TimeoutError):
            pass

    def _send_json(self, status, payload):
        data = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        if status >= 400:
            self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()

    def _send_html(self, html):
        data = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)
        self.wfile.flush()

    @staticmethod
    def _html_for_base(base):
        return HTML.replace(
            "const BASE='__PATH_PREFIX__';",
            f"const BASE={json.dumps(base)};",
        )

    def _read_json_body(self):
        raw_length = self.headers.get("Content-Length")
        if raw_length is None:
            raise RequestError(400, "Content-Length is required")
        try:
            length = int(raw_length)
        except (TypeError, ValueError):
            raise RequestError(400, "invalid Content-Length")
        if length < 0:
            raise RequestError(400, "invalid Content-Length")
        if length > MAX_CONTENT_LENGTH:
            raise RequestError(413, "request body is too large")
        try:
            raw_body = self.rfile.read(length)
        except socket.timeout:
            raise RequestError(408, "request timed out")
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise RequestError(400, "invalid JSON")
        if not isinstance(body, dict):
            raise RequestError(400, "JSON body must be an object")
        return body

    def _last_event_id(self):
        value = self.headers.get("Last-Event-ID")
        if value is None:
            return None
        try:
            event_id = int(value)
        except (TypeError, ValueError):
            return None
        return event_id if 0 <= event_id <= 2**63 - 1 else None

    @staticmethod
    def _local_path(path):
        if PATH_PREFIX and path.startswith(PATH_PREFIX + "/"):
            return path[len(PATH_PREFIX):]
        return path

    def do_GET(self):
        path = urlparse(self.path).path
        local_path = self._local_path(path)
        if local_path == "/api/state":
            self._send_json(200, state_snapshot())
        elif path == "/":
            self._send_html(self._html_for_base(""))
        elif PATH_PREFIX and path == PATH_PREFIX + "/":
            self._send_html(self._html_for_base(PATH_PREFIX))
        elif PATH_PREFIX and path == PATH_PREFIX:
            self.send_response(301)
            self.send_header("Location", PATH_PREFIX + "/")
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif path == "/stream" or (PATH_PREFIX and path == PATH_PREFIX + "/stream"):
            self._serve_sse()
        else:
            self._send_json(404, {"ok": False, "error": "not found"})

    def _serve_sse(self):
        client_queue = queue.Queue(maxsize=1)
        if not add_sse_client(client_queue):
            self._send_json(503, {"ok": False, "error": "SSE client limit reached"})
            return
        try:
            initial = current_sse_snapshot(self._last_event_id())
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Connection", "keep-alive")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.connection.settimeout(SSE_WRITE_TIMEOUT)
            self.wfile.write(initial.encode("utf-8"))
            self.wfile.flush()
            while True:
                try:
                    payload = client_queue.get(timeout=SSE_HEARTBEAT_INTERVAL)
                except queue.Empty:
                    self.wfile.write(b": heartbeat\n\n")
                    self.wfile.flush()
                    continue
                self.wfile.write(payload.encode("utf-8"))
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, socket.timeout, TimeoutError, OSError):
            pass
        finally:
            remove_sse_client(client_queue)

    def do_POST(self):
        path = urlparse(self.path).path
        local_path = self._local_path(path)
        if local_path.startswith("/api/"):
            self._handle_api_post(local_path)
            return
        if local_path != "/control":
            self._send_json(404, {"ok": False, "error": "not found"})
            return
        try:
            body = self._read_json_body()
        except RequestError as error:
            self._send_json(error.status, {"ok": False, "error": error.message})
            return
        action = body.get("action")
        allowed = {"pause", "resume", "speed", "restart"}
        if not isinstance(action, str) or action not in allowed:
            self._send_json(400, {"ok": False, "error": "unknown action"})
            return
        if action == "speed":
            speed = body.get("speed")
            try:
                value = float(speed) if not isinstance(speed, bool) and isinstance(speed, (int, float)) else None
            except (OverflowError, TypeError, ValueError):
                value = None
            if value is None or not math.isfinite(value) or value <= 0:
                self._send_json(400, {"ok": False, "error": "speed must be a finite number > 0"})
                return
            set_speed(value)
        elif action == "pause":
            set_paused(True)
        elif action == "resume":
            set_paused(False)
        elif action == "restart":
            restart_simulation()
        with STATE_LOCK:
            self._send_json(200, {
                "ok": True, "paused": CONTROL["paused"],
                "speed": CONTROL["speed"], "running": CONTROL["running"],
            })

    def _handle_api_post(self, path):
        if path == "/api/reset":
            stop_simulation()
            reset_state(_bypass_context=True)
            broadcast()
            self._send_json(200, {"ok": True, "state": state_snapshot()})
            return
        try:
            body = self._read_json_body()
        except RequestError as error:
            self._send_json(error.status, {"ok": False, "error": error.message})
            return

        # POST /api/task/{id}                  → task-level update
        # POST /api/task/{id}/node/{node_id}   → subagent-level update
        if path.startswith("/api/task/"):
            rest = path[len("/api/task/"):]
            if "/node/" in rest:
                tid_raw, nid_raw = rest.split("/node/", 1)
                tid, nid = unquote(tid_raw), unquote(nid_raw)
                if not tid or not nid or "/" in nid:
                    self._send_json(400, {"ok": False, "error": "invalid task or node id"})
                    return
                updates = {}
                if "status" in body:
                    status = body["status"]
                    if not isinstance(status, str) or status not in NODE_STATUSES:
                        self._send_json(400, {"ok": False, "error": f"node status must be one of {NODE_STATUSES}"})
                        return
                    updates["status"] = status
                if "tokens" in body:
                    tokens = body["tokens"]
                    if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
                        self._send_json(400, {"ok": False, "error": "tokens must be a non-negative integer"})
                        return
                    updates["tokens"] = tokens
                if "task" in body:
                    label_task = body["task"]
                    if not isinstance(label_task, str) or len(label_task) > 500:
                        self._send_json(400, {"ok": False, "error": "task must be a string <= 500 chars"})
                        return
                    updates["task"] = label_task
                if not updates:
                    self._send_json(400, {"ok": False, "error": "provide status, tokens, or task"})
                    return
                if not set_node(tid, nid, _bypass_context=True, **updates):
                    self._send_json(404, {"ok": False, "error": "task or node not found"})
                    return
                broadcast()
                self._send_json(200, {"ok": True, "task": tid, "node": nid, **updates})
                return

            tid = unquote(rest)
            if not tid or "/" in tid:
                self._send_json(400, {"ok": False, "error": "invalid task id"})
                return
            updates = {}
            if "status" in body:
                status = body["status"]
                if not isinstance(status, str) or status not in TASK_STATUSES:
                    self._send_json(400, {"ok": False, "error": f"status must be one of {TASK_STATUSES}"})
                    return
                updates["status"] = status
            for key in ("title", "todo", "agent", "avatar"):
                if key in body:
                    value = body[key]
                    if not isinstance(value, str) or len(value) > 500:
                        self._send_json(400, {"ok": False, "error": f"{key} must be a string <= 500 chars"})
                        return
                    updates[key] = value
            reason = None
            if "attention_reason" in body:
                reason = body["attention_reason"]
                if not isinstance(reason, str) or len(reason) > 500:
                    self._send_json(400, {"ok": False, "error": "attention_reason must be a string <= 500 chars"})
                    return
            needs_attention = None
            if "needs_attention" in body:
                needs_attention = body["needs_attention"]
                if not isinstance(needs_attention, bool):
                    self._send_json(400, {"ok": False, "error": "needs_attention must be a boolean"})
                    return
            if not updates and reason is None and needs_attention is None:
                self._send_json(400, {"ok": False, "error": "no valid fields to update"})
                return
            if not set_task(tid, _bypass_context=True, reason=reason,
                            needs_attention=needs_attention, **updates):
                self._send_json(404, {"ok": False, "error": "task not found"})
                return
            broadcast()
            self._send_json(200, {"ok": True, "id": tid})
            return

        if path == "/api/learning":
            text = body.get("text")
            if not isinstance(text, str) or not text or len(text) > 2000:
                self._send_json(400, {"ok": False, "error": "text must be a non-empty string <= 2000 chars"})
                return
            task_title = body.get("task", "")
            agent = body.get("agent", "")
            if not isinstance(task_title, str) or not isinstance(agent, str):
                self._send_json(400, {"ok": False, "error": "task and agent must be strings"})
                return
            add_learning(text, task_title=task_title[:200], agent=agent[:100], _bypass_context=True)
            broadcast()
            self._send_json(200, {"ok": True})
            return

        if path == "/api/activity":
            msg = body.get("msg")
            if not isinstance(msg, str) or not msg or len(msg) > 500:
                self._send_json(400, {"ok": False, "error": "msg must be a non-empty string <= 500 chars"})
                return
            add_activity(msg, _bypass_context=True)
            broadcast()
            self._send_json(200, {"ok": True})
            return

        self._send_json(404, {"ok": False, "error": "not found"})

    def log_message(self, *args):
        pass

    def log_error(self, *args):
        pass


if __name__ == "__main__":
    public_path = PATH_PREFIX or "/"
    fresh = not load_persisted_state()
    print("Starting fresh" if fresh else "Loaded persisted state")
    print(f"Agent Manager (v3) → http://127.0.0.1:{PORT}{public_path}")
    if fresh and AUTO_START_DEMO:
        restart_simulation()
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        stop_simulation()
        server.shutdown()
