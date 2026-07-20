#!/usr/bin/env python3
"""
Agent Loop Visualizer — Groundweave: Multi-Agent Architecture Research
Real timeline from 2026-07-17 18:03–18:55
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
    PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 8766
except (TypeError, ValueError):
    raise SystemExit("PORT must be an integer")
if not 1 <= PORT <= 65535:
    raise SystemExit("PORT must be between 1 and 65535")


def topology_path_from_args(args):
    topology_path = None
    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--topology":
            index += 1
            if index >= len(args) or args[index].startswith("--"):
                raise SystemExit("--topology requires a JSON file path")
            topology_path = args[index]
        elif arg.startswith("--topology="):
            topology_path = arg.split("=", 1)[1]
            if not topology_path:
                raise SystemExit("--topology requires a JSON file path")
        else:
            raise SystemExit(f"unknown argument: {arg}")
        index += 1
    return topology_path


TOPOLOGY_PATH = topology_path_from_args(sys.argv[2:]) or os.environ.get(
    "AGENT_VIZ_TOPOLOGY"
)
TOPOLOGY_SOURCE = "built-in GSB default"

MAX_CONTENT_LENGTH = 64 * 1024
MAX_SSE_CLIENTS = 32
REQUEST_TIMEOUT = 10
SSE_HEARTBEAT_INTERVAL = 15
SSE_WRITE_TIMEOUT = 10
STATE_FILE = os.path.join(os.getcwd(), "agent-viz-state.json")
STATE_MAX_AGE_SECONDS = 24 * 60 * 60


def normalize_path_prefix(value):
    value = (value or "").strip().strip("/")
    if not value:
        return ""
    return "/" + value


PATH_PREFIX = normalize_path_prefix(os.environ.get("AGENT_VIZ_PATH_PREFIX", ""))

# ── Multi-Agent Research: Actual Timeline ──────────────────────
# Timestamps: 18:03–18:55 HKT, 2026-07-17
# Budget: 4 passes max, 200 Brave queries, 2h wall-clock
STATE = {
    "goal": "Groundweave: Multi-Agent Architecture & Stack Research (7 dims)",
    "viewBox": "0 0 900 685",
    "elapsed": 0,
    "total_tokens": 0,
    "total_cost": 0.0,
    "loop_state": {
        "done": 0, "total": 12, "blocked": [], "summary": "",
        "passes": 0, "new_glossary": 0, "new_people": 0, "new_reading": 0,
    },
    "nodes": [
        # Row 1: Pre-Flight
        {"id": "orch", "label": "Pre-Flight", "x": 450, "y": 35,
         "status": "pending", "tokens": 0, "task": "Plan audit, scope, pre-flight checklist"},
        # Row 2: Launch 1 Source Farm (3 groups, processed serially)
        {"id": "f1", "label": "Farm: Frameworks", "x": 130, "y": 145,
         "status": "pending", "tokens": 0, "task": "LangGraph,CrewAI,AutoGen,OpenAI,Anthropic (5 subagents)"},
        {"id": "f2", "label": "Farm: Deep-Dives", "x": 450, "y": 145,
         "status": "pending", "tokens": 0, "task": "Orchestration,Protocols,Production,Commercial (4 subagents)"},
        {"id": "f3", "label": "Farm: Ecosystem", "x": 770, "y": 145,
         "status": "pending", "tokens": 0, "task": "China,Dify,Coze,KeyPeople (3 subagents)"},
        # Row 3: Integration
        {"id": "integ", "label": "Integrator", "x": 450, "y": 270,
         "status": "pending", "tokens": 0, "task": "Build Glossary/People/Reading v1 — 16T/10P/27R"},
        # Row 4: Reinforce P1 (3×3 = 9 subagents)
        {"id": "rA1", "label": "P1: Loop A", "x": 150, "y": 400,
         "status": "pending", "tokens": 0, "task": "Glossary → People (4 terms → origin/coiner)"},
        {"id": "rB1", "label": "P1: Loop B", "x": 450, "y": 400,
         "status": "pending", "tokens": 0, "task": "People → Reading+Terms (3 groups)"},
        {"id": "rC1", "label": "P1: Loop C", "x": 750, "y": 400,
         "status": "pending", "tokens": 0, "task": "Reading → Authors+Terms (2 groups)"},
        # Row 5: Reinforce P2 (1×3 = 3 subagents)
        {"id": "rA2", "label": "P2: Loop A", "x": 250, "y": 520,
         "status": "pending", "tokens": 0, "task": "NEW terms → origin (15 terms)"},
        {"id": "rB2", "label": "P2: Loop B", "x": 450, "y": 520,
         "status": "pending", "tokens": 0, "task": "NEW people → article+terms (15 people)"},
        {"id": "rC2", "label": "P2: Loop C", "x": 650, "y": 520,
         "status": "pending", "tokens": 0, "task": "NEW readings → bios (4 articles)"},
        # Row 6: Verifier
        {"id": "ver", "label": "Verifier", "x": 450, "y": 640,
         "status": "pending", "tokens": 0, "task": "Independent fact-check — 9-point checklist, 27 terms"},
    ],
    "edges": [
        {"from": "orch", "to": "f1", "label": "5 frameworks"},
        {"from": "orch", "to": "f2", "label": "patterns+prod+commercial"},
        {"from": "orch", "to": "f3", "label": "china+people"},
        {"from": "f1", "to": "integ", "label": "~120KB raw"},
        {"from": "f2", "to": "integ", "label": "~100KB raw"},
        {"from": "f3", "to": "integ", "label": "~80KB raw"},
        {"from": "integ", "to": "rA1", "label": "v1: 16T/10P/27R"},
        {"from": "integ", "to": "rB1", "label": "all entries"},
        {"from": "integ", "to": "rC1", "label": "all entries"},
        {"from": "rA1", "to": "rA2", "label": "+80T/+22P"},
        {"from": "rB1", "to": "rB2", "label": "+4R"},
        {"from": "rC1", "to": "rC2", "label": "detail-fill"},
        {"from": "rA2", "to": "ver", "label": "diminishing→STOP"},
        {"from": "rB2", "to": "ver", "label": "v2: 27T/18P/31R"},
        {"from": "rC2", "to": "ver", "label": ""},
    ],
    "logs": [],
}


def load_topology(path):
    try:
        with open(path, "r", encoding="utf-8") as topology_file:
            topology = json.load(topology_file)
    except FileNotFoundError:
        raise SystemExit(f"topology file not found: {path}")
    except OSError as error:
        raise SystemExit(f"could not read topology file {path}: {error}")
    except json.JSONDecodeError as error:
        raise SystemExit(
            f"invalid topology JSON in {path} at line {error.lineno}, "
            f"column {error.colno}: {error.msg}"
        )

    if not isinstance(topology, dict):
        raise SystemExit(f"invalid topology in {path}: root must be a JSON object")
    required = {"nodes": list, "edges": list, "goal": str}
    for key, expected_type in required.items():
        if key not in topology:
            raise SystemExit(f"invalid topology in {path}: missing '{key}'")
        if not isinstance(topology[key], expected_type):
            raise SystemExit(
                f"invalid topology in {path}: '{key}' must be "
                f"{expected_type.__name__}"
            )
    if "viewBox" in topology and not isinstance(topology["viewBox"], (str, list)):
        raise SystemExit(
            f"invalid topology in {path}: 'viewBox' must be a string or list"
        )

    STATE.update(
        {key: copy.deepcopy(topology[key]) for key in ("nodes", "edges", "goal", "viewBox")
         if key in topology}
    )


if TOPOLOGY_PATH:
    load_topology(TOPOLOGY_PATH)
    TOPOLOGY_SOURCE = os.path.abspath(TOPOLOGY_PATH)

STATE_LOCK = threading.RLock()
PERSISTENCE_LOCK = threading.Lock()
SSE_CLIENTS = []
CONTROL = {"paused": False, "speed": 1.0, "running": False}


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
    """Copy state while locked; callers may serialize the copy without the lock."""
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
            # Keep only the newest full snapshot for slow clients.
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


def toggle_paused():
    now = time.monotonic()
    with STATE_LOCK:
        context = _active_context
        paused = not CONTROL["paused"]
        if paused:
            CONTROL["paused"] = True
            if context is not None and context.started_at and context.paused_since is None:
                context.paused_since = now
        else:
            CONTROL["paused"] = False
            if context is not None and context.paused_since is not None:
                context.paused_total += now - context.paused_since
                context.paused_since = None
    broadcast()


def set_speed(speed):
    with STATE_LOCK:
        CONTROL["speed"] = speed
    broadcast()


def wait_tick(s, context=None):
    context = current_context() if context is None else context
    remaining = float(s)
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


def update_node(nid, _bypass_context=False, **kwargs):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        for n in STATE["nodes"]:
            if n["id"] == nid:
                n.update(kwargs)
                return True
    return False


def complete_node(nid, _bypass_context=False):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        for n in STATE["nodes"]:
            if n["id"] == nid:
                if n["status"] != "done":
                    n["status"] = "done"
                    STATE["loop_state"]["done"] += 1
                return True
    return False


def add_log(msg: str, _bypass_context=False):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    t = time.strftime("%H:%M:%S")
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        STATE["logs"].append(f"[{t}] {msg}")
        if len(STATE["logs"]) > 14:
            STATE["logs"] = STATE["logs"][-14:]


def update_loop_state(_bypass_context=False, **kwargs):
    context = current_context()
    if not _bypass_context:
        ensure_current(context)
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        STATE["loop_state"].update(kwargs)


def increment_loop_state(field, amount):
    context = current_context()
    ensure_current(context)
    with STATE_LOCK:
        if not _is_current_locked(context):
            raise RunCancelled
        STATE["loop_state"][field] += amount


def burn_tokens(nid, tick_count, tok_range, delay=0.6):
    context = current_context()
    lo, hi = tok_range
    for _ in range(tick_count):
        ensure_current(context)
        tok = random.randint(lo, hi)
        with STATE_LOCK:
            if not _is_current_locked(context):
                raise RunCancelled
            STATE["total_tokens"] += tok
            STATE["total_cost"] = round(STATE["total_tokens"] / 1000000 * 0.435, 4)
            for n in STATE["nodes"]:
                if n["id"] == nid:
                    n["tokens"] += tok
                    break
        wait_tick(delay, context)


def reset_state(context=None, _bypass_context=False):
    if not _bypass_context:
        ensure_current(context)
    with STATE_LOCK:
        if not _bypass_context and not _is_current_locked(context):
            raise RunCancelled
        STATE["elapsed"] = 0
        STATE["total_tokens"] = 0
        STATE["total_cost"] = 0.0
        STATE["loop_state"] = {
            "done": 0, "total": len(STATE["nodes"]), "blocked": [], "summary": "",
            "passes": 0, "new_glossary": 0, "new_people": 0, "new_reading": 0,
        }
        for n in STATE["nodes"]:
            n["status"] = "pending"
            n["tokens"] = 0
        STATE["logs"] = []


def metrics_snapshot():
    with STATE_LOCK:
        return STATE["total_tokens"], STATE["total_cost"]


def simulate_loop(context):
    with STATE_LOCK:
        if not _is_current_locked(context):
            raise RunCancelled
        context.started_at = time.monotonic()
        if CONTROL["paused"]:
            context.paused_since = context.started_at
    _thread_context.run = context

    # Reset
    reset_state(context)
    broadcast()

    # ══════════════ PHASE 0: PRE-FLIGHT (18:03–18:09) ══════════════
    update_node("orch", status="running")
    add_log("18:03 Pre-Flight: audit plan, define scope (7 research dims)")
    wait_tick(0.8)
    burn_tokens("orch", 2, (2000, 4000))
    add_log("Scope: frameworks, orchestration, protocols, production, china, commercial, people")
    wait_tick(0.6)
    burn_tokens("orch", 2, (1500, 3000))
    add_log("Domain probe: 9/9 HTTP 200 ✅ · Budget: 4 passes, 200 Brave, 2h")
    wait_tick(0.5)
    add_log("18:09 Pre-flight complete. /background Launch 1 → 12 subagents")
    complete_node("orch")
    wait_tick(0.4)

    # ══════════════ PHASE 1: LAUNCH 1 FARM (18:09–18:18, 12 subagents) ══════════════
    for fid in ["f1", "f2", "f3"]:
        update_node(fid, status="queued")
    wait_tick(0.5)

    # Farm 1 — Frameworks (5 subagents: LangGraph, CrewAI, AutoGen, OpenAI, Anthropic)
    update_node("f1", status="running")
    add_log("18:09 Farm:Frameworks — processing 5 deep-dives serially...")
    wait_tick(0.8)
    burn_tokens("f1", 4, (5000, 9000), 1.2)
    add_log("18:15 3/5 done: autogen, crewai, langgraph ~85KB")
    wait_tick(0.6)
    burn_tokens("f1", 3, (4000, 7000), 1.0)
    add_log("18:16 5/5 done: +openai-agents, anthropic-patterns ~120KB total")
    complete_node("f1")
    add_log("Farm:Frameworks ✅ 5 subagents · ~120KB")
    wait_tick(0.3)

    # Farm 2 — Deep-Dives (4 subagents: orchestration, protocols, production, commercial)
    update_node("f2", status="running")
    add_log("18:15 Farm:DeepDives — orchestration patterns, protocols, production stack...")
    wait_tick(0.6)
    burn_tokens("f2", 4, (4000, 8000), 1.0)
    add_log("18:16 3/4: protocols, production-stack, orchestration-patterns ~100KB")
    wait_tick(0.5)
    burn_tokens("f2", 2, (3000, 5000), 0.8)
    add_log("18:17 +commercial-landscape, frameworks-comparison")
    complete_node("f2")
    add_log("Farm:DeepDives ✅ 4 subagents · ~100KB")
    wait_tick(0.3)

    # Farm 3 — Ecosystem (3 subagents: China, Key People, Bee/Meta)
    update_node("f3", status="running")
    add_log("18:16 Farm:Ecosystem — China (Dify,Coze,Baidu) + Key People + emerging...")
    wait_tick(0.6)
    burn_tokens("f3", 3, (3000, 6000), 1.0)
    add_log("18:17 3/3: china-ecosystem, key-people, bee-agent ~80KB")
    complete_node("f3")
    add_log("Farm:Ecosystem ✅ 3 subagents · ~80KB")
    add_log("18:18 🎯 LAUNCH 1: 12/12 subagents complete · ~300KB raw research")
    wait_tick(0.4)

    # ══════════════ PHASE 2: INTEGRATION 1 (18:18–18:29) ══════════════
    update_node("integ", status="queued")
    wait_tick(0.3)
    update_node("integ", status="running")
    add_log("18:18 Integrator: reading 12 subagent files → extracting terms, people, articles...")
    wait_tick(0.8)
    burn_tokens("integ", 3, (3000, 5000), 0.7)
    add_log("Glossary: 16 terms across 6 dimensions")
    wait_tick(0.5)
    burn_tokens("integ", 2, (2000, 4000), 0.6)
    add_log("Key People: 10 profiles · Reading List: 27 entries · URL verify 27/30 ✅")
    wait_tick(0.5)
    burn_tokens("integ", 2, (2000, 4000), 0.6)
    add_log("18:22 Gate 1: DECISIONS_NEEDED.md — 3 blockers, 3 assumptions")
    wait_tick(0.4)
    add_log("18:25 Gate 1 approved: dim-grouping keep, focus multi-agent collab, pull CN people")
    complete_node("integ")
    add_log("Integration ✅ v1: 16T/10P/27R")
    wait_tick(0.4)

    # ══════════════ PHASE 3: REINFORCE PASS 1 (18:29–18:33, 3×3=9 subagents) ══════════════
    update_loop_state(passes=1)
    for rid in ["rA1", "rB1", "rC1"]:
        update_node(rid, status="queued")
    wait_tick(0.4)

    # P1 Loop A: Glossary→People (4 category groups)
    update_node("rA1", status="running")
    add_log("18:29 P1: Loop A — Glossary→People: who coined each term? (4 groups)")
    wait_tick(0.8)
    burn_tokens("rA1", 3, (4000, 7000), 1.0)
    add_log("Loop A: found origin/coiners for all 16 terms → +22 new Key People")
    update_loop_state(new_people=22)
    complete_node("rA1")
    add_log("P1 Loop A ✅ +22 people")
    wait_tick(0.3)

    # P1 Loop B: People→Reading+Terms (3 groups)
    update_node("rB1", status="running")
    add_log("18:30 P1: Loop B — People→Terms: what did each person originate?")
    wait_tick(0.7)
    burn_tokens("rB1", 3, (3000, 6000), 1.0)
    add_log("Loop B: Framework Creators → +17 new terms (Context Engineering, Ambient Agents...))")
    increment_loop_state("new_glossary", 17)
    add_log("Big Tech + Academic → +10 more terms (SWE-bench, MAST, 扣子空间...))")
    increment_loop_state("new_glossary", 10)
    complete_node("rB1")
    add_log("P1 Loop B ✅ +27 terms, +4 readings")
    update_loop_state(new_reading=4)
    wait_tick(0.3)

    # P1 Loop C: Reading→Authors+Terms (2 groups)
    update_node("rC1", status="running")
    add_log("18:31 P1: Loop C — Reading→Authors: who wrote these 27 articles?")
    wait_tick(0.7)
    burn_tokens("rC1", 3, (4000, 8000), 1.0)
    add_log("Loop C: +68 specialized terms from article content")
    increment_loop_state("new_glossary", 68)
    add_log("+10+ new people from author bios (Dario Amodei, Zhang Yiming, etc.)")
    increment_loop_state("new_people", 10)
    complete_node("rC1")
    add_log("P1 Loop C ✅ +68 terms, +10 people")
    wait_tick(0.4)

    add_log("18:33 REINFORCE P1: +80T/+22P/+4R (cross-ref burst!) → 🟢 continue")
    wait_tick(0.4)

    # ══════════════ PHASE 4: REINFORCE PASS 2 (18:36–18:41, 3 subagents) ══════════════
    update_loop_state(passes=2)
    for rid in ["rA2", "rB2", "rC2"]:
        update_node(rid, status="queued")
    wait_tick(0.4)

    update_node("rA2", status="running")
    add_log("18:36 P2: Loop A — NEW terms only: 15 terms → origins (Context Engineering etc.)")
    wait_tick(0.7)
    burn_tokens("rA2", 2, (3000, 5000), 0.8)
    add_log("Fix: 'Context Engineering' was Tobi Lütke (Shopify), not Chase. Chase popularized it.")
    complete_node("rA2")
    add_log("P2 Loop A ✅ 16 term origins confirmed (detail-fill, no new cross-refs)")

    update_node("rB2", status="running")
    add_log("18:39 P2: Loop B — NEW people: 15 bios (Dario Amodei, 王海峰, Charity Majors...)")
    wait_tick(0.6)
    burn_tokens("rB2", 2, (3000, 5000), 0.8)
    complete_node("rB2")
    add_log("P2 Loop B ✅ 15 bios filled")

    update_node("rC2", status="running")
    add_log("18:40 P2: Loop C — NEW readings: Chase/Moura/Chi Wang/Qingyun Wu latest")
    wait_tick(0.5)
    burn_tokens("rC2", 2, (2000, 4000), 0.7)
    complete_node("rC2")
    add_log("P2 Loop C ✅ 4 articles confirmed")

    add_log("18:41 DIMINISHING: Pass 2 is detail-fill only — 0 new cross-ref entries → STOP")
    add_log("Budget: 2/4 passes · ~130/200 Brave · ~12min/2h")
    wait_tick(0.4)

    # ══════════════ PHASE 5: VERIFIER (18:51–18:55) ══════════════
    update_node("ver", status="queued")
    wait_tick(0.5)
    update_node("ver", status="running")
    add_log("18:51 Verifier (fresh context): auditing 27 glossary entries — 9-point checklist...")
    wait_tick(0.8)
    burn_tokens("ver", 2, (2000, 4000), 0.7)
    add_log("❌ Multi-Agent System: wrong Anthropic blog title")
    wait_tick(0.5)
    burn_tokens("ver", 2, (1500, 3000), 0.6)
    add_log("❌ A2A v1.0: 'Mar 2026' → should be 'Apr 2026'")
    wait_tick(0.4)
    add_log("❌ Conversable Agents: 'COLM 2024' → 'ICLR 2024 LLM Agents Workshop'")
    wait_tick(0.5)
    burn_tokens("ver", 1, (2000, 3000), 0.5)
    add_log("Verifier ✅ 15 verified · 7 minor · 3 WRONG (fixed) · 5 unverified")
    complete_node("ver")
    wait_tick(0.4)

    # ══════════════ FINAL ══════════════
    total_tokens, total_cost = metrics_snapshot()
    update_loop_state(summary=(
        f"Groundweave complete: 27 Glossary · 18 Key People · 31 Reading List. "
        f"2 reinforce passes, diminishing at P2. 3 verifier errors fixed. "
        f"~130 Brave queries. Total: {total_tokens:,} tokens, ${total_cost:.4f}"
    ))
    add_log("18:55 🎯 DELIVERY: glossary-v2.md + key-people-v2.md + reading-list-v2.md")
    add_log("Project: multi-agent-research/")
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
            target=run_simulation,
            args=(context,),
            name=f"simulate-loop-{context.generation}",
            daemon=True,
        )
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
<title>Agent Loop Visualizer</title>
<style>
:root { --bg: #f6f8fa; --card: #fff; --text: #1f2328; --border: #d0d7de; --accent: #0969da; --muted: #656d76; --danger: #cf222e; --success: #1a7f37; --success-border: #2da44e; --success-bg: #dafbe1; --hover: #eaeef2; --canvas-center: #fff; --overlay: rgba(246,248,250,.92); --shadow-sm: rgba(0,0,0,.06); --shadow-side: rgba(0,0,0,.04); --shadow-lg: rgba(0,0,0,.12); --success-glow: rgba(45,164,78,.3); }
[data-theme="dark"] { --bg: #0d1117; --card: #161b22; --text: #c9d1d9; --border: #30363d; --accent: #58a6ff; --muted: #8b949e; --danger: #ff7b72; --success: #3fb950; --success-border: #238636; --success-bg: #173b24; --hover: #21262d; --canvas-center: #161b22; --overlay: rgba(13,17,23,.92); --shadow-sm: rgba(0,0,0,.35); --shadow-side: rgba(0,0,0,.3); --shadow-lg: rgba(0,0,0,.45); --success-glow: rgba(63,185,80,.3); }
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--text);font:14px/1.5 -apple-system,BlinkMacSystemFont,sans-serif;display:flex;flex-direction:column;height:100vh;overflow:hidden}
header{background:var(--card);border-bottom:1px solid var(--border);padding:10px 14px;display:flex;align-items:center;gap:12px;flex-shrink:0;flex-wrap:wrap;min-height:44px;box-shadow:0 1px 3px var(--shadow-sm)}
header h1{font-size:15px;font-weight:600;color:var(--accent);white-space:nowrap}
.goal{font-size:12px;color:var(--muted);flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.stats{display:flex;gap:12px;font-size:11px;font-family:'SF Mono',monospace}
.stat-val{color:var(--accent);font-weight:600}
.live-clock{color:var(--danger);font-variant-numeric:tabular-nums}
#theme-toggle{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:4px 8px;border-radius:4px;cursor:pointer;font-size:14px;min-height:32px;min-width:34px;transition:background .2s}
#theme-toggle:hover{background:var(--hover)}
#main{flex:1;display:flex;overflow:hidden;position:relative}
#canvas-panel{flex:1;display:flex;align-items:center;justify-content:center;background:radial-gradient(ellipse at center,var(--canvas-center) 0%,var(--bg) 70%);overflow:hidden;padding:12px;position:relative;touch-action:none;cursor:grab}
#canvas-panel.panning{cursor:grabbing}
svg{width:100%;height:auto;max-width:900px}
#reset-view{position:absolute;top:12px;right:12px;z-index:30;background:var(--card);border:1px solid var(--border);color:var(--text);padding:6px 10px;border-radius:4px;cursor:pointer;font-size:12px;box-shadow:0 1px 3px var(--shadow-sm)}
#reset-view:hover{background:var(--hover)}
#reset-view.hidden{display:none}
#start-overlay{position:absolute;top:0;left:0;right:0;bottom:0;display:flex;flex-direction:column;align-items:center;justify-content:center;background:var(--overlay);z-index:20;transition:opacity .4s ease;gap:16px}
#start-overlay.hidden{opacity:0;pointer-events:none}
#btn-start{font-size:24px;padding:16px 48px;border:2px solid var(--success-border);background:var(--success-bg);color:var(--success);border-radius:8px;cursor:pointer;font-weight:600;letter-spacing:.05em;transition:transform .2s,box-shadow .2s}
#btn-start:hover{transform:scale(1.05);box-shadow:0 0 24px var(--success-glow)}
#start-overlay p{color:var(--muted);font-size:13px}
#controls.hidden{display:none}
#side-panel{width:340px;background:var(--card);border-left:1px solid var(--border);display:flex;flex-direction:column;flex-shrink:0;transition:width .3s ease,opacity .3s ease,transform .3s ease;overflow:hidden;z-index:10;box-shadow:-2px 0 8px var(--shadow-side)}
#side-panel.collapsed{width:0;border-left:none;opacity:0}
#side-panel h2{font-size:13px;font-weight:600;color:var(--muted);padding:12px 16px;border-bottom:1px solid var(--border);text-transform:uppercase;letter-spacing:.05em;white-space:nowrap}
#logs{flex:1;overflow-y:auto;padding:8px;overscroll-behavior:contain}
.log-line{font-size:12px;color:var(--muted);padding:4px 8px;font-family:'SF Mono',monospace;line-height:1.6;word-break:break-all}
.log-line.bl{color:var(--danger)}
.log-line.ok{color:var(--success)}
#loop-state{padding:12px 16px;border-top:1px solid var(--border);font-size:12px}
#loop-state .ls-title{color:var(--muted);font-weight:600;margin-bottom:6px}
#loop-state .ls-item{color:var(--text);padding:2px 0;word-break:break-all}
#loop-state .ls-blocked{color:var(--danger)}
#toggle-log{background:var(--bg);border:1px solid var(--border);color:var(--muted);padding:4px 10px;border-radius:4px;cursor:pointer;font-size:12px;transition:background .2s;min-height:32px;min-width:32px}
#toggle-log:hover{background:var(--hover);color:var(--text)}
#toggle-log.active{background:var(--success-bg);border-color:var(--success-border);color:var(--success)}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.4}}
.pulse{animation:pulse 1.2s ease-in-out infinite}
.fade-in{animation:fadeIn .4s ease-out}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
#controls{display:flex;align-items:center;gap:6px;padding:6px 12px;background:var(--card);border-bottom:1px solid var(--border);flex-shrink:0;min-height:36px}
#controls button{background:var(--bg);border:1px solid var(--border);color:var(--text);padding:4px 10px;border-radius:4px;cursor:pointer;font-size:12px;min-width:28px;transition:background .2s}
#controls button:hover{background:var(--hover)}
#controls button.on{background:var(--success-bg);border-color:var(--success-border);color:var(--success)}
#speed-btns{display:flex;gap:2px}
#speed-btns button{min-width:32px;font-size:11px}
#speed-btns button.sel{background:var(--success-bg);border-color:var(--success-border);color:var(--success)}
#progress-wrap{flex:1;margin:0 8px}
#progress-bar{height:4px;background:var(--border);border-radius:2px;overflow:hidden}
#progress-fill{height:100%;width:0;background:var(--success-border);transition:width .3s ease;border-radius:2px}
.goal-input{flex:1;min-width:0;background:transparent;border:none;color:var(--text);font-size:12px;padding:2px 4px;outline:none;font-family:inherit}
.goal-input:focus{background:var(--bg)}
#node-detail{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:var(--card);border:1px solid var(--border);border-radius:8px;padding:16px 20px;z-index:100;min-width:300px;max-width:500px;box-shadow:0 4px 24px var(--shadow-lg);transition:opacity .2s ease,transform .2s ease}
#node-detail.hidden{opacity:0;transform:translateX(-50%) translateY(10px);pointer-events:none}
#detail-close{position:absolute;top:8px;right:12px;cursor:pointer;color:var(--muted);font-size:16px}
#detail-close:hover{color:var(--danger)}
#detail-content h3{color:var(--accent);font-size:14px;margin-bottom:4px}
#detail-content p{color:var(--muted);font-size:12px;margin:2px 0}
@media (max-width: 768px){
  body{overflow:hidden}
  header{padding:8px 12px;gap:8px;min-height:40px}
  header h1{font-size:14px}
  .goal{font-size:11px;max-width:140px}
  .stats{gap:8px;font-size:10px}
  #canvas-panel{padding:8px}
  #side-panel{position:absolute;top:0;right:0;bottom:0;width:100%;max-width:340px;transform:translateX(0)}
  #side-panel.collapsed{transform:translateX(100%);width:100%;opacity:0}
  #toggle-log{padding:6px 12px;font-size:13px}
  .log-line{font-size:13px;padding:6px 8px}
}
@media (max-width: 400px){
  .goal{display:none}
  .stats{gap:6px}
}
</style>
</head>
<body>
<header>
  <h1>🔭 Agent Loop</h1>
  <input class="goal-input" id="goal-input" value="Groundweave: Multi-Agent Architecture Research" />
  <div class="stats">
    <span>⏱ <span class="stat-val" id="elapsed">00:00</span></span>
    <span>🕐 <span class="stat-val live-clock" id="clock">--:--:--</span></span>
    <span>🔥 <span class="stat-val" id="tokens">0</span></span>
    <span>💰 <span class="stat-val" id="cost">$0</span></span>
  </div>
  <button id="theme-toggle" onclick="toggleTheme()" title="Switch to dark theme" aria-label="Switch color theme">🌙</button>
</header>
<div id="controls" class="hidden">
  <button id="btn-play" onclick="togglePlay()" title="Play/Pause">⏸</button>
  <div id="speed-btns">
    <button onclick="setSpeed(1)">1×</button><button onclick="setSpeed(2)">2×</button><button onclick="setSpeed(5)">5×</button>
  </div>
  <button id="btn-restart" onclick="doRestart()" title="Restart">⟳</button>
  <button id="btn-replay" onclick="doReplay()" title="Replay at 5×">⏩</button>
  <div id="progress-wrap"><div id="progress-bar"><div id="progress-fill"></div></div></div>
  <button id="toggle-log" onclick="togglePanel()">📋 Log</button>
</div>
<div id="main">
  <div id="canvas-panel">
    <svg id="svg" viewBox="0 0 900 685" preserveAspectRatio="xMidYMid meet"></svg>
    <button id="reset-view" class="hidden" onclick="resetView()">Reset view</button>
    <div id="start-overlay">
      <button id="btn-start" onclick="doStart()">▶ Start</button>
      <p>Groundweave: Multi-Agent Architecture Research</p>
    </div>
  </div>
  <div id="side-panel" class="collapsed">
    <h2>📋 Event Log</h2>
    <div id="logs"></div>
    <div id="loop-state">
      <div class="ls-title">📊 LOOP-STATE.md</div>
      <div id="ls-content"></div>
    </div>
  </div>
</div>
<div id="node-detail" class="hidden">
  <div id="detail-close" onclick="closeDetail()">✕</div>
  <div id="detail-content"></div>
</div>
<script>
const NODE_R = 8, NODE_W = 195, NODE_H = 64;
const COLOR_THEMES = {
  light: {
    nodes: {pending:'#eaeef2',queued:'#fff3cd',running:'#fff8e1',done:'#dafbe1',blocked:'#ffd8d8',warning:'#ffe8cc'},
    dots: {pending:'#8b949e',queued:'#d4a017',running:'#e6c300',done:'#2da44e',blocked:'#cf222e',warning:'#e67e00'},
    edge:'#d0d7de', edgeRunning:'#e6c300', edgeDone:'#a8e6cf', labelBg:'#f6f8fa',
    text:'#1f2328', muted:'#656d76', accent:'#0969da', badge:'#8b2e2e', badgeText:'#fff',
  },
  dark: {
    nodes: {pending:'#21262d',queued:'#3b2f12',running:'#3f3410',done:'#173b24',blocked:'#4c1f24',warning:'#462f13'},
    dots: {pending:'#8b949e',queued:'#d29922',running:'#e3b341',done:'#3fb950',blocked:'#ff7b72',warning:'#d29922'},
    edge:'#30363d', edgeRunning:'#d29922', edgeDone:'#238636', labelBg:'#0d1117',
    text:'#c9d1d9', muted:'#8b949e', accent:'#58a6ff', badge:'#6e3035', badgeText:'#f0f6fc',
  },
};
let COLORS = COLOR_THEMES.light;
let lastState = null;
let paused = false, speed = 1;
let zoom = 1, panX = 0, panY = 0;
let baseViewBox = [0, 0, 900, 685];
let baseViewBoxKey = baseViewBox.join(' ');
let panning = false, panPointerId = null, lastPointerX = 0, lastPointerY = 0;
const BASE = '__PATH_PREFIX__';

function parseViewBox(value){
  const parts = (Array.isArray(value) ? value : String(value || '').trim().split(/\s+/)).map(Number);
  if(parts.length !== 4 || parts.some(part => !Number.isFinite(part)) || parts[2] <= 0 || parts[3] <= 0){
    return [0, 0, 900, 685];
  }
  return parts;
}
function applyViewBox(){
  const [baseX, baseY, baseWidth, baseHeight] = baseViewBox;
  const width = baseWidth / zoom, height = baseHeight / zoom;
  const x = baseX + (baseWidth - width) / 2 + panX;
  const y = baseY + (baseHeight - height) / 2 + panY;
  document.getElementById('svg').setAttribute('viewBox', `${x} ${y} ${width} ${height}`);
  const changed = Math.abs(zoom - 1) > 0.001 || Math.abs(panX) > 0.001 || Math.abs(panY) > 0.001;
  document.getElementById('reset-view').classList.toggle('hidden', !changed);
}
function syncBaseViewBox(value){
  const next = parseViewBox(value);
  const nextKey = next.join(' ');
  if(nextKey !== baseViewBoxKey){
    baseViewBox = next;
    baseViewBoxKey = nextKey;
    zoom = 1;
    panX = 0;
    panY = 0;
  }
  applyViewBox();
}
function resetView(){
  zoom = 1;
  panX = 0;
  panY = 0;
  applyViewBox();
}

function applyTheme(theme, persist=true){
  const selected = theme === 'dark' ? 'dark' : 'light';
  document.documentElement.dataset.theme = selected;
  COLORS = COLOR_THEMES[selected];
  const button = document.getElementById('theme-toggle');
  const dark = selected === 'dark';
  button.textContent = dark ? '☀️' : '🌙';
  button.title = dark ? 'Switch to light theme' : 'Switch to dark theme';
  if(persist){
    try{ localStorage.setItem('agent-viz-theme', selected); }catch(_error){}
  }
  if(lastState) render(lastState);
}
function toggleTheme(){
  applyTheme(document.documentElement.dataset.theme === 'dark' ? 'light' : 'dark');
}
let initialTheme = 'light';
try{ initialTheme = localStorage.getItem('agent-viz-theme') || 'light'; }catch(_error){}
applyTheme(initialTheme, false);

function render(state){
  lastState = state;
  const gi = document.getElementById('goal-input');
  if(document.activeElement !== gi) gi.value = state.goal;
  document.getElementById('elapsed').textContent = fmtTime(state.elapsed);
  document.getElementById('tokens').textContent = fmtNum(state.total_tokens);
  document.getElementById('cost').textContent = '$'+state.total_cost.toFixed(4);
  const pct = (state.loop_state.done / state.loop_state.total) * 100;
  document.getElementById('progress-fill').style.width = pct+'%';
  const svg = document.getElementById('svg');
  syncBaseViewBox(state.viewBox);
  svg.replaceChildren();
  const ns = 'http://www.w3.org/2000/svg';
  state.edges.forEach(e => {
    const f = state.nodes.find(n=>n.id===e.from), t = state.nodes.find(n=>n.id===e.to);
    if(!f||!t) return;
    const fx = f.x, fy = f.y + NODE_H/2, tx = t.x, ty = t.y - NODE_H/2;
    const mx = (fx+tx)/2, my = (fy+ty)/2;
    const line = document.createElementNS(ns,'line');
    line.setAttribute('x1',fx); line.setAttribute('y1',fy);
    line.setAttribute('x2',tx); line.setAttribute('y2',ty);
    line.setAttribute('stroke-width','1.5');
    if(f.status==='running'){ line.setAttribute('stroke',COLORS.edgeRunning); line.classList.add('pulse'); }
    else if(f.status==='done' && t.status!=='pending') line.setAttribute('stroke',COLORS.edgeDone);
    else line.setAttribute('stroke',COLORS.edge);
    svg.appendChild(line);
    if(e.label){
      const lbg = document.createElementNS(ns,'rect');
      lbg.setAttribute('x',mx - e.label.length*3 - 4); lbg.setAttribute('y',my - 9);
      lbg.setAttribute('width',e.label.length*6 + 8); lbg.setAttribute('height',16);
      lbg.setAttribute('rx','3'); lbg.setAttribute('fill',COLORS.labelBg);
      svg.appendChild(lbg);
      const lt = document.createElementNS(ns,'text');
      lt.setAttribute('x',mx); lt.setAttribute('y',my + 3);
      lt.setAttribute('fill',COLORS.muted); lt.setAttribute('font-size','9');
      lt.setAttribute('text-anchor','middle');
      lt.setAttribute('font-family','SF Mono,monospace');
      lt.textContent = e.label;
      svg.appendChild(lt);
    }
  });
  state.nodes.forEach(n => {
    const g = document.createElementNS(ns,'g');
    g.setAttribute('transform',`translate(${n.x - NODE_W/2},${n.y - NODE_H/2})`);
    g.setAttribute('data-nid', n.id);
    g.style.cursor = 'pointer';
    g.addEventListener('click', () => showDetail(n));
    const rect = document.createElementNS(ns,'rect');
    rect.setAttribute('width',NODE_W); rect.setAttribute('height',NODE_H);
    rect.setAttribute('rx','8'); rect.setAttribute('fill',COLORS.nodes[n.status]||COLORS.nodes.pending);
    rect.setAttribute('stroke',COLORS.edge);
    rect.setAttribute('stroke-width',n.status==='running'?'2':'1.5');
    if(n.status==='running') rect.classList.add('pulse');
    g.appendChild(rect);
    const dot = document.createElementNS(ns,'circle');
    dot.setAttribute('cx',14); dot.setAttribute('cy',14);
    dot.setAttribute('r',5); dot.setAttribute('fill',COLORS.dots[n.status]||COLORS.dots.pending);
    if(n.status==='running') dot.classList.add('pulse');
    g.appendChild(dot);
    const label = document.createElementNS(ns,'text');
    label.setAttribute('x',28); label.setAttribute('y',18);
    label.setAttribute('fill',COLORS.text); label.setAttribute('font-size','12');
    label.setAttribute('font-weight','600'); label.textContent = n.label;
    g.appendChild(label);
    const task = document.createElementNS(ns,'text');
    task.setAttribute('x',12); task.setAttribute('y',38);
    task.setAttribute('fill',COLORS.muted); task.setAttribute('font-size','11');
    task.textContent = n.task;
    g.appendChild(task);
    const tok = document.createElementNS(ns,'text');
    tok.setAttribute('x',12); tok.setAttribute('y',55);
    tok.setAttribute('fill',COLORS.accent); tok.setAttribute('font-size','11');
    tok.setAttribute('font-family','SF Mono,monospace');
    tok.textContent = n.tokens ? `🔥 ${fmtNum(n.tokens)} tokens` : '';
    g.appendChild(tok);
    if(n.status==='blocked'){
      const badge = document.createElementNS(ns,'rect');
      badge.setAttribute('x',NODE_W-56); badge.setAttribute('y',6);
      badge.setAttribute('width',48); badge.setAttribute('height',16);
      badge.setAttribute('rx','4'); badge.setAttribute('fill',COLORS.badge);
      g.appendChild(badge);
      const bt = document.createElementNS(ns,'text');
      bt.setAttribute('x',NODE_W-54); bt.setAttribute('y',18);
      bt.setAttribute('fill',COLORS.badgeText); bt.setAttribute('font-size','9');
      bt.setAttribute('font-weight','600'); bt.textContent = 'BLOCKED';
      g.appendChild(bt);
    }
    svg.appendChild(g);
  });
  const logsEl = document.getElementById('logs');
  logsEl.replaceChildren();
  state.logs.forEach(l => {
    const div = document.createElement('div');
    div.className = 'log-line fade-in';
    if(l.includes('BLOCKED')||l.includes('\u26a0')) div.className += ' bl';
    if(l.includes('\u2705')) div.className += ' ok';
    div.textContent = l;
    logsEl.appendChild(div);
  });
  logsEl.scrollTop = logsEl.scrollHeight;
  const ls = document.getElementById('ls-content');
  ls.replaceChildren();
  const doneItem = document.createElement('div');
  doneItem.className = 'ls-item';
  doneItem.textContent = `Done: ${state.loop_state.done}/${state.loop_state.total}`;
  ls.appendChild(doneItem);
  state.loop_state.blocked.forEach(b => {
    const blocked = document.createElement('div');
    blocked.className = 'ls-item ls-blocked';
    blocked.textContent = `\u26a0 ${b}`;
    ls.appendChild(blocked);
  });
  if(state.loop_state.summary){
    const summary = document.createElement('div');
    summary.className = 'ls-item';
    summary.style.marginTop = '4px';
    summary.style.color = COLORS.muted;
    summary.textContent = state.loop_state.summary;
    ls.appendChild(summary);
  }
}

function fmtTime(s){return s<60?s+'s':Math.floor(s/60)+'m '+(s%60)+'s'}
function fmtNum(n){return n>=1000?(n/1000).toFixed(1)+'K':String(n)}
function updateClock(){
  const now = new Date();
  document.getElementById('clock').textContent = now.toTimeString().split(' ')[0];
}
updateClock();
setInterval(updateClock, 200);

const evt = new EventSource(BASE + '/stream');
evt.onmessage = e => { render(JSON.parse(e.data)); };

function togglePanel(){
  const p = document.getElementById('side-panel');
  const b = document.getElementById('toggle-log');
  p.classList.toggle('collapsed');
  b.classList.toggle('on');
}
function togglePlay(){
  paused = !paused;
  postControl(paused ? 'pause' : 'resume');
  document.getElementById('btn-play').textContent = paused ? '\u25b6' : '\u23f8';
  document.getElementById('btn-play').classList.toggle('on', !paused);
}
function setSpeed(s){
  speed = s;
  postControl('speed', {speed: s});
  document.querySelectorAll('#speed-btns button').forEach((b,i) => {
    b.classList.toggle('sel', (i===0&&s===1)||(i===1&&s===2)||(i===2&&s===5));
  });
}
function doRestart(){
  postControl('restart');
  document.getElementById('btn-play').textContent = '\u23f8';
  document.getElementById('btn-play').classList.add('on');
  paused = false;
}
function doStart(){
  document.getElementById('start-overlay').classList.add('hidden');
  document.getElementById('controls').classList.remove('hidden');
  doRestart();
}
function doReplay(){
  setSpeed(5);
  doRestart();
}
function showDetail(n){
  const d = document.getElementById('node-detail');
  const c = document.getElementById('detail-content');
  const colors = {pending:'\u23f3',queued:'\U0001f4cb',running:'\U0001f7e2',done:'\u2705',blocked:'\U0001f534',warning:'\u26a0\ufe0f'};
  c.replaceChildren();
  const heading = document.createElement('h3');
  heading.textContent = `${colors[n.status]||''} ${n.label}`;
  c.appendChild(heading);
  const task = document.createElement('p');
  const taskLabel = document.createElement('b');
  taskLabel.textContent = 'Task:';
  task.append(taskLabel, document.createTextNode(` ${n.task}`));
  c.appendChild(task);
  const metrics = document.createElement('p');
  const tokensLabel = document.createElement('b');
  tokensLabel.textContent = 'Tokens:';
  const statusLabel = document.createElement('b');
  statusLabel.textContent = 'Status:';
  metrics.append(
    tokensLabel,
    document.createTextNode(` ${fmtNum(n.tokens)} \u00b7 `),
    statusLabel,
    document.createTextNode(` ${n.status}`),
  );
  c.appendChild(metrics);
  d.classList.remove('hidden');
}
function closeDetail(){
  document.getElementById('node-detail').classList.add('hidden');
}
function postControl(action, extra={}){
  fetch(BASE + '/control', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({action, ...extra})
  });
}
document.getElementById('goal-input').addEventListener('keydown', e => {
  if(e.key === 'Enter'){ postControl('goal', {goal: e.target.value}); e.target.blur(); }
});
document.getElementById('goal-input').addEventListener('blur', e => {
  postControl('goal', {goal: e.target.value});
});
document.querySelector('#speed-btns button:nth-child(1)').classList.add('sel');
document.getElementById('btn-play').classList.add('on');
const canvasPanel = document.getElementById('canvas-panel');
canvasPanel.addEventListener('wheel', e => {
  e.preventDefault();
  const factor = e.deltaY < 0 ? 1.12 : 1 / 1.12;
  zoom = Math.min(3, Math.max(0.5, zoom * factor));
  applyViewBox();
}, {passive:false});
canvasPanel.addEventListener('pointerdown', e => {
  if(e.button !== 0 || e.target.closest('[data-nid], button')) return;
  panning = true;
  panPointerId = e.pointerId;
  lastPointerX = e.clientX;
  lastPointerY = e.clientY;
  canvasPanel.classList.add('panning');
  canvasPanel.setPointerCapture(e.pointerId);
});
canvasPanel.addEventListener('pointermove', e => {
  if(!panning || e.pointerId !== panPointerId) return;
  const svgRect = document.getElementById('svg').getBoundingClientRect();
  if(svgRect.width <= 0 || svgRect.height <= 0) return;
  panX -= (e.clientX - lastPointerX) * (baseViewBox[2] / svgRect.width) / zoom;
  panY -= (e.clientY - lastPointerY) * (baseViewBox[3] / svgRect.height) / zoom;
  lastPointerX = e.clientX;
  lastPointerY = e.clientY;
  applyViewBox();
});
function endPan(e){
  if(!panning || e.pointerId !== panPointerId) return;
  panning = false;
  panPointerId = null;
  canvasPanel.classList.remove('panning');
  if(canvasPanel.hasPointerCapture(e.pointerId)) canvasPanel.releasePointerCapture(e.pointerId);
}
canvasPanel.addEventListener('pointerup', endPan);
canvasPanel.addEventListener('pointercancel', endPan);
canvasPanel.addEventListener('click', e => {
  if(!e.target.closest('[data-nid]')) closeDetail();
});
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
            "const BASE = '__PATH_PREFIX__';",
            f"const BASE = {json.dumps(base)};",
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
        if body is None:
            raise RequestError(400, "JSON body must not be null")
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
        allowed_actions = {"pause", "resume", "toggle", "speed", "restart", "goal"}
        if not isinstance(action, str) or action not in allowed_actions:
            self._send_json(400, {"ok": False, "error": "unknown action"})
            return

        if action == "speed":
            speed = body.get("speed")
            try:
                speed_value = (
                    float(speed)
                    if not isinstance(speed, bool) and isinstance(speed, (int, float))
                    else None
                )
            except (OverflowError, TypeError, ValueError):
                speed_value = None
            if speed_value is None or not math.isfinite(speed_value) or speed_value <= 0:
                self._send_json(400, {"ok": False, "error": "speed must be a finite number greater than 0"})
                return
            set_speed(speed_value)
        elif action == "pause":
            set_paused(True)
        elif action == "resume":
            set_paused(False)
        elif action == "toggle":
            toggle_paused()
        elif action == "restart":
            restart_simulation()
        elif action == "goal":
            goal = body.get("goal")
            if not isinstance(goal, str) or len(goal) > 1000:
                self._send_json(400, {"ok": False, "error": "goal must be a string of at most 1000 characters"})
                return
            with STATE_LOCK:
                STATE["goal"] = goal
            broadcast()

        with STATE_LOCK:
            response = {
                "ok": True,
                "paused": CONTROL["paused"],
                "speed": CONTROL["speed"],
                "running": CONTROL["running"],
            }
        self._send_json(200, response)

    def _handle_api_post(self, path):
        if path == "/api/reset":
            stop_simulation()
            reset_state(_bypass_context=True)
            broadcast()
            self._send_json(200, {"ok": True, "state": state_snapshot()})
            return

        allowed_statuses = {"pending", "queued", "running", "done", "blocked"}
        try:
            body = self._read_json_body()
        except RequestError as error:
            self._send_json(error.status, {"ok": False, "error": error.message})
            return

        if path.startswith("/api/node/"):
            node_id = unquote(path[len("/api/node/"):])
            if not node_id or "/" in node_id:
                self._send_json(400, {"ok": False, "error": "invalid node id"})
                return
            updates = {}
            if "status" in body:
                status = body["status"]
                if not isinstance(status, str) or status not in allowed_statuses:
                    self._send_json(400, {
                        "ok": False,
                        "error": "status must be pending, queued, running, done, or blocked",
                    })
                    return
                updates["status"] = status
            if "tokens" in body:
                tokens = body["tokens"]
                if isinstance(tokens, bool) or not isinstance(tokens, int) or tokens < 0:
                    self._send_json(400, {
                        "ok": False,
                        "error": "tokens must be a non-negative integer",
                    })
                    return
                updates["tokens"] = tokens
            if not updates:
                self._send_json(400, {
                    "ok": False,
                    "error": "provide at least one of status or tokens",
                })
                return
            if not update_node(node_id, _bypass_context=True, **updates):
                self._send_json(404, {"ok": False, "error": "node not found"})
                return
            broadcast()
            self._send_json(200, {"ok": True, "id": node_id, **updates})
            return

        if path == "/api/log":
            msg = body.get("msg")
            if not isinstance(msg, str) or not msg or len(msg) > 10000:
                self._send_json(400, {
                    "ok": False,
                    "error": "msg must be a non-empty string of at most 10000 characters",
                })
                return
            add_log(msg, _bypass_context=True)
            broadcast()
            self._send_json(200, {"ok": True})
            return

        if path == "/api/metrics":
            total_tokens = body.get("total_tokens")
            total_cost = body.get("total_cost")
            if (
                isinstance(total_tokens, bool)
                or not isinstance(total_tokens, int)
                or total_tokens < 0
            ):
                self._send_json(400, {
                    "ok": False,
                    "error": "total_tokens must be a non-negative integer",
                })
                return
            if (
                isinstance(total_cost, bool)
                or not isinstance(total_cost, (int, float))
                or not math.isfinite(total_cost)
                or total_cost < 0
            ):
                self._send_json(400, {
                    "ok": False,
                    "error": "total_cost must be a finite non-negative number",
                })
                return
            with STATE_LOCK:
                STATE["total_tokens"] = total_tokens
                STATE["total_cost"] = total_cost
            broadcast()
            self._send_json(200, {"ok": True})
            return

        if path == "/api/loop-state":
            done = body.get("done")
            summary = body.get("summary")
            if isinstance(done, bool) or not isinstance(done, int) or done < 0:
                self._send_json(400, {
                    "ok": False,
                    "error": "done must be a non-negative integer",
                })
                return
            if not isinstance(summary, str) or len(summary) > 10000:
                self._send_json(400, {
                    "ok": False,
                    "error": "summary must be a string of at most 10000 characters",
                })
                return
            update_loop_state(done=done, summary=summary, _bypass_context=True)
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
    if load_persisted_state():
        print("Loaded persisted state")
    else:
        print("Starting fresh")
    print(f"Topology: {TOPOLOGY_SOURCE}")
    print(f"Agent Loop Visualizer → http://127.0.0.1:{PORT}{public_path}")
    server = ThreadingHTTPServer(("127.0.0.1", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        stop_simulation()
        server.shutdown()
