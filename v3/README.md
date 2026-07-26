# Agent Manager (v3)

A **solo developer's management layer for AI agents**. Not a chat tool — a place
to manage the several agents you're running in parallel the way you'd manage a
team: Kanban, Table, Dashboard, Inbox. Single machine, one user, no accounts, no
team/collaboration features.

> Solo ≠ single agent. One person running 5 agents at once *is* managing a team —
> the team just happens to be agents. v3 keeps the management layer and drops
> only the multi-person parts (sharing, per-person permissions, team canvases).

## Two levels of visibility

**Top level — across all tasks**
- **Dashboard** — counts by status, a live activity feed, and **shared memory**
  (learnings each agent leaves for the next one, so they stop re-deriving the
  same project facts).
- **Kanban** — Todo / In Progress / Review / Blocked / Done. Cards show the
  agent, its current step, and a mini loop-progress bar.
- **Table** — the same tasks, dense and sortable-by-eye.
- **Inbox** — only the tasks that need *you* (blocked or ready-for-review), each
  with the reason. This is the "which window was waiting on me?" fix.

**Drill-down — one task**
- Click any task to open its **loop flowchart**: orchestrator → subagents →
  verifier, with live status and token burn per node. A blocked or long-running
  subagent is highlighted, so you see **exactly which step is stuck** — the
  visualizer carried over from v1/v2, now scoped to a single task.

## Quick start

```bash
python3 server.py 8768
# → http://127.0.0.1:8768
```

The built-in solo-dev demo **auto-runs on first launch** (5 agents moving through
their loops, one getting blocked, one landing in review). **⟳** restarts it, **⏸/▶**
pauses. Toggle light/dark with 🌙. State auto-persists to `agent-manager-state.json`.
Set `AGENT_MGR_AUTOSTART=0` to launch with a quiet board instead.

### UI you can drive
- **Drag & drop** cards between Kanban columns → pushes a status change.
- **Inbox quick actions** — *Unblock* a blocked task, *Approve → Done* or *Send
  back* a review — same buttons live in the drill-down modal.
- **Search** (`/` to focus) filters tasks across Kanban / Table / Inbox.
- **Sortable table** — click any column header.
- **Keyboard** — `1`–`4` switch views, `/` search, `Esc` close modal. The 🔔
  bell (with a count) jumps to whatever needs you.

Optional path prefix for a reverse proxy:

```bash
AGENT_MGR_PATH_PREFIX=/agents python3 server.py 8768
# → http://127.0.0.1:8768/agents/
```

## How your real agents feed it

The browser updates live over SSE. Your agents push state over a small REST API
(`data source = agents report`). All endpoints return `{"ok": true}` on success.

### `POST /api/task/{id}` — task-level (feeds Kanban / Inbox / Table)
```bash
curl -X POST http://127.0.0.1:8768/api/task/t3 \
  -H 'Content-Type: application/json' \
  -d '{"status":"blocked","todo":"needs macOS runner secret",
       "attention_reason":"Blocked: missing MACOS_RUNNER_TOKEN — needs you"}'
```
Fields: `status` (`todo|running|review|blocked|done`), `title`, `todo`, `agent`,
`avatar`, `attention_reason`, `needs_attention`. A task set to `review`/`blocked`
enters the Inbox automatically.

### `POST /api/task/{id}/node/{node_id}` — subagent-level (feeds the drill-down loop)
```bash
curl -X POST http://127.0.0.1:8768/api/task/t3/node/w1 \
  -H 'Content-Type: application/json' \
  -d '{"status":"blocked","tokens":5300}'
```
Node `status`: `pending|queued|running|done|blocked`.

### `POST /api/learning` — append to shared memory
```bash
curl -X POST http://127.0.0.1:8768/api/learning \
  -H 'Content-Type: application/json' \
  -d '{"text":"Auth uses express-session + Redis, not Passport.",
       "task":"Refactor auth → JWT","agent":"auth-agent"}'
```

### `POST /api/activity` — one line to the activity feed
### `POST /api/reset` — clear the board and reload the demo tasks
### `GET  /api/state` — current state as JSON (debugging)

### Integration sketch
```python
import requests
VIZ = "http://127.0.0.1:8768"

def on_step(task_id, node_id, status, tokens):
    requests.post(f"{VIZ}/api/task/{task_id}/node/{node_id}",
                  json={"status": status, "tokens": tokens})

def on_blocked(task_id, why):
    requests.post(f"{VIZ}/api/task/{task_id}",
                  json={"status": "blocked", "attention_reason": why})
```

## Data model

```
task {
  id, title, agent, avatar, status, todo,
  needs_attention, attention_reason, tags, tokens, updated_at,
  graph: {                      # ← the drill-down loop
    viewBox,
    nodes: [ {id, label, x, y, status, tokens, task} ],
    edges: [ {from, to, label} ]
  }
}
learnings: [ {time, task, agent, text} ]   # shared memory
activity:  [ "[hh:mm:ss] …" ]              # global feed
```

## Architecture

```
Browser ← SSE ← Python server (single file, stdlib only)
                  ├── /                → HTML dashboard (Dashboard/Kanban/Table/Inbox + modal)
                  ├── /stream          → SSE
                  ├── /api/task/*      → agent push (task + subagent)
                  ├── /api/learning    → shared memory
                  ├── /api/activity    → activity feed
                  └── /control         → play / pause / restart the demo
```

Ported from v2: the SSE broadcast, atomic state persistence, and the
cancellable-simulation threading model.

## Not in this MVP (deliberately)

Multi-person sharing, per-person file permissions, custom/whiteboard views, and
the full Role/Apprentice system with verification rules. Those are the team-scale
and long-term-moat features — out of scope for the solo MVP.

## License

MIT
