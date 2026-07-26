---
name: agent-manager
description: "A solo developer's management layer for AI agents — manage many agents in parallel like a team (Kanban / Table / Dashboard / Inbox) with a per-task loop flowchart drill-down. Single-file Python server, SSE streaming, REST push API. Use when someone runs several agents at once and needs to see, at a glance, what each is doing and which one needs them — not just chat with one agent at a time."
version: 3.0.0
category: autonomous-ai-agents
---

# Agent Manager (v3)

A single-file Python server that turns "several agents running in parallel" into
a board you manage like a team. Two levels of visibility:

- **Top level** — Dashboard / Kanban / Table / Inbox across *all* tasks.
- **Drill-down** — click any task to see its loop flowchart (orchestrator →
  subagents → verifier) and exactly which subagent is stuck.

Built for one person on one machine. No accounts, no multi-person collaboration.
Zero dependencies beyond the Python stdlib.

## When to Use

- Someone is running **3+ agents at once** and tracking them "in their head."
- They keep losing track of **which window is waiting on them** (a blocked agent,
  a PR ready for review) — the Inbox is exactly this.
- They want agents to **share context** so the next agent doesn't re-derive (or
  re-mistake) project facts — the shared-memory learnings feed.
- Debugging a stuck loop: the drill-down shows **which subagent blocked and why**.

This is the management-layer sibling of the v1/v2 "Agent Loop Visualizer" — that
single-loop flowchart is now the *drill-down* inside a multi-task board.

## Architecture

```
Browser ← SSE ← Python server (single file, stdlib)
                  ├── /                → HTML dashboard (Dashboard/Kanban/Table/Inbox + modal)
                  ├── /stream          → SSE endpoint pushing live state
                  ├── /api/task/*      → agent push: task-level + subagent-level
                  ├── /api/learning    → shared-memory learnings
                  ├── /api/activity    → activity feed
                  ├── /api/reset       → clear board, reload demo tasks
                  └── /control         → play / pause / restart the demo
```

- **Server**: Python `http.server` + `threading`, single file.
- **Frontend**: embedded HTML/CSS/JS. Views render from one live STATE snapshot;
  the drill-down reuses the SVG flowchart from v1/v2.
- **Streaming**: Server-Sent Events — server pushes, browser re-renders.
- **Persistence**: atomic auto-save to `agent-manager-state.json`, reloaded on
  restart if < 24h old.

Reuses v2's hardened SSE broadcast, atomic persistence, and cancellable-simulation
threading model.

## Data Model

A **task** is owned by an agent and carries management-level fields plus a `graph`
(its internal loop, shown in the drill-down):

```
task {
  id, title, agent, avatar,
  status,                       # todo | running | review | blocked | done
  todo, tags, tokens, updated_at,
  needs_attention, attention_reason,
  graph: {
    viewBox,
    nodes: [ {id, label, x, y, status, tokens, task} ],  # subagents
    edges: [ {from, to, label} ]
  }
}
learnings: [ {time, task, agent, text} ]   # shared memory
activity:  [ "[hh:mm:ss] …" ]              # global feed
```

Setting a task to `review` or `blocked` puts it in the **Inbox** automatically.

## Quick Start

```bash
python3 server.py 8768
# → http://127.0.0.1:8768
```

The built-in solo-dev demo **auto-runs on first launch** (5 agents; one blocks,
one lands in review). Set `AGENT_MGR_AUTOSTART=0` for a quiet board. Path prefix
for a reverse proxy: `AGENT_MGR_PATH_PREFIX=/agents python3 server.py 8768`.

## REST API — how real agents feed it

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/state` | GET | Current STATE as JSON (debugging) |
| `/api/task/{id}` | POST | Task-level update (feeds Kanban / Inbox / Table) |
| `/api/task/{id}/node/{node_id}` | POST | Subagent-level update (feeds the drill-down loop) |
| `/api/learning` | POST | Append a learning to shared memory |
| `/api/activity` | POST | Push one line to the activity feed |
| `/api/reset` | POST | Reset the board and reload demo tasks |

```python
import requests
VIZ = "http://127.0.0.1:8768"

# a subagent stepped forward
requests.post(f"{VIZ}/api/task/t3/node/w1", json={"status": "running", "tokens": 3300})

# the whole task is now blocked on you
requests.post(f"{VIZ}/api/task/t3", json={
    "status": "blocked",
    "attention_reason": "Blocked: missing MACOS_RUNNER_TOKEN — needs you",
})

# leave context for the next agent
requests.post(f"{VIZ}/api/learning", json={
    "text": "Flake only repros on macOS runners; root cause is a timer race.",
    "task": "Fix flaky CI on macOS", "agent": "ci-agent",
})
```

Task status: `todo | running | review | blocked | done`. Node status:
`pending | queued | running | done | blocked`. API writes use `_bypass_context`,
so they apply whether or not the demo simulation is running.

## UI You Can Drive

- **Drag & drop** Kanban cards between columns → POSTs a task status change.
- **Quick actions** — *Unblock* (blocked), *Approve → Done* / *Send back* (review)
  in both the Inbox and the drill-down modal.
- **Search** (`/` to focus) filters across Kanban / Table / Inbox.
- **Sortable table** — click a column header.
- **Keyboard** — `1`–`4` switch views, `/` search, `Esc` close modal; the 🔔 bell
  (with count) jumps to whatever needs you.
- **Light/dark** toggle, persisted to `localStorage`.

## Pitfalls

- **Port conflicts**: `lsof -ti:PORT | xargs kill` before restart; a previous
  instance may still hold the port.
- **`agent-manager-state.json` is a runtime artifact** — it's in `.gitignore`;
  the server creates/overwrites it on every broadcast.
- **Single-file constraint**: all HTML/CSS/JS is embedded as a Python raw string
  (`r"""..."""`). Deliberate for zero-dependency deploy; edit with care.
- **Auto-start vs persisted state**: on boot the server loads persisted state if
  fresh (< 24h) and only auto-runs the demo when starting clean. Delete the state
  file (or `AGENT_MGR_AUTOSTART=0`) to control which happens.
- **SSE reconnect**: the browser auto-reconnects via `EventSource`; a mid-run
  server restart resets state unless persisted state was reloaded on boot.
- **Scrub personal paths before publishing**: demo/log strings can carry local
  paths — grep before pushing.
