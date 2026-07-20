# Agent Loop Visualizer

Real-time SVG flowchart dashboard for multi-agent loop execution. Single-file, zero-dependency Python server with SSE streaming and a REST API for external agent integration.

## What it does

Renders a live flowchart showing agent loop topology — orchestrator → workers → verifier → reinforce cycles. Each node shows status (pending/queued/running/done/blocked), token consumption, and task description. Edges show data flow between nodes.

## Quick Start

```bash
# Default (GSB demo topology)
python3 server.py 8767
# → http://127.0.0.1:8767

# Custom topology
python3 server.py 8767 --topology my-topology.json
# → http://127.0.0.1:8767
```

Optionally serve behind a reverse proxy (nginx, cloudflared) with a custom path prefix:

```bash
AGENT_VIZ_PATH_PREFIX=/research python3 server.py 8767
# → http://127.0.0.1:8767/research/
```

## Features

- **Interactive controls:** Play/Pause, 1×/2×/5× speed, Restart, Replay at 5×
- **Node detail panel:** Click any node to see task details and token count
- **Event log sidebar:** Timestamped color-coded logs + LOOP-STATE.md summary
- **Light/dark mode:** Toggle in header, persisted to localStorage
- **SVG zoom/pan:** Mouse wheel zoom (0.5–3×) + drag to pan, reset button
- **REST API:** Push real state updates from external agents (see below)
- **State persistence:** Auto-save to `agent-viz-state.json`, reloaded on restart
- **Configurable topology:** Load custom node graphs from JSON files
- **Mobile responsive:** SVG viewBox scaling, overlay panels on small screens
- **Zero dependencies:** Python stdlib only — `http.server` + `threading` + `json`

## Topology Config

Define your own node graph in a JSON file:

```json
{
  "goal": "My Research Project",
  "viewBox": [0, 0, 900, 700],
  "nodes": [
    {"id": "orch", "label": "Orchestrator", "x": 450, "y": 35, "task": "Plan and dispatch"},
    {"id": "w1", "label": "Worker A", "x": 250, "y": 150, "task": "Process dataset A"},
    {"id": "w2", "label": "Worker B", "x": 650, "y": 150, "task": "Process dataset B"},
    {"id": "ver", "label": "Verifier", "x": 450, "y": 280, "task": "Cross-check results"}
  ],
  "edges": [
    {"from": "orch", "to": "w1", "label": "delegate"},
    {"from": "orch", "to": "w2", "label": "delegate"},
    {"from": "w1", "to": "ver", "label": "results"},
    {"from": "w2", "to": "ver", "label": "results"}
  ]
}
```

Load it with:

```bash
python3 server.py 8767 --topology my-topology.json
# or via env var:
AGENT_VIZ_TOPOLOGY=my-topology.json python3 server.py 8767
```

## REST API

Real agents can push state updates via POST endpoints. All return `{"ok": true}` on success.

### `GET /api/state`
Return current dashboard state as JSON (for debugging).

### `POST /api/reset`
Reset all state for a new run.

```bash
curl -X POST http://127.0.0.1:8767/api/reset
```

### `POST /api/node/{id}`
Update a node's status and/or token count.

```bash
curl -X POST http://127.0.0.1:8767/api/node/orch \
  -H 'Content-Type: application/json' \
  -d '{"status": "running", "tokens": 2500}'
```

Valid statuses: `pending`, `queued`, `running`, `done`, `blocked`

### `POST /api/log`
Push a log entry to the event sidebar.

```bash
curl -X POST http://127.0.0.1:8767/api/log \
  -H 'Content-Type: application/json' \
  -d '{"msg": "Worker A completed — 5 subagents, ~120KB"}'
```

### `POST /api/metrics`
Update the total token/cost counters in the header.

```bash
curl -X POST http://127.0.0.1:8767/api/metrics \
  -H 'Content-Type: application/json' \
  -d '{"total_tokens": 485000, "total_cost": 0.211}'
```

### `POST /api/loop-state`
Update loop progress fields.

```bash
curl -X POST http://127.0.0.1:8767/api/loop-state \
  -H 'Content-Type: application/json' \
  -d '{"done": 3, "summary": "Phase 1 complete: 5/12 nodes done"}'
```

### Integration pattern

```python
# In your agent orchestrator:
import requests

def on_subagent_complete(node_id, status, tokens, log_msg):
    requests.post(f"{VIZ_URL}/api/node/{node_id}", json={
        "status": status, "tokens": tokens
    })
    requests.post(f"{VIZ_URL}/api/log", json={"msg": log_msg})
```

The SSE stream pushes all state changes to connected browsers in real time.

## State Persistence

State is auto-saved to `agent-viz-state.json` on every state change. On restart, the server reloads the file if it's less than 24 hours old. Add to `.gitignore` — it's a runtime artifact.

## Architecture

```
Browser ← SSE stream → Python server (single file)
                          ├── / → HTML/SVG dashboard
                          ├── /stream → SSE endpoint
                          ├── /api/* → REST state update endpoints
                          └── POST /control → play/pause/speed/restart
```

## Customizing the Simulation

The built-in `simulate_loop()` is a GSB research demo. For real use, push state via the REST API from your actual agent orchestrator. The simulation runs automatically on "Start" — skip it by loading a custom topology and using the API instead.

## License

MIT
