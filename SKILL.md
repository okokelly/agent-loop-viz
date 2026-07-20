---
name: agent-loop-viz
description: "Build real-time SVG flowchart dashboards for multi-agent loop execution — live node status, per-agent token tracking, SSE streaming, block detection. Use when the user wants to visualize a delegate_task or multi-agent workflow instead of running it blind in the background."
version: 2.0.0
category: autonomous-ai-agents
---

# Agent Loop Visualizer

A single-file Python server that renders a live SVG flowchart of agent loop execution. Orchestrator → workers → verifier, with real-time status, token counters, cost tracking, and block detection. Zero dependencies beyond Python stdlib.

## When to Use

- User wants to **see** what a multi-agent loop is doing right now (not just get the final result)
- Running orchestrator + workers + verifier patterns and need to know which agent is consuming tokens
- Debugging blocked subagents — the visualizer shows exactly which node is stuck and why
- Building trust in autonomous loops before graduating them to unattended cron jobs

## Architecture

```
Browser ← SSE stream → Python server (single file, stdlib)
                           ├── / → embedded HTML/SVG dashboard
                           ├── /stream → SSE endpoint pushing live state
                           ├── /api/* → REST endpoints for external agent state updates
                           └── POST /control → play/pause/speed/restart
```

- **Server**: Python `http.server` + `threading`, single file
- **Frontend**: SVG flowchart (nodes + edges), light theme by default with dark mode toggle. Node shapes are filled with status color (not just borders).
- **Streaming**: Server-Sent Events — server pushes state updates, frontend re-renders SVG
- **States**: `pending` (grey) → `queued` (gold) → `running` (yellow, pulsing) → `done` (green) | `blocked` (red)

## Dashboard Layout

Three panels:

| Panel | Content |
|-------|---------|
| **Header** | Goal description, elapsed time, total tokens, total cost, dark mode toggle |
| **Canvas** | SVG flowchart: orchestrator → workers → verifier, edges colored by completion, zoom/pan support |
| **Sidebar** | Event log (timestamped, color-coded) + LOOP-STATE.md summary |

Each node on the canvas shows: role label, task description, live token counter, status indicator (dot + border color), and a BLOCKED badge when stuck.

## Quick Start

```bash
# Default topology (built-in GSB demo)
python3 server.py 8767
# → http://localhost:8767

# Custom topology
python3 server.py 8767 --topology my-topology.json
# or: AGENT_VIZ_TOPOLOGY=my-topology.json python3 server.py 8767
# → http://localhost:8767
```

For public access, add a cloudflared tunnel:
```bash
~/bin/cloudflared tunnel --url http://localhost:8767
```

## Topology Config

Define custom node graphs in JSON — no Python editing needed:

```json
{
  "goal": "My Research Project",
  "viewBox": [0, 0, 900, 700],
  "nodes": [
    {"id": "orch", "label": "Orchestrator", "x": 450, "y": 35, "task": "Plan and dispatch"},
    {"id": "w1", "label": "Worker A", "x": 250, "y": 150, "task": "Process dataset A"}
  ],
  "edges": [
    {"from": "orch", "to": "w1", "label": "delegate"}
  ]
}
```

Load via `--topology path.json` CLI flag or `AGENT_VIZ_TOPOLOGY` env var. Falls back to the built-in GSB demo topology if neither is provided.

## REST API for Real Execution

The server now exposes REST endpoints so external agents (or Hermes `delegate_task` calls) can push real state updates instead of relying on the simulated loop.

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/state` | GET | Return current STATE as JSON (debugging) |
| `/api/reset` | POST | Reset all state for a new run |
| `/api/node/{id}` | POST | Update node status + token count |
| `/api/log` | POST | Push a timestamped log entry |
| `/api/metrics` | POST | Set total_tokens + total_cost |
| `/api/loop-state` | POST | Update loop_state fields |

### Node update payload
```json
{"status": "running", "tokens": 2500}
```
Valid statuses: `pending`, `queued`, `running`, `done`, `blocked`. Status defaults to `pending` if omitted.

### Integration with delegate_task
```python
import requests
VIZ = "http://localhost:8767"

# Before dispatching
requests.post(f"{VIZ}/api/reset")
requests.post(f"{VIZ}/api/node/orch", json={"status": "running"})

# After each subagent completes
requests.post(f"{VIZ}/api/node/f1", json={"status": "done", "tokens": 12500})
requests.post(f"{VIZ}/api/log", json={"msg": "Farm:Frameworks — 5/5 subagents complete"})
requests.post(f"{VIZ}/api/metrics", json={"total_tokens": 485000, "total_cost": 0.211})
```

API calls bypass the simulation `RunContext` check — they work whether or not a simulation is running.

## State Persistence

The dashboard auto-saves state to `agent-viz-state.json` on every broadcast. On restart, the file is reloaded if it's less than 24 hours old. Add to `.gitignore` — it's a runtime artifact, not source code.

## Simulated vs Real Execution

The server ships with a **simulated loop** (`simulate_loop()` function) for demo purposes — it steps through a realistic multi-agent research flow with randomized token counts.

**To hook up real execution:**

1. Start the server: `python3 server.py 8767 --topology my-topology.json`
2. From your orchestrator, POST to `/api/reset` to clear state
3. As each `delegate_task` or subagent completes, POST to `/api/node/{id}` and `/api/log`
4. Update `/api/metrics` periodically with real token/cost data
5. The SSE broadcast pushes all changes to connected browsers in real time

The `simulate_loop()` demo can coexist with real execution — click "Start" for the demo, or use the API to drive real data. API calls cancel any running simulation automatically.

## UI Features

### Dark/Light Mode
Toggle button (🌙/☀️) in the header. Switches all CSS colors via custom properties. Preference persisted to `localStorage`.

### SVG Zoom/Pan
- **Mouse wheel** over the canvas: zoom in/out (0.5× to 3×)
- **Click and drag** on the canvas: pan
- **Reset view** button appears when zoomed or panned
- Node click targets preserved at all zoom levels

## Node State Machine

```
pending ──→ queued ──→ running ──→ done
                          │
                          └──→ blocked
```

**Light mode colors** (default):
```
pending=#eaeef2 (light gray)
queued=#fff3cd (light gold)
running=#fff8e1 (light yellow)
done=#dafbe1 (light green)
blocked=#ffd8d8 (light red)
warning=#ffe8cc (light orange)
```

**Dark mode colors:**
```
pending=#21262d, queued=#3b3000, running=#1a3a2a, done=#173b24, blocked=#490202
```

When building a new viz server, default to light mode with the GitHub-style color scheme (#f6f8fa background, #fff white cards, #0969da blue accents).

Edge colors in light mode: running=#e6c300 (pulsing yellow), done=#a8e6cf (green), pending=#d0d7de (gray border).
Node fill: status color directly (e.g., done nodes are filled green, running nodes are filled yellow). Border is subtle #d0d7de for contrast.
Node text: #1f2328 for labels, #656d76 for descriptions.
Edge label background: #f6f8fa.

## Cloudflared Deployment with Custom Path

To deploy at a subpath on an existing domain (e.g., `kellyjia.com/temp0717`), two layers must align:

### 1. Named Tunnel Config (`~/.cloudflared/config.yml`)

```yaml
ingress:
  - hostname: kellyjia.com
    path: /temp0717
    service: http://localhost:8768
```

After editing, reload the tunnel: `kill -HUP <pid>` (preferred — no downtime for other routes) or full restart `kill <pid>; ~/bin/cloudflared tunnel run <name> &`.

### 2. Server Path Handling

**Critical**: cloudflared passes the full subpath through to the backend — it does NOT strip the prefix. If the server only handles `/`, requests to `/temp0717/` get 404.
The server must:
- Serve HTML at both `/` (localhost) and `/temp0717/` (public URL)
- Adjust the EventSource path in JS so SSE connects to `/temp0717/stream` instead of `/stream`
- Redirect bare `/temp0717` → `/temp0717/` to avoid relative-resource breakage

### Diagnosing Tunnel + Path Issues

| Symptom | Cause | Fix |
|---------|-------|-----|
| 502 | Tunnel up but can't reach localhost | Server process died or wrong port. Check `lsof -i:PORT` |
| 404 on custom path | Server doesn't handle the prefix | Add path handler as above |
| 404 on `/` but 200 on `/temp0717/` | Forgot the localhost `/` case | Keep both `/` and `/prefix/` handlers |
| `ERR_HTTP_RESPONSE_CODE_FAILURE` in browser | Tunnel not yet propagated | Wait 5-10s after tunnel start, retry |

## Mobile Responsiveness

The dashboard must work on phone screens (~375px wide). Key patterns:

### SVG Scaling
```html
<svg viewBox="0 0 900 600" preserveAspectRatio="xMidYMid meet"></svg>
```
CSS: `width:100%; height:auto` — the viewBox handles proportional scaling; `meet` ensures the full diagram fits.

### Log Panel on Mobile
On screens <768px, the log panel becomes a slide-over overlay instead of squeezing the canvas.

### Touch Targets
All interactive elements need `min-height:32px; min-width:32px` — Apple HIG minimum.

### Body Scroll Lock
```css
body{overflow:hidden} /* prevents whole-page scroll on mobile */
#logs{overflow-y:auto; overscroll-behavior:contain} /* only log panel scrolls */
```

## Pitfalls

- **Port conflicts**: `lsof -ti:PORT | xargs kill -9` before restart. Especially when a previous server instance didn't fully exit.
- **Port 8766 reserved**: A legacy Python process on Kelly's machine keeps port 8766 occupied (recall webhook). Use 8767+ for new services.
- **API calls vs simulation**: The REST API cancels any running simulation before accepting writes. If you need both, run two server instances on different ports.
- **`agent-viz-state.json` is a runtime artifact**: Add it to `.gitignore`. The server auto-creates and overwrites it on every broadcast.
- **SSE reconnect**: Frontend auto-reconnects via `EventSource`, but if the server restarts mid-simulation, the state resets (unless persisted state was loaded on boot).
- **Single-file constraint**: All HTML/CSS/JS is embedded as a Python string. This is deliberate for zero-dependency deployment but makes the file long (~1,500 lines). Edit with care — the HTML string uses `r"""..."""` raw strings.
- **`&` backgrounding blocked**: Hermes terminal rejects `command &` in foreground mode. Use `terminal(background=true)` for long-lived processes.
- **Cloudflared path routing**: cloudflared passes the full subpath to the backend without stripping. Server must handle the prefix explicitly (see Cloudflared Deployment section above). Unlike nginx `proxy_pass` which can strip paths.
- **Tunnel HUP reload**: After editing `config.yml`, `kill -HUP <pid>` reloads without downtime. If new routes aren't picked up, a full restart (`kill` then restart) is needed.
- **502 vs 404 diagnosis**: 502 = tunnel alive but backend unreachable (server down). 404 = tunnel routing works but server doesn't handle the path. These point to different fixes.
- **Publishing: scrub personal paths before git push.** The `simulate_loop()` log messages often contain local paths. Run a broad grep (`amber|kelly|~/amber-os|/Users/`) before pushing. Replace with generic project names.
