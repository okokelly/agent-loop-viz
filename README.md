# Agent Loop Visualizer

Real-time SVG flowchart dashboard for multi-agent loop execution. Single-file, zero-dependency Python server with SSE streaming.

## What it does

Renders a live flowchart showing agent loop topology — orchestrator → workers → verifier → reinforce cycles. Each node shows status (pending/queued/running/done/blocked), token consumption, and task description. Edges show data flow between nodes.

## Quick Start

```bash
python3 server.py 8767
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
- **Light mode:** GitHub-style light color palette with status-colored node fills
- **Mobile responsive:** SVG viewBox scaling, overlay panels on small screens
- **Zero dependencies:** Python stdlib only — `http.server` + `threading` + `json`

## Architecture

```
Browser ← SSE stream → Python server (single file)
                          ├── / → HTML/SVG dashboard
                          ├── /stream → SSE endpoint
                          └── POST /control → play/pause/speed/restart
```

## Customizing the Simulation

Edit the `STATE` dict and `simulate_loop()` function to change the node topology and timeline. Nodes have `id`, `label`, `x`, `y`, `status`, and `task` fields. Edges connect nodes with optional labels.

## License

MIT
