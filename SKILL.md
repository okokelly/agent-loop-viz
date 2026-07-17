---
name: agent-loop-viz
description: "Build real-time SVG flowchart dashboards for multi-agent loop execution — live node status, per-agent token tracking, SSE streaming, block detection. Use when the user wants to visualize a delegate_task or multi-agent workflow instead of running it blind in the background."
version: 1.0.0
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
Browser → SSE stream ← Python server (single file, stdlib)
                           ├── / → embedded HTML/SVG dashboard
                           ├── /stream → SSE endpoint pushing live state
                           └── background thread → loop simulator (or real executor)
```

- **Server**: Python `http.server` + `threading`, single file
- **Frontend**: SVG flowchart (nodes + edges), light theme by default, dark available. Node shapes are filled with status color (not just borders).
- **Streaming**: Server-Sent Events — server pushes state updates, frontend re-renders SVG
- **States**: `pending` (grey) → `queued` (gold) → `running` (yellow, pulsing) → `done` (green) | `blocked` (red)

## Dashboard Layout

Three panels:

| Panel | Content |
|-------|---------|
| **Header** | Goal description, elapsed time, total tokens, total cost |
| **Canvas** | SVG flowchart: orchestrator → workers → verifier, edges colored by completion |
| **Sidebar** | Event log (timestamped, color-coded) + LOOP-STATE.md summary |

Each node on the canvas shows: role label, task description, live token counter, status indicator (dot + border color), and a BLOCKED badge when stuck.

## Quick Start

Template: `~/agent-loop-viz/server.py`

```bash
cd ~/agent-loop-viz
python3 server.py 8767
# → http://localhost:8767
```

For public access, add a cloudflared tunnel:
```bash
~/bin/cloudflared tunnel --url http://localhost:8767
```

## Simulated vs Real Execution

The template ships with a **simulated loop** (`simulate_loop()` function) for demo purposes — it steps through a realistic multi-agent research flow with randomized token counts. To hook up real execution:

1. Replace `simulate_loop()` with an actual orchestrator that spawns `delegate_task` calls
2. After each `delegate_task` completes, call `update_node()` + `add_log()` + `broadcast()` to push state
3. Track real token usage from the subagent results

The SSE broadcast pattern (`broadcast()` → pushes full STATE to all connected clients) stays the same regardless of whether execution is simulated or real.

For a step-by-step guide to adapting the hardcoded GSB template for a new project, see `references/adapting-server-py.md`.

## Node State Machine

```
pending ──→ queued ──→ running ──→ done
                          │
                          └──→ blocked
```

**Light mode colors** (user preference — default for Kelly):
```
pending=#eaeef2 (light gray)
queued=#fff3cd (light gold)
running=#fff8e1 (light yellow)
done=#dafbe1 (light green)
blocked=#ffd8d8 (light red)
warning=#ffe8cc (light orange)
```

**Dark mode colors** (original):
```
pending=#3a3a4a, queued=#8b7a2e, running=#2e8b57 (pulsing), done=#3a6b8b, blocked=#8b2e2e
```

When building a new viz server, default to light mode with the light color palette. The user prefers light backgrounds with GitHub-style color scheme (#f6f8fa background, #fff white cards, #0969da blue accents).

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

```python
def do_GET(self):
    path = urlparse(self.path).path
    if path == "/":
        # localhost case
        self.wfile.write(HTML.encode())
    elif path in ("/temp0717/",):
        # Public URL case — inject correct SSE path
        html = HTML.replace(
            "const evt = new EventSource('/stream');",
            "const evt = new EventSource('/temp0717/stream');"
        )
        self.wfile.write(html.encode())
    elif path == "/temp0717":
        # Redirect bare → trailing slash
        self.send_response(301)
        self.send_header("Location", "/temp0717/")
        self.end_headers()
    elif path in ("/stream", "/temp0717/stream"):
        # SSE — same handler for both
        ...
```

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
On screens <768px, the log panel becomes a slide-over overlay instead of squeezing the canvas:
```css
@media (max-width: 768px){
  #side-panel{
    position:absolute; top:0; right:0; bottom:0;
    width:100%; max-width:340px;
    transform:translateX(0);
  }
  #side-panel.collapsed{
    transform:translateX(100%); /* slide off-screen */
    opacity:0;
  }
}
```

### Touch Targets
All interactive elements need `min-height:32px; min-width:32px` — Apple HIG minimum.

### Body Scroll Lock
```css
body{overflow:hidden} /* prevents whole-page scroll on mobile */
#logs{overflow-y:auto; overscroll-behavior:contain} /* only log panel scrolls */
```

### Media Query Breakpoints
```css
@media (max-width: 768px) { /* tablet: overlay panel, smaller header */ }
@media (max-width: 400px) { /* small phone: hide non-critical elements */ }
## Pitfalls

- **Port conflicts**: `lsof -ti:PORT | xargs kill -9` before restart. Especially when a previous server instance didn't fully exit.
- **Port 8766 reserved**: A legacy Python process on Kelly's machine keeps port 8766 occupied (recall webhook). Use 8767+ for new services.
- **Simulation ≠ execution**: The template simulates token counts with `random`. Real integration requires reading actual token usage from `delegate_task` results.
- **Default server.py is NOT generic**: The deployed template at `~/agent-loop-viz/server.py` is hardcoded to a specific GSB research project — node topology, labels, goal text, and task descriptions are all GSB-specific. Running `python3 server.py` as-is shows GSB content no matter what project you're working on. For a new project, you MUST rewrite STATE['goal'], STATE['loop_state'], STATE['nodes'] (topology + labels + tasks), and STATE['edges'] before the viz is meaningful. This is a template to be adapted, not a plug-and-play generic viz.
- **SSE reconnect**: Frontend auto-reconnects via `EventSource`, but if the server restarts mid-simulation, the state resets.
- **No persistence**: The dashboard is ephemeral — refresh loses state. For durable visualization, the real executor should write state to a file that the server reads on startup.
- **Single-file constraint**: All HTML/CSS/JS is embedded as a Python string. This is deliberate for zero-dependency deployment but makes the file long (~400 lines). Edit with care — the HTML string uses `r"""..."""` raw strings.
- **`&` backgrounding blocked**: Hermes terminal rejects `command &` in foreground mode. Use `terminal(background=true)` for long-lived processes.
- **Cloudflared path routing**: cloudflared passes the full subpath to the backend without stripping. Server must handle the prefix explicitly (see Cloudflared Deployment section above). Unlike nginx `proxy_pass` which can strip paths.
- **Tunnel HUP reload**: After editing `config.yml`, `kill -HUP <pid>` reloads without downtime. If new routes aren't picked up, a full restart (`kill` then restart) is needed.
- **502 vs 404 diagnosis**: 502 = tunnel alive but backend unreachable (server down). 404 = tunnel routing works but server doesn't handle the path. These point to different fixes.
- **Publishing: scrub personal paths before git push.** The `simulate_loop()` log messages often contain local paths like `~/amber-os/_Projects/...`. Run the gitpush personal-info audit (`grep -rIiE "amber|kelly|~/amber-os|/Users/"`) before pushing. Replace with generic project names.
