# claude-mcps

Custom MCP servers for Claude — shared across machines via git.

## Structure

```
mcps/
├── upstox/          # Upstox Indian market data
│   ├── server.py
│   ├── README.md
│   └── instruments_cache/   ← gitignored, rebuilt per machine
└── <next-mcp>/
    └── server.py
```

Each MCP lives in its own subdirectory with a `server.py` entry point.

## Setup on a new machine

### 1. Clone

```bash
git clone https://github.com/rakeshveggalam/claude-mcps.git ~/Claude/mcps
```

### 2. Install dependencies

```bash
pip3 install mcp httpx          # upstox
# add per-MCP deps as you add servers
```

### 3. Configure Claude Desktop

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "upstox": {
      "command": "/path/to/python3",
      "args": ["~/Claude/mcps/upstox/server.py"],
      "env": {
        "UPSTOX_ACCESS_TOKEN": "your_token_here"
      }
    }
  }
}
```

Find Python path with: `which python3`

### 4. Restart Claude

### Keeping in sync

```bash
# Pull latest on any machine
cd ~/Claude/mcps && git pull

# After editing, push
git add -p && git commit -m "..." && git push
```

## MCP Servers

| Directory | Tools | Description |
|-----------|-------|-------------|
| `upstox/` | 21 tools | NSE/MCX market data, technicals, options, fundamentals |

## What's NOT in git (per-machine)

| Path | Why |
|------|-----|
| `upstox/instruments_cache/` | Large JSON files (~100MB), rebuilt on demand |
| `UPSTOX_ACCESS_TOKEN` | Secret — set in shell or claude_desktop_config.json per machine |
| Python path in config | May differ between machines |
