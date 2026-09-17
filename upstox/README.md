# Upstox MCP Server

Local MCP server exposing Upstox market data APIs as tools for Claude.

## Setup on a new machine

### 1. Install Python dependencies

```bash
pip3 install mcp httpx
```

### 2. Set your Upstox access token

Add to `~/.zshrc` (or `~/.bashrc`):

```bash
export UPSTOX_ACCESS_TOKEN="your_token_here"
```

### 3. Register in Claude Desktop config

Edit `~/Library/Application Support/Claude/claude_desktop_config.json`
(macOS) or `%APPDATA%\Claude\claude_desktop_config.json` (Windows):

```json
{
  "mcpServers": {
    "upstox": {
      "command": "/path/to/python3",
      "args": ["/path/to/Claude/mcps/upstox_mcp.py"],
      "env": {
        "UPSTOX_ACCESS_TOKEN": "your_token_here"
      }
    }
  }
}
```

Find your Python path with: `which python3`

### 4. Restart Claude

The `mcp__upstox__*` tools will appear in Claude sessions including Cowork.

## Instrument cache

Downloaded automatically on first market tool call. Stored in
`instruments_cache/` (gitignored — rebuilt per machine).

- NSE: ~98K instruments
- MCX: ~16K instruments  
- Global: ~13 instruments

Cache refreshes automatically if older than 08:00 IST today.

## Tools

| Tool | Description |
|------|-------------|
| `search_instrument` | Resolve symbol/ISIN to instrument_key |
| `get_ohlc_quotes` | Bulk OHLC — pass `symbols=[...]` or `universe="FO_ALL"` |
| `get_bulk_intraday_technicals` | VWAP, RSI, vol_ratio for multiple stocks in parallel |
| `get_bulk_historical_technicals` | ATR, RSI, avg volume for multiple stocks in parallel |
| `get_put_call_ratio` | PCR + sentiment for multiple underlyings |
| `get_option_chain` | Full strike-wise chain with greeks |
| `get_open_interest` | OI per strike |
| `get_oi_change` | OI build-up / unwinding |
| `get_max_pain` | Max pain strike |
| `get_futures_smartlist` | F&O screener (OI gainers, price movers, etc.) |
| `get_market_holidays` | NSE/MCX holiday calendar |
| `get_balance_sheet` | Balance sheet history |
| `get_income_statement` | P&L history (yearly or quarterly) |
| `get_cash_flow` | Cash flow history |
| `get_share_holdings` | Promoter/FII/MF shareholding pattern |
| `get_competitors` | Peer list for comparison |
| `refresh_instruments` | Force refresh instrument cache |
| `cache_status` | Check cache age and record counts |
