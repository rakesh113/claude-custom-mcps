"""
Upstox MCP Server — exposes Upstox REST APIs as MCP tools for Claude.
Auth: reads UPSTOX_ACCESS_TOKEN from environment.

Instrument cache (two layers):
  1. Filesystem: instruments_cache/{NSE,MCX,Global}.json — persists across Claude restarts.
     Considered stale if older than 08:00 IST today (Upstox publishes fresh files by then).
  2. In-memory index: built from the filesystem file, lives for the current Claude session.

Symbol resolution is handled internally — Claude passes human symbols ("HDFCBANK"),
MCP resolves to instrument_keys, Upstox is called, output is keyed by symbol.
Claude never touches instrument_keys for bulk tools.

Both layers are populated lazily. No downloads on startup.
"""

import os
import gzip
import json
import httpx
import logging
import threading
import concurrent.futures
from pathlib import Path
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo

from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE = "https://api.upstox.com"
BASE_HFT = "https://api-hft.upstox.com"
IST = ZoneInfo("Asia/Kolkata")
CACHE_DIR = Path(__file__).parent / "instruments_cache"
CACHE_DIR.mkdir(exist_ok=True)

INSTRUMENT_URLS = {
    "NSE":    "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz",
    "MCX":    "https://assets.upstox.com/market-quote/instruments/exchange/MCX.json.gz",
    "Global": "https://assets.upstox.com/market-quote/instruments/exchange/global.json.gz",
}

REFRESH_AFTER = time(8, 0)  # Upstox publishes fresh files by 08:00 IST

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("upstox_mcp")

mcp = FastMCP("upstox")

# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _headers() -> dict:
    token = os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("UPSTOX_ACCESS_TOKEN is not set in environment")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _account_headers() -> dict:
    token = os.environ.get("UPSTOX_API_TOKEN") or os.environ.get("UPSTOX_ACCESS_TOKEN")
    if not token:
        raise RuntimeError("UPSTOX_API_TOKEN (or UPSTOX_ACCESS_TOKEN) is not set in environment")
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _get(path: str, params: dict = None) -> dict:
    with httpx.Client(timeout=15) as client:
        r = client.get(f"{BASE}{path}", headers=_headers(), params=params)
        r.raise_for_status()
        return r.json()


def _account_get(path: str, params: dict = None, base: str = None) -> dict:
    url_base = base or BASE
    with httpx.Client(timeout=15) as client:
        r = client.get(f"{url_base}{path}", headers=_account_headers(), params=params)
        r.raise_for_status()
        return r.json()


def _account_post(path: str, body: dict, base: str = None) -> dict:
    url_base = base or BASE_HFT
    with httpx.Client(timeout=15) as client:
        r = client.post(f"{url_base}{path}", headers=_account_headers(), json=body)
        r.raise_for_status()
        return r.json()


def _account_put(path: str, body: dict, base: str = None) -> dict:
    url_base = base or BASE_HFT
    with httpx.Client(timeout=15) as client:
        r = client.put(f"{url_base}{path}", headers=_account_headers(), json=body)
        r.raise_for_status()
        return r.json()


def _account_delete(path: str, params: dict = None, base: str = None) -> dict:
    url_base = base or BASE_HFT
    with httpx.Client(timeout=15) as client:
        r = client.delete(f"{url_base}{path}", headers=_account_headers(), params=params)
        r.raise_for_status()
        return r.json()


# ---------------------------------------------------------------------------
# Filesystem cache
# ---------------------------------------------------------------------------

def _cache_path(exchange: str) -> Path:
    return CACHE_DIR / f"{exchange}.json"


def _is_stale(exchange: str) -> bool:
    path = _cache_path(exchange)
    if not path.exists():
        return True
    now = datetime.now(IST)
    cutoff = datetime.combine(now.date(), REFRESH_AFTER, tzinfo=IST)
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=IST)
    return now >= cutoff and mtime < cutoff


_download_locks: dict[str, threading.Lock] = {e: threading.Lock() for e in INSTRUMENT_URLS}


def _download_exchange(exchange: str) -> int:
    with _download_locks[exchange]:
        if not _is_stale(exchange):
            return len(json.loads(_cache_path(exchange).read_text(encoding="utf-8")))
        url = INSTRUMENT_URLS[exchange]
        log.info(f"Fetching instruments for {exchange}")
        with httpx.Client(timeout=60) as client:
            r = client.get(url)
            r.raise_for_status()
        data = json.loads(gzip.decompress(r.content))
        _cache_path(exchange).write_text(json.dumps(data), encoding="utf-8")
        # Invalidate all derived caches
        _index.pop(exchange, None)
        _sym_map_cache.clear()
        _fo_universe_cache.clear()
        log.info(f"Saved {len(data):,} instruments for {exchange}")
        return len(data)


def _ensure_fresh(exchange: str):
    if _is_stale(exchange):
        _download_exchange(exchange)


# ---------------------------------------------------------------------------
# In-memory search index (for search_instrument tool)
# ---------------------------------------------------------------------------

_index: dict[str, dict] = {}


def _get_index(exchange: str) -> dict:
    _ensure_fresh(exchange)
    disk_mtime = _cache_path(exchange).stat().st_mtime
    if _index.get(exchange, {}).get("__mtime__") != disk_mtime:
        log.info(f"Building search index for {exchange}")
        records = json.loads(_cache_path(exchange).read_text(encoding="utf-8"))
        idx: dict = {"__mtime__": disk_mtime}
        for rec in records:
            for field in ("trading_symbol", "instrument_key", "name", "asset_symbol"):
                val = rec.get(field, "")
                if val:
                    idx.setdefault(val.lower(), []).append(rec)
        _index[exchange] = idx
        log.info(f"Search index ready for {exchange}: {len(records):,} records")
    return _index[exchange]


# ---------------------------------------------------------------------------
# Symbol → instrument_key map  (internal, used by bulk tools)
# ---------------------------------------------------------------------------

# sym_map: trading_symbol (exact, case-sensitive) → instrument_key for NSE_EQ + NSE_INDEX
_sym_map_cache: dict = {}   # {"__mtime__": float, "HDFCBANK": "NSE_EQ|...", ...}


def _get_sym_map() -> dict:
    """
    Build a trading_symbol → instrument_key map from the NSE cache.
    Covers NSE_EQ and NSE_INDEX. Exact symbol match, preserves special chars
    (BAJAJ-AUTO, M&M, GVT&D stored as-is from the instruments file).
    """
    _ensure_fresh("NSE")
    disk_mtime = _cache_path("NSE").stat().st_mtime
    if _sym_map_cache.get("__mtime__") == disk_mtime:
        return _sym_map_cache

    log.info("Building symbol→key map")
    records = json.loads(_cache_path("NSE").read_text(encoding="utf-8"))
    m: dict = {"__mtime__": disk_mtime}
    for rec in records:
        seg = rec.get("segment", "")
        sym = rec.get("trading_symbol", "")
        ikey = rec.get("instrument_key", "")
        if not sym or not ikey:
            continue
        # Prefer NSE_EQ for equity, NSE_INDEX for indices; skip FO rows
        if seg in ("NSE_EQ", "NSE_INDEX"):
            # Don't overwrite an NSE_EQ entry with an NSE_INDEX one
            if sym not in m or seg == "NSE_EQ":
                m[sym] = ikey
    _sym_map_cache.clear()
    _sym_map_cache.update(m)
    log.info(f"Symbol map ready: {len(m)-1:,} entries")
    return _sym_map_cache


def _resolve_symbols(symbols: list[str]) -> dict[str, str]:
    """
    Returns {symbol: instrument_key} for each symbol.
    Falls back to Upstox search API for any symbol not found in local cache.
    """
    sym_map = _get_sym_map()
    resolved: dict[str, str] = {}
    missing: list[str] = []

    for sym in symbols:
        if sym in sym_map:
            resolved[sym] = sym_map[sym]
        else:
            missing.append(sym)

    # API fallback for anything not in local cache
    for sym in missing:
        try:
            r = _get("/v2/instruments/search", {"query": sym, "exchanges": "NSE", "segments": "EQ", "records": 1})
            data = r.get("data", [])
            if data:
                resolved[sym] = data[0]["instrument_key"]
                sym_map[sym] = data[0]["instrument_key"]  # cache it
            else:
                resolved[sym] = None
                log.warning(f"Could not resolve symbol: {sym}")
        except Exception as e:
            resolved[sym] = None
            log.warning(f"Symbol resolution failed for {sym}: {e}")

    return resolved


# ---------------------------------------------------------------------------
# F&O universe  (derived from NSE cache, no hardcoding)
# ---------------------------------------------------------------------------

_fo_universe_cache: dict = {}   # {"__mtime__": float, "symbols": [...], "sym_key": {...}}


def _get_fo_universe() -> dict:
    """
    Derive the F&O universe from the NSE instruments cache.
    Returns {"symbols": [str], "sym_key": {symbol: underlying_key}}
    by collecting all unique underlying_symbol values from NSE_FO records
    whose underlying_type is EQUITY or INDEX.
    """
    _ensure_fresh("NSE")
    disk_mtime = _cache_path("NSE").stat().st_mtime
    if _fo_universe_cache.get("__mtime__") == disk_mtime:
        return _fo_universe_cache

    log.info("Building F&O universe from NSE cache")
    records = json.loads(_cache_path("NSE").read_text(encoding="utf-8"))
    seen: dict[str, str] = {}  # symbol → underlying_key
    for rec in records:
        if rec.get("segment") != "NSE_FO":
            continue
        utype = rec.get("underlying_type", "")
        if utype not in ("EQUITY", "INDEX"):
            continue
        sym = rec.get("underlying_symbol", "")
        ukey = rec.get("underlying_key", "")
        if sym and ukey and sym not in seen:
            seen[sym] = ukey

    result = {"__mtime__": disk_mtime, "symbols": sorted(seen.keys()), "sym_key": seen}
    _fo_universe_cache.clear()
    _fo_universe_cache.update(result)
    log.info(f"F&O universe: {len(seen)} underlyings")
    return _fo_universe_cache


def _resolve_universe(symbols: list[str] | None, universe: str | None) -> tuple[list[str], dict[str, str]]:
    """
    Given either a symbols list or universe="FO_ALL", return:
      (symbol_list, {symbol: instrument_key})
    """
    if universe and universe.upper() == "FO_ALL":
        fo = _get_fo_universe()
        sym_list = fo["symbols"]
        # For FO_ALL the underlying_key IS the instrument_key for index/equity
        return sym_list, fo["sym_key"]

    if not symbols:
        raise ValueError("Provide either 'symbols' list or universe='FO_ALL'")

    resolved = _resolve_symbols(symbols)
    return symbols, resolved


# ---------------------------------------------------------------------------
# Expiry resolution
# ---------------------------------------------------------------------------

def _resolve_expiry(expiry: str) -> str:
    """
    Resolve "current" / "current_week" / "current_month" / YYYY-MM-DD.
    "current" → nearest weekly expiry (Thursday). Adjusts for weekends.
    """
    if expiry in (None, "", "current", "current_week"):
        today = datetime.now(IST).date()
        # Find next Thursday (weekday 3)
        days_ahead = (3 - today.weekday()) % 7
        if days_ahead == 0:
            # Today is Thursday — use today if before market close, else next week
            if datetime.now(IST).hour < 15 or (datetime.now(IST).hour == 15 and datetime.now(IST).minute < 30):
                pass
            else:
                days_ahead = 7
        return (today + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    if expiry == "current_month":
        today = datetime.now(IST).date()
        # Last Thursday of current month
        next_month = today.replace(day=28) + timedelta(days=4)
        last_day = next_month - timedelta(days=next_month.day)
        days_back = (last_day.weekday() - 3) % 7
        return (last_day - timedelta(days=days_back)).strftime("%Y-%m-%d")

    return expiry  # already YYYY-MM-DD


# ---------------------------------------------------------------------------
# Technical indicator helpers
# ---------------------------------------------------------------------------

def _compute_rsi(closes: list[float], period: int = 14) -> float | None:
    if len(closes) < period + 1:
        return None
    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100 - (100 / (1 + avg_gain / avg_loss)), 2)


def _compute_atr(candles: list, period: int = 14) -> float | None:
    if len(candles) < period + 1:
        return None
    trs = []
    for i in range(1, len(candles)):
        h, l, prev_c = candles[i][2], candles[i][3], candles[i - 1][4]
        trs.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return round(atr, 2)


# ---------------------------------------------------------------------------
# 0. Instrument Search  (still useful for single-symbol deep lookup)
# ---------------------------------------------------------------------------

@mcp.tool()
def search_instrument(
    query: str,
    exchanges: str = "ALL",
    segments: str = "ALL",
    instrument_types: str = "",
    expiry: str = "",
    atm_offset: int = None,
    page_number: int = 1,
    records: int = 10,
) -> dict:
    """
    Resolve a human symbol, name, or ISIN to an instrument_key.
    Use for single-symbol lookups or option expiry/ATM queries.
    For bulk price/technical data use get_ohlc_quotes or get_bulk_*_technicals
    with symbols — those resolve internally and you never need the instrument_key.

    query: symbol (RELIANCE), name, ISIN, or symbol+strike (NIFTY 24000).
    exchanges: ALL, NSE, MCX, Global — comma-separated, no spaces.
    segments: ALL, EQ, FO, CURR, COMM, INDEX, OPT, FUT — comma-separated.
    instrument_types: CE, PE, FUT, EQ — comma-separated.
    expiry: current_week, near_week, next_week, current_month, next_month, or YYYY-MM-DD.
    atm_offset: 0=ATM, positive=above, negative=below ATM.
    """
    if expiry or atm_offset is not None:
        return _api_search(query, exchanges, segments, instrument_types, expiry, atm_offset, page_number, records)

    exc_list = [e.strip() for e in exchanges.split(",")] if exchanges != "ALL" else list(INSTRUMENT_URLS.keys())
    seg_filter = segments if segments != "ALL" else ""
    type_filter = instrument_types.split(",")[0] if instrument_types else ""

    q = query.lower().strip()
    results, seen = [], set()
    for exchange in exc_list:
        if exchange not in INSTRUMENT_URLS:
            continue
        idx = _get_index(exchange)
        for key, recs in idx.items():
            if key == "__mtime__" or q not in key:
                continue
            for rec in recs:
                ikey = rec.get("instrument_key", "")
                if ikey in seen:
                    continue
                if seg_filter and seg_filter.upper() not in rec.get("segment", "").upper():
                    continue
                if type_filter and rec.get("instrument_type", "").upper() != type_filter.upper():
                    continue
                seen.add(ikey)
                results.append(rec)
                if len(results) >= records:
                    break
    if results:
        return {"status": "success", "source": "local_cache", "data": results}
    return _api_search(query, exchanges, segments, instrument_types, expiry, atm_offset, page_number, records)


def _api_search(query, exchanges, segments, instrument_types, expiry, atm_offset, page_number, records):
    params = {"query": query, "exchanges": exchanges, "segments": segments,
              "page_number": page_number, "records": records}
    if instrument_types:
        params["instrument_types"] = instrument_types
    if expiry:
        params["expiry"] = expiry
    if atm_offset is not None:
        params["atm_offset"] = atm_offset
    result = _get("/v2/instruments/search", params)
    result["source"] = "api"
    return result


# ---------------------------------------------------------------------------
# Cache management
# ---------------------------------------------------------------------------

@mcp.tool()
def refresh_instruments(exchanges: str = "NSE,MCX,Global") -> dict:
    """
    Force-download the latest instrument files from Upstox and rebuild all indexes.
    Use if the cache is outdated mid-day (e.g. new expiry added).

    exchanges: comma-separated — NSE, MCX, Global. Default: all three.
    """
    results = {}
    for exchange in [e.strip() for e in exchanges.split(",")]:
        if exchange not in INSTRUMENT_URLS:
            results[exchange] = f"Unknown. Valid: {list(INSTRUMENT_URLS.keys())}"
            continue
        try:
            p = _cache_path(exchange)
            if p.exists():
                os.utime(p, (0, 0))
            count = _download_exchange(exchange)
            results[exchange] = f"OK — {count:,} instruments"
        except Exception as e:
            results[exchange] = f"ERROR: {e}"
    return results


@mcp.tool()
def cache_status() -> dict:
    """
    Show age, record count, and freshness of each locally cached instrument file.
    Also shows F&O universe size if NSE cache is present.
    """
    now = datetime.now(IST)
    status = {}
    for exchange in INSTRUMENT_URLS:
        path = _cache_path(exchange)
        if not path.exists():
            status[exchange] = "not cached — will download on next market tool call"
            continue
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=IST)
        age_min = int((now - mtime).total_seconds() / 60)
        try:
            count = len(json.loads(path.read_text()))
        except Exception:
            count = -1
        status[exchange] = {
            "cached_at": mtime.strftime("%Y-%m-%d %H:%M IST"),
            "age_minutes": age_min,
            "records": f"{count:,}",
            "stale": _is_stale(exchange),
        }
    if not _is_stale("NSE"):
        fo = _get_fo_universe()
        status["fo_universe_size"] = len(fo["symbols"])
    return status


# ---------------------------------------------------------------------------
# 1. get_ohlc_quotes — slim bulk snapshot, symbols in / symbols out
# ---------------------------------------------------------------------------

@mcp.tool()
def get_ohlc_quotes(
    symbols: list[str] | None = None,
    universe: str | None = None,
    interval: str = "1d",
) -> dict:
    """
    Bulk OHLC snapshot. Returns slim output: last_price, prev_close, change_pct, volume.

    Pass either:
      symbols: ["HDFCBANK", "RELIANCE", "BAJAJ-AUTO", "M&M"]
      universe: "FO_ALL"  — scans all ~212 F&O stocks at once

    interval: 1d (daily), I1 (1-min live), I30 (30-min live).
    Output is keyed by symbol name, not instrument_key.
    """
    sym_list, sym_key = _resolve_universe(symbols, universe)
    valid = {s: k for s, k in sym_key.items() if k and s in sym_list}

    # Upstox accepts up to 500 per call — batch if FO_ALL
    all_data: dict = {}
    keys = list(valid.values())
    sym_by_key = {v: k for k, v in valid.items()}

    for i in range(0, len(keys), 500):
        batch = keys[i:i+500]
        raw = _get("/v3/market-quote/ohlc", {
            "instrument_key": ",".join(batch),
            "interval": interval,
        })
        for ticker, d in raw.get("data", {}).items():
            ikey = d.get("instrument_token", "")
            sym = sym_by_key.get(ikey, ticker.split(":")[-1])
            live = d.get("live_ohlc") or {}
            prev = d.get("prev_ohlc") or {}
            last = d.get("last_price") or live.get("close") or 0
            prev_close = prev.get("close") or 0
            change_pct = round((last - prev_close) / prev_close * 100, 2) if prev_close else None
            all_data[sym] = {
                "last_price": last,
                "prev_close": prev_close,
                "change_pct": change_pct,
                "volume": live.get("volume") or prev.get("volume") or 0,
            }

    return {"status": "success", "count": len(all_data), "data": all_data}


# ---------------------------------------------------------------------------
# 2. get_bulk_intraday_technicals — parallel fetch + compute, symbols in/out
# ---------------------------------------------------------------------------

@mcp.tool()
def get_bulk_intraday_technicals(
    symbols: list[str],
    interval: int = 5,
) -> dict:
    """
    Fetch intraday candles for multiple stocks in parallel and return
    pre-computed technicals. Claude gets 8 numbers per stock instead of
    hundreds of raw candle rows.

    symbols: ["HDFCBANK", "RELIANCE", "ICICIBANK", "BAJAJ-AUTO", "M&M"]
    interval: candle size in minutes — 1, 3, 5, 10, 15, 30. Default 5.

    Returns per symbol: current_price, vwap, rsi_14, vol_ratio,
                        day_high, day_low, candle_count, above_vwap.
    """
    resolved = _resolve_symbols(symbols)

    def _fetch_one(sym: str) -> tuple[str, dict]:
        ikey = resolved.get(sym)
        if not ikey:
            return sym, {"error": "symbol not resolved"}
        try:
            encoded = ikey.replace("|", "%7C")
            raw = _get(f"/v3/historical-candle/intraday/{encoded}/minutes/{interval}")
            candles = raw.get("data", {}).get("candles", [])
            if not candles:
                return sym, {"error": "no candles"}

            highs  = [c[2] for c in candles]
            lows   = [c[3] for c in candles]
            closes = [c[4] for c in candles]
            vols   = [c[5] for c in candles]

            tp_vol = sum(((highs[i] + lows[i] + closes[i]) / 3) * vols[i] for i in range(len(candles)))
            total_vol = sum(vols)
            vwap = round(tp_vol / total_vol, 2) if total_vol else None

            avg_vol_20 = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else (sum(vols[:-1]) / max(len(vols) - 1, 1))
            vol_ratio = round(vols[-1] / avg_vol_20, 2) if avg_vol_20 else None

            current_price = closes[-1]
            return sym, {
                "current_price": current_price,
                "vwap": vwap,
                "rsi_14": _compute_rsi(closes),
                "vol_ratio": vol_ratio,
                "day_high": max(highs),
                "day_low": min(lows),
                "candle_count": len(candles),
                "above_vwap": current_price > vwap if vwap else None,
            }
        except Exception as e:
            return sym, {"error": str(e)}

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        for sym, data in pool.map(_fetch_one, symbols):
            results[sym] = data

    return {"status": "success", "data": results}


# ---------------------------------------------------------------------------
# 3. get_bulk_historical_technicals — parallel fetch + compute, symbols in/out
# ---------------------------------------------------------------------------

@mcp.tool()
def get_bulk_historical_technicals(
    symbols: list[str] | None = None,
    universe: str | None = None,
    days: int = 30,
) -> dict:
    """
    Fetch daily candles for multiple stocks in parallel and return
    pre-computed technicals. Replaces N individual candle calls.

    Pass either:
      symbols: ["HDFCBANK", "RELIANCE"]
      universe: "FO_ALL"  — all F&O stocks at once

    days: number of trading days of history (default 30, min 20).

    Returns per symbol: avg_volume_20d, atr_14d, rsi_14d.
    """
    sym_list, sym_key = _resolve_universe(symbols, universe)
    to_date = datetime.now(IST).strftime("%Y-%m-%d")
    from_date = (datetime.now(IST) - timedelta(days=days + 30)).strftime("%Y-%m-%d")

    def _fetch_one(sym: str) -> tuple[str, dict]:
        ikey = sym_key.get(sym)
        if not ikey:
            return sym, {"error": "symbol not resolved"}
        try:
            encoded = ikey.replace("|", "%7C")
            raw = _get(f"/v3/historical-candle/{encoded}/days/1/{to_date}/{from_date}")
            candles = list(reversed(raw.get("data", {}).get("candles", [])))[-days:]
            if len(candles) < 15:
                return sym, {"error": f"insufficient data ({len(candles)} candles)"}
            closes = [c[4] for c in candles]
            vols   = [c[5] for c in candles]
            return sym, {
                "avg_volume_20d": int(sum(vols[-20:]) / min(20, len(vols))),
                "atr_14d": _compute_atr(candles),
                "rsi_14d": _compute_rsi(closes),
            }
        except Exception as e:
            return sym, {"error": str(e)}

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        for sym, data in pool.map(_fetch_one, sym_list):
            results[sym] = data

    return {"status": "success", "count": len(results), "data": results}


# ---------------------------------------------------------------------------
# 4. get_put_call_ratio — bulk, symbols in, sentiment out
# ---------------------------------------------------------------------------

@mcp.tool()
def get_put_call_ratio(
    symbols: list[str],
    expiry: str = "current",
    bucket_interval: int = 60,
) -> dict:
    """
    Fetch PCR for multiple underlyings in parallel. Returns sentiment labels,
    not raw OI numbers.

    symbols: ["HDFCBANK", "RELIANCE", "NIFTY", "BANKNIFTY"]
             Use index names (NIFTY, BANKNIFTY) for index PCR.
    expiry: "current" (nearest weekly Thursday), "current_month", or YYYY-MM-DD.
    bucket_interval: intraday bucket size in minutes (default 60).

    Sentiment: PCR > 1.2 = BULLISH, < 0.8 = BEARISH, else NEUTRAL.
    """
    resolved = _resolve_symbols(symbols)
    expiry_date = _resolve_expiry(expiry)
    today = datetime.now(IST).strftime("%Y-%m-%d")

    def _fetch_one(sym: str) -> tuple[str, dict]:
        ikey = resolved.get(sym)
        if not ikey:
            return sym, {"error": "symbol not resolved"}
        try:
            raw = _get("/v2/market/pcr", {
                "instrument_key": ikey,
                "expiry": expiry_date,
                "date": today,
                "bucket_interval": bucket_interval,
            })
            pcr_val = raw.get("data", {}).get("pcr")
            if pcr_val is None:
                return sym, {"error": "no PCR data"}
            pcr_val = round(float(pcr_val), 2)
            sentiment = "BULLISH" if pcr_val > 1.2 else "BEARISH" if pcr_val < 0.8 else "NEUTRAL"
            return sym, {"pcr": pcr_val, "sentiment": sentiment, "expiry": expiry_date}
        except Exception as e:
            return sym, {"error": str(e)}

    results = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        for sym, data in pool.map(_fetch_one, symbols):
            results[sym] = data

    return {"status": "success", "data": results}


# ---------------------------------------------------------------------------
# 5. get_intraday_candles — single instrument raw candles (advanced use)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_intraday_candles(
    instrument_key: str,
    unit: str = "minutes",
    interval: int = 5,
) -> dict:
    """
    Raw OHLCV candles for the current trading day — single instrument.
    For multiple instruments use get_bulk_intraday_technicals (symbols-based).

    instrument_key: e.g. NSE_EQ|INE002A01018 (from search_instrument).
    unit: minutes (1-300), hours (1-5), days (1).
    interval: numeric value matching the unit range.
    """
    key = instrument_key.replace("|", "%7C")
    return _get(f"/v3/historical-candle/intraday/{key}/{unit}/{interval}")


# ---------------------------------------------------------------------------
# 6. get_historical_candles — single instrument raw candles (advanced use)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_historical_candles(
    instrument_key: str,
    unit: str,
    interval: int,
    to_date: str,
    from_date: str,
) -> dict:
    """
    Raw historical OHLCV candles — single instrument.
    For multiple instruments use get_bulk_historical_technicals (symbols-based).

    instrument_key: from search_instrument.
    unit: minutes, hours, days, weeks, months.
    to_date / from_date: YYYY-MM-DD.
    """
    key = instrument_key.replace("|", "%7C")
    return _get(f"/v3/historical-candle/{key}/{unit}/{interval}/{to_date}/{from_date}")


# ---------------------------------------------------------------------------
# 7. get_full_quotes — with depth, for order routing
# ---------------------------------------------------------------------------

@mcp.tool()
def get_full_quotes(instrument_keys: list[str]) -> dict:
    """
    Full snapshot with top-5 bid/ask depth, OI, circuit limits.
    Use only when depth data is needed. For screening use get_ohlc_quotes.
    instrument_keys: from search_instrument (up to 500).
    """
    return _get("/v2/market-quote/quotes", {"instrument_key": ",".join(instrument_keys)})


# ---------------------------------------------------------------------------
# 8. get_option_chain
# ---------------------------------------------------------------------------

@mcp.tool()
def get_option_chain(instrument_key: str, expiry_date: str) -> dict:
    """
    Full strike-wise call & put option chain with greeks and IV.

    instrument_key: underlying key (from search_instrument).
    expiry_date: YYYY-MM-DD.
    """
    return _get("/v2/option/chain", {"instrument_key": instrument_key, "expiry_date": expiry_date})


# ---------------------------------------------------------------------------
# 9. get_open_interest
# ---------------------------------------------------------------------------

@mcp.tool()
def get_open_interest(instrument_key: str, expiry: str, date: str) -> dict:
    """
    OI per strike for a given underlying, expiry, and date.

    instrument_key: underlying key. expiry / date: YYYY-MM-DD.
    """
    return _get("/v2/market/oi", {"instrument_key": instrument_key, "expiry": expiry, "date": date})


# ---------------------------------------------------------------------------
# 10. get_oi_change
# ---------------------------------------------------------------------------

@mcp.tool()
def get_oi_change(instrument_key: str, expiry: str, date: str, interval: int = 1) -> dict:
    """
    Change in OI per strike — positive = build-up, negative = unwinding.

    instrument_key: underlying key. expiry / date: YYYY-MM-DD.
    interval: number of days for the OI delta.
    """
    return _get("/v2/market/change-oi", {
        "instrument_key": instrument_key, "expiry": expiry, "date": date, "interval": interval,
    })


# ---------------------------------------------------------------------------
# 11. get_max_pain
# ---------------------------------------------------------------------------

@mcp.tool()
def get_max_pain(instrument_key: str, expiry: str, date: str, bucket_interval: int = 60) -> dict:
    """
    Daily max-pain strike and intraday max-pain movement.

    instrument_key: underlying key. expiry / date: YYYY-MM-DD.
    """
    return _get("/v2/market/max-pain", {
        "instrument_key": instrument_key, "expiry": expiry, "date": date, "bucket_interval": bucket_interval,
    })


# ---------------------------------------------------------------------------
# 12. get_futures_smartlist
# ---------------------------------------------------------------------------

@mcp.tool()
def get_futures_smartlist(asset_type: str, category: str, page_number: int = 1, page_size: int = 20) -> dict:
    """
    Ranked F&O screener lists.

    asset_type: INDEX, STOCK, COMMODITY.
    category for INDEX/STOCK: TOP_TRADED, MOST_ACTIVE, OI_GAINERS, OI_LOSERS,
        PRICE_GAINERS, PRICE_LOSERS, PREMIUM, DISCOUNT.
    category for COMMODITY: TOP_TRADED, MOST_ACTIVE, OI_GAINERS, OI_LOSERS.
    """
    return _get("/v2/market/smartlist/futures", {
        "asset_type": asset_type, "category": category,
        "page_number": page_number, "page_size": page_size,
    })


# ---------------------------------------------------------------------------
# 13. get_market_holidays
# ---------------------------------------------------------------------------

@mcp.tool()
def get_market_holidays(date: str = "") -> dict:
    """
    Market holiday calendar. Check before scheduling tasks on Indian markets.
    date: YYYY-MM-DD for a specific date, or empty for the full year.
    """
    path = f"/v2/market/holidays/{date}" if date else "/v2/market/holidays"
    return _get(path)


# ---------------------------------------------------------------------------
# 14–18. Fundamentals
# ---------------------------------------------------------------------------

@mcp.tool()
def get_balance_sheet(isin: str, report_type: str = "consolidated", full_statement: bool = True) -> dict:
    """Balance sheet history (INR Crore). isin from search_instrument."""
    return _get(f"/v2/fundamentals/{isin}/balance-sheet", {"type": report_type, "fs": str(full_statement).lower()})


@mcp.tool()
def get_income_statement(
    isin: str, report_type: str = "consolidated", time_period: str = "yearly", full_statement: bool = True
) -> dict:
    """Income statement history (INR Crore). time_period: yearly or quarterly."""
    return _get(f"/v2/fundamentals/{isin}/income-statement", {
        "type": report_type, "time_period": time_period, "fs": str(full_statement).lower(),
    })


@mcp.tool()
def get_cash_flow(isin: str, report_type: str = "consolidated", full_statement: bool = True) -> dict:
    """Cash flow statement history (INR Crore)."""
    return _get(f"/v2/fundamentals/{isin}/cash-flow", {"type": report_type, "fs": str(full_statement).lower()})


@mcp.tool()
def get_share_holdings(isin: str) -> dict:
    """Quarterly shareholding pattern (promoters, FII, mutual funds, retail, other DII)."""
    return _get(f"/v2/fundamentals/{isin}/share-holdings")


@mcp.tool()
def get_competitors(isin: str) -> dict:
    """Peer/competitor list with instrument_keys for fan-out analysis."""
    return _get(f"/v2/fundamentals/{isin}/competitors")


# ---------------------------------------------------------------------------
# Account — Profile & Funds
# ---------------------------------------------------------------------------

@mcp.tool()
def get_profile() -> dict:
    """Fetch authenticated user's profile: name, email, enabled exchanges, order types, products."""
    return _account_get("/v2/user/profile")


@mcp.tool()
def get_funds_margin(segment: str = "") -> dict:
    """
    Detailed balance breakdown: cash available, pledge margin, margin in use, unsettled amounts.

    segment: NSE, BSE, MCX, BFO, NCD — leave empty for all segments.
    Uses the V3 endpoint with full breakdown.
    """
    params = {}
    if segment:
        params["segment"] = segment
    return _account_get("/v3/user/get-funds-and-margin", params=params)


# ---------------------------------------------------------------------------
# Portfolio — Positions & Holdings
# ---------------------------------------------------------------------------

@mcp.tool()
def get_positions() -> dict:
    """
    Current-day trading positions with real-time P&L, quantity, buy average, and margin details
    for all active intraday and overnight positions.
    """
    return _account_get("/v2/portfolio/short-term-positions")


@mcp.tool()
def get_holdings() -> dict:
    """
    Long-term demat holdings: quantity, average price, last price, P&L, ISIN, exchange.
    """
    return _account_get("/v2/portfolio/long-term-holdings")


@mcp.tool()
def get_mtf_positions() -> dict:
    """
    Margin Trade Funding (MTF) positions with quantity, price, and interest details.
    """
    return _account_get("/v2/portfolio/mtf-positions")


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------

@mcp.tool()
def get_order_book() -> dict:
    """
    Complete order book for the trading day — all open, pending, filled, and rejected orders.
    """
    return _account_get("/v2/order/retrieve-all")


@mcp.tool()
def get_order_details(order_id: str) -> dict:
    """
    Latest status and details for a specific order.

    order_id: the order ID returned when the order was placed.
    """
    return _account_get("/v2/order/details", params={"order_id": order_id})


@mcp.tool()
def get_order_history(order_id: str = "", tag: str = "") -> dict:
    """
    Full state history of an order — all transitions from placement to execution/rejection.

    Provide either order_id or tag (custom tag set at placement).
    """
    params = {}
    if order_id:
        params["order_id"] = order_id
    if tag:
        params["tag"] = tag
    return _account_get("/v2/order/history", params=params)


@mcp.tool()
def get_order_trades(order_id: str) -> dict:
    """
    All trades (fills) executed for a specific order: fill price, quantity, trade ID.

    order_id: the order ID to query.
    """
    return _account_get("/v2/order/trades/get-trades-for-order", params={"order_id": order_id})


@mcp.tool()
def get_trades() -> dict:
    """
    All executed trades for the current trading day across all orders.
    """
    return _account_get("/v2/order/trades/get-trades-for-day")


@mcp.tool()
def place_order(
    instrument_token: str,
    transaction_type: str,
    quantity: int,
    order_type: str,
    product: str,
    price: float = 0,
    trigger_price: float = 0,
    validity: str = "DAY",
    disclosed_quantity: int = 0,
    is_amo: bool = False,
    tag: str = "",
    market_protection: int = -1,
) -> dict:
    """
    Place a single trading order.

    instrument_token: e.g. NSE_EQ|INE002A01018 (from search_instrument).
    transaction_type: BUY or SELL.
    quantity: number of units (or lots for commodity).
    order_type: MARKET, LIMIT, SL, SL-M.
    product: I (intraday), D (delivery), MTF.
    price: limit price — set 0 for MARKET / SL-M orders.
    trigger_price: required for SL and SL-M orders.
    validity: DAY (default) or IOC.
    disclosed_quantity: quantity to show in market depth (0 = full).
    is_amo: true for After Market Order (auto-detected in practice).
    tag: optional custom label for this order.
    market_protection: -1 = auto, 0 = none, 1–25 = custom % (MARKET/SL-M only).
    """
    body = {
        "instrument_token": instrument_token,
        "transaction_type": transaction_type.upper(),
        "quantity": quantity,
        "order_type": order_type.upper(),
        "product": product.upper(),
        "price": price,
        "trigger_price": trigger_price,
        "validity": validity.upper(),
        "disclosed_quantity": disclosed_quantity,
        "is_amo": is_amo,
        "market_protection": market_protection,
    }
    if tag:
        body["tag"] = tag
    return _account_post("/v2/order/place", body)


@mcp.tool()
def modify_order(
    order_id: str,
    quantity: int = None,
    price: float = None,
    order_type: str = None,
    validity: str = None,
    trigger_price: float = None,
    disclosed_quantity: int = None,
) -> dict:
    """
    Modify a pending or open order. Only pass the fields you want to change.

    order_id: the order to modify.
    quantity: new quantity.
    price: new limit price.
    order_type: MARKET, LIMIT, SL, SL-M.
    validity: DAY or IOC.
    trigger_price: new trigger price for SL/SL-M.
    disclosed_quantity: new disclosed quantity.
    """
    body: dict = {"order_id": order_id}
    if quantity is not None:
        body["quantity"] = quantity
    if price is not None:
        body["price"] = price
    if order_type is not None:
        body["order_type"] = order_type.upper()
    if validity is not None:
        body["validity"] = validity.upper()
    if trigger_price is not None:
        body["trigger_price"] = trigger_price
    if disclosed_quantity is not None:
        body["disclosed_quantity"] = disclosed_quantity
    return _account_put("/v2/order/modify", body)


@mcp.tool()
def cancel_order(order_id: str) -> dict:
    """
    Cancel a pending or open order.

    order_id: the order ID to cancel.
    """
    return _account_delete("/v2/order/cancel", params={"order_id": order_id})


# ---------------------------------------------------------------------------
# P&L and Trade Charges
# ---------------------------------------------------------------------------

@mcp.tool()
def get_pnl_report(
    segment: str,
    financial_year: str,
    from_date: str = "",
    to_date: str = "",
    page_number: int = 1,
    page_size: int = 50,
) -> dict:
    """
    Trade-wise realised P&L report: buy/sell dates, prices, quantities, amounts per trade.

    segment: EQ (equity), FO (futures & options), COM (commodity), CD (currency derivatives).
    financial_year: last 2 digits of each year concatenated — e.g. "2425" for FY2024-25.
    from_date / to_date: optional date range in dd-mm-yyyy format (must be within the FY).
    page_number: starts at 1.
    page_size: max records per page (check metadata for total).
    """
    params: dict = {
        "segment": segment,
        "financial_year": financial_year,
        "page_number": page_number,
        "page_size": page_size,
    }
    if from_date:
        params["from_date"] = from_date
    if to_date:
        params["to_date"] = to_date
    return _account_get("/v2/trade/profit-loss/data", params=params)


@mcp.tool()
def get_trade_charges(
    segment: str,
    financial_year: str,
    from_date: str = "",
    to_date: str = "",
    page_number: int = 1,
    page_size: int = 50,
) -> dict:
    """
    Brokerage and statutory charges breakdown per trade.

    segment: EQ, FO, COM, CD.
    financial_year: e.g. "2425" for FY2024-25.
    from_date / to_date: optional date range in dd-mm-yyyy format.
    """
    params: dict = {
        "segment": segment,
        "financial_year": financial_year,
        "page_number": page_number,
        "page_size": page_size,
    }
    if from_date:
        params["from_date"] = from_date
    if to_date:
        params["to_date"] = to_date
    return _account_get("/v2/trade/profit-loss/charges", params=params)


# ---------------------------------------------------------------------------
# Mutual Funds
# ---------------------------------------------------------------------------

@mcp.tool()
def get_mf_holdings() -> dict:
    """Current mutual fund holdings: scheme name, units, NAV, current value, P&L."""
    return _account_get("/v2/mf/holdings")


@mcp.tool()
def get_mf_order_book(status: str = "") -> dict:
    """
    Mutual fund order book.

    status: optional filter — open, cancelled, completed.
    """
    params = {}
    if status:
        params["status"] = status
    return _account_get("/v2/mf/orders", params=params)


@mcp.tool()
def get_mf_order_details(order_id: str) -> dict:
    """Details for a specific mutual fund order."""
    return _account_get("/v2/mf/order", params={"order_id": order_id})


@mcp.tool()
def get_mf_sips() -> dict:
    """List of all active and paused SIPs with scheme details, amount, frequency, and next date."""
    return _account_get("/v2/mf/sip")


# ---------------------------------------------------------------------------
# Margin
# ---------------------------------------------------------------------------

@mcp.tool()
def get_margin(
    instruments: list[dict],
) -> dict:
    """
    Calculate margin required for one or more potential orders (up to 20 instruments).
    Returns span, exposure, equity, net_buy_premium, total_margin, and final_margin
    (after margin benefit).

    instruments: list of dicts, each with:
      instrument_key: e.g. "NSE_EQ|INE002A01018" (from search_instrument)
      quantity:       int — number of units / lots
      transaction_type: "BUY" or "SELL"
      product:        "I", "D", "CO", or "MTF"
      price:          float (optional) — limit price for more accurate margin calc

    Example:
      [{"instrument_key": "NSE_EQ|INE002A01018", "quantity": 10,
        "transaction_type": "BUY", "product": "D"}]
    """
    return _account_post("/v2/charges/margin", {"instruments": instruments}, base=BASE)


if __name__ == "__main__":
    mcp.run(transport="stdio")
