import os
import time
import asyncio
import random
import logging
from datetime import datetime, timedelta, date, time as dtime
from pathlib import Path
from typing import Dict, List, Any, Optional, Callable, TypeVar

import pytz
import pandas as pd
from fastapi import FastAPI
from fastapi.responses import JSONResponse, FileResponse
from kiteconnect import KiteConnect
from kiteconnect.exceptions import NetworkException, DataException

# ---------------------------
# LOGGING
# ---------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("intraday-boost")

# ---------------------------
# SECTORS / STOCKS
# ---------------------------
SECTOR_DEFINITIONS = {
    "METAL": ["ADANIENT","HINDALCO","JSWSTEEL","HINDZINC","APLAPOLLO","TATASTEEL","JINDALSTEL","VEDL","SAIL","NATIONALUM","NMDC"],
    "PSUS": ["BANKINDIA","PNB","INDIANB","SBIN","UNIONBANK","BANKBARODA","CANBK"],
    "REALTY": ["PHOENIXLTD","GODREJPROP","LODHA","OBEROIRLTY","DLF","PRESTIGE","NBCC","NCC"],
    "ENERGY": ["CGPOWER","RELIANCE","GMRAIRPORT","JSWENERGY","ONGC","POWERGRID","BLUESTARCO","COALINDIA","SUZLON","IREDA",
               "IOC","IGL","TATAPOWER","INOXWIND","MAZDOCK","PETRONET","SOLARINDS","ADANIGREEN","NTPC","OIL","BDL","BPCL",
               "NHPC","POWERINDIA","ADANIENSOL","HINDPETRO","TORNTPOWER"],
    "AUTO": ["BOSCHLTD","TIINDIA","HEROMOTOCO","M&M","EICHERMOT","EXIDEIND","BAJAJ-AUTO","ASHOKLEY","MARUTI","TITAGARH",
             "TVSMOTOR","MOTHERSON","SONACOMS","UNOMINDA","TMPV","BHARATFORG"],
    "IT": ["KAYNES","TATATECH","LTIM","CYIENT","MPHASIS","TCS","CAMS","OFSS","HFCL","TECHM","TATAELXSI","HCLTECH","WIPRO",
           "KPITTECH","COFORGE","PERSISTENT","INFY"],
    "PHARMA": ["CIPLA","ALKEM","BIOCON","DRREDDY","MANKIND","TORNTPHARM","ZYDUSLIFE","DIVISLAB","LUPIN","PPLPHARMA",
               "LAURUSLABS","FORTIS","AUROPHARMA","GLENMARK","SUNPHARMA"],
    "FMCG": ["ETERNAL","MARICO","NYKAA","NESTLEIND","VBL","COLPAL","HINDUNILVR","PATANJALI","DMART","DABUR","GODREJCP",
             "BRITANNIA","UNITDSPR","ITC","TATACONSUM","KALYANKJIL","SUPREMEIND"],
    "CEMENT": ["SHREECEM","DALBHARAT","AMBUJACEM","ULTRACEMCO"],
    "FINANCE": ["PNBHOUSING","BAJAJFINSV","ICICIPRULI","NUVAMA","HDFCLIFE","SAMMAANCAP","ANGELONE","RECLTD","BAJFINANCE","BSE",
                "MAXHEALTH","ICICIGI","HUDCO","CHOLAFIN","PFC","HDFCAMC","MUTHOOTFIN","PAYTM","JIOFIN","SHRIRAMFIN","SBICARD",
                "POLICYBZR","SBILIFE","LICHSGFIN","LICI","MANAPPURAM","IRFC","IIFL","CDSL"],
    "BANK": ["IDFCFIRSTB","FEDERALBNK","INDUSINDBK","HDFCBANK","SBIN","KOTAKBANK","AUBANK","CANBK","BANDHANBNK","RBLBANK","ICICIBANK","AXISBANK"],
    "NIFTY_50": ["ADANIENT","ADANIPORTS","APOLLOHOSP","ASIANPAINT","AXISBANK","BAJAJ-AUTO","BAJFINANCE","BAJAJFINSV","BEL","BHARTIARTL",
                "CIPLA","COALINDIA","DRREDDY","EICHERMOT","GRASIM","HCLTECH","HDFCBANK","HDFCLIFE","HINDALCO","HINDUNILVR",
                "ICICIBANK","INFY","INDIGO","ITC","JIOFIN","JSWSTEEL","KOTAKBANK","LT","M&M","MARUTI","MAXHEALTH","NESTLEIND","NTPC",
                "ONGC","POWERGRID","RELIANCE","SBILIFE","SHRIRAMFIN","SBIN","SUNPHARMA","TCS","TATACONSUM","TATASTEEL","TECHM","TITAN",
                "TRENT","ULTRACEMCO","WIPRO","TATAMOTORS","ETERNAL"],
    "MIDCAP": ["RVNL","MPHASIS","HINDPETRO","PAGEIND","POLYCAB","LUPIN","IDFCFIRSTB","CONCOR","CUMMINSIND","VOLTAS","BHARATFORG",
               "FEDERALBNK","INDHOTEL","COFORGE","ASHOKLEY","PERSISTENT","UPL","GODREJPROP","AUROPHARMA","AUBANK","ASTRAL","HDFCAMC",
               "JUBLFOOD","PIIND"],
}

# ---------------------------
# CONFIG
# ---------------------------
IST = pytz.timezone("Asia/Kolkata")

LOOKBACK_SESSIONS = int(os.getenv("LOOKBACK_SESSIONS", "20"))
REFRESH_EVERY_SEC = int(os.getenv("REFRESH_EVERY_SEC", "15"))
QUOTE_CHUNK_SIZE = int(os.getenv("QUOTE_CHUNK_SIZE", "120"))  # safer under load
QUOTE_CHUNK_SLEEP = float(os.getenv("QUOTE_CHUNK_SLEEP", "0.18"))

KITE_TIMEOUT_SEC = int(os.getenv("KITE_TIMEOUT_SEC", "15"))
KITE_RETRIES = int(os.getenv("KITE_RETRIES", "4"))
KITE_RETRY_BASE_SLEEP = float(os.getenv("KITE_RETRY_BASE_SLEEP", "0.5"))

KITE_API_KEY = os.getenv("KITE_API_KEY")
KITE_ACCESS_TOKEN = os.getenv("KITE_ACCESS_TOKEN")

MARKET_OPEN_HHMM = os.getenv("MARKET_OPEN_HHMM", "09:15")
MARKET_CLOSE_HHMM = os.getenv("MARKET_CLOSE_HHMM", "15:30")

HERE = Path(__file__).resolve().parent
INDEX_PATH = HERE / "index.html"

# ---------------------------
# APP + STATE
# ---------------------------
app = FastAPI(title="Intraday Boost (RFactor)")

kite: Optional[KiteConnect] = None
symbols: List[str] = []
missing_symbols: List[str] = []
symbol_to_token: Dict[str, int] = {}
stats_by_token: Dict[int, Dict[str, Optional[float]]] = {}
sym_to_sectors: Dict[str, List[str]] = {}

bootstrap_task: Optional[asyncio.Task] = None
seeding_task: Optional[asyncio.Task] = None
scanner_task: Optional[asyncio.Task] = None

# latest computed universe rows (used for sector drilldown)
universe_rows: List[Dict[str, Any]] = []

# tick metrics (scan cycles while market OPEN)
tick_count: int = 0
tick_t0: Optional[float] = None
tick_prev: Optional[float] = None
ticks_per_sec_avg: float = 0.0
ticks_per_sec_inst: Optional[float] = None

# cache seeding metrics
cache_total: int = 0
cache_done: int = 0
cache_status: str = "NOT_STARTED"  # NOT_STARTED | BOOTSTRAP | SEEDING | READY | ERROR
cache_current_symbol: Optional[str] = None

last_snapshot: Dict[str, Any] = {
    "timestamp": None,
    "universe_count": 0,
    "gainers": [],
    "losers": [],
    "movers": [],
    "sectors": [],
    "missing_symbols": [],
    "market": {},
    "tick_count": 0,
    "ticks_per_sec_avg": 0.0,
    "ticks_per_sec_inst": None,
    "cache_total": 0,
    "cache_done": 0,
    "cache_status": "NOT_STARTED",
    "cache_current_symbol": None,
    "refresh_every_sec": REFRESH_EVERY_SEC,
    "lookback_sessions": LOOKBACK_SESSIONS,
    "note": "Waiting for backend bootstrap…",
}

# ---------------------------
# HELPERS
# ---------------------------
def now_ist() -> datetime:
    return datetime.now(IST)

def now_ist_str() -> str:
    return now_ist().strftime("%Y-%m-%d %H:%M:%S")

def unique_symbols(sector_defs: dict) -> List[str]:
    s = set()
    for _, lst in sector_defs.items():
        for sym in lst:
            s.add(sym.strip().upper())
    return sorted(s)

def build_sym_to_sectors(sector_defs: dict) -> Dict[str, List[str]]:
    m: Dict[str, List[str]] = {}
    for sector, lst in sector_defs.items():
        for sym in set(x.strip().upper() for x in lst):
            m.setdefault(sym, []).append(sector)
    return m

def chunked(lst: List[str], n: int) -> List[List[str]]:
    return [lst[i:i+n] for i in range(0, len(lst), n)]

T = TypeVar("T")

def kite_retry(fn: Callable[..., T], *args, **kwargs) -> T:
    last_exc: Optional[Exception] = None
    for i in range(KITE_RETRIES):
        try:
            return fn(*args, **kwargs)
        except (NetworkException, DataException) as e:
            last_exc = e
            sleep_s = KITE_RETRY_BASE_SLEEP * (2 ** i) + random.random() * 0.2
            log.warning("Kite transient error (%s) retry %d/%d sleep=%.2fs",
                        repr(e), i + 1, KITE_RETRIES, sleep_s)
            time.sleep(sleep_s)
        except Exception as e:
            last_exc = e
            sleep_s = KITE_RETRY_BASE_SLEEP * (2 ** i) + random.random() * 0.2
            log.warning("Kite unexpected error (%s) retry %d/%d sleep=%.2fs",
                        repr(e), i + 1, KITE_RETRIES, sleep_s)
            time.sleep(sleep_s)
    assert last_exc is not None
    raise last_exc

def load_instrument_map(k: KiteConnect, exchange: str = "NSE") -> Dict[str, int]:
    inst = kite_retry(k.instruments, exchange)
    return {row["tradingsymbol"]: row["instrument_token"] for row in inst}

# ---- Market session helpers (Mon–Fri only; no holiday calendar) ----
def _parse_hhmm(x: str) -> dtime:
    h, m = [int(p) for p in x.strip().split(":")]
    return dtime(hour=h, minute=m)

MARKET_OPEN_T = _parse_hhmm(MARKET_OPEN_HHMM)
MARKET_CLOSE_T = _parse_hhmm(MARKET_CLOSE_HHMM)

def is_trading_day(d: date) -> bool:
    return d.weekday() < 5

def next_trading_day(d: date) -> date:
    nd = d + timedelta(days=1)
    while not is_trading_day(nd):
        nd += timedelta(days=1)
    return nd

def session_bounds(d: date):
    open_dt = IST.localize(datetime.combine(d, MARKET_OPEN_T))
    close_dt = IST.localize(datetime.combine(d, MARKET_CLOSE_T))
    return open_dt, close_dt

def market_status(now: datetime) -> Dict[str, Any]:
    d = now.date()

    if not is_trading_day(d):
        nd = next_trading_day(d)
        nopen, nclose = session_bounds(nd)
        return {
            "is_open": False,
            "state": "CLOSED",
            "open": nopen.strftime("%Y-%m-%d %H:%M:%S"),
            "close": nclose.strftime("%Y-%m-%d %H:%M:%S"),
            "open_hhmm": MARKET_OPEN_HHMM,
            "close_hhmm": MARKET_CLOSE_HHMM,
        }

    open_dt, close_dt = session_bounds(d)
    if now < open_dt:
        return {
            "is_open": False,
            "state": "PREOPEN",
            "open": open_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "close": close_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "open_hhmm": MARKET_OPEN_HHMM,
            "close_hhmm": MARKET_CLOSE_HHMM,
        }
    if now >= close_dt:
        nd = next_trading_day(d)
        nopen, nclose = session_bounds(nd)
        return {
            "is_open": False,
            "state": "CLOSED",
            "open": nopen.strftime("%Y-%m-%d %H:%M:%S"),
            "close": nclose.strftime("%Y-%m-%d %H:%M:%S"),
            "open_hhmm": MARKET_OPEN_HHMM,
            "close_hhmm": MARKET_CLOSE_HHMM,
        }

    return {
        "is_open": True,
        "state": "OPEN",
        "open": open_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "close": close_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "open_hhmm": MARKET_OPEN_HHMM,
        "close_hhmm": MARKET_CLOSE_HHMM,
    }

def _snapshot_meta(mkt: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "missing_symbols": missing_symbols,
        "market": mkt,
        "tick_count": tick_count,
        "ticks_per_sec_avg": ticks_per_sec_avg,
        "ticks_per_sec_inst": ticks_per_sec_inst,
        "cache_total": cache_total,
        "cache_done": cache_done,
        "cache_status": cache_status,
        "cache_current_symbol": cache_current_symbol,
        "refresh_every_sec": REFRESH_EVERY_SEC,
        "lookback_sessions": LOOKBACK_SESSIONS,
    }

def set_snapshot(note: str, **fields):
    global last_snapshot
    mkt = market_status(now_ist())
    base = {
        "timestamp": now_ist_str(),
        "universe_count": len(symbols),
        "gainers": [],
        "losers": [],
        "movers": [],
        "sectors": [],
        **_snapshot_meta(mkt),
        "note": note,
    }
    base.update(fields)
    last_snapshot = base

# ---- Stats & scoring ----
def get_20d_stats(k: KiteConnect, token: int, asof: date) -> Dict[str, Optional[float]]:
    """
    Computes 20-session averages using daily candles.
    Caches result in stats_by_token. On failure, caches None stats
    to avoid repeated historical-data calls.
    """
    if token in stats_by_token:
        return stats_by_token[token]

    to_dt = datetime.combine(asof - timedelta(days=1), datetime.min.time())
    from_dt = to_dt - timedelta(days=90)

    try:
        candles = kite_retry(
            k.historical_data,
            instrument_token=token,
            from_date=from_dt,
            to_date=to_dt,
            interval="day",
            continuous=False,
            oi=False
        )
    except Exception as e:
        st = {"avg_vol_20": None, "avg_range_20": None, "avg_abs_ret_20": None}
        stats_by_token[token] = st
        log.warning("historical_data failed token=%s err=%s", token, repr(e))
        return st

    df = pd.DataFrame(candles)
    if df.empty or len(df) < LOOKBACK_SESSIONS + 1:
        st = {"avg_vol_20": None, "avg_range_20": None, "avg_abs_ret_20": None}
        stats_by_token[token] = st
        return st

    df = df.tail(LOOKBACK_SESSIONS + 1).copy()
    df["range"] = (df["high"] - df["low"]).astype(float)
    df["prev_close"] = df["close"].shift(1)
    df["ret_pct"] = (df["close"] - df["prev_close"]) / df["prev_close"] * 100.0
    df = df.dropna().tail(LOOKBACK_SESSIONS)

    st = {
        "avg_vol_20": float(df["volume"].mean()),
        "avg_range_20": float(df["range"].mean()),
        "avg_abs_ret_20": float(df["ret_pct"].abs().mean()),
    }
    stats_by_token[token] = st
    return st

def compute_rfactor(quote: dict, stats: dict) -> Dict[str, Optional[float]]:
    ohlc = quote.get("ohlc", {}) or {}
    prev_close = float(ohlc.get("close") or 0.0)
    high = float(ohlc.get("high") or 0.0)
    low = float(ohlc.get("low") or 0.0)

    ltp = float(quote.get("last_price") or 0.0)
    vol_today = float(quote.get("volume") or 0.0)

    if prev_close <= 0 or ltp <= 0:
        return {"rfactor": None, "dir_rfactor": None, "pct": None}

    pct_today = (ltp - prev_close) / prev_close * 100.0
    range_today = max(0.0, high - low)

    avg_vol_20 = stats.get("avg_vol_20")
    avg_range_20 = stats.get("avg_range_20")
    avg_abs_ret_20 = stats.get("avg_abs_ret_20")

    if not avg_vol_20 or not avg_range_20 or not avg_abs_ret_20:
        return {"rfactor": None, "dir_rfactor": None, "pct": pct_today}

    eps = 1e-9
    rvol = vol_today / (avg_vol_20 + eps)
    range_factor = range_today / (avg_range_20 + eps)
    move_factor = abs(pct_today) / (avg_abs_ret_20 + eps)

    rfactor = rvol * range_factor * move_factor
    dir_rfactor = (1.0 if pct_today >= 0 else -1.0) * rfactor

    return {"rfactor": float(rfactor), "dir_rfactor": float(dir_rfactor), "pct": float(pct_today)}

def sector_rankings(df: pd.DataFrame) -> pd.DataFrame:
    out = []
    for sector, lst in SECTOR_DEFINITIONS.items():
        syms = sorted(set(x.strip().upper() for x in lst))
        sub = df[df["symbol"].isin(syms)]
        if sub.empty:
            continue
        out.append({
            "sector": sector,
            "count": int(len(sub)),
            "avg_dir_rfactor": float(sub["dir_rfactor"].mean()),
            "avg_abs_rfactor": float(sub["rfactor"].abs().mean()),
        })
    sdf = pd.DataFrame(out)
    if not sdf.empty:
        sdf = sdf.sort_values("avg_dir_rfactor", ascending=False)
    return sdf

def _fetch_quotes_sync(k: KiteConnect, syms: List[str]) -> Dict[str, dict]:
    """
    Fetch quotes in chunks with retries.
    If one chunk fails after retries, continue with remaining chunks.
    """
    quotes: Dict[str, dict] = {}
    parts = chunked(syms, QUOTE_CHUNK_SIZE)

    for idx, part in enumerate(parts, 1):
        instruments = [f"NSE:{s}" for s in part]
        try:
            q = kite_retry(k.quote, instruments)
            if isinstance(q, dict):
                quotes.update(q)
        except Exception as e:
            log.warning("quote chunk failed idx=%d/%d size=%d err=%s",
                        idx, len(parts), len(part), repr(e))
        time.sleep(QUOTE_CHUNK_SLEEP)

    return quotes

def _build_df_sync(k: KiteConnect, syms: List[str]) -> pd.DataFrame:
    asof = now_ist().date()
    quotes = _fetch_quotes_sync(k, syms)

    rows = []
    for sym in syms:
        q = quotes.get(f"NSE:{sym}")
        if not q:
            continue

        token = symbol_to_token.get(sym)
        if not token:
            continue

        st = stats_by_token.get(token)
        if st is None:
            st = get_20d_stats(k, token, asof)

        r = compute_rfactor(q, st)
        if r["rfactor"] is None or r["pct"] is None:
            continue

        rows.append({
            "symbol": sym,
            "pct": r["pct"],
            "rfactor": r["rfactor"],
            "dir_rfactor": r["dir_rfactor"],
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    return df.dropna(subset=["rfactor", "pct"])

def build_snapshot(df: pd.DataFrame) -> Dict[str, Any]:
    TOP_N = 20
    gainers = df[df["pct"] > 0].sort_values("rfactor", ascending=False).head(TOP_N)
    losers  = df[df["pct"] < 0].sort_values("rfactor", ascending=False).head(TOP_N)
    movers  = df.assign(abs_r=df["rfactor"].abs()).sort_values("abs_r", ascending=False).head(10)
    sdf = sector_rankings(df)

    def rows(d: pd.DataFrame) -> List[dict]:
        return [
            {
                "symbol": str(r["symbol"]),
                "pct": float(r["pct"]),
                "rfactor": float(r["rfactor"]),
                "sectors": sym_to_sectors.get(str(r["symbol"]), []),
            }
            for _, r in d.iterrows()
        ]

    return {
        "timestamp": now_ist_str(),
        "universe_count": len(symbols),
        "gainers": rows(gainers),
        "losers": rows(losers),
        "movers": rows(movers),
        "sectors": [
            {
                "sector": row["sector"],
                "count": int(row["count"]),
                "avg_dir_rfactor": float(row["avg_dir_rfactor"]),
                "avg_abs_rfactor": float(row["avg_abs_rfactor"]),
            }
            for _, row in sdf.iterrows()
        ],
        "note": "OK",
    }

def _tick_update():
    global tick_count, tick_t0, tick_prev, ticks_per_sec_avg, ticks_per_sec_inst
    t = time.monotonic()
    if tick_t0 is None:
        tick_t0 = t
    tick_count += 1

    if tick_prev is not None:
        dt = max(1e-9, t - tick_prev)
        ticks_per_sec_inst = 1.0 / dt
    tick_prev = t

    elapsed = max(1e-9, t - tick_t0)
    ticks_per_sec_avg = tick_count / elapsed

# ---------------------------
# BACKGROUND TASKS
# ---------------------------
async def seed_cache_task(k: KiteConnect, syms: List[str]):
    global cache_total, cache_done, cache_status, cache_current_symbol, scanner_task

    asof = now_ist().date()
    cache_total = len(syms)
    cache_done = 0
    cache_status = "SEEDING"
    cache_current_symbol = None

    set_snapshot(f"Seeding 20D cache for {cache_total} symbols…")

    for i, s in enumerate(syms, 1):
        cache_current_symbol = s
        token = symbol_to_token.get(s)

        try:
            if token:
                await asyncio.to_thread(get_20d_stats, k, token, asof)
        except Exception as e:
            if token:
                stats_by_token[token] = {"avg_vol_20": None, "avg_range_20": None, "avg_abs_ret_20": None}
            log.warning("seed_cache failed sym=%s err=%s", s, repr(e))

        cache_done = i
        set_snapshot(f"Seeding 20D cache… {cache_done}/{cache_total} ({cache_current_symbol})")
        await asyncio.sleep(0.35)

    cache_current_symbol = None
    cache_status = "READY"
    set_snapshot("Cache ready. Starting scanner…")

    scanner_task = asyncio.create_task(scanner_loop())

async def scanner_loop():
    global last_snapshot, universe_rows

    while True:
        now = now_ist()
        mkt = market_status(now)

        try:
            if kite is None or not symbols:
                set_snapshot("Kite not ready (check env vars / bootstrap).")
                await asyncio.sleep(1)
                continue

            if cache_status != "READY":
                await asyncio.sleep(1)
                continue

            if not mkt["is_open"]:
                last_snapshot = {
                    **last_snapshot,
                    "timestamp": now_ist_str(),
                    **_snapshot_meta(mkt),
                    "note": f"Market {mkt['state']}. No scanning.",
                }
                await asyncio.sleep(5)
                continue

            df = await asyncio.to_thread(_build_df_sync, kite, symbols)
            _tick_update()

            if df.empty:
                last_snapshot = {
                    **last_snapshot,
                    "timestamp": now_ist_str(),
                    **_snapshot_meta(mkt),
                    "note": "No live rows (quotes missing or stats unavailable).",
                }
            else:
                # store full universe rows for sector drilldown
                universe_rows = [
                    {
                        "symbol": str(r["symbol"]),
                        "pct": float(r["pct"]),
                        "rfactor": float(r["rfactor"]),
                        "sectors": sym_to_sectors.get(str(r["symbol"]), []),
                    }
                    for _, r in df.iterrows()
                ]

                snap = build_snapshot(df)
                last_snapshot = {**snap, **_snapshot_meta(mkt)}

        except Exception as e:
            last_snapshot = {
                **last_snapshot,
                "timestamp": now_ist_str(),
                **_snapshot_meta(mkt),
                "note": f"Transient error: {repr(e)}",
            }

        await asyncio.sleep(REFRESH_EVERY_SEC)

async def bootstrap():
    global kite, symbols, missing_symbols, symbol_to_token
    global cache_status, seeding_task, universe_rows

    cache_status = "BOOTSTRAP"
    set_snapshot("Bootstrapping…")

    sym_to_sectors.update(build_sym_to_sectors(SECTOR_DEFINITIONS))
    all_syms = unique_symbols(SECTOR_DEFINITIONS)

    if not KITE_API_KEY or not KITE_ACCESS_TOKEN:
        cache_status = "ERROR"
        set_snapshot("Set env vars KITE_API_KEY and KITE_ACCESS_TOKEN")
        return

    try:
        kite_local = KiteConnect(api_key=KITE_API_KEY, timeout=KITE_TIMEOUT_SEC)
        kite_local.set_access_token(KITE_ACCESS_TOKEN)
        kite = kite_local

        set_snapshot("Loading instrument map…")
        inst_map = await asyncio.to_thread(load_instrument_map, kite, "NSE")

        symbol_to_token = {}
        missing_symbols = []
        for s in all_syms:
            tok = inst_map.get(s)
            if tok:
                symbol_to_token[s] = tok
            else:
                missing_symbols.append(s)

        symbols = [s for s in all_syms if s in symbol_to_token]
        universe_rows = []

        set_snapshot(f"Bootstrap OK. Symbols={len(symbols)} Missing={len(missing_symbols)}. Starting seeding…")
        seeding_task = asyncio.create_task(seed_cache_task(kite, symbols))

    except Exception as e:
        cache_status = "ERROR"
        set_snapshot(f"Bootstrap error: {repr(e)}")

# ---------------------------
# ROUTES
# ---------------------------
@app.get("/")
def index():
    if not INDEX_PATH.exists():
        return JSONResponse({"error": "index.html not found next to app.py"}, status_code=404)
    return FileResponse(INDEX_PATH)

@app.get("/api/snapshot")
def api_snapshot():
    return JSONResponse(last_snapshot)

@app.get("/api/sector-stocks")
def api_sector_stocks(sector: str = ""):
    """
    Returns all stocks for a sector from the latest computed universe_rows,
    sorted by rfactor DESC (as requested).
    """
    sector = (sector or "").strip().upper()
    if not sector:
        return JSONResponse({"sector": "", "timestamp": last_snapshot.get("timestamp"), "rows": [], "count": 0})

    rows = [r for r in universe_rows if sector in (r.get("sectors") or [])]
    rows.sort(key=lambda x: float(x.get("rfactor") or 0.0), reverse=True)

    return JSONResponse({
        "sector": sector,
        "timestamp": last_snapshot.get("timestamp"),
        "rows": rows,
        "count": len(rows),
    })

@app.get("/health")
def health():
    mkt = market_status(now_ist())
    return {
        "ok": bool(kite) and bool(symbols),
        "symbols": len(symbols),
        "missing_symbols": len(missing_symbols),
        "refresh_sec": REFRESH_EVERY_SEC,
        "lookback_sessions": LOOKBACK_SESSIONS,
        "market": mkt,
        "tick_count": tick_count,
        "ticks_per_sec_avg": ticks_per_sec_avg,
        "ticks_per_sec_inst": ticks_per_sec_inst,
        "cache_total": cache_total,
        "cache_done": cache_done,
        "cache_status": cache_status,
        "cache_current_symbol": cache_current_symbol,
        "note": last_snapshot.get("note"),
    }

# ---------------------------
# STARTUP / SHUTDOWN
# ---------------------------
@app.on_event("startup")
async def on_startup():
    global bootstrap_task
    set_snapshot("Waiting for backend bootstrap…")
    bootstrap_task = asyncio.create_task(bootstrap())

@app.on_event("shutdown")
async def on_shutdown():
    global bootstrap_task, seeding_task, scanner_task
    for t in (scanner_task, seeding_task, bootstrap_task):
        if t:
            t.cancel()
    scanner_task = None
    seeding_task = None
    bootstrap_task = None