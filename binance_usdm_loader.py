import os
import sys
import json
import time
import math
import hashlib
import logging
import gc
from logging.handlers import RotatingFileHandler
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple

try:
    import ccxt  # type: ignore
except Exception as e:  # pragma: no cover
    ccxt = None

try:
    import pandas as pd  # type: ignore
except Exception as e:  # pragma: no cover
    pd = None

try:
    from dateutil import parser as dateparser  # type: ignore
except Exception:
    dateparser = None

try:
    import pyarrow as pa  # type: ignore
    import pyarrow.parquet as pq  # type: ignore
except Exception:  # pragma: no cover
    pa = None
    pq = None

# --------------------------
# Configuration and Defaults
# --------------------------
# Market selection: 'futures' (USDT-margined perpetual via binanceusdm) or 'spot'
MARKET_TYPE = os.environ.get("MARKET_TYPE", "futures")  # futures | spot
EXCHANGE_ID = "binanceusdm" if MARKET_TYPE == "futures" else "binance"
SYMBOL = "BTC/USDT"  # Symbol name (ccxt format)
TIMEFRAME = "1m"
START_ISO_UTC = "2020-01-01T00:00:00Z"  # starting at 01.01.2020 00:00 UTC
# Legacy OHLCV limit (not used for klines anymore)
LIMIT = 1000
# Binance klines max limit per request
KLINE_LIMIT = 1500
CHUNK_SIZE = 1440  # rows per chunk file 1440 minuten = 24h


def _symbol_to_pair_token(symbol: str) -> str:
    try:
        return symbol.replace(" ", "").replace("-", "/").upper().replace("/", "")
    except Exception:
        return "BTCUSDT"


PAIR_TOKEN = _symbol_to_pair_token(SYMBOL)
# Allow moving data outside the project (reduces IDE filesystem churn). Default to C:\\MarketData
DEFAULT_DATA_BASE = os.environ.get("MARKET_DATA_DIR", r"C:\\MarketData")
DATA_ROOT = os.path.join(DEFAULT_DATA_BASE, EXCHANGE_ID, PAIR_TOKEN, TIMEFRAME)
CHUNKS_DIR = os.path.join(DATA_ROOT, "chunks")
# Legacy combined CSV directory (no longer used with Parquet dataset)
COMBINED_DIR = os.path.join(DATA_ROOT, "combined")
COMBINED_FILE = os.path.join(COMBINED_DIR, f"{PAIR_TOKEN}_{TIMEFRAME}_all.parquet")
MANIFEST_FILE = os.path.join(DATA_ROOT, "manifest.json")
HEALTH_FILE = os.path.join(DATA_ROOT, "health.json")
PENDING_DIR = os.path.join(DATA_ROOT, "pending")
PENDING_TMP_DIR = os.path.join(PENDING_DIR, ".tmp")
# Live options
LIVE_USE_PENDING = os.environ.get("LIVE_USE_PENDING", "1") in ("1","true","TRUE","yes","YES")
MANIFEST_WRITE_INTERVAL_SEC = int(os.environ.get("MANIFEST_WRITE_INTERVAL_SEC", "300"))
LOG_DIR = os.path.join("logs")
LOG_FILE = os.path.join(LOG_DIR, "loader.log")

# Kline schema (12 fields)
KLINE_COLUMNS = [
    "open_time", "open", "high", "low", "close", "volume",
    "close_time", "quote_volume", "trades", "taker_base_volume", "taker_quote_volume", "ignore"
]
PARQUET_COMPRESSION = "zstd"

# How many retries for transient errors
MAX_RETRIES = 5
RETRY_BACKOFF_BASE_SEC = 1.5

# Sleep safety margin between requests (additional to ccxt enableRateLimit)
EXTRA_SLEEP_SEC = 0.0  # keep 0 by default; adjust if needed

# --------------------------
# Helpers
# --------------------------

# Health/metrics state (lightweight)
_HEALTH_STATE: Dict[str, Any] = {
    "last_success_utc": None,
    "last_error_utc": None,
    "retry_timestamps": [],  # epoch seconds
}

# Timezone and alignment
ALIGN_TZ = os.environ.get("ALIGN_TZ", "UTC")  # IANA tz name or fixed offset like UTC

try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


def _tzinfo(name: str):
    if name.upper() in ("UTC", "Z"):
        return timezone.utc
    if ZoneInfo is not None:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    # Fallback: fixed offset like +02:00 or -0500
    try:
        s = name.replace(" ", "")
        if s.startswith("+") or s.startswith("-"):
            sign = 1 if s[0] == "+" else -1
            hh, mm = 0, 0
            if ":" in s:
                hh, mm = s[1:].split(":", 1)
            else:
                if len(s) in (3, 5):  # +HH or +HHMM
                    hh = s[1:3]
                    mm = s[3:] if len(s) == 5 else "0"
            return timezone(sign * timedelta(hours=int(hh or 0), minutes=int(mm or 0)))
    except Exception:
        pass
    return timezone.utc


def align_midnight_ms(ms: int, tz_name: str, to: str) -> int:
    """Align a UTC ms timestamp to midnight boundary of given tz.
    to: 'floor' -> 00:00 of that day; 'ceil' -> next day's 00:00 unless already at 00:00; 'start' -> same as floor; 'end' -> last minute 23:59 for inclusive windows
    Returns UTC ms.
    """
    tz = _tzinfo(tz_name)
    dt_utc = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
    dt_local = dt_utc.astimezone(tz)
    if to in ("floor", "start"):
        aligned_local = datetime(dt_local.year, dt_local.month, dt_local.day, tzinfo=tz)
        return int(aligned_local.astimezone(timezone.utc).timestamp() * 1000)
    elif to == "ceil":
        is_midnight = (dt_local.hour == 0 and dt_local.minute == 0 and dt_local.second == 0 and dt_local.microsecond == 0)
        base = dt_local if is_midnight else (datetime(dt_local.year, dt_local.month, dt_local.day, tzinfo=tz) + timedelta(days=1))
        aligned_local = base if is_midnight else base
        return int(aligned_local.astimezone(timezone.utc).timestamp() * 1000)
    elif to == "end":
        # Inclusive window end ms at 23:59:00 of local day -> last candle open
        start_local = datetime(dt_local.year, dt_local.month, dt_local.day, tzinfo=tz)
        end_local = start_local + timedelta(days=1)  # next midnight
        end_utc_ms = int(end_local.astimezone(timezone.utc).timestamp() * 1000) - 60_000
        return end_utc_ms
    else:
        return ms

def ensure_dirs():
    os.makedirs(CHUNKS_DIR, exist_ok=True)
    os.makedirs(COMBINED_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    # Pending deltas directory for live mode (minute files)
    try:
        os.makedirs(os.path.join(DATA_ROOT, "pending"), exist_ok=True)
        os.makedirs(os.path.join(DATA_ROOT, "pending", ".tmp"), exist_ok=True)
    except Exception:
        pass


def sweep_tmp_files(logger: Optional[logging.Logger] = None):
    """Remove lingering temporary files that indicate interrupted writes.
    Cleans up *.tmp under chunks and combined directories.
    """
    try:
        for root in (CHUNKS_DIR, COMBINED_DIR):
            if not os.path.isdir(root):
                continue
            for name in os.listdir(root):
                if name.endswith('.tmp') or name.endswith('.tmp.parquet'):
                    path = os.path.join(root, name)
                    try:
                        os.remove(path)
                        if logger:
                            logger.info(f"Removed lingering temp file: {path}")
                    except Exception as e:
                        if logger:
                            logger.warning(f"Failed to remove temp file {path}: {e}")
    except Exception:
        # best-effort cleanup
        pass


def shutdown_logging():
    """Close and remove all handlers attached to the 'dataloader' logger to prevent file descriptor leaks."""
    logger = logging.getLogger("dataloader")
    for h in list(logger.handlers):
        try:
            try:
                h.flush()
            except Exception:
                pass
            # On Windows, before closing a RotatingFileHandler, try to acquire lock and flush
            try:
                if isinstance(h, RotatingFileHandler):
                    # ensure stream is flushed and closed to release file lock
                    if getattr(h, 'stream', None) is not None:
                        try:
                            h.stream.flush()
                        except Exception:
                            pass
            except Exception:
                pass
            h.close()
        except Exception:
            pass
        try:
            logger.removeHandler(h)
        except Exception:
            pass


def setup_logging(verbose: bool = True):
    ensure_dirs()
    logger = logging.getLogger("dataloader")
    logger.setLevel(logging.INFO)
    logger.propagate = False
    # Avoid duplicate handlers if setup_logging is called multiple times
    if logger.handlers:
        logger.handlers.clear()
    formatter = logging.Formatter(fmt="%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # Rotating file handler with safe rollover on Windows
    class SafeRotatingFileHandler(RotatingFileHandler):
        def rotate(self, source, dest):
            # On Windows, os.rename fails if dest exists or file is locked; use replace and retry
            try:
                if os.path.exists(dest):
                    try:
                        os.remove(dest)
                    except Exception:
                        pass
                os.replace(source, dest)
            except Exception:
                # Fallback: try copy-then-truncate to avoid holding locks
                try:
                    import shutil
                    shutil.copy2(source, dest)
                    # Truncate source file
                    with open(source, 'w', encoding='utf-8') as f:
                        f.truncate(0)
                except Exception:
                    raise
    fh = SafeRotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    if verbose:
        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    # Sweep lingering temp files on startup
    try:
        sweep_tmp_files(logger)
        # Also cleanup pending tmp files
        if os.path.isdir(PENDING_TMP_DIR):
            for name in os.listdir(PENDING_TMP_DIR):
                try:
                    os.remove(os.path.join(PENDING_TMP_DIR, name))
                except Exception:
                    pass
    except Exception:
        pass

    # Register an atexit hook to ensure handlers are closed
    try:
        import atexit
        atexit.register(shutdown_logging)
    except Exception:
        pass

    return logger


def utc_now_ms() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def iso_to_ms(iso_str: str) -> int:
    if dateparser is None:
        # Fallback: try fromisoformat (requires 'Z' removal)
        s = iso_str.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp() * 1000)
    dt = dateparser.isoparse(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def hash_file_sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_kline_rows(rows: List[List[Any]], logger: logging.Logger) -> List[List[Any]]:
    """Coerce kline rows to proper numeric types where needed.
    Ensures open_time (index 0) is an int so time comparisons don't fail when API returns strings.
    Also tries to coerce close_time (6) and trades (8) to ints. Rows with non-convertible open_time are skipped.
    """
    normalized: List[List[Any]] = []
    for r in rows:
        if not r or len(r) < 12:
            continue
        try:
            # open_time
            open_time = int(r[0]) if not isinstance(r[0], (int, float)) else int(r[0])
        except Exception:
            try:
                open_time = int(float(r[0]))
            except Exception:
                logger.warning(f"Skipping kline with non-numeric open_time: {r[0]}")
                continue
        rr = list(r)
        rr[0] = open_time
        # best-effort coercions
        for idx in (6, 8):  # close_time, trades
            try:
                rr[idx] = int(rr[idx]) if not isinstance(rr[idx], (int, float)) else int(rr[idx])
            except Exception:
                pass
        normalized.append(rr)
    return normalized


def ensure_numeric_schema(df: 'pd.DataFrame', logger: logging.Logger) -> 'pd.DataFrame':
    """Ensure DataFrame has consistent numeric dtypes for Parquet writing.
    - Enforce column order to KLINE_COLUMNS
    - Cast price/volume columns to float64
    - Cast time columns and trades to int64
    Drops rows that cannot be coerced (very rare) with a warning.
    """
    if pd is None:
        return df
    # Ensure columns presence/order
    try:
        df = df[KLINE_COLUMNS]
    except Exception:
        missing = [c for c in KLINE_COLUMNS if c not in df.columns]
        if missing:
            logger.warning(f"ensure_numeric_schema: missing columns {missing}; attempting to continue with existing")
    # Define dtypes
    float_cols = ["open", "high", "low", "close", "volume", "quote_volume", "taker_base_volume", "taker_quote_volume"]
    int_cols = ["open_time", "close_time", "trades"]
    # Coerce floats
    for c in float_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    # Coerce ints
    for c in int_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    # After coercion, drop rows with NaNs in critical fields
    before = len(df)
    critical = ["open_time", "open", "high", "low", "close", "close_time"]
    mask = df[critical].isna().any(axis=1)
    dropped = int(mask.sum()) if hasattr(mask, 'sum') else 0
    if dropped:
        logger.warning(f"ensure_numeric_schema: dropping {dropped} invalid row(s) out of {before}")
        df = df[~mask]
    # Final cast to concrete dtypes
    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].astype("int64")
    for c in float_cols:
        if c in df.columns:
            df[c] = df[c].astype("float64")
    return df


# --------------------------
# Manifest Management
# --------------------------

def load_manifest() -> Dict[str, Any]:
    if not os.path.exists(MANIFEST_FILE):
        return {
            "exchange": EXCHANGE_ID,
            "symbol": SYMBOL,
            "timeframe": TIMEFRAME,
            "chunk_size": CHUNK_SIZE,
            "format": "parquet",
            "columns": KLINE_COLUMNS,
            "chunks": [],
            "last_candle_ms": None,
            "created_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
    with open(MANIFEST_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


# Debounced manifest write controls
_last_manifest_write_ts: float = 0.0


def _atomic_write_json(path: str, obj: dict) -> None:
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
        try:
            f.flush()
            os.fsync(f.fileno())
        except Exception:
            pass
    try:
        os.replace(tmp_path, path)
    except Exception:
        # Fallback: remove existing and move
        try:
            if os.path.exists(path):
                os.remove(path)
        except Exception:
            pass
        os.replace(tmp_path, path)


def save_manifest(manifest: Dict[str, Any]):
    """Immediate, atomic manifest write (use for chunk close or critical updates)."""
    manifest["updated_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    _atomic_write_json(MANIFEST_FILE, manifest)


def maybe_write_manifest(manifest: Dict[str, Any], force: bool = False):
    """Debounced manifest write. Writes at most every MANIFEST_WRITE_INTERVAL_SEC unless forced."""
    global _last_manifest_write_ts
    if force:
        save_manifest(manifest)
        _last_manifest_write_ts = time.time()
        return
    now = time.time()
    if (now - _last_manifest_write_ts) >= MANIFEST_WRITE_INTERVAL_SEC:
        save_manifest(manifest)
        _last_manifest_write_ts = now


def get_resume_since_ms(manifest: Dict[str, Any], default_start_ms: int) -> int:
    last_ms = manifest.get("last_candle_ms")
    if last_ms is None:
        return default_start_ms
    return int(last_ms) + 60_000  # next minute


# --------------------------
# Exchange Client
# --------------------------

def resolve_market_symbol(ex, logger: logging.Logger, requested_symbol: str) -> str:
    markets = ex.load_markets()
    # Normalize requested symbol to CCXT format (BASE/QUOTE upper)
    try:
        rs = requested_symbol.replace(" ", "").replace("-", "/").upper()
    except Exception:
        rs = requested_symbol
    if rs in markets:
        return rs
    # Extract base/quote if possible
    base, quote = None, None
    if "/" in rs:
        base, quote = rs.split("/", 1)
    # Resolution differs by market type
    def _search_with(b: str, q: str) -> Optional[str]:
        if MARKET_TYPE == 'futures':
            candidates = []
            for m in markets.values():
                try:
                    if m.get('base') == b and m.get('quote') == q and m.get('linear'):
                        candidates.append(m)
                except Exception:
                    continue
            swaps = [m for m in candidates if m.get('type') == 'swap']
            chosen = swaps[0] if swaps else (candidates[0] if candidates else None)
            if chosen is not None:
                return chosen.get('symbol')
            for k in markets.keys():
                if k.startswith(f'{b}/{q}'):
                    return k
            return None
        else:
            for k, m in markets.items():
                try:
                    if m.get('spot') and m.get('base') == b and m.get('quote') == q:
                        return m.get('symbol', k)
                except Exception:
                    continue
            for k in markets.keys():
                if k.startswith(f'{b}/{q}'):
                    return k
            return None
    # Try requested base/quote
    if base and quote:
        resolved = _search_with(base, quote)
        if resolved:
            return resolved
    # Fallback to BTC/USDT
    logger.warning(f"Could not resolve requested symbol '{requested_symbol}' on {EXCHANGE_ID} {MARKET_TYPE}; falling back to BTC/USDT")
    fallback = _search_with('BTC', 'USDT')
    if fallback:
        return fallback
    sample = next(iter(markets.keys())) if markets else 'N/A'
    raise ValueError(f"Could not resolve symbol. Sample market key: {sample}")


def create_exchange(logger: logging.Logger):
    if ccxt is None:
        raise RuntimeError("ccxt is not installed. Please install dependencies from requirements.txt")
    if MARKET_TYPE == 'futures':
        ex = ccxt.binanceusdm({
            "enableRateLimit": True,
        })
    else:
        # Spot API has higher rate limits (6000 weights/min vs 2400 for futures)
        ex = ccxt.binance({
            "enableRateLimit": True,
        })
    # Resolve the correct symbol on this exchange (with fallback inside resolver)
    resolved = resolve_market_symbol(ex, logger, SYMBOL)
    return ex, resolved


# --------------------------
# IO helpers
# --------------------------

def _parquet_engine() -> Optional[str]:
    # Prefer pyarrow if available for consistency and performance
    return "pyarrow" if pq is not None else None


def _atomic_write_parquet(df: 'pd.DataFrame', path: str, logger: logging.Logger):
    """Write a parquet file atomically: write to .tmp then replace.
    Uses a consistent engine when possible to avoid plugin mismatches.
    """
    tmp_path = path + ".tmp"
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except Exception:
        pass
    engine = _parquet_engine()
    if engine:
        df.to_parquet(tmp_path, index=False, compression=PARQUET_COMPRESSION, engine=engine)
    else:
        df.to_parquet(tmp_path, index=False, compression=PARQUET_COMPRESSION)
    # Replace atomically
    try:
        if os.path.exists(path):
            os.remove(path)
    except Exception:
        pass
    os.replace(tmp_path, path)


def _read_parquet(path: str) -> 'pd.DataFrame':
    engine = _parquet_engine()
    if engine:
        return pd.read_parquet(path, engine=engine)
    return pd.read_parquet(path)


def _atomic_write_parquet_path(df: 'pd.DataFrame', final_path: str, logger: logging.Logger):
    """Write a DataFrame to Parquet atomically to an arbitrary final path."""
    tmp_path = final_path + ".tmp"
    try:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
    except Exception:
        pass
    engine = _parquet_engine()
    df = ensure_numeric_schema(df, logger)
    if engine:
        df.to_parquet(tmp_path, index=False, compression=PARQUET_COMPRESSION, engine=engine)
    else:
        df.to_parquet(tmp_path, index=False, compression=PARQUET_COMPRESSION)
    try:
        if os.path.exists(final_path):
            os.remove(final_path)
    except Exception:
        pass
    os.replace(tmp_path, final_path)


def _write_pending_delta(row_df: 'pd.DataFrame', logger: logging.Logger) -> Optional[str]:
    """Write a minute delta parquet into pending/ as delta_<open_time>.parquet atomically.
    Returns final path, or None if pending disabled.
    """
    if not LIVE_USE_PENDING:
        return None
    try:
        os.makedirs(PENDING_TMP_DIR, exist_ok=True)
    except Exception:
        pass
    if len(row_df) == 0:
        return None
    open_time = int(row_df.iloc[0]["open_time"])  # type: ignore
    final_path = os.path.join(PENDING_DIR, f"delta_{open_time}.parquet")
    tmp_path = os.path.join(PENDING_TMP_DIR, f"delta_{open_time}.parquet.tmp")
    # Write tmp then atomic replace
    engine = _parquet_engine()
    row_df = ensure_numeric_schema(row_df, logger)
    if engine:
        row_df.to_parquet(tmp_path, index=False, compression=PARQUET_COMPRESSION, engine=engine)
    else:
        row_df.to_parquet(tmp_path, index=False, compression=PARQUET_COMPRESSION)
    try:
        os.replace(tmp_path, final_path)
    except Exception:
        # Fallback remove and move
        try:
            if os.path.exists(final_path):
                os.remove(final_path)
        except Exception:
            pass
        os.replace(tmp_path, final_path)
    return final_path


def _read_pending_deltas(start_ms: int, end_ms: int, logger: logging.Logger) -> 'pd.DataFrame':
    """Read all pending/*.parquet deltas within [start_ms, end_ms]."""
    if pd is None:
        raise RuntimeError("pandas required")
    if not os.path.isdir(PENDING_DIR):
        return pd.DataFrame(columns=KLINE_COLUMNS)  # type: ignore
    import glob
    paths = [p for p in glob.glob(os.path.join(PENDING_DIR, "*.parquet")) if os.path.isfile(p)]
    frames: List['pd.DataFrame'] = []
    for pth in paths:
        try:
            df = pd.read_parquet(pth)
            df = ensure_numeric_schema(df, logger)
            if len(df) == 0:
                continue
            ot0 = int(df.iloc[0]["open_time"])  # type: ignore
            if start_ms <= ot0 <= end_ms:
                frames.append(df)
        except Exception as e:
            logger.warning(f"Pending delta read failed for {pth}: {e}")
    if not frames:
        return pd.DataFrame(columns=KLINE_COLUMNS)  # type: ignore
    out = pd.concat(frames, ignore_index=True)
    # Sort and dedupe by open_time
    out = out.sort_values("open_time").drop_duplicates(subset=["open_time"], keep="last").reset_index(drop=True)
    return out


def _compact_today_if_needed(manifest: Dict[str, Any], logger: logging.Logger, chunk_size: int) -> None:
    """If the last chunk reached chunk_size or a local day boundary closed, compact pending deltas into the chunk.
    Compute sha256 once upon finalization and remove consumed pending files.
    """
    if pd is None:
        return
    chunks_meta = manifest.get("chunks", [])
    if not chunks_meta:
        return
    last_chunk = chunks_meta[-1]
    # Only compact when the last chunk is full
    if int(last_chunk.get("count", 0)) < chunk_size:
        return
    # Read the finalized chunk file and compute sha, then clean pending deltas within its span
    path = os.path.join(DATA_ROOT, last_chunk["filename"])  # type: ignore
    if not os.path.exists(path):
        return
    # Compute sha256 for final artifact
    sha256 = hash_file_sha256(path)
    last_chunk["sha256"] = sha256
    manifest["chunks"] = chunks_meta
    maybe_write_manifest(manifest, force=True)
    # Remove pending deltas that are <= end_ms of this chunk
    try:
        import glob
        for p in glob.glob(os.path.join(PENDING_DIR, "*.parquet")):
            try:
                df = pd.read_parquet(p)
                if len(df) == 0:
                    os.remove(p)
                    continue
                ot = int(df.iloc[0]["open_time"])  # type: ignore
                if ot <= int(last_chunk["end_ms"]):
                    os.remove(p)
            except Exception:
                pass
    except Exception:
        pass


def write_chunk_parquet(df: 'pd.DataFrame', chunk_index: int, start_ms: int, end_ms: int, logger: logging.Logger) -> Tuple[str, str]:
    filename = f"chunk_{chunk_index:06d}_{start_ms}_{end_ms}.parquet"
    path = os.path.join(CHUNKS_DIR, filename)
    # Ensure proper dtypes and column order
    df = ensure_numeric_schema(df, logger)
    _atomic_write_parquet(df, path, logger)
    sha256 = hash_file_sha256(path)
    logger.info(f"Saved chunk #{chunk_index} -> {path} (rows={len(df)}, sha256={sha256[:12]}...)")
    return path, sha256


def append_to_combined(df: 'pd.DataFrame', logger: logging.Logger):
    # No-op with Parquet dataset. We keep this function for backward compatibility of calls.
    logger.info(f"Appended {len(df)} rows to dataset (Parquet chunks); combined file maintenance is optional and can be enabled via BUILD_COMBINED_ON_BACKFILL or external compaction.")


def build_combined_parquet(manifest: Dict[str, Any], logger: logging.Logger, from_index: Optional[int] = None, to_index: Optional[int] = None, tz_name: Optional[str] = None) -> Optional[str]:
    """Build a single combined Parquet file from all existing chunk Parquet files.
    Uses pyarrow.ParquetWriter to stream row groups and avoid high memory usage.
    Returns the combined file path on success, or None if skipped/failure.
    """
    if pq is None:
        logger.warning("pyarrow is not available; skipping combined Parquet build.")
        return None

    chunks_all = sorted(manifest.get("chunks", []), key=lambda c: c["index"]) if manifest else []
    if from_index is not None or to_index is not None:
        lo = from_index if from_index is not None else (chunks_all[0]["index"] if chunks_all else 0)
        hi = to_index if to_index is not None else (chunks_all[-1]["index"] if chunks_all else -1)
        chunks = [c for c in chunks_all if lo <= c["index"] <= hi]
    else:
        chunks = chunks_all
    if not chunks:
        logger.info("No chunks present; skipping combined Parquet build.")
        return None

    os.makedirs(COMBINED_DIR, exist_ok=True)
    tz_label = None
    if tz_name:
        # Build filesystem-safe label, e.g., Europe_Zurich or UTC_plus02
        safe = tz_name.replace('/', '_').replace(':', '')
        tz_label = safe
    combined_tmp = os.path.join(COMBINED_DIR, f"{PAIR_TOKEN}_{TIMEFRAME}_all{('_' + tz_label) if tz_label else ''}.tmp.parquet")
    combined_path = os.path.join(COMBINED_DIR, f"{PAIR_TOKEN}_{TIMEFRAME}_all{('_' + tz_label) if tz_label else ''}.parquet")

    # Remove temp file if it exists
    try:
        if os.path.exists(combined_tmp):
            os.remove(combined_tmp)
    except Exception:
        pass

    writer: Optional[pq.ParquetWriter] = None
    import gc
    total_rows = 0
    chunk_count = 0

    try:
        for ch in chunks:
            rel = ch.get("filename")
            if not rel:
                continue
            path = os.path.join(DATA_ROOT, rel)
            if not os.path.exists(path):
                logger.warning(f"Chunk file missing during combine: {path}")
                continue
            # Ensure ParquetFile is closed promptly
            with pq.ParquetFile(path) as pf:
                if writer is None:
                    # Initialize writer with the first file's schema
                    schema = pf.schema_arrow
                    writer = pq.ParquetWriter(combined_tmp, schema=schema, compression=PARQUET_COMPRESSION)
                # Write each row group to the combined file
                for rg in range(pf.num_row_groups):
                    table = pf.read_row_group(rg)
                    total_rows += table.num_rows
                    writer.write_table(table)
                    del table
                    gc.collect()
                chunk_count += 1
    finally:
        if writer is not None:
            writer.close()

    if chunk_count == 0:
        logger.info("No valid chunk files found to combine.")
        try:
            if os.path.exists(combined_tmp):
                os.remove(combined_tmp)
        except Exception:
            pass
        return None

    # Atomically replace the previous combined file
    try:
        if os.path.exists(combined_path):
            os.remove(combined_path)
    except Exception:
        pass
    os.replace(combined_tmp, combined_path)
    logger.info(f"Built combined Parquet from {chunk_count} chunks -> {combined_path} (rows={total_rows})")
    return combined_path


# --------------------------
# Interval Utilities
# --------------------------

def compute_missing_intervals(request_start_ms: int, request_end_ms: int, chunks: List[Dict[str, Any]]) -> List[Tuple[int, int]]:
    """Compute the missing [start,end] inclusive 1-minute intervals within the requested window,
    given already existing chunk coverages in the manifest.

    - Both request and chunk intervals are treated as inclusive on open_time.
    - Returns a sorted list of non-overlapping intervals covering all gaps within [request_start_ms, request_end_ms].
    """
    if request_start_ms > request_end_ms:
        return []
    # Collect covered intervals intersected with request window
    covered: List[Tuple[int, int]] = []
    for ch in sorted(chunks or [], key=lambda c: (int(c.get("start_ms", 0)), int(c.get("end_ms", 0)))):
        s = int(ch.get("start_ms", 0))
        e = int(ch.get("end_ms", -1))
        if e < request_start_ms or s > request_end_ms:
            continue
        s = max(s, request_start_ms)
        e = min(e, request_end_ms)
        if s <= e:
            covered.append((s, e))
    # Merge covered intervals
    covered.sort()
    merged: List[Tuple[int, int]] = []
    for s, e in covered:
        if not merged or s > merged[-1][1] + 60_000:
            merged.append((s, e))
        else:
            merged[-1] = (merged[-1][0], max(merged[-1][1], e))
    # Subtract merged from request to get missing
    missing: List[Tuple[int, int]] = []
    cur = request_start_ms
    for s, e in merged:
        if cur < s:
            missing.append((cur, s - 60_000))  # inclusive minutes up to s-1min
        cur = e + 60_000
    if cur <= request_end_ms:
        missing.append((cur, request_end_ms))
    return missing


# --------------------------
# Core Loader
# --------------------------

def fetch_klines_page(ex, symbol: str, timeframe: str, since_ms: int, limit: int, logger: logging.Logger) -> List[List[Any]]:
    """Fetch a page of Binance klines (12 fields) using raw endpoint.
    Uses futures (fapiPublicGetKlines) for MARKET_TYPE='futures' and spot (publicGetKlines) for MARKET_TYPE='spot'.
    Fields: [open_time, open, high, low, close, volume, close_time, quote_volume, trades, taker_base_volume, taker_quote_volume, ignore]
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            market = ex.market(symbol)
            symbol_id = market.get('id', market.get('symbol', symbol).replace('/', ''))
            interval = ex.timeframes.get(timeframe, timeframe) if hasattr(ex, 'timeframes') else timeframe
            params = {
                'symbol': symbol_id,
                'interval': interval,
                'startTime': since_ms,
                'limit': min(limit, KLINE_LIMIT),
            }
            if MARKET_TYPE == 'futures':
                # USDT-M futures
                rows = ex.fapiPublicGetKlines(params)
            else:
                # Spot API
                rows = ex.publicGetKlines(params)
            if EXTRA_SLEEP_SEC > 0:
                time.sleep(EXTRA_SLEEP_SEC)
            # mark success
            _HEALTH_STATE["last_success_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            return rows
        except Exception as e:  # pragma: no cover (network-dependent)
            last_exc = e
            wait = RETRY_BACKOFF_BASE_SEC ** attempt
            logger.warning(f"fetch_klines failed (attempt {attempt}/{MAX_RETRIES}): {e}; sleeping {wait:.2f}s")
            # record retry timestamp and last_error
            try:
                _HEALTH_STATE["retry_timestamps"].append(time.time())
                # keep only last hour worth to bound size
                cutoff = time.time() - 3600
                _HEALTH_STATE["retry_timestamps"] = [t for t in _HEALTH_STATE["retry_timestamps"] if t >= cutoff]
                _HEALTH_STATE["last_error_utc"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            except Exception:
                pass
            time.sleep(wait)
    if last_exc:
        raise last_exc
    return []


def backfill(logger: logging.Logger,
             start_ms: Optional[int] = None,
             end_ms: Optional[int] = None,
             chunk_size: int = CHUNK_SIZE,
             align_window: Optional[bool] = None):
    ensure_dirs()
    if pd is None:
        raise RuntimeError("pandas is required. Please install dependencies from requirements.txt")

    manifest = load_manifest()
    ex, resolved_symbol = create_exchange(logger)
    # Persist requested/resolved symbol and schema for transparency
    manifest["symbol_requested"] = SYMBOL
    manifest["symbol_resolved"] = resolved_symbol
    manifest["format"] = "parquet"
    manifest["columns"] = KLINE_COLUMNS
    manifest["chunk_size"] = int(chunk_size)
    save_manifest(manifest)

    # Determine requested window (UTC), then align to chosen timezone midnight boundaries
    request_start_ms = iso_to_ms(START_ISO_UTC) if start_ms is None else start_ms
    if end_ms is None:
        request_end_ms = utc_now_ms() - 60_000  # up to last fully closed minute
    else:
        request_end_ms = end_ms
    # Decide whether to auto-align window to midnight boundaries. Default: align only when invoked via CLI without explicit ms
    do_align = False
    if align_window is True:
        do_align = True
    elif align_window is None and start_ms is None and end_ms is None:
        do_align = True
    if do_align:
        request_start_ms = align_midnight_ms(request_start_ms, ALIGN_TZ, "start")
        request_end_ms = align_midnight_ms(request_end_ms, ALIGN_TZ, "end")

    # Compute missing intervals relative to existing chunks
    chunks_meta = manifest.get("chunks", [])
    missing_intervals = compute_missing_intervals(request_start_ms, request_end_ms, chunks_meta)

    if not missing_intervals:
        logger.info(f"No backfill needed for {ms_to_iso(request_start_ms)} to {ms_to_iso(request_end_ms)}; data already present.")
        return

    logger.info(
        f"Starting backfill for {len(missing_intervals)} gap(s) within {ms_to_iso(request_start_ms)} to {ms_to_iso(request_end_ms)} "
        f"with chunk_size={chunk_size}"
    )

    total_added = 0
    chunk_index = len(chunks_meta) + 1

    for gap_start, gap_end in missing_intervals:
        logger.info(f"Filling missing interval: {ms_to_iso(gap_start)} -> {ms_to_iso(gap_end)}")
        buffer: List[List[Any]] = []
        last_page_last_ms: Optional[int] = None
        since_ms = gap_start

        while since_ms <= gap_end:
            rows = fetch_klines_page(ex, resolved_symbol, TIMEFRAME, since_ms, KLINE_LIMIT, logger)
            if not rows:
                logger.info("No more klines returned by exchange; stopping this interval.")
                break

            # Normalize to ensure numeric open_time and relevant fields
            rows = normalize_kline_rows(rows, logger)

            # Ensure only within our gap_end (open_time index 0)
            rows = [r for r in rows if r and len(r) >= 12 and r[0] <= gap_end]
            if not rows:
                break

            # Continuity check with previous page of the same interval
            if last_page_last_ms is not None:
                expected = last_page_last_ms + 60_000
                if rows[0][0] != expected:
                    logger.warning(f"Gap or overlap detected between pages: expected {expected} got {rows[0][0]}")
            last_page_last_ms = rows[-1][0]

            buffer.extend(rows)
            total_added += len(rows)

            # If we exceeded chunk size OR boundary crossed, flush full days aligned to ALIGN_TZ
            def boundary_index(buf: List[List[Any]]) -> int:
                if not buf:
                    return 0
                # Find last index such that next open_time belongs to next local day
                tz = _tzinfo(ALIGN_TZ)
                for i in range(len(buf)-1):
                    t0 = buf[i][0]
                    t1 = buf[i+1][0]
                    d0 = datetime.fromtimestamp(t0/1000, tz=timezone.utc).astimezone(tz)
                    d1 = datetime.fromtimestamp(t1/1000, tz=timezone.utc).astimezone(tz)
                    if (d0.date() != d1.date()):
                        return i+1
                return 0
            # Flush as many full days (or chunk_size groups) as available
            while True:
                # First, if there is an existing partial chunk, append into it before creating new chunks
                chunks_meta = manifest.get("chunks", [])
                if chunks_meta and int(chunks_meta[-1].get("count", 0)) < chunk_size and buffer:
                    # Only append rows that belong to the same local day as the last chunk's start (ALIGN_TZ)
                    tz = _tzinfo(ALIGN_TZ)
                    last_start = int(chunks_meta[-1]["start_ms"])
                    last_day = datetime.fromtimestamp(last_start/1000, tz=timezone.utc).astimezone(tz).date()
                    # Find how many rows at the head of buffer are in the same local day
                    same_day_count = 0
                    for r in buffer:
                        d = datetime.fromtimestamp(int(r[0])/1000, tz=timezone.utc).astimezone(tz).date()
                        if d == last_day:
                            same_day_count += 1
                        else:
                            break
                    if same_day_count > 0:
                        remainder = chunk_size - int(chunks_meta[-1]["count"])  # allowed to complete the day
                        take_append = min(remainder, same_day_count)
                        head = buffer[:take_append]
                        buffer = buffer[take_append:]
                        # Append head to existing parquet file
                        prev_path = os.path.join(DATA_ROOT, chunks_meta[-1]["filename"])  # type: ignore
                        prev_df = _read_parquet(prev_path)
                        prev_df = ensure_numeric_schema(prev_df, logger)
                        add_df = pd.DataFrame(head, columns=KLINE_COLUMNS)  # type: ignore
                        add_df = ensure_numeric_schema(add_df, logger)
                        new_df = pd.concat([prev_df, add_df], ignore_index=True)
                        new_df = ensure_numeric_schema(new_df, logger)
                        end_ms_chunk = int(new_df.iloc[-1]["open_time"])  # type: ignore
                        _atomic_write_parquet(new_df, prev_path, logger)
                        sha256 = hash_file_sha256(prev_path)
                        chunks_meta[-1].update({
                            "end_ms": end_ms_chunk,
                            "count": len(new_df),
                            "sha256": sha256,
                        })
                        manifest["chunks"] = chunks_meta
                        manifest["last_candle_ms"] = end_ms_chunk
                        save_manifest(manifest)
                        try:
                            del prev_df
                            del add_df
                            del new_df
                        except Exception:
                            pass
                        gc.collect()
                        # Continue loop to see if more appending or new chunk creation is needed
                        continue
                
                bidx = boundary_index(buffer)
                if bidx == 0 and len(buffer) < chunk_size:
                    break
                take = bidx if bidx > 0 else chunk_size
                chunk = buffer[:take]
                buffer = buffer[take:]
                df = pd.DataFrame(chunk, columns=KLINE_COLUMNS)  # type: ignore
                start_ms_chunk = int(df.iloc[0]["open_time"])  # type: ignore
                end_ms_chunk = int(df.iloc[-1]["open_time"])  # type: ignore
                # If previous chunk is partial but different local day, we are safe to start a new file
                path, sha256 = write_chunk_parquet(df, chunk_index, start_ms_chunk, end_ms_chunk, logger)
                append_to_combined(df, logger)
                # Capture count before freeing the dataframe
                row_count = len(chunk)
                # Proactively drop dataframe and trigger GC to free memory between chunks
                try:
                    del df
                except Exception:
                    pass
                gc.collect()
                # update manifest
                manifest.setdefault("chunks", []).append({
                    "index": chunk_index,
                    "filename": os.path.relpath(path, DATA_ROOT),
                    "start_ms": start_ms_chunk,
                    "end_ms": end_ms_chunk,
                    "count": row_count,
                    "sha256": sha256,
                })
                manifest["last_candle_ms"] = max(int(manifest.get("last_candle_ms") or 0), end_ms_chunk)
                save_manifest(manifest)
                chunk_index += 1

            # Advance since_ms to one minute after the last kline open_time
            since_ms = rows[-1][0] + 60_000

            # Progress log occasionally
            if total_added % (1000 * 50) == 0:  # every ~50k
                logger.info(f"Progress: {total_added} klines fetched so far. Current since={ms_to_iso(since_ms)}")

        # Flush any remaining buffer for this interval; append to existing partial day first
        if buffer:
            # Try to append to the last partial chunk if it exists and belongs to the same local day
            chunks_meta = manifest.get("chunks", [])
            if chunks_meta and int(chunks_meta[-1].get("count", 0)) < chunk_size:
                tz = _tzinfo(ALIGN_TZ)
                last_start = int(chunks_meta[-1]["start_ms"])
                last_day = datetime.fromtimestamp(last_start/1000, tz=timezone.utc).astimezone(tz).date()
                same_day_count = 0
                for r in buffer:
                    d = datetime.fromtimestamp(int(r[0])/1000, tz=timezone.utc).astimezone(tz).date()
                    if d == last_day:
                        same_day_count += 1
                    else:
                        break
                if same_day_count > 0:
                    remainder = chunk_size - int(chunks_meta[-1]["count"])
                    take_append = min(remainder, same_day_count)
                    head = buffer[:take_append]
                    buffer = buffer[take_append:]
                    prev_path = os.path.join(DATA_ROOT, chunks_meta[-1]["filename"])  # type: ignore
                    prev_df = _read_parquet(prev_path)
                    prev_df = ensure_numeric_schema(prev_df, logger)
                    add_df = pd.DataFrame(head, columns=KLINE_COLUMNS)  # type: ignore
                    add_df = ensure_numeric_schema(add_df, logger)
                    new_df = pd.concat([prev_df, add_df], ignore_index=True)
                    new_df = ensure_numeric_schema(new_df, logger)
                    end_ms_chunk = int(new_df.iloc[-1]["open_time"])  # type: ignore
                    _atomic_write_parquet(new_df, prev_path, logger)
                    sha256 = hash_file_sha256(prev_path)
                    chunks_meta[-1].update({
                        "end_ms": end_ms_chunk,
                        "count": len(new_df),
                        "sha256": sha256,
                    })
                    manifest["chunks"] = chunks_meta
                    manifest["last_candle_ms"] = end_ms_chunk
                    save_manifest(manifest)
                    try:
                        del prev_df
                        del add_df
                        del new_df
                    except Exception:
                        pass
                    gc.collect()
            # If buffer still has rows left (either different day or last chunk became full), write them as new chunk(s)
            if buffer:
                df = pd.DataFrame(buffer, columns=KLINE_COLUMNS)  # type: ignore
                start_ms_chunk = int(df.iloc[0]["open_time"])  # type: ignore
                end_ms_chunk = int(df.iloc[-1]["open_time"])  # type: ignore
                path, sha256 = write_chunk_parquet(df, chunk_index, start_ms_chunk, end_ms_chunk, logger)
                append_to_combined(df, logger)
                # Capture count before freeing the dataframe
                row_count = len(buffer)
                try:
                    del df
                except Exception:
                    pass
                gc.collect()
                manifest.setdefault("chunks", []).append({
                    "index": chunk_index,
                    "filename": os.path.relpath(path, DATA_ROOT),
                    "start_ms": start_ms_chunk,
                    "end_ms": end_ms_chunk,
                    "count": row_count,
                    "sha256": sha256,
                })
                manifest["last_candle_ms"] = max(int(manifest.get("last_candle_ms") or 0), end_ms_chunk)
                save_manifest(manifest)
                chunk_index += 1

    # Build combined Parquet from all chunks at the end of backfill only if environment variable requests it
    try:
        if os.environ.get("BUILD_COMBINED_ON_BACKFILL", "0") in ("1", "true", "TRUE", "yes", "YES"):
            build_combined_parquet(manifest, logger)
        else:
            logger.info("Skipping combined Parquet build after backfill (BUILD_COMBINED_ON_BACKFILL not enabled).")
    except Exception as e:
        logger.warning(f"Failed to build combined Parquet: {e}")

    logger.info(f"Backfill complete. Total klines added: {total_added}")
    # Explicitly free large objects and trigger a full garbage collection to minimize memory usage after backfill
    try:
        ex.close() if hasattr(ex, 'close') else None
    except Exception:
        pass
    locals_to_clear = ['df', 'buffer', 'rows', 'chunk', 'manifest']
    for name in locals_to_clear:
        if name in locals():
            try:
                del locals()[name]
            except Exception:
                pass
    gc.collect()


def verify_continuity(logger: logging.Logger, start_iso: Optional[str] = None, end_iso: Optional[str] = None, repair: bool = False) -> bool:
    """Verify that the dataset has every 1-minute candle without gaps.

    Checks both intra-chunk spacing and cross-chunk boundaries. Optionally restricts
    verification to a given [start,end] ISO window. If repair=True, removes all chunks
    from the first detected gap onward and triggers backfill to re-collect from the
    missing minute up to end (or last closed minute if end not provided).
    """
    manifest = load_manifest()
    chunks = sorted(manifest.get("chunks", []), key=lambda c: c["index"])
    if not chunks:
        logger.info("No chunks present; nothing to verify.")
        return True

    # Parse window in ms if provided
    window_start_ms = iso_to_ms(start_iso) if start_iso else chunks[0]["start_ms"]
    window_end_ms = iso_to_ms(end_iso) if end_iso else int(manifest.get("last_candle_ms") or chunks[-1]["end_ms"])  # type: ignore

    gap_found_at: Optional[int] = None

    # Verify intra-chunk spacing and locate first gap inside window
    for ch in chunks:
        # Skip chunks outside window
        if ch["end_ms"] < window_start_ms or ch["start_ms"] > window_end_ms:
            continue
        # Load parquet and check 1m diffs
        try:
            path = os.path.join(DATA_ROOT, ch["filename"])  # type: ignore
            if not os.path.exists(path):
                logger.error(f"Missing chunk file on disk: {path}")
                gap_found_at = gap_found_at or ch["start_ms"]
                break
            df = _read_parquet(path) if pd is not None else None
            if df is not None and len(df) > 0:
                # Time monotonicity and uniqueness
                if not df["open_time"].is_monotonic_increasing:  # type: ignore
                    logger.error(f"Chunk {ch['index']} has non-monotonic open_time ordering")
                    gap_found_at = gap_found_at or ch["start_ms"]
                    break
                if not df["open_time"].is_unique:  # type: ignore
                    logger.error(f"Chunk {ch['index']} has duplicate open_time values")
                    gap_found_at = gap_found_at or ch["start_ms"]
                    break
                # Boundaries match manifest
                first_ot = int(df.iloc[0]["open_time"])  # type: ignore
                last_ot = int(df.iloc[-1]["open_time"])  # type: ignore
                if first_ot != int(ch["start_ms"]) or last_ot != int(ch["end_ms"]):
                    logger.error(f"Chunk {ch['index']} boundary mismatch: df [{first_ot},{last_ot}] vs manifest [{ch['start_ms']},{ch['end_ms']}]")
                    gap_found_at = gap_found_at or min(first_ot, int(ch["start_ms"]))
                    break
                if len(df) > 1:
                    diffs = df["open_time"].diff().dropna().astype(int)
                    if not (diffs == 60_000).all():
                        # Find first bad index
                        bad_idx = diffs[diffs != 60_000].index[0]
                        gap_found_at = gap_found_at or int(df.iloc[bad_idx]["open_time"]) - 60_000
                        logger.error(f"Intra-chunk gap detected in chunk {ch['index']} around {ms_to_iso(int(df.iloc[bad_idx]['open_time']))}")
                        break
        except Exception as e:
            logger.error(f"Failed to read/verify chunk {ch['index']}: {e}")
            gap_found_at = gap_found_at or ch["start_ms"]
            break

    # Invariant: no new chunk should start while previous chunk is not full
    for i, ch in enumerate(chunks[:-1]):
        # Only consider chunks within window
        if ch["end_ms"] < window_start_ms or ch["start_ms"] > window_end_ms:
            continue
        if int(ch.get("count", 0)) < int(manifest.get("chunk_size", CHUNK_SIZE)):
            logger.error(f"Invariant violation: chunk {ch['index']} has count {ch.get('count')} < chunk_size while a subsequent chunk exists")
            gap_found_at = gap_found_at or int(ch["end_ms"]) + 60_000
            break

    # Verify cross-chunk boundaries
    prev_end = None
    for ch in chunks:
        if ch["end_ms"] < window_start_ms or ch["start_ms"] > window_end_ms:
            continue
        if prev_end is None:
            prev_end = ch["end_ms"]
            continue
        expected = prev_end + 60_000
        if ch["start_ms"] != expected:
            logger.error(f"Continuity error between chunks {ch['index']-1} and {ch['index']}: expected start {expected} got {ch['start_ms']}")
            gap_found_at = gap_found_at or expected
            break
        prev_end = ch["end_ms"]

    if gap_found_at is None:
        logger.info("Continuity verified: no gaps found in requested window.")
        return True

    # If repair requested, remove bad tail and backfill from gap_found_at
    if repair:
        logger.warning(f"Repair requested. Will remove chunks from first affected at/after {ms_to_iso(gap_found_at)} and re-download.")
        # Determine first affected chunk index
        first_bad_idx = None
        for ch in chunks:
            if ch["end_ms"] >= gap_found_at:
                first_bad_idx = ch["index"]
                break
        if first_bad_idx is None:
            first_bad_idx = chunks[-1]["index"]
        # Delete files and trim manifest
        kept = []
        for ch in chunks:
            if ch["index"] < first_bad_idx:
                kept.append(ch)
            else:
                try:
                    path = os.path.join(DATA_ROOT, ch["filename"])  # type: ignore
                    if os.path.exists(path):
                        os.remove(path)
                        logger.info(f"Deleted chunk file during repair: {path}")
                except Exception as e:
                    logger.warning(f"Failed to delete chunk during repair: {e}")
        manifest["chunks"] = kept
        manifest["last_candle_ms"] = kept[-1]["end_ms"] if kept else None
        save_manifest(manifest)
        # Trigger backfill from the missing minute up to window_end_ms
        target_end = window_end_ms if end_iso else (utc_now_ms() - 60_000)
        logger.info(f"Re-collecting from {ms_to_iso(gap_found_at)} to {ms_to_iso(target_end)}...")
        backfill(logger, start_ms=gap_found_at, end_ms=target_end, chunk_size=manifest.get("chunk_size", CHUNK_SIZE), align_window=False)
        # Re-verify quickly
        manifest = load_manifest()
        chunks = sorted(manifest.get("chunks", []), key=lambda c: c["index"])
        ok = True
        prev_end = None
        for ch in chunks:
            if ch["end_ms"] < window_start_ms or ch["start_ms"] > target_end:
                continue
            if prev_end is None:
                prev_end = ch["end_ms"]
                continue
            expected = prev_end + 60_000
            if ch["start_ms"] != expected:
                ok = False
                break
            prev_end = ch["end_ms"]
        if ok:
            logger.info("Repair complete and continuity re-verified.")
        else:
            logger.error("Repair attempted but continuity still broken.")
        return ok

    # No repair; just report failure
    return False


def verify_schema_and_values(df: 'pd.DataFrame') -> Tuple[bool, str]:
    """Strong numeric schema and bounds assertions for a single chunk dataframe.
    Returns (ok, message)."""
    if pd is None or df is None:
        return True, "pandas not available"
    # Dtypes
    try:
        if not pd.api.types.is_integer_dtype(df["open_time"]):
            return False, "open_time must be int64"
        if not pd.api.types.is_integer_dtype(df["close_time"]):
            return False, "close_time must be int64"
        if not pd.api.types.is_integer_dtype(df["trades"]):
            return False, "trades must be int64"
        for c in ["open","high","low","close","volume","quote_volume","taker_base_volume","taker_quote_volume"]:
            if not pd.api.types.is_float_dtype(df[c]):
                return False, f"{c} must be float64"
    except Exception as e:
        return False, f"schema fields missing: {e}"
    # Time grid
    if not (df["open_time"] % 60000 == 0).all():
        return False, "open_time must be aligned to 1-minute"
    if not (df["close_time"] == df["open_time"] + 59_999).all():
        return False, "close_time must equal open_time + 59_999"
    # OHLC ordering and non-negativity
    if (df[["open","high","low","close","volume","quote_volume","taker_base_volume","taker_quote_volume"]] < 0).any().any():
        return False, "negative values present"
    # low <= min(open,close) <= high
    mins = df[["open","close"]].min(axis=1)
    if (df["low"] > mins).any():
        return False, "low greater than min(open,close)"
    if (df["high"] < mins).any():
        return False, "high less than min(open,close)"
    # open <= high and low <= close
    if (df["open"] > df["high"]).any():
        return False, "open greater than high"
    if (df["low"] > df["close"]).any():
        return False, "low greater than close"
    # Time sort
    if not df["open_time"].is_monotonic_increasing:
        return False, "rows not sorted by open_time"
    if not df["open_time"].is_unique:
        return False, "duplicate open_time within chunk"
    return True, "ok"


def verify_manifest(logger: logging.Logger) -> bool:
    """Comprehensive manifest vs filesystem parity and filename/content checks.
    Returns True if all checks pass, else False.
    """
    manifest = load_manifest()
    chunks = sorted(manifest.get("chunks", []), key=lambda c: c["index"]) if manifest else []
    ok = True

    # Check chunk indices strictly increase by 1
    for i, ch in enumerate(chunks, start=1):
        if ch.get("index") != i:
            logger.error(f"Manifest indices not contiguous at position {i}: found {ch.get('index')}")
            ok = False
            break

    # Orphan files detection
    try:
        on_disk = set(os.listdir(CHUNKS_DIR)) if os.path.isdir(CHUNKS_DIR) else set()
        referenced = set(os.path.basename(c.get("filename","")) for c in chunks)
        orphans = [n for n in on_disk if n.endswith('.parquet') and n not in referenced]
        if orphans:
            logger.error(f"Orphan parquet files not in manifest: {orphans}")
            ok = False
    except Exception as e:
        logger.warning(f"Failed to scan chunks dir for orphans: {e}")

    # Validate each manifest entry
    for ch in chunks:
        rel = ch.get("filename")
        if not rel:
            logger.error(f"Chunk {ch.get('index')} missing filename in manifest")
            ok = False
            continue
        path = os.path.join(DATA_ROOT, rel)
        if not os.path.exists(path):
            logger.error(f"Manifest refers to missing file: {path}")
            ok = False
            continue
        # sha256 match
        actual_sha = hash_file_sha256(path)
        if actual_sha != ch.get("sha256"):
            logger.error(f"SHA256 mismatch for {rel}: manifest={ch.get('sha256')} actual={actual_sha}")
            ok = False
        # Load and verify boundaries and counts
        try:
            df = _read_parquet(path)
            # Schema/value sanity
            sch_ok, msg = verify_schema_and_values(df)
            if not sch_ok:
                logger.error(f"Schema/value check failed for {rel}: {msg}")
                ok = False
            # Count
            if int(ch.get("count", -1)) != len(df):
                logger.error(f"Count mismatch for {rel}: manifest={ch.get('count')} actual={len(df)}")
                ok = False
            # Start/End
            first_ot = int(df.iloc[0]["open_time"]) if len(df) else None
            last_ot = int(df.iloc[-1]["open_time"]) if len(df) else None
            if first_ot != int(ch.get("start_ms")) or last_ot != int(ch.get("end_ms")):
                logger.error(f"Boundary mismatch for {rel}: manifest [{ch.get('start_ms')},{ch.get('end_ms')}] vs df [{first_ot},{last_ot}]")
                ok = False
        except Exception as e:
            logger.error(f"Failed to read/validate {rel}: {e}")
            ok = False
        # Filename tokens consistency
        base = os.path.basename(rel)
        try:
            name, ext = base.split('.', 1)
            parts = name.split('_')
            _, idx_str, s_str, e_str = parts[0], parts[1], parts[2], parts[3]
            if int(idx_str) != int(ch.get("index")) or int(s_str) != int(ch.get("start_ms")) or int(e_str) != int(ch.get("end_ms")):
                logger.error(f"Filename tokens mismatch for {base} vs manifest {ch}")
                ok = False
        except Exception:
            logger.error(f"Filename format invalid for {base}")
            ok = False

    # Cross-chunk overlap/continuity check at manifest level
    for i in range(1, len(chunks)):
        prev = chunks[i-1]
        cur = chunks[i]
        if int(cur.get("start_ms")) != int(prev.get("end_ms")) + 60_000:
            logger.error(f"Manifest continuity error between index {prev.get('index')} and {cur.get('index')}")
            ok = False

    return ok


def _fs_stats(path: str) -> Tuple[int, int, int]:
    try:
        import shutil
        total, used, free = shutil.disk_usage(path)
        return int(total), int(used), int(free)
    except Exception:
        return 0, 0, 0


def _last_closed_minute_ms() -> int:
    return utc_now_ms() - 60_000


def _compute_minute_lag(manifest: Dict[str, Any]) -> int:
    try:
        last_ms = int(manifest.get("last_candle_ms") or 0)
    except Exception:
        last_ms = 0
    return max(0, int((_last_closed_minute_ms() - last_ms) // 60_000))


def _pending_files_count() -> int:
    try:
        if not os.path.isdir(PENDING_DIR):
            return 0
        return sum(1 for n in os.listdir(PENDING_DIR) if n.endswith('.parquet'))
    except Exception:
        return 0


def build_health(logger: logging.Logger) -> Dict[str, Any]:
    manifest = load_manifest()
    chunks = manifest.get("chunks", []) or []
    chunk_count = len(chunks)
    last_chunk_index = chunks[-1]["index"] if chunk_count else None
    last_chunk_count = chunks[-1]["count"] if chunk_count else None
    last_ms = manifest.get("last_candle_ms")
    health = {
        "exchange": manifest.get("exchange", EXCHANGE_ID),
        "symbol": manifest.get("symbol", SYMBOL),
        "timeframe": manifest.get("timeframe", TIMEFRAME),
        "data_root": os.path.abspath(DATA_ROOT),
        "chunk_count": chunk_count,
        "last_chunk_index": last_chunk_index,
        "last_chunk_count": last_chunk_count,
        "last_candle_ms": int(last_ms) if last_ms is not None else None,
        "last_candle_iso": ms_to_iso(last_ms) if last_ms else None,
        "minute_lag": _compute_minute_lag(manifest),
        "pending_files_count": _pending_files_count() if LIVE_USE_PENDING else None,
        "last_success_utc": _HEALTH_STATE.get("last_success_utc"),
        "last_error_utc": _HEALTH_STATE.get("last_error_utc"),
        "retry_rate_5m": None,
        "retry_rate_1h": None,
        "fs_free_bytes": None,
        "fs_total_bytes": None,
        "fs_free_ratio": None,
        "updated_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    # retry rates
    try:
        now = time.time()
        rts = _HEALTH_STATE.get("retry_timestamps", [])
        rate5 = len([t for t in rts if t >= now - 300])
        rate1h = len([t for t in rts if t >= now - 3600])
        health["retry_rate_5m"] = rate5
        health["retry_rate_1h"] = rate1h
    except Exception:
        pass
    # fs stats
    total, used, free = _fs_stats(DATA_ROOT if os.path.isdir(DATA_ROOT) else os.getcwd())
    if total > 0:
        health["fs_free_bytes"] = int(free)
        health["fs_total_bytes"] = int(total)
        health["fs_free_ratio"] = float(free / total)
    return health


def write_heartbeat(logger: logging.Logger) -> Dict[str, Any]:
    health = build_health(logger)
    try:
        _atomic_write_json(HEALTH_FILE, health)
    except Exception as e:
        logger.warning(f"Failed to write heartbeat: {e}")
    return health


def status(logger: logging.Logger):
    manifest = load_manifest()
    chunks = manifest.get("chunks", [])
    last_ms = manifest.get("last_candle_ms")
    print(json.dumps({
        "exchange": manifest.get("exchange"),
        "symbol": manifest.get("symbol"),
        "timeframe": manifest.get("timeframe"),
        "format": manifest.get("format"),
        "columns": manifest.get("columns"),
        "chunk_count": len(chunks),
        "last_candle_iso": ms_to_iso(last_ms) if last_ms else None,
        "data_root": os.path.abspath(DATA_ROOT),
        "dataset_dir": os.path.abspath(CHUNKS_DIR),
    }, indent=2))


def live(logger: logging.Logger, chunk_size: int = CHUNK_SIZE):
    if pd is None:
        raise RuntimeError("pandas is required. Please install dependencies from requirements.txt")

    ensure_dirs()
    ex, resolved_symbol = create_exchange(logger)
    manifest = load_manifest()
    stop = False

    if manifest.get("last_candle_ms") is None:
        logger.info("Manifest has no data yet. Starting initial backfill first...")
        backfill(logger, chunk_size=chunk_size, align_window=False)
        manifest = load_manifest()

    # Always ensure we are fully caught up to now by computing gaps from last_candle_ms to last closed minute
    target_end = utc_now_ms() - 60_000
    request_start = int(manifest.get("last_candle_ms", 0)) + 60_000
    if request_start <= target_end:
        logger.info(f"Live mode pre-sync: filling gaps from {ms_to_iso(request_start)} to {ms_to_iso(target_end)} before starting the minute loop")
        backfill(logger, start_ms=request_start, end_ms=target_end, chunk_size=chunk_size, align_window=False)
        manifest = load_manifest()

    last_ms = int(manifest["last_candle_ms"])  # last closed candle time

    logger.info("Starting live updater. Will fetch any new klines every minute and maintain chunking.")
    # Heartbeat to provide visibility even when there are no new klines
    last_heartbeat_min = None
    heartbeat_interval_sec = 60
    while not stop:  # pragma: no cover (long-running loop)
        try:
            # Periodic heartbeat write regardless of data arrival
            try:
                hb = write_heartbeat(logger)
                if hb.get("minute_lag", 9999) <= 1:
                    logger.info("Heartbeat: up-to-date")
            except Exception:
                pass
            # Target up to the last fully-closed candle
            target_end = utc_now_ms() - 60_000
            since_ms = last_ms + 60_000
            if since_ms <= target_end:
                # Stream pages directly into chunk files without building a large in-memory list
                behind_min = int((target_end - since_ms) / 60000) + 1 if target_end >= since_ms else 0
                logger.info(f"New data available: catching up {behind_min} minute(s) from {ms_to_iso(since_ms)} to {ms_to_iso(target_end)}")
                page_since = since_ms
                # Work buffer that we will flush into chunk files; keep it bounded
                work_buffer: Optional['pd.DataFrame'] = None

                # Load current manifest/chunk meta for appending logic
                manifest = load_manifest()
                chunks_meta = manifest.get("chunks", [])
                if chunks_meta:
                    last_chunk = chunks_meta[-1]
                    current_index = last_chunk["index"]
                    current_count = last_chunk["count"]
                else:
                    current_index = 1
                    current_count = 0

                rows = fetch_klines_page(ex, resolved_symbol, TIMEFRAME, page_since, KLINE_LIMIT, logger)
                while rows:
                    rows = normalize_kline_rows(rows, logger)
                    rows = [r for r in rows if r and len(r) >= 12 and r[0] <= target_end]
                    if not rows:
                        break

                    # Convert this page to a small DataFrame, process, then discard
                    page_df = pd.DataFrame(rows, columns=KLINE_COLUMNS)  # type: ignore
                    page_df = ensure_numeric_schema(page_df, logger)

                    # If there is an existing partial chunk, attempt to append remainder up to chunk_size
                    if chunks_meta and current_count < chunk_size:
                        prev_path = os.path.join(DATA_ROOT, chunks_meta[-1]["filename"])  # type: ignore
                        prev_df = _read_parquet(prev_path)
                        prev_df = ensure_numeric_schema(prev_df, logger)
                        remainder = chunk_size - current_count
                        head = page_df.iloc[:remainder]
                        if len(head) > 0:
                            head = ensure_numeric_schema(head.copy(), logger)
                            new_df = pd.concat([prev_df, head], ignore_index=True)
                            new_df = ensure_numeric_schema(new_df, logger)
                            end_ms_chunk = int(new_df.iloc[-1]["open_time"])  # type: ignore
                            _atomic_write_parquet(new_df, prev_path, logger)
                            # Persist per-minute deltas into pending/ for readers
                            try:
                                if LIVE_USE_PENDING and len(head) > 0:
                                    for _, r in head.iterrows():
                                        _write_pending_delta(pd.DataFrame([r.values], columns=head.columns), logger)
                            except Exception:
                                pass
                            # Skip sha256 on minute append; only compute on chunk close
                            sha256_val = chunks_meta[-1].get("sha256")
                            if len(new_df) >= chunk_size:
                                sha256_val = hash_file_sha256(prev_path)
                            chunks_meta[-1].update({
                                "end_ms": end_ms_chunk,
                                "count": len(new_df),
                                "sha256": sha256_val,
                            })
                            manifest["chunks"] = chunks_meta
                            manifest["last_candle_ms"] = end_ms_chunk
                            maybe_write_manifest(manifest, force=False)
                            if len(new_df) >= chunk_size:
                                _compact_today_if_needed(manifest, logger, chunk_size)
                            # Update counters and trim page_df
                            current_count = len(new_df)
                            page_df = page_df.iloc[len(head):]
                            try:
                                del new_df
                                del prev_df
                            except Exception:
                                pass
                            gc.collect()

                    # Fill the existing last chunk fully before creating any new chunk
                    while len(page_df) > 0:
                        # Refresh manifest and chunk meta to get the latest count/state
                        manifest = load_manifest()
                        chunks_meta = manifest.get("chunks", [])

                        # If there is a partial chunk, append to it up to the remainder
                        # Determine allowed size for the last day considering timezone switches (24 or 25 hours)
                        if chunks_meta and chunks_meta[-1]["count"] < chunk_size:
                            prev_path = os.path.join(DATA_ROOT, chunks_meta[-1]["filename"])  # type: ignore
                            prev_df = _read_parquet(prev_path)
                            prev_df = ensure_numeric_schema(prev_df, logger)
                            remainder = chunk_size - int(chunks_meta[-1]["count"])
                            head = page_df.iloc[:remainder]
                            if len(head) == 0:
                                break
                            head = ensure_numeric_schema(head.copy(), logger)
                            new_df = pd.concat([prev_df, head], ignore_index=True)
                            new_df = ensure_numeric_schema(new_df, logger)
                            end_ms_chunk = int(new_df.iloc[-1]["open_time"])  # type: ignore
                            _atomic_write_parquet(new_df, prev_path, logger)
                            # Write per-minute deltas into pending/
                            try:
                                if LIVE_USE_PENDING and len(head) > 0:
                                    for _, r in head.iterrows():
                                        _write_pending_delta(pd.DataFrame([r.values], columns=head.columns), logger)
                            except Exception:
                                pass
                            # Skip sha256 until chunk is full
                            sha256_val = chunks_meta[-1].get("sha256")
                            if len(new_df) >= chunk_size:
                                sha256_val = hash_file_sha256(prev_path)
                            chunks_meta[-1].update({
                                "end_ms": end_ms_chunk,
                                "count": len(new_df),
                                "sha256": sha256_val,
                            })
                            manifest["chunks"] = chunks_meta
                            manifest["last_candle_ms"] = end_ms_chunk
                            maybe_write_manifest(manifest, force=False)
                            if len(new_df) >= chunk_size:
                                _compact_today_if_needed(manifest, logger, chunk_size)
                            # Trim consumed rows and free
                            page_df = page_df.iloc[len(head):]
                            try:
                                del prev_df
                                del new_df
                            except Exception:
                                pass
                            gc.collect()
                            continue

                        # Otherwise, start a new chunk from remaining page_df (up to chunk_size)
                        # HARD GUARD: do not start a new chunk if the last chunk is not yet full
                        manifest = load_manifest()
                        chunks_meta = manifest.get("chunks", [])
                        if chunks_meta and int(chunks_meta[-1]["count"]) < chunk_size:
                            # There is still a partial chunk; continue the while loop to append
                            continue
                        take = min(chunk_size, len(page_df))
                        part = page_df.iloc[:take]
                        page_df = page_df.iloc[take:]

                        # Determine new chunk index (last chunk is full or absent)
                        current_index = (chunks_meta[-1]["index"] + 1) if chunks_meta else 1  # type: ignore
                        start_ms_chunk = int(part.iloc[0]["open_time"])  # type: ignore
                        end_ms_chunk = int(part.iloc[-1]["open_time"])  # type: ignore
                        path, sha256 = write_chunk_parquet(part, current_index, start_ms_chunk, end_ms_chunk, logger)
                        chunks_meta.append({
                            "index": current_index,
                            "filename": os.path.relpath(path, DATA_ROOT),
                            "start_ms": start_ms_chunk,
                            "end_ms": end_ms_chunk,
                            "count": len(part),
                            "sha256": sha256,
                        })
                        manifest["chunks"] = chunks_meta
                        manifest["last_candle_ms"] = end_ms_chunk
                        maybe_write_manifest(manifest, force=True)
                        # Explicitly free part DataFrame
                        try:
                            del part
                        except Exception:
                            pass
                        gc.collect()

                    # Update last_ms for the loop
                    if chunks_meta:
                        last_ms = chunks_meta[-1]["end_ms"]

                    # Fetch next page if we still have to catch up
                    next_since = rows[-1][0] + 60_000
                    if next_since > target_end:
                        rows = []
                    else:
                        page_since = next_since
                        rows = fetch_klines_page(ex, resolved_symbol, TIMEFRAME, page_since, KLINE_LIMIT, logger)

                    # Cleanup per page
                    try:
                        del page_df
                    except Exception:
                        pass
                    gc.collect()

                # End while rows
            else:
                logger.debug("Already up-to-date with last closed kline.")
        except KeyboardInterrupt:
            logger.info("Live updater stopped by user (KeyboardInterrupt). Shutting down gracefully...")
            stop = True
            break
        except Exception as e:
            logger.exception(f"Live updater error: {e}")
        # Periodic cleanup to keep memory low
        gc.collect()
        # Heartbeat once per minute so the terminal shows activity even when up-to-date
        now = datetime.now(timezone.utc)
        current_min = now.replace(second=0, microsecond=0)
        if last_heartbeat_min != current_min:
            last_heartbeat_min = current_min
            try:
                # Guard against logging failures impacting the loop
                logger.info(f"Heartbeat: up-to-date. Last candle: {ms_to_iso(last_ms)}; next check at {now.replace(second=0, microsecond=0) + timedelta(minutes=1)} UTC")
            except Exception:
                # Swallow logging errors (e.g., rotation races) and continue
                pass
        sleep_sec = 60 - (now.second + now.microsecond / 1_000_000.0)
        try:
            time.sleep(max(1.0, sleep_sec))
        except KeyboardInterrupt:
            logger.info("Live updater sleep interrupted by user. Exiting...")
            stop = True
            break

    # Cleanup exchange on exit from live
    try:
        ex.close() if hasattr(ex, 'close') else None
    except Exception:
        pass


# --------------------------
# CLI
# --------------------------

def _parse_args(argv: List[str]):
    import argparse
    p = argparse.ArgumentParser(description="Binance 1m Data Loader (spot or USDM futures) with chunking and live updates")
    sub = p.add_subparsers(dest="cmd", required=True)

    # Global options
    p.add_argument("--market", choices=["spot", "futures"], default=MARKET_TYPE, help="Market type: spot (Binance) or futures (Binance USDM)")
    p.add_argument("--pair", default=SYMBOL, help="Trading pair in CCXT format, e.g., BTC/USDT. Will fall back to BTC/USDT if unavailable.")
    p.add_argument("--quiet", action="store_true", help="Reduce console logging")
    p.add_argument("--align-tz", default=os.environ.get("ALIGN_TZ", "UTC"), help="Alignment timezone for start/end day boundaries (IANA name like Europe/Zurich or offset like +02:00). Storage is always UTC.")

    p_backfill = sub.add_parser("backfill", help="Run historical backfill")
    p_backfill.add_argument("--start", help="Start ISO time (UTC). Default 2020-01-01T01:00:00Z", default=START_ISO_UTC)
    p_backfill.add_argument("--end", help="End ISO time (UTC). Default last closed minute", default=None)
    p_backfill.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)

    p_live = sub.add_parser("live", help="Run continuous live updater (will trigger backfill if needed)")
    p_live.add_argument("--chunk-size", type=int, default=CHUNK_SIZE)

    p_verify = sub.add_parser("verify", help="Verify continuity across chunks and within chunks; optionally repair")
    p_verify.add_argument("--start", help="Optional start ISO (UTC) for verification window", default=None)
    p_verify.add_argument("--end", help="Optional end ISO (UTC) for verification window", default=None)
    p_verify.add_argument("--repair", action="store_true", help="If set, will delete bad tail and backfill from first gap")

    p_combine = sub.add_parser("combine", help="Combine all chunk Parquet files into a single Parquet; filename annotated with timezone label")
    p_combine.add_argument("--from-index", type=int, default=None, help="Optional first chunk index to include")
    p_combine.add_argument("--to-index", type=int, default=None, help="Optional last chunk index to include")
    p_combine.add_argument("--tz", default=os.environ.get("VIEW_TZ", None), help="Timezone label for filename (e.g., Europe/Zurich or +02:00). If omitted, no tz suffix.")

    sub.add_parser("status", help="Print current status from manifest")

    p_health = sub.add_parser("health", help="Print health JSON and exit non-zero on threshold violations")
    p_health.add_argument("--warn-minute-lag", type=int, default=3, help="Warn if minute_lag exceeds this (exit 2)")
    p_health.add_argument("--crit-minute-lag", type=int, default=10, help="Critical if minute_lag exceeds this (exit 3)")
    p_health.add_argument("--min-free-ratio", type=float, default=0.15, help="Critical if free disk ratio below this (exit 3)")
    p_health.add_argument("--max-retries-5m", type=int, default=10, help="Warn if retries in last 5m exceed this (exit 2)")

    p_audit = sub.add_parser("audit", help="Run full dataset audit (manifest parity + continuity). Exits non-zero on failure")
    p_audit.add_argument("--start", default=None, help="Optional start ISO for continuity window")
    p_audit.add_argument("--end", default=None, help="Optional end ISO for continuity window")

    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None):
    global MARKET_TYPE, EXCHANGE_ID, SYMBOL, PAIR_TOKEN, DATA_ROOT, CHUNKS_DIR, COMBINED_DIR, COMBINED_FILE, MANIFEST_FILE
    args = _parse_args(argv or sys.argv[1:])

    # Apply market and pair selection at runtime
    MARKET_TYPE = args.market
    EXCHANGE_ID = "binanceusdm" if MARKET_TYPE == "futures" else "binance"
    # Normalize pair
    SYMBOL = args.pair.replace(" ", "").replace("-", "/").upper()
    PAIR_TOKEN = _symbol_to_pair_token(SYMBOL)
    # Apply alignment timezone for this session
    global ALIGN_TZ
    ALIGN_TZ = args.align_tz
    # Recompute data paths based on market and pair
    DEFAULT_DATA_BASE = os.environ.get("MARKET_DATA_DIR", r"C:\\MarketData")
    DATA_ROOT = os.path.join(DEFAULT_DATA_BASE, EXCHANGE_ID, PAIR_TOKEN, TIMEFRAME)
    CHUNKS_DIR = os.path.join(DATA_ROOT, "chunks")
    COMBINED_DIR = os.path.join(DATA_ROOT, "combined")
    COMBINED_FILE = os.path.join(COMBINED_DIR, f"{PAIR_TOKEN}_{TIMEFRAME}_all.parquet")
    MANIFEST_FILE = os.path.join(DATA_ROOT, "manifest.json")
    global HEALTH_FILE
    HEALTH_FILE = os.path.join(DATA_ROOT, "health.json")
    # Recompute pending paths
    global PENDING_DIR, PENDING_TMP_DIR
    PENDING_DIR = os.path.join(DATA_ROOT, "pending")
    PENDING_TMP_DIR = os.path.join(PENDING_DIR, ".tmp")

    logger = setup_logging(verbose=not args.quiet)

    try:
        if args.cmd == "backfill":
            start_ms = iso_to_ms(args.start) if args.start else iso_to_ms(START_ISO_UTC)
            end_ms = iso_to_ms(args.end) if args.end else None
            backfill(logger, start_ms=start_ms, end_ms=end_ms, chunk_size=args.chunk_size, align_window=True)
        elif args.cmd == "live":
            live(logger, chunk_size=args.chunk_size)
        elif args.cmd == "verify":
            ok = verify_continuity(logger, start_iso=args.start, end_iso=args.end, repair=args.repair)
            sys.exit(0 if ok else 2)
        elif args.cmd == "combine":
            manifest = load_manifest()
            out = build_combined_parquet(manifest, logger, from_index=args.from_index, to_index=args.to_index, tz_name=args.tz)
            if out:
                logger.info(f"Combined parquet built: {out}")
            else:
                logger.info("Combine skipped or failed.")
        elif args.cmd == "status":
            status(logger)
        elif args.cmd == "health":
            health = build_health(logger)
            print(json.dumps(health, indent=2))
            # evaluate thresholds
            exit_code = 0
            lag = health.get("minute_lag") or 0
            free_ratio = health.get("fs_free_ratio") or 0.0
            retries5 = health.get("retry_rate_5m") or 0
            if lag >= args.crit_minute_lag or (free_ratio and free_ratio < args.min_free_ratio):
                exit_code = 3
            elif lag >= args.warn_minute_lag or retries5 > args.max_retries_5m:
                exit_code = 2
            sys.exit(exit_code)
        elif args.cmd == "audit":
            man_ok = verify_manifest(logger)
            cont_ok = verify_continuity(logger, start_iso=args.start, end_iso=args.end, repair=False)
            if man_ok and cont_ok:
                logger.info("AUDIT OK: manifest parity and continuity verified.")
                sys.exit(0)
            else:
                logger.error("AUDIT FAILED.")
                sys.exit(3)
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting.")
        try:
            # best-effort cleanup
            pass
        finally:
            sys.exit(0)


if __name__ == "__main__":
    main()
