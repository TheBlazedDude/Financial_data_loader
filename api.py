import os
from typing import Optional
from fastapi import FastAPI, BackgroundTasks, Query, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from binance_usdm_loader import (
    setup_logging,
    backfill as _backfill,
    live as _live,
    build_combined_parquet as _combine,
    load_manifest,
    DATA_ROOT,
    CHUNKS_DIR,
    COMBINED_DIR,
)

app = FastAPI(title="Financial Data Loader API")


class BackfillRequest(BaseModel):
    start: Optional[str] = None  # ISO8601
    end: Optional[str] = None    # ISO8601
    chunk_size: Optional[int] = None
    align_window: Optional[bool] = None
    tz: Optional[str] = None     # Alignment timezone override (IANA or +HH:MM)
    market: Optional[str] = None # 'spot' or 'futures'
    pair: Optional[str] = None   # e.g., BTC/USDT


def _apply_market_pair_tz(market: Optional[str], pair: Optional[str], tz: Optional[str]):
    """Adjust loader globals for market/pair/tz for this request.
    Keeps changes within current process; suitable for background tasks.
    """
    import binance_usdm_loader as l
    changed = False
    if market or pair:
        # Normalize
        m = market if market else l.MARKET_TYPE
        s = (pair if pair else l.SYMBOL).replace(" ", "").replace("-", "/").upper()
        l.MARKET_TYPE = m
        l.EXCHANGE_ID = "binanceusdm" if m == "futures" else "binance"
        l.SYMBOL = s
        l.PAIR_TOKEN = l._symbol_to_pair_token(s)
        # Recompute paths
        l.DEFAULT_DATA_BASE = os.environ.get("MARKET_DATA_DIR", r"C:\\MarketData")
        l.DATA_ROOT = os.path.join(l.DEFAULT_DATA_BASE, l.EXCHANGE_ID, l.PAIR_TOKEN, l.TIMEFRAME)
        l.CHUNKS_DIR = os.path.join(l.DATA_ROOT, "chunks")
        l.COMBINED_DIR = os.path.join(l.DATA_ROOT, "combined")
        l.COMBINED_FILE = os.path.join(l.COMBINED_DIR, f"{l.PAIR_TOKEN}_{l.TIMEFRAME}_all.parquet")
        l.MANIFEST_FILE = os.path.join(l.DATA_ROOT, "manifest.json")
        l.HEALTH_FILE = os.path.join(l.DATA_ROOT, "health.json")
        l.PENDING_DIR = os.path.join(l.DATA_ROOT, "pending")
        l.PENDING_TMP_DIR = os.path.join(l.PENDING_DIR, ".tmp")
        changed = True
    if tz:
        l.ALIGN_TZ = tz
        changed = True
    return changed


@app.on_event("startup")
async def on_startup():
    # Ensure logs and env are set up
    setup_logging(verbose=True)


@app.post("/backfill")
async def backfill_endpoint(req: BackfillRequest, background_tasks: BackgroundTasks):
    logger = setup_logging()

    def task():
        # Apply overrides for this job
        _apply_market_pair_tz(req.market, req.pair, req.tz)
        if req.start:
            from binance_usdm_loader import iso_to_ms
            start_ms_local = iso_to_ms(req.start)
        else:
            start_ms_local = None
        if req.end:
            from binance_usdm_loader import iso_to_ms
            end_ms_local = iso_to_ms(req.end)
        else:
            end_ms_local = None
        _backfill(
            logger,
            start_ms=start_ms_local,
            end_ms=end_ms_local,
            chunk_size=req.chunk_size if req.chunk_size else 1440,
            align_window=req.align_window,
        )

    background_tasks.add_task(task)
    return {"status": "scheduled"}


@app.post("/live")
async def live_endpoint(
    background_tasks: BackgroundTasks,
    market: Optional[str] = Query(None),
    pair: Optional[str] = Query(None),
    tz: Optional[str] = Query(None),
):
    logger = setup_logging()

    def task():
        _apply_market_pair_tz(market, pair, tz)
        _live(logger)

    background_tasks.add_task(task)
    return {"status": "scheduled"}


@app.post("/combine")
async def combine_endpoint(
    tz: Optional[str] = Query(None),
    from_index: Optional[int] = Query(None),
    to_index: Optional[int] = Query(None),
    market: Optional[str] = Query(None),
    pair: Optional[str] = Query(None),
    background_tasks: BackgroundTasks = None,
):
    logger = setup_logging()

    def task():
        _apply_market_pair_tz(market, pair, tz)
        manifest = load_manifest()
        _combine(manifest, logger, from_index=from_index, to_index=to_index, tz_name=tz)

    if background_tasks is not None:
        background_tasks.add_task(task)
        return {"status": "scheduled"}
    else:
        # For direct calling without FastAPI background
        task()
        return {"status": "done"}


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/")
async def root():
    # Redirect landing page to the dataset browser as requested
    return RedirectResponse(url="/browse")


# --------- Simple Browser UI ---------

def _html_page(title: str, body: str) -> str:
    # Use str.format with escaped braces to avoid f-string parsing issues
    tpl = (
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "<head>\n"
        "<meta charset=\"UTF-8\" />\n"
        "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n"
        "<title>{title}</title>\n"
        "<style>\n"
        "  :root {{ --bg:#0f172a; --card:#111827; --text:#e5e7eb; --muted:#9ca3af; --accent:#22d3ee; --link:#93c5fd; }}\n"
        "  body {{ margin:0; font-family: system-ui, -apple-system, Segoe UI, Roboto, Ubuntu, Cantarell, 'Helvetica Neue', Arial; background: var(--bg); color: var(--text); }}\n"
        "  header {{ padding:16px 24px; border-bottom:1px solid #1f2937; position:sticky; top:0; backdrop-filter: blur(6px); background: rgba(15,23,42,0.8); }}\n"
        "  h1 {{ font-size:20px; margin:0; }}\n"
        "  .container {{ max-width: 1080px; padding: 24px; margin: 0 auto; }}\n"
        "  .grid {{ display:grid; grid-template-columns: 1fr; gap:16px; }}\n"
        "  @media(min-width:900px){{ .grid {{ grid-template-columns: 320px 1fr; }} }}\n"
        "  .card {{ background: var(--card); border:1px solid #1f2937; border-radius:12px; padding:16px; }}\n"
        "  .title {{ font-weight:600; margin:0 0 8px 0; color:#f3f4f6; }}\n"
        "  .muted {{ color: var(--muted); font-size: 12px; }}\n"
        "  a {{ color: var(--link); text-decoration: none; }}\n"
        "  a:hover {{ text-decoration: underline; }}\n"
        "  ul {{ list-style: none; padding:0; margin:0; max-height: 65vh; overflow:auto; }}\n"
        "  li {{ padding:8px 6px; border-bottom:1px dashed #1f2937; display:flex; justify-content:space-between; gap:8px; }}\n"
        "  .pill {{ font-size:11px; color:#0f172a; background: var(--accent); padding:2px 6px; border-radius:999px; }}\n"
        "  table {{ width:100%; border-collapse: collapse; font-size: 13px; }}\n"
        "  th, td {{ border-bottom:1px solid #1f2937; padding:8px; text-align:left; }}\n"
        "  th {{ position: sticky; top: 0; background: #0b1220; z-index:1; }}\n"
        "  .toolbar {{ display:flex; gap:8px; align-items:center; margin-bottom:12px; }}\n"
        "  input, select {{ background:#0b1220; color:#e5e7eb; border:1px solid #1f2937; border-radius:8px; padding:6px 8px; }}\n"
        "  .path {{ font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace; font-size:12px; }}\n"
        "</style>\n"
        "</head>\n"
        "<body>\n"
        "  <header><h1>Financial Loader — Browser</h1></header>\n"
        "  <div class=\"container\">{body}</div>\n"
        "</body>\n"
        "</html>\n"
    )
    return tpl.format(title=title, body=body)


def _safe_join(root: str, rel: str) -> str:
    base = os.path.abspath(root)
    target = os.path.abspath(os.path.join(base, rel))
    if not target.startswith(base):
        raise HTTPException(status_code=400, detail="Invalid path")
    return target


def _format_bytes(n: int) -> str:
    for unit in ['B','KB','MB','GB','TB']:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024.0
    return f"{n:.1f} PB"


@app.get("/browse", response_class=HTMLResponse)
async def browse(exchange: Optional[str] = Query(None), pair: Optional[str] = Query(None), tf: Optional[str] = Query("1m")):
    # Multi-dataset browser. When exchange/pair selected, show that dataset's files.
    # Otherwise, show available exchanges and pairs discovered under MARKET_DATA_DIR.
    from binance_usdm_loader import DEFAULT_DATA_BASE
    market_root = os.path.abspath(DEFAULT_DATA_BASE)

    def list_dir(path: str):
        try:
            return sorted([d for d in os.listdir(path)])
        except Exception:
            return []

    # If no exchange/pair selected, render overview (fast: no deep file scans)
    if not exchange or not pair:
        # Restrict to known exchanges to match expected structure
        known_exchanges = ["binance", "binanceusdm"]
        exchanges = [ex for ex in known_exchanges if os.path.isdir(os.path.join(market_root, ex))]
        items = []
        labels = {"binance": "Spot (binance)", "binanceusdm": "Futures (binanceusdm)"}
        for ex in exchanges:
            ex_path = os.path.join(market_root, ex)
            pairs = [p for p in list_dir(ex_path) if os.path.isdir(os.path.join(ex_path, p))]
            if not pairs:
                continue
            pair_links = [f"<li><a href='/browse?exchange={ex}&pair={p}&tf=1m'>{p}</a></li>" for p in pairs]
            items.append(
                "<div class='card'>"
                + f"<h3 class='title'>{labels.get(ex, ex)}</h3>"
                + f"<div class='muted'>Root</div><div class='path'>{os.path.join(market_root, ex)}</div>"
                + "<div class='muted' style='margin-top:8px'>Pairs</div>"
                + "<ul>" + (''.join(pair_links) or "<li class='muted'>No pairs</li>") + "</ul>"
                + "</div>"
            )
        body = (
            "<div class='grid'>"
            + ("".join(items) or "<div class='card'><div class='muted'>No datasets found under </div><div class='path'>" + market_root + "</div></div>")
            + "</div>"
        )
        return HTMLResponse(content=_html_page("Browse datasets", body))

    # Specific dataset view
    dataset_root = os.path.join(market_root, exchange, pair, tf or "1m")
    chunks_dir = os.path.join(dataset_root, "chunks")
    combined_dir = os.path.join(dataset_root, "combined")

    def list_files(path: str):
        try:
            return sorted([f for f in os.listdir(path) if f.lower().endswith('.parquet')])
        except Exception:
            return []

    chunk_files = list_files(chunks_dir)
    combined_files = list_files(combined_dir)

    chunk_items = []
    for f in chunk_files:
        p = os.path.join(chunks_dir, f)
        try:
            size = os.path.getsize(p)
        except Exception:
            size = 0
        rel = os.path.relpath(p, dataset_root)
        chunk_items.append(
            f"<li ondblclick=\"window.open('/browse/file?file={rel}&exchange={exchange}&pair={pair}&tf={tf}','_blank');\"><span class='path'>{f}</span><span class='pill'>{_format_bytes(size)}</span> <a target='_blank' href='/browse/file?file={rel}&exchange={exchange}&pair={pair}&tf={tf}'>view</a></li>"
        )

    combined_items = []
    for f in combined_files:
        p = os.path.join(combined_dir, f)
        try:
            size = os.path.getsize(p)
        except Exception:
            size = 0
        rel = os.path.relpath(p, dataset_root)
        combined_items.append(
            f"<li ondblclick=\"window.open('/browse/file?file={rel}&exchange={exchange}&pair={pair}&tf={tf}','_blank');\"><span class='path'>{f}</span><span class='pill'>{_format_bytes(size)}</span> <a target='_blank' href='/browse/file?file={rel}&exchange={exchange}&pair={pair}&tf={tf}'>view</a></li>"
        )

    body = (
        "<div class='grid'>"
        + "  <div class='card'>"
        + "    <h3 class='title'>Dataset</h3>"
        + "    <div class='muted'>Dataset root</div>"
        + "    <div class='path'>" + os.path.abspath(dataset_root) + "</div>"
        + "    <div class='muted' style='margin-top:8px'>Chunks directory</div>"
        + "    <div class='path'>" + os.path.abspath(chunks_dir) + "</div>"
        + "    <div class='muted' style='margin-top:8px'>Combined directory</div>"
        + "    <div class='path'>" + os.path.abspath(combined_dir) + "</div>"
        + f"    <div style='margin-top:12px;'><a href='/browse'>&larr; All datasets</a> &middot; <a href='/browse?exchange={exchange}&pair={pair}&tf={tf}'>Refresh</a></div>"
        + "  </div>"
        + "  <div class='card'>"
        + "    <h3 class='title'>Files</h3>"
        + "    <div class='muted'>Combined</div>"
        + "    <ul>" + (''.join(combined_items) or "<li class='muted'>No combined parquet yet.</li>") + "</ul>"
        + "    <div class='muted' style='margin-top:12px'>Chunks</div>"
        + "    <ul>" + (''.join(chunk_items) or "<li class='muted'>No chunks yet.</li>") + "</ul>"
        + "  </div>"
        + "</div>"
    )
    return HTMLResponse(content=_html_page(f"Browser — {exchange}/{pair}/{tf}", body))


@app.get("/browse/file", response_class=HTMLResponse)
async def browse_file(file: str, exchange: Optional[str] = Query(None), pair: Optional[str] = Query(None), tf: Optional[str] = Query("1m"), limit: int = Query(200, ge=1, le=5000), offset: int = Query(0, ge=0)):
    # Render selected Parquet file as a table
    # Resolve dataset root based on query if provided; else fall back to current module's DATA_ROOT
    if exchange and pair:
        from binance_usdm_loader import DEFAULT_DATA_BASE
        dataset_root = os.path.join(os.path.abspath(DEFAULT_DATA_BASE), exchange, pair, tf or "1m")
    else:
        dataset_root = DATA_ROOT
    try:
        abs_path = _safe_join(dataset_root, file)
    except HTTPException:
        raise
    if not os.path.exists(abs_path):
        raise HTTPException(status_code=404, detail="File not found")

    # Lazy import pandas to avoid overhead on health calls
    try:
        import pandas as pd  # type: ignore
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"pandas not available: {e}")

    try:
        df = pd.read_parquet(abs_path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read parquet: {e}")

    total = len(df)
    end = min(offset + limit, total)
    view = df.iloc[offset:end]
    # Ensure numbers displayed nicely and timestamps readable if present
    html_table = view.to_html(classes=['data'], border=0, index=False)

    nav = (
      "<div class='toolbar'>"
      + "<span class='muted'>Showing rows " + str(offset) + "–" + str(end) + " of " + str(total) + "</span>"
      + "<a href='/browse/file?file=" + file + "&limit=" + str(limit) + "&offset=" + str(max(0, offset - limit)) + "'>&larr; Prev</a>"
      + "<a href='/browse/file?file=" + file + "&limit=" + str(limit) + "&offset=" + str(end if end < total else offset) + "'>Next &rarr;</a>"
      + "<a href='/browse' style='margin-left:auto'>Back to files</a>"
      + "</div>"
    )

    body = (
    "<div class='card'>"
    "  <h3 class='title'>File</h3>"
    + "  <div class='path'>" + abs_path + "</div>"
    "</div>"
    "<div class='card'>"
    + "  " + nav + ""
    "  <div style='overflow:auto; max-height:70vh;'>"
    + "    " + html_table + ""
    "  </div>"
    + "  " + nav + ""
    "</div>"
    )
    return HTMLResponse(content=_html_page("View: " + os.path.basename(abs_path), body))
