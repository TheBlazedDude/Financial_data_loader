Financial Data Loader — Docker Guide

Overview
- This guide focuses on using the loader as a Docker image: build, run, call endpoints, view logs, and run helper commands.
- The image exposes a FastAPI server with background jobs (backfill, live, combine) and writes detailed logs.
- Data is stored on a mounted volume as Parquet chunk files with a manifest; combined Parquet is optional.

Image
- Dockerfile included. Default command runs: uvicorn api:app on port 8000.
- Exposes port 8000.
- Volumes: mount your MarketData directory and logs.

Build the image
- docker build -t financial-loader .

Run the container (detached)
Windows PowerShell:
- docker run -d -p 8000:8000 -v C:\\MarketData:/data/MarketData -v ${PWD}\\logs:/app/logs --name financial-loader financial-loader

Windows cmd.exe:
- docker run -d -p 8000:8000 -v C:\\MarketData:/data/MarketData -v %CD%\\logs:/app/logs --name financial-loader financial-loader

WSL/Linux/macOS:
- docker run -d -p 8000:8000 -v /mnt/c/MarketData:/data/MarketData -v $PWD/logs:/app/logs --name financial-loader financial-loader

What the container does
- Starts an HTTP API on http://localhost:8000.
- You trigger jobs via REST: /backfill, /live, /combine. Each runs in the background and logs progress to /app/logs/loader.log and stdout (docker logs).
- Data is written under /data/MarketData/<exchange>/<PAIR_TOKEN>/1m/ (mounted from your host).

Environment variables (defaults baked into image)
- MARKET_DATA_DIR=/data/MarketData
  Where datasets are stored inside the container. Mount your host path here.
- ALIGN_TZ=UTC
  Default timezone used to align backfill start/end to local midnights; can be overridden per request.
- MARKET_TYPE=futures
  Default market: futures (binanceusdm) or spot (binance). Can be overridden per request.
- SYMBOL=BTC/USDT
  Default pair used at startup; can be overridden per request.
- BUILD_COMBINED_ON_BACKFILL=0
  If set to 1, automatically rebuild combined Parquet after backfill (may be heavy). Otherwise, use /combine endpoint.
- MANIFEST_WRITE_INTERVAL_SEC=300
  Debounce interval for manifest writes.

Ports
- Host 8000 -> Container 8000 (FastAPI).

Volumes
- /data/MarketData: dataset root (mount your host directory here, e.g., C:\\MarketData on Windows).
- /app/logs: logs directory (mount to persist logs on host).

Health and UI
- GET /health -> {"status":"ok"}
- GET / -> redirects to /browse
- GET /browse -> simple HTML to navigate datasets and open Parquet files (preview via /browse/file).

Examples: call API endpoints
PowerShell (backfill for a window with tz/market/pair):
- Invoke-WebRequest -Method POST -Uri http://localhost:8000/backfill -Body '{"start":"2020-01-01T00:00:00Z","end":"2020-02-01T00:00:00Z","tz":"Europe/Zurich","market":"futures","pair":"ETH/USDT"}' -ContentType 'application/json'

curl (Linux/macOS/WSL):
- curl -X POST http://localhost:8000/backfill -H 'Content-Type: application/json' -d '{"start":"2020-01-01T00:00:00Z","end":"2020-01-10T00:00:00Z","chunk_size":1440,"align_window":true,"tz":"UTC","market":"spot","pair":"BTC/USDT"}'

Start live updates (keeps data current minute-by-minute):
- PowerShell: Invoke-WebRequest -Method POST 'http://localhost:8000/live?market=futures&pair=BTC/USDT&tz=Europe/Zurich'
- curl: curl -X POST 'http://localhost:8000/live?market=futures&pair=BTC/USDT&tz=Europe/Zurich'

Build combined Parquet (optional):
- All chunks with timezone label in filename: curl -X POST 'http://localhost:8000/combine?tz=Europe/Zurich&market=spot&pair=SOL/USDT'
- Range-limited: curl -X POST 'http://localhost:8000/combine?from_index=100&to_index=500&tz=+02:00&market=futures&pair=ETH/USDT'

Shortcuts: inside-container helper commands (no HTTP)
Run from your HOST shell using docker exec (don’t type these inside the container):
- docker exec -it financial-loader python -m container_tools --market futures --pair BTC/USDT backfill --start 2020-01-01 --end 2020-01-03
  Runs a bounded backfill window.
- docker exec -it financial-loader python -m container_tools --market futures --pair ETH/USDT live --minutes 2
  Fetches last N minutes (quick live test) without running the infinite loop.
- docker exec -it financial-loader python -m container_tools --market spot --pair SOL/USDT combine --tz Europe/Zurich
  Builds the combined parquet for the dataset (optionally set --from-index/--to-index in code or via API).
- docker exec -it financial-loader python -m container_tools --market futures --pair BTC/USDT status
  Prints dataset status from manifest.
- docker exec -it financial-loader python -m container_tools --market spot --pair BTC/USDT health
  Prints health metrics dictionary.

One-off container command (override default CMD)
Windows PowerShell:
- docker run --rm -e MARKET_DATA_DIR=/data/MarketData -v C:\\MarketData:/data/MarketData -v ${PWD}\\logs:/app/logs financial-loader python -m container_tools status

Windows cmd.exe:
- docker run --rm -e MARKET_DATA_DIR=/data/MarketData -v C:\\MarketData:/data/MarketData -v %CD%\\logs:/app/logs financial-loader python -m container_tools status

WSL/Linux/macOS:
- docker run --rm -e MARKET_DATA_DIR=/data/MarketData -v /mnt/c/MarketData:/data/MarketData -v $PWD/logs:/app/logs financial-loader python -m container_tools status

View logs
- docker logs -f financial-loader
  Shows real-time logs from the application (console logging set to INFO). Also see mounted /app/logs/loader.log for rotating file logs.

Updating defaults via env
Examples (PowerShell):
- docker run -d -p 8000:8000 -e ALIGN_TZ=Europe/Zurich -e MARKET_TYPE=spot -e SYMBOL=ETH/USDT -v C:\\MarketData:/data/MarketData -v ${PWD}\\logs:/app/logs --name financial-loader financial-loader

Sidecar usage (compose snippet)
Example docker-compose.yml:
- version: '3.9'
- services:
-   loader:
-     image: financial-loader
-     build: .
-     ports:
-       - "8000:8000"
-     environment:
-       - MARKET_DATA_DIR=/data/MarketData
-       - ALIGN_TZ=UTC
-       - MARKET_TYPE=futures
-       - SYMBOL=BTC/USDT
-     volumes:
-       - ./MarketData:/data/MarketData
-       - ./logs:/app/logs
-   your-app:
-     image: your/app:latest
-     depends_on:
-       - loader
-     # your-app can call http://loader:8000/backfill etc.

Troubleshooting
- If you run docker exec commands inside the container shell, you may see errors like "/bin/sh: 1: docker: not found"; exit the container and run from the host shell.
- On Windows PowerShell, use ${PWD}\logs (not %CD%\logs which is for cmd.exe). Ensure directories exist before mounting.
- Verify mounts: docker exec -it financial-loader sh -lc "ls -la /data/MarketData && ls -la /app/logs" (if a shell is available); otherwise, check via docker logs and API.

What each endpoint does
- /backfill (POST): Schedules a background job to fill historical 1-minute candles within the requested window. Respects tz alignment if align_window is true and tz is provided. Writes chunked Parquet and updates manifest. Logs progress (chunks saved, rows appended).
- /live (POST): Starts a background live updater that appends the most recent closed candle every minute. It checks for gaps and backfills if needed. Writes to chunks and updates manifest.
- /combine (POST): Compacts all chunks (or a specified index range) into a single combined Parquet file under combined/. You can tag the filename with tz for clarity.
- /health (GET): Lightweight health JSON. /browse provides a minimal UI to navigate datasets and preview Parquet files.
