# Minimal Dockerfile to run the Financial Data Loader API
FROM python:3.11-slim

# Install system deps if needed (none currently)

WORKDIR /app

# Copy only requirements first for better layer caching
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the project
COPY . .

# Expose port for the API
EXPOSE 8000

# Environment variables with sensible defaults
ENV MARKET_DATA_DIR=/data/MarketData \
    BUILD_COMBINED_ON_BACKFILL=0 \
    MANIFEST_WRITE_INTERVAL_SEC=300 \
    ALIGN_TZ=UTC \
    MARKET_TYPE=futures \
    SYMBOL=BTC/USDT

# Create mount points for data and logs
VOLUME ["/data", "/app/logs"]

# Install helper tools script
RUN chmod +x /app/tools.sh || true

# Default command: run FastAPI app
# You can override the command to run helper CLI, e.g.:
#   docker run --rm -it financial-loader python -m container_tools status
# Or use the helper wrapper:
#   docker exec -it financial-loader /app/tools.sh status
CMD ["uvicorn", "api:app", "--host", "0.0.0.0", "--port", "8000"]
