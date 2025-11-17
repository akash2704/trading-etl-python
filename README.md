# 📈 Real-Time Trading ETL

This is a high-throughput, low-latency ETL pipeline for processing real-time stock market data, calculating indicators, and serving it via API.

## Tech Stack

* **Orchestration**: Docker Compose
* **Messaging**: RedPanda (Kafka-compatible broker)
* **Database**: TimescaleDB (Time-series on Postgres)
* **Cache**: Redis (Indicator/session caching)
* **Ingestion**: `yfinance`
* **Processing**: `pandas`, `pandas-ta`, `kafka-python`
* **API**: FastAPI
* **Dashboard**: Streamlit
* **Tooling**: `uv` (Python package manager)

## Get it Running

### Prerequisites
* Docker & Docker Compose
* `uv` (Install with `pip install uv`)

### 1. Start Infrastructure

This spins up RedPanda, TimescaleDB, Redis, and the RedPanda Console.

```bash
docker compose up -d
```

* RedPanda Console (UI): http://localhost:8080
* Postgres (Timescale): localhost:5433
* Redis: localhost:6380

### 2. Install Dependencies

I'm using uv because it's fast.

```bash
# Create a virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Install all dependencies (main + dev)
uv sync --group dev
```

### 3. Run Services

These scripts are defined in pyproject.toml.

```bash
# 1. Start the producer (fetches from yfinance, pushes to Kafka)
uv run producer

# 2. Start the consumer (reads from Kafka, processes, saves to DB)
uv run consumer

# 3. Start the API server
uv run uvicorn src.api.main:app --host 0.0.0.0 --port 8000 --reload

# 4. Run the dashboard
uv run dashboard
```
