# 🚀 Project Battle Plan

This is the path. We build, we test, we optimize. We don't overthink.

## 📍 PHASE 1: Ingestion & Setup

**Goal**: Get live stock data flowing into Kafka (RedPanda).

**Services**: docker-compose.yml, redpanda  
**Code**: src.ingestion.producer

### Tasks
- Build a yfinance fetcher for NSE stocks.
- Create a Kafka producer (using kafka-python).
- Batch messages (sending one by one is for amateurs).
- Serialize data (JSON is fine for now).

**Milestone**: Live trades visible in RedPanda Console (http://localhost:8080).

## 📍 PHASE 2: Processing & Storage

**Goal**: Consume data, calculate indicators, and store in TimescaleDB.

**Services**: timescale, redis  
**Code**: src.processing.consumer, src.db.setup

### Tasks
- Set up TimescaleDB (Hypertables required).
- Build a Kafka consumer.
- Deserialize messages.
- Use pandas-ta to calculate SMA, EMA, RSI, MACD.
- Batch-write results to TimescaleDB using execute_values.

**Milestone**: Query TimescaleDB and see fresh indicator data.

## 📍 PHASE 3: Caching & API

**Goal**: Serve data (fast) via a REST API.

**Services**: redis, fastapi  
**Code**: src.api.main, src.cache.redis_client

### Tasks
- Build FastAPI endpoints: /latest_price, /indicators.
- Cache hot data in Redis.
- Query TimescaleDB for historical/indicator data.
- Add validation using Pydantic.

**Milestone**: Visit http://localhost:8000/docs and get real-time data.

## 📍 PHASE 4: Real-time Dashboard

**Goal**: Visualize the data.

**Services**: streamlit  
**Code**: src.dashboard.app

### Tasks
- Build a Streamlit dashboard.
- Use FastAPI to fetch data.
- Add auto-refresh.

**Milestone**: Charts updating without manually refreshing.

## 📍 PHASE 5: Testing & CI/CD

**Goal**: Automated testing.

**Services**: GitHub Actions  
**Code**: tests/, .github/workflows/ci.yml

### Tasks
- Write unit tests (pytest).
- Write integration tests (producer ➜ consumer flow).
- CI pipeline (lint + test on every push).

**Milestone**: Green checkmark on pull requests.

## 📍 PHASE 6: Deployment

**Goal**: Deploy to the cloud.

### Tasks
- Containerize all services (Producer, Consumer, API).
- Use managed DB, Kafka, and Redis.
- Configure environment variables.
- Deploy to AWS / Railway / Render.

**Milestone**: Publicly accessible live API.
