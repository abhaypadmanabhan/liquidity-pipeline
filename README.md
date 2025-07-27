# Real-Time Liquidity Forecasting – Data Pipeline

Ingest **Plaid Sandbox** actuals, simulate **forecasted cash-flow events**, stream via **Pub/Sub (AVRO)** into **BigQuery**, transform with **dbt**, and generate **policy-aware KPIs & alerts**. A **BigQuery ML** model predicts 30-day cash balance and first breach days, visualized in **Looker Studio**.

> **dbt project** (models & marts) lives in a separate repo/branch:  
> 👉 **https://github.com/abhaypadmanabhan/dataengineering1** (branch: `olist`)

---

## Table of Contents

- [Architecture](#architecture)
- [Repo Layout](#repo-layout)
- [Prerequisites](#prerequisites)
- [Setup](#setup)
- [Generate Forecast CSV](#generate-forecast-csv)
- [Pub/Sub → BigQuery (Streaming)](#pubsub--bigquery-streaming)
- [Pull Plaid Actuals](#pull-plaid-actuals)
- [Opening Balances](#opening-balances)
- [dbt Models (Separate Repo)](#dbt-models-separate-repo)
- [BigQuery ML (Model & Predictions)](#bigquery-ml-model--predictions)
- [Looker Studio (Dashboard)](#looker-studio-dashboard)
- [Automation Plan](#automation-plan)
- [Security & Secrets](#security--secrets)
- [Sanity Checks / Troubleshooting](#sanity-checks--troubleshooting)
- [Branching Note](#branching-note)
- [License](#license)

---

## Architecture

**Phase 1 – Data & Ingestion**  
Plaid Sandbox → (Python) → GCS/CSV → BigQuery: transactions_raw
Forecast CSV → (Python) → Pub/Sub (AVRO) → BigQuery: forecast_events_raw
Config CSV  → (CSV)   → GCS → BigQuery: cfg_opening_balances, cfg_alert_policies

**Phase 2 – Storage & Transform (dbt)**  

stg__  → parse/typing on raw (views)
int__  → daily frames (actual/forecast deltas)
fct__  → KPIs (DOCH, balance_actual, balance_projected)
rpt__  → policy-aware alerts (GREEN/AMBER/RED)

**Phase 3 – ML & BI**  
BigQuery ML (Boosted Tree Regressor) → predict 30d balance & breach
Looker Studio → KPIs, alerts, predicted breach days

---

## Repo Layout
.
├── scripts/
│   ├── forecast_generator.py               # Build forecast_plan.csv mock data
│   ├── publish_forecast_events.py          # Publish CSV → Pub/Sub (JSON/AVRO)
│   ├── pull_plaid_actuals.py               # Pull Plaid transactions (sandbox)
│   └── pull_plaid_opening_balances.py      # Pull Plaid accounts → opening balances
├── schemas/
│   ├── forecast_event_schema.avsc          # Pub/Sub AVRO schema for forecasts
│   └── forecast_events_bq_schema.json      # BigQuery schema for forecast_events_raw
├── requirements.txt                        # Python dependencies
├── .env.example                            # Copy to .env & fill with real values
├── .gitignore
└── README.md

---

## Prerequisites

- **Python** 3.10+  
- **Google Cloud SDK** (`gcloud`, `bq`, `gsutil`)  
- **BigQuery** & **Pub/Sub** APIs enabled in project `liquidity-forecasting`  
- **Plaid Sandbox** credentials (Client ID + Secret)

---

## Setup

git clone https://github.com/abhaypadmanabhan/liquidity-pipeline.git
cd liquidity-pipeline

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

gcloud auth login
gcloud auth application-default login
gcloud config set project liquidity-forecasting

cp .env.example .env
# Edit .env: fill PLAID_CLIENT_ID, PLAID_SECRET, PLAID_ENV, GCP_PROJECT, GCS_BUCKET, PUBSUB_TOPIC

Generate Forecast CSV

python scripts/forecast_generator.py \
  --start 2025-08-01 --end 2026-01-31 \
  --businesses BIZ-001 BIZ-002 BIZ-003 \
  --rows 500 \
  --out forecast_plan.csv

Optionally upload to GCS:
DT=$(date +%F)
gsutil cp forecast_plan.csv gs://liquidity-data-bucket/forecast_plan/load_dt=${DT}/

Pub/Sub → BigQuery (Streaming)
# 1) Create AVRO schema & topic
gcloud pubsub schemas create forecast_event_schema_v2 \
  --type=avro --definition-file=schemas/forecast_event_schema.avsc

gcloud pubsub topics create forecast-events \
  --schema="projects/$(gcloud config get project)/schemas/forecast_event_schema_v2" \
  --message-encoding=json

# 2) Create BQ table & subscription
bq mk --table liquidity-forecasting:liquidity_forecasting.forecast_events_raw schemas/forecast_events_bq_schema.json

gcloud pubsub subscriptions create forecast-events-bq-sub \
  --topic=forecast-events \
  --bigquery-table=liquidity-forecasting:liquidity_forecasting.forecast_events_raw \
  --use-topic-schema

# 3) Publish events from CSV
python scripts/publish_forecast_events.py --csv forecast_plan.csv

Sanity-check:
bq query --use_legacy_sql=false \
"SELECT COUNT(*) AS rows FROM \`liquidity-forecasting.liquidity_forecasting.forecast_events_raw\`"

Pull Plaid Actuals
python scripts/pull_plaid_actuals.py \
  --start 2025-06-01 --end 2025-07-31 \
  --out plaid_transactions.csv
Load to BigQuery (UI or CLI). Example CLI:
bq load --source_format=CSV \
  --autodetect \
  liquidity-forecasting:liquidity_forecasting.transactions_raw \
  plaid_transactions.csv


















