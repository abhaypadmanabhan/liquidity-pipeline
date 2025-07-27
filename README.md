# liquidity-pipeline
## Quickstart
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill PLAID_* and GCP_* values

# Generate & publish forecasts
python forecast_generator.py --start 2025-08-01 --end 2026-01-31 --rows 500 --out forecast_plan.csv
python publish_forecast_events.py --csv forecast_plan.csv

# Pull Plaid actuals
python pull_plaid_actuals.py --start 2025-06-01 --end 2025-07-31 --out plaid_transactions.csv
