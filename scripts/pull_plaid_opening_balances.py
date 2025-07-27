#!/usr/bin/env python3
"""
pull_plaid_opening_balances.py
Fetch current balances from Plaid Sandbox for N items (one per business),
aggregate to a single opening balance per business_id, and write CSV locally + to GCS.

Usage:
  python pull_plaid_opening_balances.py \
    --business-ids BIZ-001 BIZ-002 BIZ-003 \
    --opening-date 2025-07-01 \
    --bucket liquidity-data-bucket

Prereqs:
  pip install plaid-python google-cloud-storage pandas python-dotenv
  export PLAID_CLIENT_ID=... PLAID_SECRET=...
  gcloud auth application-default login   # or set GOOGLE_APPLICATION_CREDENTIALS
"""

import os
import argparse
import uuid
import pandas as pd
from datetime import date
from google.cloud import storage
from plaid.api import plaid_api
from plaid.configuration import Configuration, Environment
from plaid import ApiClient
from plaid.model.products import Products
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.accounts_get_request import AccountsGetRequest

# ------------------ CONFIG DEFAULTS ------------------
DEFAULT_INSTITUTION = "ins_109508"  # Chase (Sandbox). Any institution works.
# -----------------------------------------------------

def get_plaid_client():
    config = Configuration(
        host=Environment.Sandbox,
        api_key={"clientId": os.environ["PLAID_CLIENT_ID"],
                 "secret": os.environ["PLAID_SECRET"]}
    )
    api_client = ApiClient(config)
    return plaid_api.PlaidApi(api_client)

def create_item_and_get_access_token(client, institution_id):
    """Create a sandbox public token, exchange for access token."""
    pub_req = SandboxPublicTokenCreateRequest(
        institution_id=institution_id,
        initial_products=[Products('transactions')]  # 'transactions' brings balances too
    )
    pub_resp = client.sandbox_public_token_create(pub_req)
    public_token = pub_resp.public_token

    exch_req = ItemPublicTokenExchangeRequest(public_token=public_token)
    exch_resp = client.item_public_token_exchange(exch_req)
    return exch_resp.access_token

def fetch_balances(client, access_token):
    """Return list of accounts (with balances) from Plaid."""
    acc_resp = client.accounts_get(AccountsGetRequest(access_token=access_token))
    accounts = acc_resp.accounts
    rows = []
    for a in accounts:
        bal = a.balances
        rows.append({
            "account_id": a.account_id,
            "name": a.name,
            "official_name": getattr(a, "official_name", None),
            "mask": a.mask,
            "type": a.type,
            "subtype": a.subtype,
            "current": bal.current,
            "available": bal.available,
            "limit": bal.limit
        })
    return rows

def upload_to_gcs(local_path, bucket_name, blob_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    print(f"✅ Uploaded to gs://{bucket_name}/{blob_path}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--business-ids", nargs="+", default=["BIZ-001"],
                        help="List of business IDs; one Plaid item per business.")
    parser.add_argument("--opening-date", default=str(date.today()),
                        help="Opening balance date (YYYY-MM-DD)")
    parser.add_argument("--bucket", required=True, help="GCS bucket to upload CSV")
    parser.add_argument("--folder", default="config/opening_balances",
                        help="Folder path prefix inside the bucket")
    args = parser.parse_args()

    # Plaid client
    client = get_plaid_client()

    all_rows = []
    for biz_id in args.business_ids:
        print(f"👉 Creating sandbox item for {biz_id}...")
        access_token = create_item_and_get_access_token(client, DEFAULT_INSTITUTION)
        accounts = fetch_balances(client, access_token)

        # Aggregate to one opening balance per business_id (sum 'current' across accounts)
        total_current = sum([acc["current"] for acc in accounts if acc["current"] is not None])
        all_rows.append({
            "business_id": biz_id,
            "opening_balance_date": args.opening_date,
            "opening_balance_amount": total_current
        })
        print(f"   {biz_id} total_current = {total_current:.2f}")

    df = pd.DataFrame(all_rows)
    print(df)

    # Write local CSV
    local_csv = "opening_balances.csv"
    df.to_csv(local_csv, index=False)
    print(f"✅ Local CSV written: {local_csv}")

    # Upload to GCS
    load_dt = date.today().isoformat()
    blob_path = f"{args.folder}/load_dt={load_dt}/opening_balances_{load_dt}.csv"
    upload_to_gcs(local_csv, args.bucket, blob_path)

    print("🎉 Done. Next: load this CSV into BigQuery as cfg_opening_balances.")

if __name__ == "__main__":
    # Safety checks for env vars
    missing = [v for v in ("PLAID_CLIENT_ID", "PLAID_SECRET") if v not in os.environ]
    if missing:
        raise RuntimeError(f"Missing env vars: {missing}. Export them before running.")
    main()