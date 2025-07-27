import os, time, hashlib, json
from datetime import date, datetime
from datetime import timezone


import pandas as pd
from plaid.api import plaid_api
from plaid.model.products import Products
from plaid.model.sandbox_public_token_create_request import SandboxPublicTokenCreateRequest
from plaid.model.item_public_token_exchange_request import ItemPublicTokenExchangeRequest
from plaid.model.accounts_get_request import AccountsGetRequest
from plaid.configuration import Configuration, Environment
from plaid import ApiClient
import certifi

from plaid.model.transactions_get_request import TransactionsGetRequest
from plaid.model.transactions_get_request_options import TransactionsGetRequestOptions
from plaid.exceptions import ApiException

from google.cloud import storage

# ------------ CONFIG ------------
PROJECT_ID          = "liquidity-forecasting"
GCS_BUCKET          = "liquidity-data-bucket"
GCS_PREFIX          = f"raw/plaid_transactions/load_dt={date.today().isoformat()}/"
LOCAL_CSV           = "plaid_transactions_norm.csv"
# LOCAL_PARQUET       = "plaid_transactions_norm.parquet"
START_DATE          = date(2022, 1, 1)
END_DATE            = date.today()
# Plaid config
BUSINESS_ID         = "BIZ-001"
NUM_ITEMS                 = 10
WEBHOOK_FIRES_PER_ITEM    = 0
INSTITUTIONS              = ["ins_109508", "ins_116834", "ins_128026", "ins_127287"]
TARGET_ROWS              = 400
# --------------------------------

def get_plaid_client():
    config = Configuration(
        host=Environment.Sandbox,
        api_key={"clientId": os.environ["PLAID_CLIENT_ID"],
                 "secret":    os.environ["PLAID_SECRET"]}
    )
    config.ssl_ca_cert = certifi.where()
    api_client = ApiClient(config)
    return plaid_api.PlaidApi(api_client)

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode("utf-8")).hexdigest()

def main():
    client = get_plaid_client()

    # Multi-item pull to bulk up sandbox data
    all_txns = []
    account_lookup_global = {}

    for i in range(NUM_ITEMS):
        inst_id = INSTITUTIONS[i % len(INSTITUTIONS)]
        # 1) Create public token & exchange for access token (with error handling)
        try:
            pub_resp = client.sandbox_public_token_create(
                SandboxPublicTokenCreateRequest(
                    institution_id=inst_id,
                    initial_products=[Products("transactions")]
                )
            )
            public_token = pub_resp.public_token

            exch_resp = client.item_public_token_exchange(
                ItemPublicTokenExchangeRequest(public_token=public_token)
            )
            access_token = exch_resp.access_token
        except ApiException as e:
            try:
                err = json.loads(e.body)
                code = err.get("error_code")
            except Exception:
                raise
            if code in ["ITEM_LOGIN_REQUIRED", "PRODUCT_NOT_READY"]:
                print(f"[item {i}] {code} during token create/exchange – skipping this item.")
                continue
            else:
                raise

        # Optional small wait
        time.sleep(2)

        # 3) Accounts for this item
        accounts = client.accounts_get(AccountsGetRequest(access_token=access_token)).accounts
        for a in accounts:
            account_lookup_global[a.account_id] = a.name

        # 4) Skipping webhook fire (endpoint removed in SDK v35)

        # 5) Fetch transactions via /transactions/get with retries
        offset = 0
        page_size = 500
        while True:
            options = TransactionsGetRequestOptions(offset=offset, count=page_size)
            req = TransactionsGetRequest(
                access_token=access_token,
                start_date=START_DATE,
                end_date=END_DATE,
                options=options
            )

            # Retry loop to handle PRODUCT_NOT_READY and ITEM_LOGIN_REQUIRED
            for attempt in range(8):
                try:
                    resp = client.transactions_get(req)
                    break
                except ApiException as e:
                    try:
                        err = json.loads(e.body)
                    except Exception:
                        raise
                    code = err.get("error_code")
                    if code == "PRODUCT_NOT_READY":
                        wait_s = 3 * (attempt + 1)
                        print(f"[item {i}] PRODUCT_NOT_READY, retrying in {wait_s}s (attempt {attempt+1}/8)...")
                        time.sleep(wait_s)
                        continue
                    elif code == "ITEM_LOGIN_REQUIRED":
                        print(f"[item {i}] ITEM_LOGIN_REQUIRED – skipping this item.")
                        resp = None
                        break
                    else:
                        raise
            else:
                raise RuntimeError("Plaid transactions still not ready after retries")

            if resp is None:
                # skip this item entirely
                break
            if resp.transactions:
                all_txns.extend(resp.transactions)

            offset += len(resp.transactions)
            if offset >= resp.total_transactions:
                break

        print(f"[item {i}] collected so far: {len(all_txns)} txns")

        # Stop early if we already have enough transactions collected
        if len(all_txns) >= TARGET_ROWS:
            print(f"Reached target rows ({TARGET_ROWS}), stopping item loop.")
            break

    # 5) Normalize
    rows = []
    for t in all_txns:
        # Plaid amounts: positive = outflow (debit), negative = inflow (credit)
        direction = "OUTFLOW" if t.amount >= 0 else "INFLOW"
        amount_abs = abs(t.amount)
        currency_code = getattr(t, "iso_currency_code", "USD")

        # Personal finance category may not exist; fallback to category
        cat1 = cat2 = None
        if t.category:
            cat1 = t.category[0] if len(t.category) > 0 else None
            cat2 = t.category[1] if len(t.category) > 1 else None

        # Use Plaid ID or surrogate
        actual_id = ((getattr(t, "transaction_id", None) or md5_hex(f"{t.date}{t.name}{t.amount}")) + "_" + (t.account_id or ""))

        rows.append({
            "actual_id": actual_id,
            "business_id": BUSINESS_ID,
            "account_id": t.account_id,
            "account_name": account_lookup_global.get(t.account_id),
            "source_system": "plaid_sandbox",
            "cashflow_type": "ACTUAL_TXN",
            "direction": direction,
            "amount": amount_abs,
            "currency": currency_code,
            "post_date": t.date,
            "authorized_date": getattr(t, "authorized_date", None),
            "merchant_name": t.merchant_name,
            "original_name": t.name,
            "category_l1": cat1,
            "category_l2": cat2,
            "payment_channel": t.payment_channel,
            "transaction_type": t.transaction_type,
            "pending": t.pending,
            "ingest_ts": datetime.now(timezone.utc).isoformat()
        })

    df = pd.DataFrame(rows)
    df.drop_duplicates(subset=["actual_id"], inplace=True)
    print(f"🔁 After de-dup: {len(df)} rows")
    df.to_csv(LOCAL_CSV, index=False)
    #df.to_parquet(LOCAL_PARQUET, index=False)
    print(f"✅ Wrote {len(df)} rows to {LOCAL_CSV}")

    # 6) Upload to GCS
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(GCS_BUCKET)
    files_to_upload = [LOCAL_CSV]
    for local_file in files_to_upload:
        blob = bucket.blob(GCS_PREFIX + os.path.basename(local_file))
        blob.upload_from_filename(local_file)
        print(f"⬆️  Uploaded to gs://{GCS_BUCKET}/{blob.name}")

    print("\nNext step (run in terminal or UI):")
    print(f"""bq load \\
  --source_format=CSV \\
  --skip_leading_rows=1 \\
  --time_partitioning_type=DAY \\
  --time_partitioning_field=post_date \\
  --clustering_fields=business_id,cashflow_type \\
  {PROJECT_ID}:liquidity_forecasting.transactions_raw \\
  gs://{GCS_BUCKET}/{GCS_PREFIX}{LOCAL_CSV} \\
  transactions_schema.json""")

if __name__ == "__main__":
    main()