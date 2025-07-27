#!/usr/bin/env python3
import os, json, uuid, datetime, time, io, math
import pandas as pd
from google.cloud import pubsub_v1
from google.cloud import storage

# -------- CONFIG --------
PROJECT_ID   = "liquidity-forecasting"
TOPIC_ID     = "forecast-events"
INPUT_URI    = "gs://liquidity-data-bucket/forecast_plan/load_dt=2025-07-24/forecast_plan_2025-08-01_2026-01-31_v1.csv"  # or local path
BATCH_SIZE   = 100  # publish in batches
# ------------------------

# Helper to load CSV from local or GCS
def load_df(input_uri: str) -> pd.DataFrame:
    """Load a CSV from either local disk or GCS (gs://...)."""
    if input_uri.startswith("gs://"):
        # Parse bucket and blob
        # gs://bucket-name/path/to/file.csv
        parts = input_uri[5:].split("/", 1)
        bucket_name, blob_path = parts[0], parts[1]
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        data = blob.download_as_bytes()
        return pd.read_csv(io.BytesIO(data))
    else:
        return pd.read_csv(input_uri)

def map_event_type(status: str) -> str:
    status = (status or "").upper()
    if status == "ADJUSTED":
        return "UPDATE"
    if status == "CANCELLED":
        return "CANCEL"
    return "CREATE"

def now_utc_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

# --- Helper functions for type conversions ---
def none_if_nan(v):
    return None if (v is None or (isinstance(v, float) and math.isnan(v))) else v

def req_str(v):
    v2 = none_if_nan(v)
    return "" if v2 is None else str(v2)

def opt_str(v, default=""):
    v2 = none_if_nan(v)
    return default if v2 is None else str(v2)

def opt_float(v, default=0.0):
    v2 = none_if_nan(v)
    return float(default) if v2 is None else float(v2)

def req_float(v, default=0.0):
    v2 = none_if_nan(v)
    return float(v2) if v2 is not None else float(default)

def req_int(v, default=1):
    v2 = none_if_nan(v)
    return int(v2) if v2 is not None else int(default)

def main():
    # Auth must be set: GOOGLE_APPLICATION_CREDENTIALS or gcloud ADC
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    df = load_df(INPUT_URI)
    print(f"Loaded {len(df)} forecast rows from {INPUT_URI}")

    futures = []  # will store tuples of (future, event)
    for idx, row in df.iterrows():
        raw_status = row.get("event_status") or "PLANNED"
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": req_str(map_event_type(raw_status)),
            "event_status": req_str(raw_status),
            "forecast_id": req_str(row.get("forecast_id")),
            "business_id": req_str(row.get("business_id")),
            "cashflow_type": req_str(row.get("cashflow_type")),
            "direction": req_str(row.get("direction")),
            "amount": req_float(row.get("amount"), 0.0),
            "currency": req_str(row.get("currency", "USD")),
            "due_date": opt_str(row.get("due_date")),
            "probability": opt_float(row.get("probability", 1.0)),
            "scenario": opt_str(row.get("scenario")),
            "cost_center": opt_str(row.get("cost_center")),
            "department": opt_str(row.get("department")),
            "gl_account": opt_str(row.get("gl_account")),
            "counterparty": opt_str(row.get("counterparty")),
            "note": opt_str(row.get("note")),
            "created_at": opt_str(row.get("created_at")),
            "updated_at": opt_str(row.get("updated_at")),
            "ingest_ts": now_utc_iso(),
            "source_system": req_str("SIMULATED_CSV"),
            "version": req_int(row.get("version"), 1)
        }

        try:
            data_bytes = json.dumps(event).encode("utf-8")
            future = publisher.publish(topic_path, data=data_bytes)
        except Exception as e:
            print(f"Publish failed for idx={idx}, forecast_id={event['forecast_id']}: {e}\nPayload: {event}")
            raise
        futures.append((future, event))

        if len(futures) % BATCH_SIZE == 0:
            # wait for this batch to finish and surface any schema errors
            for fut, ev in futures:
                try:
                    fut.result(timeout=30)
                except Exception as e:
                    print("\n🚨 Publish failed for payload:")
                    print(json.dumps(ev, indent=2))
                    print(f"Error: {e}")
                    raise
            futures.clear()
            print(f"Published {idx+1} messages...")

    # wait for leftovers and surface any errors
    for fut, ev in futures:
        try:
            fut.result(timeout=30)
        except Exception as e:
            print("\n🚨 Publish failed for payload (final batch):")
            print(json.dumps(ev, indent=2))
            print(f"Error: {e}")
            raise

    print("✅ All messages published.")

if __name__ == "__main__":
    main()