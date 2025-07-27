"""
forecast_generator.py
Generate simulated forecasted cashflows for a liquidity forecasting project.

Author: You :)
"""

# ---------------------- CONFIG ---------------------- #
START_DATE_STR   = "2025-08-01"
END_DATE_STR     = "2026-01-31"
N_BUSINESSES     = 3
TARGET_ROWS      = 500
UPDATE_RATE      = 0.10       # 10% adjusted
CANCEL_RATE      = 0.03       # 3% cancelled
CURRENCY         = "USD"
SCENARIO         = "base"
USE_UUID_IDS     = False      # switch to True if you prefer UUID forecast_ids
WRITE_EVENTS     = False      # set True to also emit Pub/Sub-style events JSONL
RANDOM_SEED      = 42
OUTPUT_CSV       = "forecast_plan.csv"
OUTPUT_EVENTS    = "forecast_events.jsonl"

# -------------------- IMPORTS ----------------------- #
import uuid
import random
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd

# -------------------- SETUP ------------------------- #
np.random.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)

START_DATE = date.fromisoformat(START_DATE_STR)
END_DATE   = date.fromisoformat(END_DATE_STR)

business_ids  = [f"BIZ-{i:03d}" for i in range(1, N_BUSINESSES + 1)]
departments   = ["Sales", "Operations", "Finance", "HR", "IT"]
cost_centers  = ["CC-100", "CC-200", "CC-300", "CC-400"]
gl_inflow     = ["4000", "4010", "4020"]
gl_outflow    = ["6000", "6010", "7000", "7100"]
customers     = [
    "Acme Retailers", "BlueSky Corp", "Northwind Traders", "Globex LLC", "Innotech",
    "Wayne Enterprises", "Stark Industries", "Wonka Imports", "Umbrella Co", "Oscorp"
]
vendors       = [
    "Okta Inc", "AWS", "Google Cloud", "Microsoft 365", "Salesforce", "Zoom Video",
    "PG&E", "Comcast Business", "WeWork", "Square Payroll"
]

# Track incremental readable IDs
id_counters = {
    "AR_INVOICE":   1,
    "PAYROLL":      1,
    "AP_BILL":      1,
    "TAX":          1,
    "LOAN_PAYMENT": 1,
    "CREDIT_DRAW":  1,
    "OTHER":        1
}

def make_uuid():
    return str(uuid.uuid4())

def gen_readable_id(prefix):
    c = id_counters[prefix]
    id_counters[prefix] += 1
    return f"{prefix[:3]}-{c:05d}"  # e.g. PAY-00001

def new_forecast_id(prefix):
    return make_uuid() if USE_UUID_IDS else gen_readable_id(prefix)

def rrule_string(freq):
    # keep simple textual recurrence tag
    return freq

def generate_dates(start_d, end_d, freq, anchor_day=None):
    """Return list of due dates between start_d & end_d for a given simple freq."""
    dates = []
    if freq == "MONTHLY":
        if anchor_day is None:
            anchor_day = 1
        first = date(start_d.year, start_d.month, min(anchor_day, 28))
        if first < start_d:
            first = first + relativedelta(months=1)
        d = first
        while d <= end_d:
            dates.append(d)
            d = d + relativedelta(months=1)
    elif freq == "BIWEEKLY":
        d = start_d
        while d <= end_d:
            dates.append(d)
            d = d + timedelta(days=14)
    elif freq == "WEEKLY":
        d = start_d
        while d <= end_d:
            dates.append(d)
            d = d + timedelta(days=7)
    elif freq == "QUARTERLY":
        if anchor_day is None:
            anchor_day = 15
        first = date(start_d.year, start_d.month, min(anchor_day, 28))
        if first < start_d:
            first = first + relativedelta(months=3)
        d = first
        while d <= end_d:
            dates.append(d)
            d = d + relativedelta(months=3)
    else:
        dates = [start_d]
    return dates

def random_amount(low, high, skew_positive=True):
    if skew_positive:
        val = np.random.lognormal(mean=np.log((low + high) / 2), sigma=0.5)
        return float(np.clip(val, low, high))
    return float(np.random.uniform(low, high))

def pick_probability(cf_type):
    if cf_type in ["PAYROLL", "TAX", "LOAN_PAYMENT", "AP_BILL"]:
        return 1.0
    if cf_type == "AR_INVOICE":
        return round(np.random.uniform(0.8, 0.98), 2)
    return round(np.random.uniform(0.85, 0.99), 2)

def pick_direction(cf_type):
    return "INFLOW" if cf_type in ["AR_INVOICE", "CREDIT_DRAW"] else "OUTFLOW"

def pick_category(cf_type):
    mapping = {
        "AR_INVOICE":   "Revenue > Customer Invoice",
        "PAYROLL":      "Payroll > Salaries",
        "AP_BILL":      "Ops > Vendor Bill",
        "TAX":          "Finance > Taxes",
        "LOAN_PAYMENT": "Finance > Loan Payment",
        "CREDIT_DRAW":  "Finance > Credit Line",
        "OTHER":        "Misc > One-off"
    }
    return mapping.get(cf_type, "Misc")

def pick_counterparty(cf_type):
    if cf_type == "AR_INVOICE":
        return random.choice(customers)
    elif cf_type == "PAYROLL":
        return "Company Staff"
    elif cf_type == "TAX":
        return "IRS"
    elif cf_type in ["LOAN_PAYMENT", "CREDIT_DRAW"]:
        return "Bank of Gotham"
    elif cf_type == "AP_BILL":
        return random.choice(vendors)
    return random.choice(customers + vendors)

def pick_gl(direction):
    return random.choice(gl_inflow if direction == "INFLOW" else gl_outflow)

def pick_cost_center():
    return random.choice(cost_centers)

def pick_department():
    return random.choice(departments)

def synthesize_timestamps(due_date):
    created_days_before = np.random.randint(15, 60)
    created_at = datetime.combine(due_date, datetime.min.time()) - timedelta(days=created_days_before)
    updated_at = created_at
    return created_at, updated_at

# -------------------- GENERATION -------------------- #
rows = []

def add_row(d):
    rows.append(d)

for biz in business_ids:
    # PAYROLL monthly on 15th
    parent_payroll = f"RR_PAYROLL_{biz}"
    for d in generate_dates(START_DATE, END_DATE, "MONTHLY", anchor_day=15):
        amt = random_amount(25000, 40000)
        fid = new_forecast_id("PAYROLL")
        c_at, u_at = synthesize_timestamps(d)
        add_row({
            "forecast_id": fid,
            "business_id": biz,
            "source_system": "mock_csv",
            "cashflow_type": "PAYROLL",
            "direction": pick_direction("PAYROLL"),
            "amount": round(amt, 2),
            "currency": CURRENCY,
            "due_date": d,
            "expected_post_date": d,
            "recurrence_rule": rrule_string("MONTHLY"),
            "parent_recurring_id": parent_payroll,
            "counterparty_name": pick_counterparty("PAYROLL"),
            "counterparty_id": "",
            "category": pick_category("PAYROLL"),
            "probability": pick_probability("PAYROLL"),
            "scenario": SCENARIO,
            "status": "PLANNED",
            "cost_center": pick_cost_center(),
            "department": pick_department(),
            "gl_account": pick_gl("OUTFLOW"),
            "created_at": c_at,
            "updated_at": u_at,
            "ingest_ts": datetime.utcnow()
        })

    # RENT monthly on 1st
    parent_rent = f"RR_RENT_{biz}"
    for d in generate_dates(START_DATE, END_DATE, "MONTHLY", anchor_day=1):
        amt = random_amount(5000, 12000)
        fid = new_forecast_id("AP_BILL")
        c_at, u_at = synthesize_timestamps(d)
        add_row({
            "forecast_id": fid,
            "business_id": biz,
            "source_system": "mock_csv",
            "cashflow_type": "AP_BILL",
            "direction": pick_direction("AP_BILL"),
            "amount": round(amt, 2),
            "currency": CURRENCY,
            "due_date": d,
            "expected_post_date": d,
            "recurrence_rule": rrule_string("MONTHLY"),
            "parent_recurring_id": parent_rent,
            "counterparty_name": "WeWork",
            "counterparty_id": "",
            "category": "Ops > Rent",
            "probability": 1.0,
            "scenario": SCENARIO,
            "status": "PLANNED",
            "cost_center": pick_cost_center(),
            "department": pick_department(),
            "gl_account": pick_gl("OUTFLOW"),
            "created_at": c_at,
            "updated_at": u_at,
            "ingest_ts": datetime.utcnow()
        })

    # SaaS (3 vendors) monthly on 5th
    saas_vendors = random.sample(vendors, 3)
    for v in saas_vendors:
        parent_saas = f"RR_SAAS_{v.replace(' ', '').upper()}_{biz}"
        for d in generate_dates(START_DATE, END_DATE, "MONTHLY", anchor_day=5):
            amt = random_amount(100, 2000)
            fid = new_forecast_id("AP_BILL")
            c_at, u_at = synthesize_timestamps(d)
            add_row({
                "forecast_id": fid,
                "business_id": biz,
                "source_system": "mock_csv",
                "cashflow_type": "AP_BILL",
                "direction": pick_direction("AP_BILL"),
                "amount": round(amt, 2),
                "currency": CURRENCY,
                "due_date": d,
                "expected_post_date": d,
                "recurrence_rule": rrule_string("MONTHLY"),
                "parent_recurring_id": parent_saas,
                "counterparty_name": v,
                "counterparty_id": "",
                "category": "Ops > SaaS",
                "probability": 1.0,
                "scenario": SCENARIO,
                "status": "PLANNED",
                "cost_center": pick_cost_center(),
                "department": pick_department(),
                "gl_account": pick_gl("OUTFLOW"),
                "created_at": c_at,
                "updated_at": u_at,
                "ingest_ts": datetime.utcnow()
            })

    # Taxes quarterly on 15th
    parent_tax = f"RR_TAX_{biz}"
    for d in generate_dates(START_DATE, END_DATE, "QUARTERLY", anchor_day=15):
        amt = random_amount(8000, 40000)
        fid = new_forecast_id("TAX")
        c_at, u_at = synthesize_timestamps(d)
        add_row({
            "forecast_id": fid,
            "business_id": biz,
            "source_system": "mock_csv",
            "cashflow_type": "TAX",
            "direction": pick_direction("TAX"),
            "amount": round(amt, 2),
            "currency": CURRENCY,
            "due_date": d,
            "expected_post_date": d,
            "recurrence_rule": rrule_string("QUARTERLY"),
            "parent_recurring_id": parent_tax,
            "counterparty_name": "IRS",
            "counterparty_id": "",
            "category": pick_category("TAX"),
            "probability": 1.0,
            "scenario": SCENARIO,
            "status": "PLANNED",
            "cost_center": pick_cost_center(),
            "department": pick_department(),
            "gl_account": pick_gl("OUTFLOW"),
            "created_at": c_at,
            "updated_at": u_at,
            "ingest_ts": datetime.utcnow()
        })

    # Loan payments monthly on 20th
    parent_loan = f"RR_LOANPAY_{biz}"
    for d in generate_dates(START_DATE, END_DATE, "MONTHLY", anchor_day=20):
        amt = random_amount(3000, 15000)
        fid = new_forecast_id("LOAN_PAYMENT")
        c_at, u_at = synthesize_timestamps(d)
        add_row({
            "forecast_id": fid,
            "business_id": biz,
            "source_system": "mock_csv",
            "cashflow_type": "LOAN_PAYMENT",
            "direction": pick_direction("LOAN_PAYMENT"),
            "amount": round(amt, 2),
            "currency": CURRENCY,
            "due_date": d,
            "expected_post_date": d,
            "recurrence_rule": rrule_string("MONTHLY"),
            "parent_recurring_id": parent_loan,
            "counterparty_name": "Bank of Gotham",
            "counterparty_id": "",
            "category": pick_category("LOAN_PAYMENT"),
            "probability": 1.0,
            "scenario": SCENARIO,
            "status": "PLANNED",
            "cost_center": pick_cost_center(),
            "department": pick_department(),
            "gl_account": pick_gl("OUTFLOW"),
            "created_at": c_at,
            "updated_at": u_at,
            "ingest_ts": datetime.utcnow()
        })

    # AR invoices (random freq per customer)
    cust_count = random.randint(3, 6)
    chosen_customers = random.sample(customers, cust_count)
    for cust in chosen_customers:
        freq = random.choice(["WEEKLY", "BIWEEKLY", "MONTHLY"])
        parent_ar = f"RR_AR_{cust.replace(' ', '').upper()}_{biz}"
        ar_dates = generate_dates(START_DATE, END_DATE, freq, anchor_day=random.randint(1, 28))
        for d in ar_dates:
            amt = random_amount(5000, 35000)
            fid = new_forecast_id("AR_INVOICE")
            c_at, u_at = synthesize_timestamps(d)
            expected_post = d + timedelta(days=random.randint(0, 7))
            add_row({
                "forecast_id": fid,
                "business_id": biz,
                "source_system": "mock_csv",
                "cashflow_type": "AR_INVOICE",
                "direction": pick_direction("AR_INVOICE"),
                "amount": round(amt, 2),
                "currency": CURRENCY,
                "due_date": d,
                "expected_post_date": expected_post,
                "recurrence_rule": rrule_string(freq),
                "parent_recurring_id": parent_ar,
                "counterparty_name": cust,
                "counterparty_id": "",
                "category": pick_category("AR_INVOICE"),
                "probability": pick_probability("AR_INVOICE"),
                "scenario": SCENARIO,
                "status": "PLANNED",
                "cost_center": pick_cost_center(),
                "department": pick_department(),
                "gl_account": pick_gl("INFLOW"),
                "created_at": c_at,
                "updated_at": u_at,
                "ingest_ts": datetime.utcnow()
            })

    # Credit line draws (3–6 random)
    for _ in range(random.randint(3, 6)):
        d = START_DATE + timedelta(days=random.randint(0, (END_DATE-START_DATE).days))
        amt = random_amount(20000, 100000)
        fid = new_forecast_id("CREDIT_DRAW")
        c_at, u_at = synthesize_timestamps(d)
        add_row({
            "forecast_id": fid,
            "business_id": biz,
            "source_system": "mock_csv",
            "cashflow_type": "CREDIT_DRAW",
            "direction": pick_direction("CREDIT_DRAW"),
            "amount": round(amt, 2),
            "currency": CURRENCY,
            "due_date": d,
            "expected_post_date": d,
            "recurrence_rule": "",
            "parent_recurring_id": "",
            "counterparty_name": "Bank of Gotham",
            "counterparty_id": "",
            "category": pick_category("CREDIT_DRAW"),
            "probability": 1.0,
            "scenario": SCENARIO,
            "status": "PLANNED",
            "cost_center": pick_cost_center(),
            "department": pick_department(),
            "gl_account": pick_gl("INFLOW"),
            "created_at": c_at,
            "updated_at": u_at,
            "ingest_ts": datetime.utcnow()
        })

    # Misc one-offs (5–10)
    for _ in range(random.randint(5, 10)):
        d = START_DATE + timedelta(days=random.randint(0, (END_DATE-START_DATE).days))
        amt = random_amount(500, 10000)
        fid = new_forecast_id("OTHER")
        c_at, u_at = synthesize_timestamps(d)
        direction = random.choice(["INFLOW", "OUTFLOW"])
        add_row({
            "forecast_id": fid,
            "business_id": biz,
            "source_system": "mock_csv",
            "cashflow_type": "OTHER",
            "direction": direction,
            "amount": round(amt, 2),
            "currency": CURRENCY,
            "due_date": d,
            "expected_post_date": d,
            "recurrence_rule": "",
            "parent_recurring_id": "",
            "counterparty_name": pick_counterparty("OTHER"),
            "counterparty_id": "",
            "category": pick_category("OTHER"),
            "probability": pick_probability("OTHER"),
            "scenario": SCENARIO,
            "status": "PLANNED",
            "cost_center": pick_cost_center(),
            "department": pick_department(),
            "gl_account": pick_gl(direction),
            "created_at": c_at,
            "updated_at": u_at,
            "ingest_ts": datetime.utcnow()
        })

# ----------------- SHAPE & MUTATE ------------------- #
df = pd.DataFrame(rows)

# Trim or pad to TARGET_ROWS
if len(df) > TARGET_ROWS:
    df = df.sample(TARGET_ROWS, random_state=RANDOM_SEED).reset_index(drop=True)
elif len(df) < TARGET_ROWS:
    need = TARGET_ROWS - len(df)
    dupes = df.sample(need, replace=True, random_state=RANDOM_SEED).copy()

    def adjust_dup(row):
        # give a new ID & slight tweak to timestamps so it's unique-ish
        row["forecast_id"] = make_uuid() if USE_UUID_IDS else f"DUP-{uuid.uuid4().hex[:6]}"
        row["created_at"] = row["created_at"] - timedelta(days=random.randint(1, 5))
        return row

    dupes = dupes.apply(adjust_dup, axis=1)
    df = pd.concat([df, dupes], ignore_index=True)

# Simulate UPDATE and CANCEL
n_update = int(len(df) * UPDATE_RATE)
n_cancel = int(len(df) * CANCEL_RATE)

update_idx = df.sample(n_update, random_state=RANDOM_SEED).index
cancel_idx = df.drop(update_idx).sample(n_cancel, random_state=RANDOM_SEED).index

# UPDATE rows
for idx in update_idx:
    amt = df.at[idx, "amount"]
    tweak = np.random.uniform(-0.15, 0.15)  # +/-15%
    df.at[idx, "amount"] = round(max(0, amt * (1 + tweak)), 2)

    shift = np.random.randint(-3, 6)        # -3 to +5 days
    orig_due = pd.to_datetime(df.at[idx, "due_date"]).date()
    df.at[idx, "due_date"] = orig_due + timedelta(days=shift)

    df.at[idx, "updated_at"] = datetime.utcnow()
    df.at[idx, "status"] = "ADJUSTED"

# CANCEL rows
df.loc[cancel_idx, "status"] = "CANCELLED"
df.loc[cancel_idx, "updated_at"] = datetime.utcnow()
df.loc[cancel_idx, "probability"] = 0.0

# Order & save
df = df.sort_values(["business_id", "due_date", "forecast_id"]).reset_index(drop=True)
df.to_csv(OUTPUT_CSV, index=False)
print(f"✅ Wrote {len(df)} rows to {OUTPUT_CSV}")

# --------------- OPTIONAL: EVENTS FILE -------------- #
if WRITE_EVENTS:
    import json
    events = []
    def emit_event(row, e_type="CREATE"):
        payload = row.to_dict()
        # Convert dates/timestamps to ISO strings
        for k, v in payload.items():
            if isinstance(v, (pd.Timestamp, datetime)):
                payload[k] = v.isoformat()
            if isinstance(v, date):
                payload[k] = v.isoformat()
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": e_type,
            "event_ts": datetime.utcnow().isoformat(),
            "payload_version": "v1",
            "payload": payload
        }
        events.append(event)

    # initial CREATE for all
    for _, r in df.iterrows():
        emit_event(r, "CREATE")

    # UPDATE events for adjusted rows
    for idx in update_idx:
        emit_event(df.loc[idx], "UPDATE")

    # CANCEL events
    for idx in cancel_idx:
        emit_event(df.loc[idx], "CANCEL")

    with open(OUTPUT_EVENTS, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")
    print(f"✅ Wrote {len(events)} events to {OUTPUT_EVENTS}")