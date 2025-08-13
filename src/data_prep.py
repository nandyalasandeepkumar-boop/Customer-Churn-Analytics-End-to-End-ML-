import json
import pandas as pd
from datetime import datetime, timedelta
from utils import load_config, get_engine

def main():
    cfg = load_config()
    df = pd.read_csv(cfg["data"]["sample_csv_path"])

    # explode usage events json into a long table
    rows = []
    for _, r in df.iterrows():
        events = json.loads(r["events_json"])
        for e in events:
            rows.append({
                "customer_id": r["customer_id"],
                "event_ts": pd.to_datetime(e["ts"]),
                "sessions": int(e["sessions"]),
                "minutes_used": int(e["minutes"])
            })
    usage = pd.DataFrame(rows)

    # write normalized tables
    customers = df[["customer_id","signup_date","segment"]].copy()
    customers["signup_date"] = pd.to_datetime(customers["signup_date"])

    fb = df[["customer_id","nps","last_contact_ts"]].copy()
    fb["last_contact_ts"] = pd.to_datetime(fb["last_contact_ts"])

    labels = df[["customer_id","churned"]].copy().astype({"churned":"int"})

    # Load schema and write to DB
    eng = get_engine(cfg)
    with eng.begin() as conn:
        # You should run sql/schema.sql once before this (psql or DB tool).
        customers.to_sql("customers", conn, if_exists="append", index=False)
        usage.to_sql("usage_events", conn, if_exists="append", index=False)
        fb.to_sql("feedback", conn, if_exists="append", index=False)
        labels.to_sql("churn_labels", conn, if_exists="append", index=False)

    print("✅ Loaded customers, usage_events, feedback, churn_labels")

if __name__ == "__main__":
    main()
