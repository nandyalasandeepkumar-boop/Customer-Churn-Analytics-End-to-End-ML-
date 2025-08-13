import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text
from utils import load_config, get_engine

def main():
    cfg = load_config()
    eng = get_engine(cfg)
    lookback = cfg["features"]["lookback_days"]
    end_ts = datetime(2025, 8, 12)  # freeze for reproducibility
    start_ts = end_ts - timedelta(days=lookback)

    with eng.begin() as conn:
        # aggregate 30d usage
        q_usage = text("""
            SELECT customer_id,
                   SUM(sessions) AS sessions_30d,
                   SUM(minutes_used) AS minutes_30d
            FROM usage_events
            WHERE event_ts BETWEEN :start_ts AND :end_ts
            GROUP BY 1
        """)
        usage30 = pd.read_sql(q_usage, conn, params={"start_ts": start_ts, "end_ts": end_ts})

        # customers + tenure
        customers = pd.read_sql(text("SELECT * FROM customers"), conn)
        customers["signup_date"] = pd.to_datetime(customers["signup_date"])
        customers["tenure_days"] = (end_ts - customers["signup_date"]).dt.days

        fb = pd.read_sql(text("SELECT customer_id, nps FROM feedback"), conn)
        labels = pd.read_sql(text("SELECT * FROM churn_labels"), conn)

    feat = (customers
            .merge(usage30, on="customer_id", how="left")
            .merge(fb, on="customer_id", how="left")
            .merge(labels, on="customer_id", how="left"))

    feat.fillna({"sessions_30d":0, "minutes_30d":0, "nps":0, "churned":0}, inplace=True)

    # write feature table version
    with eng.begin() as conn:
        feat[["customer_id","tenure_days","sessions_30d","minutes_30d","nps","segment","churned"]] \
            .rename(columns={"churned":"label"}) \
            .to_sql("features_churn_v1", conn, if_exists="replace", index=False)

    print("✅ Built features_churn_v1")

if __name__ == "__main__":
    main()
