import joblib
import pandas as pd
from sqlalchemy import text
from utils import load_config, get_engine

def main():
    cfg = load_config()
    eng = get_engine(cfg)
    model_path = f"models/churn_{cfg['project']['model_version']}.joblib"
    bundle = joblib.load(model_path)
    pipe = bundle["pipeline"]
    version = bundle["model_version"]

    feat = pd.read_sql("SELECT * FROM features_churn_v1", eng)
    X = feat.drop(columns=["label","customer_id"])
    probs = pipe.predict_proba(X)[:,1]

    out = pd.DataFrame({
        "model_version": version,
        "customer_id": feat["customer_id"],
        "churn_prob": probs
    })

    with eng.begin() as conn:
        out.to_sql("ml_churn_predictions", conn, if_exists="append", index=False)

    print(f"✅ Wrote {len(out)} predictions to ml_churn_predictions")

if __name__ == "__main__":
    main()
