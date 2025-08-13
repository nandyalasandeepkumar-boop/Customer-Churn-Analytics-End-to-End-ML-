import joblib
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from utils import load_config, get_engine

def main():
    cfg = load_config()
    eng = get_engine(cfg)
    feat = pd.read_sql("SELECT * FROM features_churn_v1", eng)

    y = feat["label"].astype(int)
    X = feat.drop(columns=["label","customer_id"])

    cat_cols = ["segment"]
    num_cols = [c for c in X.columns if c not in cat_cols]

    pre = ColumnTransformer(
        [("ohe", OneHotEncoder(handle_unknown="ignore"), cat_cols)],
        remainder="passthrough"
    )

    clf = Pipeline([
        ("prep", pre),
        ("lr", LogisticRegression(max_iter=1000))
    ])

    X_train, X_test, y_train, y_test = train_test_split(X, y, stratify=y, test_size=0.33, random_state=42)
    clf.fit(X_train, y_train)

    proba = clf.predict_proba(X_test)[:,1]
    auc = roc_auc_score(y_test, proba)
    print(f"✅ AUC: {auc:.3f}")

    # save model
    import os
    os.makedirs("models", exist_ok=True)
    joblib.dump({"pipeline": clf, "columns": X.columns.tolist(), "model_version": cfg["project"]["model_version"]},
                f"models/churn_{cfg['project']['model_version']}.joblib")
    print("✅ Model saved")

if __name__ == "__main__":
    main()
