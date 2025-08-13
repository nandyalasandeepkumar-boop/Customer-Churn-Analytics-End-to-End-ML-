-- Core entities
CREATE TABLE IF NOT EXISTS customers(
  customer_id VARCHAR PRIMARY KEY,
  signup_date DATE,
  segment VARCHAR
);

CREATE TABLE IF NOT EXISTS usage_events(
  customer_id VARCHAR,
  event_ts TIMESTAMP,
  sessions INT,
  minutes_used INT
);

CREATE TABLE IF NOT EXISTS feedback(
  customer_id VARCHAR,
  nps INT,
  last_contact_ts TIMESTAMP
);

-- Supervised label (1=churned, 0=active)
CREATE TABLE IF NOT EXISTS churn_labels(
  customer_id VARCHAR PRIMARY KEY,
  churned INT CHECK (churned IN (0,1))
);

-- Feature store (wide table, versioned)
CREATE TABLE IF NOT EXISTS features_churn_v1(
  customer_id VARCHAR PRIMARY KEY,
  tenure_days INT,
  sessions_30d INT,
  minutes_30d INT,
  nps INT,
  segment VARCHAR,
  label INT
);

-- Scored predictions written back for BI
CREATE TABLE IF NOT EXISTS ml_churn_predictions(
  prediction_id BIGSERIAL PRIMARY KEY,
  scored_at TIMESTAMP DEFAULT NOW(),
  model_version VARCHAR,
  customer_id VARCHAR,
  churn_prob DOUBLE PRECISION
);
