\c fraud_detections

CREATE TABLE scores (
    transaction_id TEXT PRIMARY KEY,
    us_state TEXT,
    merch TEXT,
    cat_id TEXT,
    amount DOUBLE PRECISION,
    score NUMERIC,
    fraud_flag INT,
    model_version TEXT DEFAULT 'initial model',
    created_at TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE labeled_transactions (
    id SERIAL PRIMARY KEY,
    transaction_time TIMESTAMPTZ NOT NULL,
    merch TEXT,
    cat_id TEXT,
    amount DOUBLE PRECISION,
    name_1 TEXT,
    name_2 TEXT,
    gender TEXT,
    street TEXT,
    one_city TEXT,
    us_state TEXT,
    post_code INTEGER,
    lat DOUBLE PRECISION,
    lon DOUBLE PRECISION,
    population_city INTEGER,
    jobs TEXT,
    merchant_lat DOUBLE PRECISION,
    merchant_lon DOUBLE PRECISION,
    target SMALLINT
);

CREATE INDEX labeled_transactions_time_idx
ON labeled_transactions (transaction_time);