CREATE TABLE IF NOT EXISTS merchants (
    merchant_id         VARCHAR(50) PRIMARY KEY,
    merchant_name       VARCHAR(255),
    merchant_category   VARCHAR(100),
    state               VARCHAR(100),
    city                VARCHAR(100),
    merchant_risk_level VARCHAR(50),
    merchant_rating     NUMERIC(3,2),
    merchant_status     VARCHAR(50),
    merchant_since      DATE,
    inserted_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);