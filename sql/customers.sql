CREATE TABLE IF NOT EXISTS customers (
    customer_id         VARCHAR(50) PRIMARY KEY,
    customer_name       VARCHAR(255),
    gender              VARCHAR(20),
    age                 INT,
    marital_status      VARCHAR(50),
    occupation          VARCHAR(100),
    annual_income       NUMERIC(15,2),
    customer_segment    VARCHAR(100),
    state               VARCHAR(100),
    city                VARCHAR(100),
    account_type        VARCHAR(100),
    customer_since      DATE,
    inserted_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);