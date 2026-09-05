CREATE TABLE IF NOT EXISTS cards (
    card_id             VARCHAR(50) PRIMARY KEY,
    customer_id         VARCHAR(50),
    card_type           VARCHAR(50),
    card_network        VARCHAR(50),
    credit_limit        NUMERIC(15,2),
    card_status         VARCHAR(50),
    contactless         BOOLEAN,
    card_mode           VARCHAR(50),
    issue_date          DATE,
    expiry_date         DATE,
    inserted_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);