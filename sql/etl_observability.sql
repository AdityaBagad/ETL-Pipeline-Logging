CREATE TABLE IF NOT EXISTS etl_observability (
    id              SERIAL PRIMARY KEY,
    run_id          VARCHAR(255) NOT NULL,
    dag_id          VARCHAR(255) NOT NULL,
    task_id         VARCHAR(255),
    operator        VARCHAR(100),
    state           VARCHAR(50) NOT NULL,
    try_number      INTEGER,
    start_date      TIMESTAMP,
    end_date        TIMESTAMP,
    duration_sec    FLOAT,
    rows_inserted   BIGINT,
    load_type       VARCHAR(20),
    target_table    VARCHAR(100),
    error_msg       TEXT,
    logged_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);