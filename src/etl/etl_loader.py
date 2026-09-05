from pathlib import Path
# from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text


def create_tables() -> None:
    """
    Execute all DDL files in order.
    """

    # engine = create_engine(conn_string)

    hook = PostgresHook(
        postgres_conn_id="bankdb_postgres"
    )

    engine = hook.get_sqlalchemy_engine()

    sql_dir = Path("/opt/airflow/sql")

    ddl_files = [
        "customers.sql",
        "cards.sql",
        "merchants.sql",
        "transactions.sql",
        "etl_observability.sql"
    ]

    with engine.begin() as conn:

        for ddl_file in ddl_files:

            sql = (sql_dir / ddl_file).read_text()

            conn.execute(text(sql))

            print(f"Executed {ddl_file}")