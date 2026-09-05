import pandas as pd
from sqlalchemy import create_engine, text
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_csv_to_postgres(
    csv_path: str,
    table_name: str,
    # postgres_conn: str,
    load_type='append',
    **context
) -> None:
    """
    Generic CSV loader.
    """

    hook = PostgresHook(postgres_conn_id="bankdb_postgres")

    # engine = create_engine(postgres_conn)
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_csv(csv_path)

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
    )

    if load_type.lower() == 'truncate':
        with engine.begin() as conn:
            conn.execute(text(f'TRUNCATE TABLE {table_name}'))
            print(f'{table_name} has been TRUNCATED')

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=5000
    )


    context["ti"].xcom_push(
        key="load_metadata",
        value={
            "rows_inserted": len(df),
            "load_type": load_type,
            "target_table": table_name
        }
    )

    print(f'Loaded {len(df)} rows into {table_name}')


    