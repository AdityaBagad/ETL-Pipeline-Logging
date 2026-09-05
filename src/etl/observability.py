from sqlalchemy import create_engine, text
from airflow.providers.postgres.hooks.postgres import PostgresHook

def log_airflow_execution(context):
    # conn_string = context["params"]["postgres_conn"]
    # engine = create_engine(conn_string)

    hook = PostgresHook(
        postgres_conn_id="bankdb_postgres"
    )

    engine = hook.get_sqlalchemy_engine()

    task_instance = context["task_instance"]
    
    error_msg = None

    # Get Error Message if Any of the task fails
    if context.get("exception"):
        error_msg = str(context["exception"])

    # Calculate Total Time taken for the Task Run
    duration = None
    if (task_instance.start_date and task_instance.end_date):
        duration = (task_instance.end_date - task_instance.start_date).total_seconds()

    # Get Metadata of Table Operations
    metadata = task_instance.xcom_pull(
        task_ids = task_instance.task_id,
        key = "load_metadata"
    ) or {}

    rows_inserted = metadata.get("rows_inserted")
    load_type = metadata.get("load_type")
    target_table = metadata.get("target_table")

    query = text(
        """
        INSERT INTO etl_observability
        (
            run_id,
            dag_id,
            task_id,
            operator,
            state,
            try_number,
            start_date,
            end_date,
            duration_sec,
            rows_inserted,
            load_type,
            target_table,
            error_msg
        )
        VALUES
        (
            :run_id,
            :dag_id,
            :task_id,
            :operator,
            :state,
            :try_number,
            :start_date,
            :end_date,
            :duration_sec,
            :rows_inserted,
            :load_type,
            :target_table,
            :error_msg
        )
        """
    )

    with engine.begin() as conn:
        conn.execute(
            query,
            {
                "run_id": context["run_id"],
                "dag_id": context["dag"].dag_id,
                "task_id": task_instance.task_id,
                "operator": task_instance.operator,
                "state": task_instance.state,
                "try_number": task_instance.try_number,
                "start_date": task_instance.start_date,
                "end_date": task_instance.end_date,
                "duration_sec": duration,
                "rows_inserted": rows_inserted,
                "load_type": load_type,
                "target_table": target_table,
                "error_msg": error_msg
            }
        )