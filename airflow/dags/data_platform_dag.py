from __future__ import annotations

import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

PROJECT_ROOT = "/opt/project"

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def _audit_start(**context):
    run_id = context["ti"].xcom_pull(key="run_id", task_ids="make_run_id")
    raw_file = context["ti"].xcom_pull(key="raw_file", task_ids="push_raw_file")
    dt = context["ds"]

    hook = PostgresHook(postgres_conn_id="dp_postgres")
    hook.run(
        """
        INSERT INTO run_audit(run_id, dag_id, started_at, status, raw_path, curated_path, notes)
        VALUES (%s, %s, NOW(), %s, %s, %s, %s)
        """,
        parameters=(
            run_id,
            context["dag"].dag_id,
            "RUNNING",
            f"data/raw/{raw_file}",
            f"warehouse/curated/dt={dt}",
            "started",
        ),
    )

def _audit_finish(status: str, **context):
    run_id = context["ti"].xcom_pull(key="run_id", task_ids="make_run_id")
    hook = PostgresHook(postgres_conn_id="dp_postgres")
    hook.run(
        "UPDATE run_audit SET finished_at=NOW(), status=%s, notes=%s WHERE run_id=%s",
        parameters=(status, status.lower(), run_id),
    )

def _push_raw_file(**context):
    raw_file = context["ti"].xcom_pull(task_ids="capture_raw_filename")
    context["ti"].xcom_push(key="raw_file", value=raw_file)
    return raw_file

def _should_process(**context) -> bool:
    raw_file = context["ti"].xcom_pull(key="raw_file", task_ids="push_raw_file")
    hook = PostgresHook(postgres_conn_id="dp_postgres")
    records = hook.get_records(
        "SELECT 1 FROM processed_files WHERE raw_file = %s LIMIT 1",
        parameters=(raw_file,),
    )
    return len(records) == 0

with DAG(
    dag_id="data_platform_guardrails_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["data-platform", "spark", "airflow", "guardrails"],
) as dag:

    make_run_id = PythonOperator(
        task_id="make_run_id",
        python_callable=lambda **ctx: ctx["ti"].xcom_push(key="run_id", value=str(uuid.uuid4())),
    )

    generate_events = BashOperator(
        task_id="generate_events",
        bash_command=(
            "RUN_ID='{{ ti.xcom_pull(task_ids=\"make_run_id\", key=\"run_id\") }}' "
            "RAW_OUT_DIR=/opt/project/data/raw "
            "N_EVENTS=20000 "
            "python /opt/project/scripts/generate_events.py "
            "&& ls -1t /opt/project/data/raw | head -n 1 > /tmp/latest_raw_file"
        ),
    )

    capture_raw_filename = BashOperator(
        task_id="capture_raw_filename",
        bash_command="cat /tmp/latest_raw_file",
        do_xcom_push=True,
    )

    push_raw_file = PythonOperator(
        task_id="push_raw_file",
        python_callable=_push_raw_file,
    )

    skip_if_processed = ShortCircuitOperator(
        task_id="skip_if_processed",
        python_callable=_should_process,
    )

    audit_start = PythonOperator(
        task_id="audit_start",
        python_callable=_audit_start,
    )

    spark_transform = SparkSubmitOperator(
        task_id="spark_transform",
        application="/opt/project/jobs/spark/transform_events.py",
        conn_id="spark_default",
        verbose=True,
        application_args=[
            "--project_root", PROJECT_ROOT,
            "--run_id", "{{ ti.xcom_pull(task_ids='make_run_id', key='run_id') }}",
            "--raw_file", "{{ ti.xcom_pull(task_ids='push_raw_file', key='raw_file') }}",
            "--dt", "{{ ds }}",
            "--pg_url", "jdbc:postgresql://postgres:5432/dp",
            "--pg_user", "dp",
            "--pg_password", "dp",
        ],
    )

    audit_success = PythonOperator(
        task_id="audit_success",
        python_callable=lambda **ctx: _audit_finish("SUCCESS", **ctx),
        trigger_rule="all_success",
    )

    audit_failed = PythonOperator(
        task_id="audit_failed",
        python_callable=lambda **ctx: _audit_finish("FAILED", **ctx),
        trigger_rule="one_failed",
    )

    make_run_id >> generate_events >> capture_raw_filename >> push_raw_file >> skip_if_processed >> audit_start >> spark_transform
    spark_transform >> [audit_success, audit_failed]
