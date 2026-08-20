"""
servicenow_pipeline DAG

Orchestrates the full ServiceNow Ops Analytics pipeline:
  1. Extract 5 tables from ServiceNow into raw.* (in parallel — no
     extractor depends on another's output)
  2. (added in a later step) dbt run — build staging/intermediate/marts
  3. (added in a later step) dbt test — validate the built models

For now, this DAG only contains the 5 extraction tasks, so we can verify
they run correctly (and in parallel) before adding dbt on top.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.bash import BashOperator

logger = logging.getLogger(__name__)


def alert_on_failure(context):
    """
    Runs automatically whenever any task in this DAG fails (after all
    retries are exhausted). Logs a clear, structured failure message.

    In production, this is the exact spot to add a real notification —
    e.g. requests.post(SLACK_WEBHOOK_URL, json={...}) or an smtplib
    email send. The information available here (task, dag, execution
    date, error) is the same regardless of which channel receives it.
    """
    task_instance = context["task_instance"]
    exception = context.get("exception")

    logger.error(
        "PIPELINE FAILURE — dag=%s task=%s execution_date=%s error=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context.get("execution_date"),
        exception,
    )
    # Production extension point, e.g.:
    # requests.post(SLACK_WEBHOOK_URL, json={"text": f"{task_instance.task_id} failed: {exception}"})


default_args = {
    "owner": "anusha",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": alert_on_failure,
}

with DAG(
    dag_id="servicenow_pipeline",
    default_args=default_args,
    description="Extract ServiceNow ITSM data and build dbt models",
    schedule="0 6 * * *",  # daily at 6:00 AM UTC — verified working via manual triggers first
    start_date=datetime(2026, 8, 1),
    catchup=False,  # don't backfill runs for every day since start_date — only run going forward
    tags=["servicenow", "etl"],
) as dag:

    extract_incidents = BashOperator(
        task_id="extract_incidents",
        bash_command="cd /opt/airflow && python extractors/extract_incidents.py",
    )

    extract_change_requests = BashOperator(
        task_id="extract_change_requests",
        bash_command="cd /opt/airflow && python extractors/extract_change_requests.py",
    )

    extract_sys_user_group = BashOperator(
        task_id="extract_sys_user_group",
        bash_command="cd /opt/airflow && python extractors/extract_sys_user_group.py",
    )

    extract_sys_user = BashOperator(
        task_id="extract_sys_user",
        bash_command="cd /opt/airflow && python extractors/extract_sys_user.py",
    )

    extract_cmdb_ci = BashOperator(
        task_id="extract_cmdb_ci",
        bash_command="cd /opt/airflow && python extractors/extract_cmdb_ci.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/dbt_project && dbt test",
    )

    # dbt_run can only start once ALL 5 extractors have finished successfully —
    # it reads from raw.* tables, so incomplete extraction means incomplete
    # or stale models. dbt_test only makes sense after dbt_run has actually
    # built the models it's testing.
    extractors = [
        extract_incidents,
        extract_change_requests,
        extract_sys_user_group,
        extract_sys_user,
        extract_cmdb_ci,
    ]

    extractors >> dbt_run >> dbt_test