from datetime import datetime, timedelta
import os
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.models import Variable

logger = logging.getLogger(__name__)


def fetch_new_filings(**context):
    """Fetch new filings from EDGAR API."""
    run_id = context["run_id"]
    logger.info(f"Fetching new filings for run {run_id}")
    if os.getenv("MOCK_EDGAR"):
        return {"count": 5}
    # In production, call EdgarClient to fetch filings
    return {"count": 0}


def download_raw_documents(**context):
    """Download raw filing documents."""
    run_id = context["run_id"]
    filings = context["task_instance"].xcom_pull(task_ids="fetch_new_filings")
    logger.info(f"Downloading {filings.get('count', 0)} documents for run {run_id}")
    if os.getenv("MOCK_EDGAR"):
        return {"count": filings.get("count", 0)}
    return {"count": 0}


def parse_xbrl_facts(**context):
    """Parse XBRL facts from documents."""
    run_id = context["run_id"]
    documents = context["task_instance"].xcom_pull(task_ids="download_raw_documents")
    logger.info(f"Parsing XBRL facts for run {run_id}")
    if os.getenv("MOCK_EDGAR"):
        return {"facts_count": documents.get("count", 0) * 8}
    return {"facts_count": 0}


def validate_quality_gates(**context):
    """Validate quality gates."""
    run_id = context["run_id"]
    facts = context["task_instance"].xcom_pull(task_ids="parse_xbrl_facts")
    logger.info(f"Validating quality gates for run {run_id}")

    if facts.get("facts_count", 0) == 0:
        logger.info(f"No facts found, skipping quality validation")
        raise AirflowSkipException("No filings to process")

    # In production, run check_completeness and check_psi_drift
    return {"quality_status": "PASS"}


def load_to_warehouse(**context):
    """Load facts into PostgreSQL warehouse."""
    run_id = context["run_id"]
    quality = context["task_instance"].xcom_pull(task_ids="validate_quality_gates")
    logger.info(f"Loading facts to warehouse for run {run_id}")
    if os.getenv("MOCK_EDGAR"):
        return {"rows_loaded": 40}
    return {"rows_loaded": 0}


def update_audit_trail(**context):
    """Update pipeline audit trail."""
    run_id = context["run_id"]
    logger.info(f"Updating audit trail for run {run_id}")
    return {"audit_updated": True}


def send_alerts_on_failure(**context):
    """Send alerts if pipeline fails."""
    run_id = context["run_id"]
    logger.info(f"Checking for failures in run {run_id}")


default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    "edgar_pipeline",
    default_args=default_args,
    description="SEC EDGAR filing ingestion pipeline",
    schedule_interval="@daily",
    catchup=False,
)

t1 = PythonOperator(
    task_id="fetch_new_filings",
    python_callable=fetch_new_filings,
    dag=dag,
)

t2 = PythonOperator(
    task_id="download_raw_documents",
    python_callable=download_raw_documents,
    dag=dag,
)

t3 = PythonOperator(
    task_id="parse_xbrl_facts",
    python_callable=parse_xbrl_facts,
    dag=dag,
)

t4 = PythonOperator(
    task_id="validate_quality_gates",
    python_callable=validate_quality_gates,
    dag=dag,
)

t5 = PythonOperator(
    task_id="load_to_warehouse",
    python_callable=load_to_warehouse,
    dag=dag,
)

t6 = PythonOperator(
    task_id="update_audit_trail",
    python_callable=update_audit_trail,
    dag=dag,
)

t7 = PythonOperator(
    task_id="send_alerts_on_failure",
    python_callable=send_alerts_on_failure,
    trigger_rule="one_failed",
    dag=dag,
)

t1 >> t2 >> t3 >> t4 >> t5 >> t6
[t2, t3, t4, t5, t6] >> t7
