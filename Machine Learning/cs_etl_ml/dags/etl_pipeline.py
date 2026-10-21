
from datetime import datetime, timedelta
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator


SRC_PATH = "/opt/airflow/src"
MODEL_PATH = "/opt/airflow/models/retail_quantity_model.joblib"

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def run_etl_task():
    sys.path.insert(0, SRC_PATH)
    from etl import run_etl

    run_etl()


def run_predictions_task():
    if not os.path.isfile(MODEL_PATH):
        raise FileNotFoundError(
            f"Trained model not found: {MODEL_PATH}. "
            "Train the model before running predictions."
        )

    sys.path.insert(0, SRC_PATH)
    from predict import run_predictions

    run_predictions()


with DAG(
    dag_id="cs_etl_pipeline",
    default_args=default_args,
    description="Retail data ETL and machine learning prediction pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="*/10 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["etl", "machine-learning", "retail"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_retail_etl",
        python_callable=run_etl_task,
        execution_timeout=timedelta(hours=1),
    )

    run_predictions = PythonOperator(
        task_id="run_retail_predictions",
        python_callable=run_predictions_task,
        execution_timeout=timedelta(hours=1),
    )

    run_etl >> run_predictions
