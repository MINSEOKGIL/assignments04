from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_topn_spark_submit",
    description="Calculate Daily Top-N Product Views using Spark (SparkSubmitOperator)",
    default_args=default_args,
    schedule_interval="30 0 * * *",   # 매일 00:30
    start_date=datetime(2025, 11, 16),
    catchup=False,
    tags=["spark", "topn", "batch"],
) as dag:

    run_daily_topn = SparkSubmitOperator(
        task_id="run_daily_topn",
        application="/opt/spark/jobs/spark_topn_job.py",   
        name="DailyTopNJob",
        conn_id="spark_default",                          
        jars="/opt/spark/extra-jars/postgresql-42.7.1.jar",
        executor_memory="2g",
        driver_memory="1g",
        executor_cores=2,
        verbose=True
    )
