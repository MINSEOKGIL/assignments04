from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "admin",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

with DAG(
    dag_id="session_behavior_daily",
    description="Daily Session Behavior Analytics (Spark Submit)",
    default_args=default_args,
    schedule_interval="15 0 * * *",   # 매일 00:15 실행
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "session_behavior", "batch"],
) as dag:

    run_session_behavior = SparkSubmitOperator(
        task_id="run_session_behavior",
        application="/opt/spark/jobs/spark_session_behavior_job.py",   # Spark Job 파일
        name="SessionBehaviorDailyJob",
        conn_id="spark_default",                                      # Airflow → Spark 연결 ID
        application_args=["{{ ds }}"],                                 # YYYY-MM-DD 전달
        jars="/opt/spark/extra-jars/postgresql-42.7.1.jar",
        executor_memory="2g",
        driver_memory="1g",
        executor_cores=2,
        verbose=True
    )

    run_session_behavior
