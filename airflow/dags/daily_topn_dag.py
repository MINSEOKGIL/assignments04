# airflow/dags/daily_topn_dag.py

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_topn_calculator',
    default_args=default_args,
    description='매일 자동으로 전날 TopN 조회수 계산',
    schedule_interval='0 1 * * *',  # 매일 새벽 1시
    start_date=days_ago(1),
    catchup=False,
    tags=['topn', 'daily', 'spark'],
)

# Spark Submit으로 TopN 계산
calculate_daily_topn = SparkSubmitOperator(
    task_id='calculate_daily_topn',
    application='/opt/spark/jobs/spark_topn_job.py',
    conn_id='spark_default',  # Airflow Connection ID
    application_args=['daily', '{{ ds }}', '10'],  # [mode, date, top_n]
    conf={
        'spark.driver.memory': '1g',
        'spark.executor.memory': '2g',
        'spark.executor.cores': '2',
    },
    jars='/opt/spark/jars/postgresql-42.7.1.jar',
    verbose=True,
    dag=dag,
)

# 성공 알림
notify_success = BashOperator(
    task_id='notify_success',
    bash_command='echo "✅ Daily TopN for {{ ds }} completed!"',
    dag=dag,
)

calculate_daily_topn >> notify_success