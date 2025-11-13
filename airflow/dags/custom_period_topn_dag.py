# airflow/dags/custom_period_topn_dag.py

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
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
    'custom_period_topn_calculator',
    default_args=default_args,
    description='특정 기간의 TopN 조회수 계산 (수동 실행)',
    schedule_interval=None,  # 수동 트리거만
    start_date=days_ago(1),
    catchup=False,
    tags=['topn', 'period', 'spark', 'manual'],
)

# Spark Submit으로 기간별 TopN 계산
calculate_period_topn = SparkSubmitOperator(
    task_id='calculate_period_topn',
    application='/opt/spark/jobs/spark_topn_job.py',
    conn_id='spark_default',
    application_args=[
        'period',
        '{{ var.value.get("topn_start_date", "2019-10-01") }}',
        '{{ var.value.get("topn_end_date", "2019-10-07") }}',
        '{{ var.value.get("topn_limit", "10") }}'
    ],
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
    bash_command="""
    echo "✅ Period TopN completed!"
    echo "   Start: {{ var.value.get('topn_start_date', 'N/A') }}"
    echo "   End: {{ var.value.get('topn_end_date', 'N/A') }}"
    """,
    dag=dag,
)

calculate_period_topn >> notify_success
```

---

## 3. Airflow Spark Connection 설정

### 방법 1: Airflow UI에서 설정
```
1. http://localhost:8080 접속
2. Admin → Connections
3. + 버튼 클릭
4. 다음 정보 입력:

Connection Id: spark_default
Connection Type: Spark
Host: spark-master
Port: 7077
Extra: {"deploy-mode": "client"}

5. Save