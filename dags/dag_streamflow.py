"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'student',
    # TODO: Add retry logic, email alerts, etc.
}

with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py
    # - spark_etl: spark-submit etl_job.py
    # - validate: Check output files
    ingest_kafka = BashOperator(
        task_id="ingest_kafka",
        bash_command="""
python /opt/airflow/jobs/ingest_kafka_to_landing.py --topic user_events --duration 30 --output /opt/spark-data/landing
python /opt/airflow/jobs/ingest_kafka_to_landing.py --topic transaction_events --duration 30 --output /opt/spark-data/landing
"""
    )

    # 2) Spark ETL
    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command="""
        spark-submit \
          --master spark://spark-master:7077 \
          /opt/spark/jobs/etl_job.py \
          --name StreamFlow-ETL \
          --landing /opt/spark-data/landing \
          --gold /opt/spark-data/gold
        """,
    )

    
    # TODO: Set dependencies
    ingest_kafka >> spark_etl #>> validate
    