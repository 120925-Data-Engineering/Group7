"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os

# TODO: Add retry logic, email alerts, etc.
#If any task in this DAG fails ... how to behave. default_args
default_args = {
    'owner': 'student',
    'retries': 2,                       #if task fails .. retry 2 more times
    'retry_delay': timedelta(minutes=2), #wait 2 mins between retries. 
    'execution_timeout': timedelta(minutes=60), # if task runs for more than 1 hr it kills
    #need to do email alert implementation. 
}
def validate_gold_output() -> None:
    
    #Validates that Spark wrote output to the gold layer. 
    #implemented below
    #Fails the task if the gold directory doesn't exist or is empty.
  
    gold_path = "/opt/spark-data/gold"

    if not os.path.exists(gold_path):
        raise FileNotFoundError(f"Gold output directory not found: {gold_path}")

    if not os.listdir(gold_path):
        raise FileNotFoundError(f"Gold output directory is empty: {gold_path}")

    print(f" Validation successful: Gold data exists at {gold_path}")

with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py : done
    # - spark_etl: spark-submit etl_job.py : done
    # - validate: Check output files: done 
    #part 1) 
    ingest_kafka = BashOperator(
        task_id="ingest_kafka",
        bash_command="""
    python /opt/spark-jobs/ingest_kafka_to_landing.py \
        --topic user_events --duration 30
    python /opt/spark-jobs/ingest_kafka_to_landing.py \
        --topic transaction_events --duration 30
    """,
    )

    # 2) Spark ETL
    spark_etl = BashOperator(
        task_id="spark_etl",
        bash_command="""
    spark-submit \
      --master spark://spark-master:7077 \
      /opt/spark-jobs/etl_job.py \
      --name StreamFlow-ETL \
      --landing /opt/spark-data/landing \
      --gold /opt/spark-data/gold
    """,
    )
    #part 3: 
    validate = PythonOperator(
    task_id="validate_output",
    python_callable=validate_gold_output,
    )
    
    # TODO: Set dependencies
    ingest_kafka >> spark_etl >> validate
    