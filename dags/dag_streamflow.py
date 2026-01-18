"""
StreamFlow Analytics Platform - Main Orchestration DAG

Orchestrates: Kafka Ingest -> Spark ETL -> Validation
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

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

STAGE = '@BRONZE.SPARK_STAGE'
LOCAL_DIR = '/opt/spark-data'
def put_file_to_stage():
    hook = SnowflakeHook(snowflake_conn_id = 'snowflake_connection')
    files = [
        (f'{LOCAL_DIR}/gold/transactions/*.parquet', 'transactions'),
        (f'{LOCAL_DIR}/gold/user_activities/*.parquet', 'user_activities')
    ]
    for path, prefix in files:
        sql = f'PUT file://{path} {STAGE}/{prefix} AUTO_COMPRESS=TRUE OVERWRITE=TRUE;'
        hook.run(sql)
with DAG(
    dag_id='streamflow_main',
    default_args=default_args,
    start_date=datetime(2026, 1, 18),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    # TODO: Define tasks
    # - ingest_kafka: Run ingest_kafka_to_landing.py : done
    # - spark_etl: spark-submit etl_job.py : done
    # - validate: Check output files: done 
    #part 1) 
    user_consumer = BashOperator(
        task_id="user_consumer",
        bash_command="""
    python /opt/spark-jobs/ingest_kafka_to_landing.py \
        --topic user_events --duration 5
    """
    )

    transaction_consumer = BashOperator(
        task_id='transaction_consumer',
        bash_command="""
        python /opt/spark-jobs/ingest_kafka_to_landing.py \
        --topic transaction_events --duration 5
    """
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

    upload_to_stage = PythonOperator(
        task_id='upload_to_internal_stage',
        python_callable=put_file_to_stage
    )

    copy_trans_to_table = SnowflakeOperator(
        task_id='copy_trans_command',
        snowflake_conn_id='snowflake_connection',
        sql="""
        COPY INTO BRONZE.RAW_TRANSACTIONS
        FROM @BRONZE.SPARK_STAGE/transactions/
        FILE_FORMAT=(FORMAT_NAME = BRONZE.file_parquet)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR='ABORT_STATEMENT';
        """
    )

    copy_user_act_to_table = SnowflakeOperator(
        task_id='copy_user_act_command',
        snowflake_conn_id='snowflake_connection',
        sql="""
        COPY INTO BRONZE.RAW_USER_EVENTS
        FROM @BRONZE.SPARK_STAGE/user_activities/
        FILE_FORMAT=(FORMAT_NAME = BRONZE.file_parquet)
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR='ABORT_STATEMENT';
        """
    )
    
    [user_consumer, transaction_consumer] >> spark_etl >> validate >> upload_to_stage >> [copy_trans_to_table, copy_user_act_to_table]
