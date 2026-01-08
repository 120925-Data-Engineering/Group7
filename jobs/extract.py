from pyspark.sql import SparkSession, DataFrame
import os

def read_json_lines(spark: SparkSession, file_path: str, schema=None) -> DataFrame:
    if not os.path.exists(file_path):
        print(f'File not found: {file_path}')
        return None
    json_path = os.path.join(file_path, '*.json')
    if schema:
        df = spark.read.schema(schema).json(json_path)
    else:
        df = spark.read.json(json_path)
    
    record_count = df.count()
    if not record_count:
        print(f'There is no data in {json_path}')
        return None
    print(f'Loaded {record_count} records')

    return df

def extract(spark: SparkSession, input_path: str) -> tuple:
    topic = {
        'transaction': 'transaction_events', 
        'user': 'user_events'
        }
    user_events_path  = os.path.join(input_path, topic['user'])
    transaction_events_path = os.path.join(input_path, topic['transaction'])

    user_events = read_json_lines(spark, user_events_path)
    transaction_events = read_json_lines(spark, transaction_events_path)

    return (user_events, transaction_events)
    