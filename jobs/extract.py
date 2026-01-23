from pyspark.sql import SparkSession, DataFrame
import os, glob

def read_json_lines(spark: SparkSession, file_path: str, schema=None) -> tuple[DataFrame | None, list[str]]:
    if not os.path.exists(file_path):
        print(f'File not found: {file_path}')
        return None, []
    files = glob.glob(os.path.join(file_path, '*.json'))
    if not files:
        print(f'No new json files in {file_path}')
        return None, []
    reader = spark.read.schema(schema) if schema else spark.read
    df = reader.json(files).cache()
    record_count = df.count()
    if not record_count:
        print(f'There is no data in {file_path}')
        return None, []
    print(f'Loaded {record_count} records')

    return df, files

def extract(spark: SparkSession, input_path: str) -> tuple:
    topic = {
        'transaction': 'transaction_events', 
        'user': 'user_events'
        }
    user_events_path  = os.path.join(input_path, topic['user'])
    transaction_events_path = os.path.join(input_path, topic['transaction'])

    user_events, user_files = read_json_lines(spark, user_events_path)
    transaction_events, trans_files = read_json_lines(spark, transaction_events_path)

    return (user_events, user_files), (transaction_events, trans_files)
    