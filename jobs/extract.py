from pyspark.sql import SparkSession, DataFrame

def read_json_lines(spark: SparkSession, file_path: str, schema=None) -> DataFrame:
    if not os.path.exists(file_path):
        print(f'File not found: {file_path}')
        return None
 
    if schema:
        df = spark.read.schema(schema).json(file_path)
    else:
        df = spark.read.json(file_path)
    
    record_count = df.count()
    if not record_count:
        print(f'There is no data in {file_path}')
        return None
    print(f'Loaded {record_count} records')

    return df

def extract(spark: SparkSession, input_path: str) -> tuple:
    topics = ['transaction_events', 'user_events']
    user_events_path  = os.path.join(input_path, topics[0])
    transaction_events_path = os.path.join(input_path, topics[1])

    user_events = read_json_lines(spark, user_events_path)