"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes Parquet to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from jobs import spark_session_factory
import argparse

def helper(args_config: list) -> dict:
    config_dict = None
    if args_config:
        config_dict = {}
        for conf in args_config:
            key,value = conf.split('=')
            config_dict[key] = value
    return config_dict

def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    # TODO: Implement
    pass


if __name__ == "__main__":
    # TODO: Create SparkSession, parse args, run ETL    
    #For SparkSession
    parser = argparse.ArgumentParser(description='ETL Job')
    parser.add_argument('--name', default='Group7-Pipeline', help='App Name')
    parser.add_argument('--master', help='Spark Master URL')
    #Parsing arguments for spark config (OPTIONAL) 
    #spark.config works with key,value pairs so have to convert to dictionary
    parser.add_argument('--config', action='append',help='Spark Configurations (OPTIONAL)')

    #For ETL Job
    parser.add_argument('--landing', required=True, help='Landing Zone Directory') 
    parser.add_argument('--gold', required=True, help='Gold Zone Directory')

    args = parser.parse_args()
    config_dict = None
    if args.config:
        config_dict = helper(args.config)
    spark = spark_session_factory(app_name=args.name, master=args.master, config_overrides=config_dict)

    try:
        run_etl(spark=spark, input_path=args.landing, output_path=args.gold)
    except Exception as e:
        print(f'ETL job failed: {e}')
    finally:
        spark.stop()