"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes Parquet to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session_factory import create_spark_session
import argparse
from extract import extract
from load import load
from transform import transform

#Convert args.config to dict 
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
    user_events_df, transaction_events_df = extract(spark, input_path)
    (
        daily_revenue, customer_monthly_spent, 
        transaction_evts, user_evts,
        cart_to_purchase, ranked_spent
    ) = transform(user_events_df, transaction_events_df)
    
    daily_ok = load(daily_revenue, f"{output_path}/daily_revenue")
    customer_monthly_ok = load(customer_monthly_spent, f"{output_path}/customer_monthly_spent")
    user_evts_ok = load(user_evts, f"{output_path}/user_events" )
    transaction_evts_ok = load(transaction_evts, f"{output_path}/transaction_events")
    cart_to_purchase_ok = load(cart_to_purchase, f"{output_path}/cart_to_purchase")
    ranked_spent_ok = load(ranked_spent, f"{output_path}/customer_ranked")
    
    print(f'ETL JOB DONE!!!!')


if __name__ == "__main__":
    #For SparkSession
    parser = argparse.ArgumentParser(description='ETL Job')
    parser.add_argument('--name', default='Group7-Pipeline', help='App Name')
    parser.add_argument('--master', default='local[*]', help='Spark Master URL')
    #Parsing arguments for spark config (OPTIONAL) 
    #spark.config works with key,value pairs so have to convert to dictionary
    parser.add_argument('--config', action='append',help='Spark Configurations (OPTIONAL)')

    #For ETL Job
    parser.add_argument('--landing', required=True, help='/opt/spark-data/landing') 
    parser.add_argument('--gold', required=True, help='/opt/spark-data/gold')

    args = parser.parse_args()
    config_dict = None
    if args.config:
        config_dict = helper(args.config)
    spark = create_spark_session(app_name=args.name, master=args.master, config_overrides=config_dict)

    try:
        run_etl(spark=spark, input_path=args.landing, output_path=args.gold)
        print('ETL JOB Successful')
    except Exception as e:
        print(f'ETL job failed: {e}')
    finally:
        spark.stop()