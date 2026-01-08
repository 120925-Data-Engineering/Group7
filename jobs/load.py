from pyspark.sql import DataFrame

def load (df: DataFrame, output_path:str) -> bool:
    if df is None:
        print('No data to load')
        return False

    try:
        df.write.mode('append').parquet(output_path)
        print(f'Successfully wrote {df.count()} records to gold zone')
        return True
    except Exception as e:
        print(f'Failed to load data: {e}')
        return False
    