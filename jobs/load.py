from pyspark.sql import DataFrame
import os, shutil

def load (df: DataFrame, output_path:str) -> bool:
    if df is None:
        print('No data to load')
        return False

    try:
        df.coalesce(1).write.mode('OVERWRITE').parquet(output_path)
        print(f'Successfully wrote records to gold zone')
        return True
    except Exception as e:
        print(f'Failed to load data: {e}')
        return False
    
def to_archive(files: list[str], file_path: str) -> None:
    processed_dir = os.path.join(file_path, 'archive')
    os.makedirs(processed_dir, exist_ok=True)
    for f in files:
        shutil.move(f, os.path.join(processed_dir, os.path.basename(f)))