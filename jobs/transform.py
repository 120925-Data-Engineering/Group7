from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def transform(user_events: DataFrame, transaction_events: DataFrame):

    standardize_user_events = standardize(user_events)
    standardize_transaction_events = standardize(
        transaction_events,
        ['transaction_type', 'status', 'line_items', 'transaction_id', 'user_id'],
        {
            'payment_method': 'USD'
        },
        ['transaction_id']
    )
    
    return (
       standardize_transaction_events,
       standardize_user_events
    )

def standardize(df: DataFrame, drop_na: list = None, fill_na: dict = None, drop_dup: list = None) -> DataFrame:
    if drop_na: df = df.dropna(subset=drop_na)
    if fill_na: df = df.fillna(fill_na)
    if drop_dup: df = df.dropDuplicates(subset=drop_dup)
    df = df.withColumn(
        'event_date',
        F.to_date('timestamp')
    )
    return df