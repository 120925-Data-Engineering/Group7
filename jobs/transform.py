from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

us_state = {
    "AL", "AK", "AZ", "AR", "CA",
    "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA",
    "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO",
    "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT",
    "VA", "WA", "WV", "WI", "WY"
}

def transform(user_events: DataFrame, transaction_events: DataFrame) -> tuple[DataFrame, DataFrame]:

    standardize_user_events = standardize(user_events)
    standardize_transaction_events = standardize(
        transaction_events,
        ['transaction_type', 'status', 'products', 'transaction_id', 'user_id'],
        {
            'payment_method': 'USD'
        },
        ['transaction_id']
    )

    geo_customers = us_and_non_us_customers(standardize_transaction_events)
    daily_rev = daily_revenue(geo_customers)
    customer_monthly_spent = customer_mothly(geo_customers)
    
    return (
       daily_rev,
       customer_monthly_spent
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

def enrich (df1: DataFrame, df2: DataFrame, join_on: str, join_type: str) -> DataFrame:
    if df1 is not None and df2 is not None:
        return (
            df1.join(
                df2,
                join_on,
                join_type
            )
        )
    if df1 is not None:
        return df1
    if df2 is not None:
        return df2
    print('WARNING: No data to transform')
    return None

def daily_revenue (df: DataFrame) -> DataFrame:
    return (
        df.groupBy('event_date')
        .agg(
            F.sum(
                F.when(
                    (F.col('transaction_type') == 'purchase') & (F.col('status') == 'completed')
                    , F.col('total')
                ).otherwise(0)
            ).alias('revenue'),
            F.sum(
                F.when(
                    (F.col('transaction_type') == 'refund') & (F.col('status') == 'completed')
                    , F.col('total')
                ).otherwise(0)
            ).alias('refund'),
            F.sum(
                F.when(
                    (F.col('transaction_type') == 'chargeback') & (F.col('status') == 'completed')
                    , F.col('total')
                ).otherwise(0)
            ).alias('chargeback')
        )
        .withColumn(
            'net_revenue',
            F.col('revenue') - F.col('refund') - F.col('chargeback')
        )
        .withColumnRenamed('event_date', 'date')
    )

def customer_mothly(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        'signed_amount',
        F.when((F.col('transaction_type') == 'purchase') & (F.col('status') == 'completed'), F.col('total'))
        .when((F.col('transaction_type').isin('refund', 'chargeback')) & (F.col('status') == 'completed'), -F.col('total'))
        .otherwise(0)
    )

    return (
        df.groupBy('user_id', F.month('timestamp').alias('month'), 'consumer_geo')
        .agg(
            F.sum('signed_amount').alias('monthly_spent'),
            F.sum(F.when(F.col('transaction_type') == 'refund', 1).otherwise(0)).alias('refund_counts'),
            F.sum(F.when(F.col('transaction_type') == 'chargeback', 1).otherwise(0)).alias('chargeback_counts')
        )
        .withColumn(
            'risk_consumer',
            F.when((F.col('refund_counts') >= 10) | (F.col('chargeback_counts') >= 5), True).otherwise(False)
        )
    )

def us_and_non_us_customers (df: DataFrame) -> DataFrame:
    return(
        df.withColumn(
            'consumer_geo',
            F.when(
                F.col('billing_address.state').isin(us_state),
                'US'
            ).otherwise('Non_US')
        )
    )
