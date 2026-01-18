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
    #cleaned_trans = transactions(standardize_transaction_events)
    """daily_rev = daily_revenue(standardize_transaction_events)
    customer_monthly_spent = customer_mothly(standardize_transaction_events)

    selected_transaction_events = transaction_evts(standardize_transaction_events)
    selected_user_events = user_evts(standardize_user_events)
    cart_to_purchase = add_to_cart_to_purchase(selected_user_events, selected_transaction_events)
    ranked_spent = rank_user_spent(selected_transaction_events)"""
    
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
"""
def daily_revenue (df: DataFrame) -> DataFrame:
    return (
        df.groupBy('event_date')
        .agg(
            F.sum(
                F.when(
                    ((F.col('transaction_type') == 'purchase')) & (F.col('status') == 'completed')
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
    df =(
            df.filter(F.col('status') != 'failed')
            .withColumn(
            'geo_customers',
            F.when(
                F.col('billing_address.state').isin(us_state),
                'US'
            ).otherwise('Non_US')
        )
        )

    return (
        df.groupBy('user_id', F.month('timestamp').alias('month'))
        .agg(
            F.sum('total').alias('monthly_spent'),
            F.sum(F.when(F.col('transaction_type') == 'refund', 1).otherwise(0)).alias('refund_counts'),
            F.sum(F.when(F.col('transaction_type') == 'chargeback', 1).otherwise(0)).alias('chargeback_counts')
        )
        .withColumn(
            'risk_consumer',
            F.when((F.col('refund_counts') >= 10) | (F.col('chargeback_counts') >= 5), True).otherwise(False)
        )
    )

def transaction_evts(df: DataFrame) -> DataFrame:
    return (
        df
        .select(
            'transaction_id', 'user_id', 'transaction_type', 'status', 
            'payment_method', 'currency', 'timestamp',
            F.col('billing_address.city').alias('bill_city'),
            F.col('billing_address.country').alias('bill_country'),
            F.col('billing_address.state').alias('bill_state'),
            F.col('shipping_address.city').alias('ship_city'),
            F.col('shipping_address.country').alias('ship_country'),
            F.explode('products').alias('p'),
            'subtotal', 'tax', 'total'
        )
        .select(
            'transaction_id', 'user_id', 'transaction_type', 'status', 'payment_method', 'currency',
            'bill_city', 'bill_country', 'bill_state', 'ship_city', 'ship_country', 'timestamp',
            F.col("p.product_id").alias("product_id"),
            F.col("p.product_name").alias("product_name"),
            F.col("p.category").alias("category"),
            F.col("p.quantity").alias("item_qty"),
            F.col("p.unit_price").alias("unit_price"),
            (F.col('p.quantity') * F.col('p.unit_price')).alias('item_revenue')           
        )
        .withColumn(
            'geo_customers',
            F.when(
                F.col('bill_state').isin(us_state),
                'US'
            ).otherwise('Non_US')
        )
    )

def user_evts(df: DataFrame) -> DataFrame:
    return (
        df.select(
            'event_id', 'user_id', 'session_id', 'event_type', 'timestamp',
            'page', 'country', 'city', 'product_id'
        )
    )

def add_to_cart_to_purchase(user_evn: DataFrame, transaction_env: DataFrame) -> DataFrame:
    carts = (
        user_evn
        .filter(
            F.col('event_type') == 'add_to_cart'
        )
        .select(
            'user_id', 'timestamp', 'product_id'
        )
    )

    return (
        carts.alias('c')
        .join(
            transaction_env.alias('t'),
            on=[
                F.col('c.user_id') == F.col('t.user_id'),
                F.col('c.product_id') == F.col('t.product_id'),
                F.col('c.timestamp') <= F.col('t.timestamp')
            ],
            how='inner'
        )
        .select(
            F.col('t.transaction_id'),
            F.col('c.user_id'),
            F.col('t.product_id'),
            F.col('c.timestamp').alias('add_to_cart_ts'),
            F.col('t.timestamp').alias('purchase_ts'),
            F.col('t.item_qty').alias('purchase_qty'),
            F.col('t.item_revenue')
        )
    )

def rank_user_spent(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy('user_id', 'transaction_id')
        .agg(
            F.sum('item_revenue').alias('total_spent')
        )
        .withColumn(
            'rank',
            F.dense_rank().over(Window.orderBy(F.col("total_spent").desc()))
        )
    )"""

"""def transactions(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn('event_ts', F.to_timestamp('timestamp'))
        .withColumn('p', F.explode('line_items'))
        .select(
            'transaction_id', 'user_id', 'transaction_type', 'event_ts',
            'status', 'payment_method', 'currency', 'original_transaction_id',
            F.col('p.product_id').alias('product_id'),
            F.col('p.product_name').alias('product_name'),
            F.col('p.category').alias('category'),
            F.col('p.quantity').cast('int').alias('quantity'),
            F.col('p.unit_price').cast('decimal(15,2)').alias('unit_price'),
            F.col('subtotal').cast('decimal(15,2)').alias('subtotal'),
            F.col('tax').cast('decimal(15,2)').alias('tax'),
            F.col('total').cast('decimal(15,2)').alias('total'),
            F.col('billing_address.city').alias('bill_city'),
            F.col('billing_address.country').alias('bill_country'),
            F.col('billing_address.state').alias('bill_state'),
            F.col('shipping_address.country').alias('ship_country'),
            F.col('shipping_address.city').alias('ship_city')
        )
        .withColumn(
            'item_revenue',
            (F.col('quantity') * F.col('unit_price')).cast('decimal(15,2)')
        )
    )

def user_activities(df: DataFrame) -> DataFrame:
    return(
        df
        .select(
            'event_id', 'user_id', 'event_type',
            'page', 'country', 'device', 'timestamp'
        )
        .withColumn('event_ts', F.to_timestamp('timestamp'))
    )

def product_events(df: DataFrame) -> DataFrame:
    return(
        df
        .filter(F.col('product_id').isNotNull())
        .select(
            'product_id', 'event_id', 'user_id', 'event_type',
            'timestamp', 'page', 'country',
            F.col('quantity').cast('int')
        )
    )"""

