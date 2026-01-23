# consumer.poll(timeout_ms = number )
Use consumer.poll over `for message in consumer` because:
    ⚪️ required for batch window
    ⚪️ for-loop runs forever / no time window
    ⚪️ timeout_ms - read kafka message every (number) second
    ⚪️ Returns ConsumerRecords<K,V> object 
    ⚪️ ConsumerRecords<K, V> = Map<TopicPartition,List<ConsumerRecord<K,V>>>
    ⚪️ ConsumerRecord<K,V> = kafka message. E.g: `ConsumerRecord(value=orderA, offset=0)`

# Parse args 
Command-line arguments are values you pass when you run a script
Example: `python consumer.py --topic orders --duration 30 --output ./data/landing`
Python don't know what `orders`, `30` and `./data/landing` mean => using argparse
Create an Argument Parse
    `parser = argparse.ArgumentParser(description=String)`
Define Arguments
    `parser.add_argument('--topic',type=str,required=True,default= 'defaultName',help= 'Kafka Topic Name')`
Parse Argument
    `args = parser.parse_args()`

# Docker commands
Run producer - docker compose exec airflow python /opt/producers/transaction_events_producer.py
            - docker compose exec airflow python /opt/producers/user_events_producer.py

Run consumer - docker compose exec airflow python /opt/spark-jobs/ingest_kafka_to_landing.py --topic user_events --duration 30
            - docker compose exec airflow python /opt/spark-jobs/ingest_kafka_to_landing.py --topic transaction_events --duration 30

Run etl job - docker compose exec spark-master spark-submit /opt/spark-jobs/etl_job.py \
                --name "Group7-Pipeline" \
                --master spark://spark-master:7077 \
                --landing /opt/spark-data/landing \
                --gold /opt/spark-data/gold

# Transformation
    - Transaction -
- Total of purchases daily/monthly
- Total of chargeback daily/monthly
- Total of refund daily/monthly

- Users that has more than 10 refunds in a month
- Users that has chargeback more than 5 in a month

- Compare the purchases made by US consumers to others

# Dockerfile.airflow for MAC
# Install OpenJDK-17 (Required for Spark 3.5)
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

- Have to rebuild docker by:
    docker compose build --no-cache
    docker compose up -d --force-recreate

# Spark Gold Zone
1. Transactions (line-item or transaction grain)

Example: transactions.csv
One row per transaction (or per transaction item — either is OK, just be consistent)
Fully flattened (no arrays, no nested structs)
Correct data types
Derived numeric fields allowed (e.g. item_revenue)
Business-neutral (no KPIs yet)

transaction_id
user_id
transaction_ts
status
payment_method
currency
product_id
quantity
unit_price
item_revenue

2. User events

Example: user_events.csv
One row per event
Clean timestamps
Standardized event names
No aggregations

event_id
user_id
event_type
event_ts
page
device
country

3. Products

Example: products.csv
One row per product
Latest known attributes
No historical tracking needed at this stage

product_id
product_name
category
price
is_active

4. Customers

Example: customers.csv
One row per customer
Cleaned demographics
Stable IDs

customer_id
first_name
last_name
email
country
signup_date
