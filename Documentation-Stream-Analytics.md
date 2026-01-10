# Stream Analytics Platform Documentation

## Architecture Diagram

Below is the high-level architecture of our stream analytics pipeline:

```
┌───────────────┐
│   Producers  (2) │       
│ (User, Txn)   │
└───────┬───────┘
        │
        ▼
┌───────────────┐
│     Kafka     │
│  (Topics)     │
└───────┬───────┘
        │
        ▼
┌──────────────────────────┐
│ Kafka Batch Consumer     │
│ (Timed poll window)      │
└───────┬──────────────────┘
        │
        ▼
┌──────────────────────────┐
│ Landing Zone (JSON)      │
│ Bounded micro-batches    │
└───────┬──────────────────┘
        │
        ▼
┌──────────────────────────┐
│ Spark ETL (Batch)        │
│ DataFrames / SparkSQL    │
└───────┬──────────────────┘
        │
        ▼
┌──────────────────────────┐
│ Gold Zone (Clean Data)   │
│ Analytics-ready outputs  │
└──────────────────────────┘
```

## Design Decisions

*left this blank for now *

## Setup Instructions

### 1. Start the Platform

```bash
docker compose up -d --build
```

### 2. Run Kafka Producers

```bash
docker compose exec airflow python /opt/producers/user_events_producer.py
docker compose exec airflow python /opt/producers/transaction_events_producer.py
```

Stop the producers using `CTRL + C` after sufficient data has been generated.

### 3. Run Kafka Batch Consumers

```bash
docker compose exec airflow python /opt/spark-jobs/ingest_kafka_to_landing.py --topic user_events --duration 30
docker compose exec airflow python /opt/spark-jobs/ingest_kafka_to_landing.py --topic transaction_events --duration 30
```

Verify landing files:

```bash
ls data/landing
```

> Do not rerun producers to avoid duplicate data.

### 4. Run Spark ETL Job

```bash
docker compose exec spark-master spark-submit /opt/spark-jobs/etl_job.py \
  --name Group7-Pipeline \
  --master spark://spark-master:7077 \
  --landing /opt/spark-data/landing \
  --gold /opt/spark-data/gold
```

Verify outputs:

```bash
ls data/gold
```
Should see .parquet files in gold
### 5. Airflow DAG Testing

```bash
docker compose exec airflow ls /opt/spark-data
```

Access the Airflow UI at:

http://localhost:8082 
