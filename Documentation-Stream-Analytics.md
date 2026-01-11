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
- Used Spark DataFrames for optimized batch analytics and maintainability.
- Isolated Kafka consumers per topic to improve fault tolerance, and enable parallel processing
- Used Parquet because it’s columnar, compressed, and much faster for Spark analytics
- Command-line arguments (argparse) are used to dynamically configure DAG execution parameters (e.g., input paths, topics, durations) without modifying code.

## Setup Instructions

### 1. Start Docker 

```
docker compose up -d --build
```

### 2. Run Kafka Producers

```
docker compose exec airflow python /opt/producers/user_events_producer.py
docker compose exec airflow python /opt/producers/transaction_events_producer.py
```

### 3. Run DAG in Docker

```
docker compose exec airflow airflow dags trigger streamflow_main
```

### 4. Or Run DAG in Airflow Web UI

```
http://localhost:8082/home
```



Verify outputs:

Should see .parquet files in Gold
