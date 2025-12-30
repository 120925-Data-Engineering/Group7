"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""
from kafka import KafkaConsumer
import json
import time
import os
import argparse


def consume_batch(topic: str, batch_duration_sec: int, output_path: str) -> int:
    """
    Consume from Kafka for specified duration and write to landing zone.
    
    Args:
        topic: Kafka topic to consume from
        batch_duration_sec: How long to consume before writing
        output_path: Directory to write output JSON files
        
    Returns:
        Number of messages consumed
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers = ['localhost:9092'],
        auto_offset_reset = 'earliest',
        enable_auto_commit = False,
        value_deserializer = lambda v: json.loads(v.decode('utf-8'))
    )

    messages = []
    start = time.time()
    #consume messages in a timed window
    while time.time() - start < batch_duration_sec:
        records = consumer.poll(timeout_ms=500)
        for _, batch in records.items():
            for record in batch:
                messages.append(record.value)

    if not messages:
        return 0
    
    os.makedirs(output_path, exist_ok=True)

    timestamp = int(time.time())
    filename = f"{topic}_{timestamp}.json"
    filepath = os.path.join(output_path, filename)

    with open(filepath, 'w') as json_file:
        json.dump(messages, json_file, indent=4)

    consumer.commit()
    consumer.close()
    return len(messages)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Running Kafka Consumer")
    parser.add_argument("--topic", required=True, help="Topic Name")
    parser.add_argument("--duration", type=int, default=30, help="Duration")
    parser.add_argument("--output", default="./data/landing", help="Output Path")

    args = parser.parse_args()

    consume_batch(
        topic=args.topic,
        batch_duration_sec=args.duration,
        output_path=args.output
    )