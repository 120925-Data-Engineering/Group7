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