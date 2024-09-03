from confluent_kafka import Consumer, KafkaError
import json

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'keycheck',
    'auto.offset.reset': 'earliest'  # Start from the earliest message
}

# Kafka consumer setup
consumer = Consumer(conf)
consumer.subscribe(['raw_ais_reports'])  # Replace 'your_topic' with your actual Kafka topic

def is_json(myjson):
    """Check if the given string is a valid JSON object."""
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                print("End of partition reached")
                break
            else:
                print(f"Error: {msg.error()}")
                continue
        
        key = msg.key().decode('utf-8')
        
        if is_json(key):
            print(f"First JSON key found: {key}")
            print(f"At offset: {msg.offset()}")
            break

except KeyboardInterrupt:
    pass
finally:
    consumer.close()

print("Finished searching for JSON key.")
