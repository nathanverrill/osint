import sqlite3
from confluent_kafka import Consumer, TopicPartition, KafkaError
import orjson as json

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'localhost:19092',
    'group.id': 'exporter',
    'auto.offset.reset': 'earliest',
    'fetch.message.max.bytes': 10485760,  # 10MB
    'queued.max.messages.kbytes': 10240,  # 10GB max queue
    'enable.auto.commit': False  # Manually commit offsets
}

# SQLite database connection
conn = sqlite3.connect('streamio.db')
cursor = conn.cursor()

# Set WAL mode for better performance
cursor.execute('PRAGMA journal_mode=WAL;')
# Increase cache size
cursor.execute('PRAGMA cache_size = -20000;')  # Approx 20MB cache

# Create the table
cursor.execute('''
CREATE TABLE IF NOT EXISTS kafka_data (
    uuid TEXT PRIMARY KEY,
    timestamp INTEGER,
    message_type TEXT,
    mmsi TEXT,
    ship_name TEXT,
    latitude REAL,
    longitude REAL,
    report_time_utc TEXT,
    full_message JSON
)
''')

# Specify the topic, partition, and offset
topic = 'raw_ais_reports'  # Replace with your actual Kafka topic
partition = 0  # Replace with your actual partition if needed
start_offset = 811602  # The offset from which to start processing

# Assign the consumer to the specific partition and offset
consumer = Consumer(conf)
consumer.assign([TopicPartition(topic, partition, start_offset)])

def is_json(myjson):
    """Check if the given string is a valid JSON object."""
    try:
        json_object = json.loads(myjson)
    except ValueError as e:
        return False
    return True

# Batch processing
batch_size = 1000
batch_count = 0

# Counter to track the number of messages processed
count = 0
progress_interval = 100000  # Print progress after every 100,000 records

try:
    while True:
        msg_batch = consumer.consume(num_messages=1000, timeout=1.0)
        if msg_batch is None:
            continue
        for msg in msg_batch:
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("End of partition reached")
                    break
                else:
                    print(f"Error: {msg.error()}")
                    continue

            key = msg.key().decode('utf-8')

            if is_json(key):
                # Parse the key and value
                key = json.loads(msg.key())
                value = json.loads(msg.value())

                # Extract fields from key and value
                uuid = key.get("uuid")
                timestamp = key.get("timestamp")
                message_type = value.get("MessageType")
                metadata = value.get("MetaData", {})
                
                mmsi = metadata.get("MMSI")
                ship_name = metadata.get("ShipName")
                latitude = metadata.get("latitude")
                longitude = metadata.get("longitude")
                report_time_utc = metadata.get("time_utc")
                
                full_message = json.dumps(value.get("Message", {}))

                cursor.execute('''
                INSERT OR REPLACE INTO kafka_data (
                    uuid, timestamp, message_type, mmsi, ship_name, latitude, longitude, report_time_utc, full_message
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    uuid, 
                    timestamp, 
                    message_type, 
                    mmsi, 
                    ship_name, 
                    latitude, 
                    longitude, 
                    report_time_utc, 
                    full_message
                ))
                batch_count += 1
                count += 1

                # Batch commit for better performance
                if batch_count % batch_size == 0:
                    conn.commit()
                    batch_count = 0

                # Print progress every 100,000 records
                if count % progress_interval == 0:
                    print(f"Processed {count} records...")

    # Final commit for any remaining records
    conn.commit()

except KeyboardInterrupt:
    print("Processing interrupted by user.")
finally:
    consumer.close()
    cursor.close()
    conn.close()

print(f"Finished processing {count} records.")
