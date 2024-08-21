"""
AIS Streamio Subscriber

This Python script subscribes to the AIS Streamio WebSocket API, receives AIS reports, and produces them to a Kafka topic.
The script uses the `confluent-kafka` library to produce messages to Kafka and the `websockets` library to connect to the AIS Streamio API.
The script is optimized for high-throughput message production and uses a separate thread to periodically flush messages to Kafka.

Configuration:
The script loads its configuration from a YAML file named `config.yaml`.
The configuration file should contain the following settings:
  - `aisstream.api_key`: the API key for the AIS Streamio API
  - `area_of_interest.bounding_boxes`: a list of bounding boxes defining the area of interest for AIS reports

Dependencies:
  - `confluent-kafka`: a Python library for producing messages to Kafka
  - `websockets`: a Python library for connecting to WebSocket APIs
  - `orjson`: a fast JSON parsing library
  - `yaml`: a human-readable serialization format
"""
import asyncio
import websockets
import orjson  # Fast JSON parsing library
import time
import yaml
import re
import os
import threading
from confluent_kafka import Producer, KafkaError

# Load API key and area of interest bounding boxes from config.yaml
def load_config():
    regex_pattern = r".*?(\$\{(\w+)\}).*?"
    with open("config.yaml", "r") as config_file:
        config_string = config_file.read()
        matches = re.finditer(regex_pattern, config_string, re.MULTILINE)
        for env in matches:
            variable_value = os.environ.get(env[2])
            if variable_value:
                config_string = config_string.replace(env[1], variable_value)
            else:
                print("WARNING:  You are missing the following environmental variable on your system:", env[2])
                print(f"          Consider adding it from the command line like so: export {env[2]}=your_secret_value")
        return yaml.safe_load(config_string)

config = load_config()
api_key = config['aisstream']['api_key']
bounding_boxes = config['area_of_interest']['bounding_boxes']

# Kafka producer configuration optimized for high throughput
producer_config = {
    'bootstrap.servers': '127.00.1:19092',
    'queue.buffering.max.messages': 50000,
    'queue.buffering.max.kbytes': 524288,
    'linger.ms': 50,
    'batch.num.messages': 1000,
    'compression.type': 'snappy', #requires some downstream config so opting not to use it; leaving here for future ref on setting it or if there's a need
    'acks': 'all'
}

producer = Producer(producer_config)

# Parameters for managing flush and performance
flush_threshold = 1000  # Flush after every 10,000 messages
flush_interval = 5  # Flush every 5 seconds
last_flush_time = time.time()
message_count = 0

# Function to handle periodic flushing
def producer_flusher(producer, flush_interval):
    while True:
        time.sleep(flush_interval)
        producer.flush()

# Start the flusher thread
flusher_thread = threading.Thread(target=producer_flusher, args=(producer, flush_interval))
flusher_thread.daemon = True
flusher_thread.start()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")

async def connect_ais_stream(api_key):
    global message_count
    async with websockets.connect("wss://stream.aisstream.io/v0/stream", ping_interval=30, ping_timeout=30) as websocket:
        # Subscribe to the AIS stream
        subscribe_message = {
            "APIKey": api_key,
            "BoundingBoxes": bounding_boxes,
        }
        await websocket.send(orjson.dumps(subscribe_message))

        while True:
            try:

                message = await websocket.recv()
 
                # keyed by message type
                msg_json = orjson.loads(message)
                msg_key = msg_json['MessageType']
                
                producer.produce(
                    topic='ais_streamio_reports_raw',
                    key=msg_key,
                    value=message
                )                                  
                
                message_count += 1

                # Flush based on message count (now handled by flusher thread)
                if message_count >= flush_threshold:
                    producer.flush()
                    message_count = 0

            except websockets.ConnectionClosed:
                print("WebSocket connection closed")
                break
            except KafkaError as e:
                print(f"Kafka error: {e}")
            except Exception as e:
                print(f"Unexpected error: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(connect_ais_stream(api_key))
    except KeyboardInterrupt:
        print("Application interrupted by user.")
    finally:
        producer.flush()  # Ensure all messages are sent before exiting
        print("Application exiting gracefully.")
