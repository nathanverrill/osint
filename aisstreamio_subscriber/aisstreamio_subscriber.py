import asyncio
import websockets
import orjson  # Fast JSON parsing library
import time
import yaml
import re
import os
import threading
from confluent_kafka import Producer, KafkaError
import tenacity
import os
import re
import yaml

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
    'bootstrap.servers': 'localhost:19092',
    'queue.buffering.max.messages': 50000,
    'queue.buffering.max.kbytes': 524288,
    'linger.ms': 50,
    'batch.num.messages': 1000,
    'compression.type': 'snappy', 
    'acks': 'all'
}

@tenacity.retry(wait=tenacity.wait_exponential(multiplier=1, min=4, max=10),
                stop=tenacity.stop_after_attempt(5),
                retry=tenacity.retry_if_exception_type(KafkaError))
def create_producer(producer_config):
    return Producer(producer_config)

producer = create_producer(producer_config)

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
