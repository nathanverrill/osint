import asyncio
import websockets
import orjson  # Fast JSON parsing library
import uuid
import zlib
import time
import sys
import threading
from confluent_kafka import Producer, KafkaError
from schemas.ais_streamio_pb2 import AISStreamIOMessage  # Adjust the import path as needed

# Load API key and area of interest bounding boxes from config.yaml
def load_config():
    import yaml
    with open('config.yaml', 'r') as file:
        return yaml.safe_load(file)

config = load_config()
api_key = config['aisstream']['api_key']
bounding_boxes = config['area_of_interest']['bounding_boxes']

# Kafka producer configuration optimized for high throughput
producer_config = {
    'bootstrap.servers': 'localhost:19092',
    'queue.buffering.max.messages': 500000,
    'queue.buffering.max.kbytes': 2097152,
    'linger.ms': 50,
    'batch.num.messages': 10000,
    'compression.type': 'snappy',
    'acks': 'all'
}

producer = Producer(producer_config)

# Parameters for managing flush and performance
flush_threshold = 10000  # Flush after every 10,000 messages
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
                # Receive and compress message
                message_json = await websocket.recv()
                compressed_message_json = zlib.compress(message_json, level=1)  # Adjusted compression level
                # print(message_json)
                # Generate a UUID and parse the message
                message_uuid = str(uuid.uuid4())
                message = orjson.loads(message_json)

                # Create the Protobuf message
                ais_streamio_message = AISStreamIOMessage(
                    uuid=message_uuid,
                    mmsi=str(message['MetaData'].get("MMSI")),
                    ship_name=message['MetaData'].get("ShipName", "").strip(),
                    message_type=message.get("MessageType", ""),
                    time_utc=message['MetaData'].get("time_utc"),
                    latitude=message['MetaData'].get("latitude"),
                    longitude=message['MetaData'].get("longitude"),
                    full_message_zlib_compressed=compressed_message_json
                )

                # Serialize the Protobuf message to bytes
                serialized_message = ais_streamio_message.SerializeToString()

                # Produce the serialized message to the Kafka topic
                producer.produce(
                    topic='ais_streamio_raw',
                    key=str(message['MetaData'].get("MMSI")),
                    value=serialized_message,
                    callback=delivery_report
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
