import asyncio
import websockets
import orjson
import time
from dotenv import load_dotenv
import os
import logging
from uuid import uuid4
from confluent_kafka import Producer, KafkaError

load_dotenv()

api_key = os.getenv('AIS_API_KEY')
if not api_key:
    raise ValueError("AIS_API_KEY must be set in .env file for this environment")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Kafka configuration
kafka_config = {
  "bootstrap.servers": "redpanda-0:9092",
  "compression.type": "snappy",
  "batch.size": 1048576,
  "linger.ms": 200,
  "acks": "all",
  "retries": 5,
  "retry.backoff.ms": 100,
  "client.id": "streamio-producer",
  "max.in.flight.requests.per.connection": 5,
  "request.timeout.ms": 30000
}
producer = Producer(kafka_config)
kafka_topic = 'raw_ais_reports'

# Function to send batched messages to Kafka
async def send_to_kafka(message):
    try:
        producer.produce(kafka_topic, key=str(uuid4()), value=message)
        producer.flush()
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")

async def monitor_websocket():
    while True:  # Keep trying to connect
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                message_count = 0
                total_bytes = 0
                start_time = time.time()
                first_message = True

                subscribe_message = {
                    "APIKey": api_key, 
                    "BoundingBoxes": [[[-90, -180], [90, 180]]]
                }

                subscribe_message_json = orjson.dumps(subscribe_message)
                await websocket.send(subscribe_message_json)

                while True:
                    try:
                        message = await websocket.recv()
                        await send_to_kafka(message)
                        
                        if first_message:
                            logging.info(f"Subscription established for: {subscribe_message_json}")
                            logging.info(f"First message received: {message}")
                            logging.info(f"Application will log number of messages and kilobytes received every 60 seconds.")
                            logging.info(f"Application is publishing messages to kafka with the following configuration: {kafka_config}")
                            first_message = False
    
                        
                        message_count += 1
                        total_bytes += len(message)  # Calculate message size in bytes

                        current_time = time.time()
                        elapsed_time = current_time - start_time

                        if elapsed_time >= 60.0:
                            kilobytes_received = total_bytes / 1024
                            logging.info(f"Messages received last 60 seconds: {message_count}, Kilobytes received: {kilobytes_received:.2f} KB")

                            # Reset counters for the next second
                            message_count = 0
                            total_bytes = 0
                            start_time = current_time
                    except websockets.ConnectionClosed:
                        logging.warning("WebSocket connection closed unexpectedly. Reconnecting...")
                        break  # Exit the inner loop to attempt reconnection
        except (websockets.ConnectionClosed, OSError) as e:
            logging.error(f"Failed to connect or connection lost: {e}. Will try again in 10 seconds...")
            await asyncio.sleep(10)  # Wait 10 seconds before retrying

async def main():
    await monitor_websocket()

if __name__ == "__main__":
    asyncio.run(main())
