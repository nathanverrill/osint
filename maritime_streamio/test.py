import asyncio
import websockets
import json
import orjson
import time
from datetime import datetime, timezone
from uuid import uuid4
from confluent_kafka import Producer, KafkaError
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.json_schema import JSONSerializer

# Kafka configuration
kafka_config = {
  "bootstrap.servers": "redpanda:9092",
  "compression.type": "snappy",
  "batch.size": 1048576,
  "linger.ms": 200,
  "acks": "all",
  "retries": 5,
  "retry.backoff.ms": 100,
  "client.id": "streamio-producer",
  "max.in.flight.requests.per.connection": 5,
#   "buffer.memory": 33554432,
  "request.timeout.ms": 30000
}
producer = Producer(kafka_config)
kafka_topic = 'raw_ais_reports'

# Function to send batched messages to Kafka
async def send_to_kafka(message):

    try:
        producer.produce(kafka_topic, key=str(uuid4()),value=message)
        producer.flush()
    except Exception as e:
        print(f"Failed to send data to Kafka: {e}")


async def monitor_websocket():
    while True:  # Keep trying to connect
        try:
            async with websockets.connect("wss://stream.aisstream.io/v0/stream") as websocket:
                message_count = 0
                total_bytes = 0
                start_time = time.time()

                subscribe_message = {
                    "APIKey": "23f01fb154ef06c103887b7518f1482bc4b0f25d", 
                    "BoundingBoxes": [[[-90, -180], [90, 180]]]
                }

                subscribe_message_json = json.dumps(subscribe_message)
                await websocket.send(subscribe_message_json)

                while True:
                    try:
                        message = await websocket.recv()
                        await send_to_kafka(message)
                        
                        message_count += 1
                        total_bytes += len(message)  # Calculate message size in bytes

                        current_time = time.time()
                        elapsed_time = current_time - start_time

                        if elapsed_time >= 1.0:
                            kilobytes_received = total_bytes / 1024
                            print(f"Messages received: {message_count}, Kilobytes received: {kilobytes_received:.2f} KB")

                            # Reset counters for the next second
                            message_count = 0
                            total_bytes = 0
                            start_time = current_time
                    except websockets.ConnectionClosed:
                        print("WebSocket connection closed unexpectedly. Reconnecting...")
                        break  # Exit the inner loop to attempt reconnection
        except (websockets.ConnectionClosed, OSError) as e:
            print(f"Failed to connect or connection lost: {e}. Will try again in 10 seconds...")
            await asyncio.sleep(10)  # Wait  seconds before retrying


async def main():
    await monitor_websocket()

if __name__ == "__main__":
    asyncio.run(main())