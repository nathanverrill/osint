import asyncio
import websockets
import orjson
import time
from dotenv import load_dotenv
import os
import logging
from uuid import uuid4
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient

BROKER = "redpanda-0:9092"
KAFKA_RETRY_INTERVAL = 60  # seconds to wait before retrying Kafka connection
WEBSOCKET_URL = "wss://stream.aisstream.io/v0/stream"

load_dotenv()

api_key = os.getenv('AIS_API_KEY')
if not api_key:
    raise ValueError("AIS_API_KEY must be set in .env file for this environment")

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def kafka_health_check(broker, timeout=30):
    """
    Check if Kafka is ready by attempting to list topics.
    :param broker: Kafka broker address
    :param timeout: Timeout for the health check in seconds
    :return: True if Kafka is ready, False otherwise
    """
    admin_client = AdminClient({'bootstrap.servers': broker})
    end_time = time.time() + timeout

    while time.time() < end_time:
        try:
            topics = admin_client.list_topics(timeout=5)
            if topics.topics:
                logging.info("Kafka is ready!")
                return True
        except Exception as e:
            logging.warning(f"Kafka not ready: {e}")
        time.sleep(5)  # Wait before retrying

    logging.error("Kafka did not become ready in time.")
    return False

# Kafka configuration
kafka_config = {
    "bootstrap.servers": BROKER,
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

kafka_topic = 'raw_ais_reports'

async def send_to_kafka(producer, message):
    try:
        producer.produce(kafka_topic, key=str(uuid4()), value=message)
        producer.poll(0)  # Trigger delivery reports
    except Exception as e:
        logging.error(f"Failed to send data to Kafka: {e}")

async def monitor_websocket(producer):
    websocket = None
    try:
        websocket = await websockets.connect(WEBSOCKET_URL)
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
                await send_to_kafka(producer, message)
                
                if first_message:
                    logging.info(f"Subscription established for: {subscribe_message_json}")
                    logging.info(f"First message received: {message}")
                    logging.info(f"Application will log number of messages and kilobytes received every 60 seconds.")
                    logging.info(f"Application is publishing messages to kafka with the following configuration: {kafka_config}")
                    first_message = False

                message_count += 1
                total_bytes += len(message)

                current_time = time.time()
                elapsed_time = current_time - start_time

                if elapsed_time >= 60.0:
                    kilobytes_received = total_bytes / 1024
                    logging.info(f"Messages received last 60 seconds: {message_count}, Kilobytes received: {kilobytes_received:.2f} KB")

                    message_count = 0
                    total_bytes = 0
                    start_time = current_time

            except websockets.ConnectionClosed:
                logging.warning("WebSocket connection closed unexpectedly. Reconnecting...")
                break

    except Exception as e:
        logging.error(f"Error in monitor_websocket: {e}")
    finally:
        if websocket:
            await websocket.close()
        producer.flush()  # Ensure any remaining messages are sent

async def main():
    while True:
        if kafka_health_check(BROKER):
            producer = Producer(kafka_config)
            try:
                await monitor_websocket(producer)
            except Exception as e:
                logging.error(f"Unexpected error in main loop: {e}")
            finally:
                producer.flush()
                logging.info("Flushed producer, waiting before retry...")
        else:
            logging.warning(f"Kafka is not ready. Retrying in {KAFKA_RETRY_INTERVAL} seconds...")
        
        await asyncio.sleep(KAFKA_RETRY_INTERVAL)

if __name__ == "__main__":
    time.sleep(30)  # let other services warm up
    asyncio.run(main())
