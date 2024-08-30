import asyncio
import orjson
import aiohttp
from confluent_kafka import Producer, KafkaError
import yaml
from uuid import uuid4
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv
import os
import time
import signal

# Load environment variables
load_dotenv()

class AISStreamProcessor:
    def __init__(self, config_path="config.yaml"):
        self.config = self.load_config(config_path)
        
        # Configuration
        self.api_key = os.getenv('AIS_API_KEY')
        if not self.api_key:
            raise ValueError("AIS_API_KEY must be set in the environment")

        self.broker = os.getenv('KAFKA_BROKER', self.config.get('kafka', {}).get('broker', 'redpanda:9092'))
        self.topic = os.getenv('KAFKA_TOPIC', self.config.get('kafka', {}).get('topic', 'raw_maritime_streamio'))
        self.ws_url = os.getenv('AIS_STREAM_URL', self.config.get('ais', {}).get('streamio', {}).get('url', 'wss://stream.aisstream.io/v0/stream'))
        self.buffer_flush_interval = float(os.getenv('BUFFER_FLUSH_INTERVAL', self.config.get('buffer', {}).get('flush_interval', 0.250)))
        self.reconnect_delay = float(os.getenv('RECONNECT_DELAY', self.config.get('websocket', {}).get('reconnect_delay', 5)))
        self.max_reconnect_attempts = int(os.getenv('MAX_RECONNECT_ATTEMPTS', self.config.get('websocket', {}).get('max_reconnect_attempts', 10)))
        self.max_buffer_size = int(os.getenv('MAX_BUFFER_SIZE', self.config.get('buffer', {}).get('max_size', 1000)))
        self.max_message_size = int(os.getenv('MAX_MESSAGE_SIZE', self.config.get('kafka', {}).get('max_message_size', 1000000)))  # 1MB default
        self.metrics_log_interval = int(os.getenv('METRICS_LOG_INTERVAL', self.config.get('metrics', {}).get('log_interval', 60)))
        self.connectivity_check_interval = 5  # Check connectivity every 5 seconds
        self.max_disconnected_time = 15  # Reconnect after 15 seconds of no connectivity

        self.bounding_boxes = self.config.get('ais', {}).get('bounding_boxes', [[[-90, -180], [90, 180]]])

        # Kafka Producer
        self.producer = Producer({
            'bootstrap.servers': self.broker,
            'compression.type': 'lz4',
        })

        # State variables
        self.message_queue = asyncio.Queue(maxsize=self.max_buffer_size)
        self.flush_task = None
        self.metrics_task = None
        self.connectivity_task = None
        self.message_count = 0
        self.start_time = time.time()
        self.reconnect_count = 0
        self.last_flush_time = time.time()
        self.last_flush_count = 0
        self.flush_retry_delay = 1  # Start with 1 second delay
        self.last_connectivity_time = time.time()
        self.running = True
        self.ws = None

    @staticmethod
    def load_config(config_path):
        with open(config_path, "r") as config_file:
            return yaml.safe_load(config_file)

    async def check_internet_connectivity(self):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('http://www.google.com', timeout=5) as response:
                    if response.status == 200:
                        self.last_connectivity_time = time.time()
                        return True
        except aiohttp.ClientError:
            pass
        return False

    async def connectivity_monitor(self):
        while self.running:
            await asyncio.sleep(self.connectivity_check_interval)
            if not await self.check_internet_connectivity():
                if time.time() - self.last_connectivity_time > self.max_disconnected_time:
                    logging.warning("No internet connectivity for 15 seconds. Closing WebSocket connection.")
                    if self.ws:
                        await self.ws.close()
                    break

    async def flush_queue(self):
        if self.message_queue.empty():
            return

        messages = []
        try:
            while not self.message_queue.empty():
                messages.append(self.message_queue.get_nowait())
                self.message_queue.task_done()
        except asyncio.QueueEmpty:
            pass

        if messages:
            try:
                await self.produce_to_kafka(messages)
                self.flush_retry_delay = 1  # Reset delay on successful flush
                logging.info(f"Successfully flushed {len(messages)} messages to Kafka")
            except Exception as e:
                logging.error(f"Failed to flush messages: {e}")
                # Put messages back in the queue
                for message in reversed(messages):
                    await self.message_queue.put(message)
                # Implement exponential backoff
                await asyncio.sleep(self.flush_retry_delay)
                self.flush_retry_delay = min(300, self.flush_retry_delay * 2)  # Cap at 5 minutes

    async def produce_to_kafka(self, messages):
        key = str(uuid4())
        serialized_messages = orjson.dumps(messages)
        
        if len(serialized_messages) > self.max_message_size:
            logging.warning(f"Message size ({len(serialized_messages)} bytes) exceeds max size ({self.max_message_size} bytes). Splitting batch.")
            await self.split_and_send(messages)
        else:
            try:
                self.producer.produce(self.topic, key=key, value=serialized_messages)
                self.producer.flush()  # Synchronous flush
                self.last_flush_time = time.time()
                self.last_flush_count = len(messages)
            except KafkaError as e:
                if e.code() == KafkaError._MSG_SIZE_TOO_LARGE:
                    logging.warning(f"Message size too large. Splitting batch.")
                    await self.split_and_send(messages)
                else:
                    raise

    async def split_and_send(self, messages):
        half = len(messages) // 2
        if half == 0:
            logging.error(f"Cannot split message further. Message size: {len(orjson.dumps(messages))} bytes")
            return

        await self.produce_to_kafka(messages[:half])
        await self.produce_to_kafka(messages[half:])

    async def process_messages(self):
        subscribe_message = {
            "APIKey": self.api_key,
            "BoundingBoxes": self.bounding_boxes
        }
        await self.ws.send_json(subscribe_message)
        logging.info("Sent subscription message to AIS stream")

        while self.running:
            try:
                msg = await self.ws.receive(timeout=30)  # Add a timeout to prevent potential infinite waiting
                if msg.type == aiohttp.WSMsgType.BINARY:
                    message = orjson.loads(msg.data)
                    
                    # Try to put the message in the queue, wait if full
                    try:
                        await self.message_queue.put(message)
                        self.message_count += 1
                    except asyncio.QueueFull:
                        logging.warning("Message queue full. Attempting to flush.")
                        await self.flush_queue()
                        await self.message_queue.put(message)
                        self.message_count += 1

                    if self.message_count % 10000 == 0:
                        elapsed_time = time.time() - self.start_time
                        rate = self.message_count / elapsed_time
                        logging.info(f"Processed {self.message_count} messages. Rate: {rate:.2f} messages/second")

                elif msg.type == aiohttp.WSMsgType.CLOSE:
                    logging.warning(f"WebSocket connection closed: {msg.data}")
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logging.error(f"WebSocket error: {msg.data}")
                    break
            except asyncio.TimeoutError:
                logging.warning("No message received for 30 seconds. Checking connection.")
                if not await self.check_internet_connectivity():
                    logging.error("No internet connectivity. Closing WebSocket connection.")
                    await self.ws.close()
                    break
            except asyncio.CancelledError:
                logging.info("Message processing cancelled.")
                break
            except Exception as e:
                logging.error(f"Error processing message: {e}")

    async def log_kafka_metrics(self):
        while self.running:
            try:
                current_time = time.time()
                elapsed_time = current_time - self.start_time
                total_rate = self.message_count / elapsed_time if elapsed_time > 0 else 0
                
                time_since_last_flush = current_time - self.last_flush_time
                flush_rate = self.last_flush_count / time_since_last_flush if time_since_last_flush > 0 else 0
                
                logging.info(f"Kafka Producer Stats: "
                             f"Total messages: {self.message_count}, "
                             f"Overall rate: {total_rate:.2f} msg/s, "
                             f"Last flush: {self.last_flush_count} messages, "
                             f"Last flush rate: {flush_rate:.2f} msg/s, "
                             f"Queue size: {self.message_queue.qsize()}")
                
                await asyncio.sleep(self.metrics_log_interval)
            except Exception as e:
                logging.error(f"Error logging Kafka metrics: {e}")
                await asyncio.sleep(self.metrics_log_interval)

    async def periodic_flush(self):
        while self.running:
            await asyncio.sleep(self.buffer_flush_interval)
            await self.flush_queue()

    async def run(self):
        self.flush_task = asyncio.create_task(self.periodic_flush())
        self.metrics_task = asyncio.create_task(self.log_kafka_metrics())
        self.connectivity_task = asyncio.create_task(self.connectivity_monitor())

        try:
            async with aiohttp.ClientSession() as session:
                while self.running:
                    try:
                        async with session.ws_connect(self.ws_url) as ws:
                            self.ws = ws
                            logging.info(f"Successfully connected to WebSocket at {self.ws_url}")
                            self.reconnect_count = 0  # Reset reconnect count on successful connection
                            await self.process_messages()
                    except aiohttp.ClientError as e:
                        self.reconnect_count += 1
                        backoff = min(300, self.reconnect_delay * (2 ** (self.reconnect_count - 1)))
                        logging.error(f"WebSocket connection error (attempt {self.reconnect_count}/{self.max_reconnect_attempts}): {e}")
                        if self.reconnect_count >= self.max_reconnect_attempts:
                            logging.critical("Max reconnection attempts reached. Exiting.")
                            self.running = False
                        else:
                            logging.info(f"Attempting to reconnect in {backoff} seconds...")
                            await asyncio.sleep(backoff)
        finally:
            self.running = False
            if self.flush_task:
                self.flush_task.cancel()
            if self.metrics_task:
                self.metrics_task.cancel()
            if self.connectivity_task:
                self.connectivity_task.cancel()
            try:
                await asyncio.gather(self.flush_task, self.metrics_task, self.connectivity_task, return_exceptions=True)
            except asyncio.CancelledError:
                pass

        logging.info("AIS Stream Processor shutting down.")

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

def main():
    setup_logging()
    processor = AISStreamProcessor()
    
    loop = asyncio.get_event_loop()
    
    def signal_handler():
        logging.info("Received stop signal, shutting down...")
        processor.running = False

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        loop.run_until_complete(processor.run())
    finally:
        loop.close()

if __name__ == "__main__":
    main()
