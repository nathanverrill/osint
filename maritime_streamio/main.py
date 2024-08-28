import asyncio
import re
import orjson
import os
import aiohttp
from confluent_kafka import Producer
import yaml
from uuid import uuid4

def load_config():
    regex_pattern = r".*?(\$\{(\w+)\}).*?"
    with open("config.yaml", "r") as config_file:
        config_string = config_file.read()
        print(config_string)
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

# streamio parameters
API_KEY = config['ais']['streamio']['api_key']
BOUNDING_BOXES = [[[-90, -180], [90, 180]]] # entire earth

# kafka config
BROKER = config['kafka']['broker']
TOPIC = config['kafka']['producer']['topic']

print(BROKER)
# Kafka configuration
producer = Producer({
    'bootstrap.servers': BROKER,  # Updated to use your Kafka server
    'compression.type': 'lz4'
})
topic = TOPIC

# Buffer to store messages
message_buffer = []

async def buffer_and_send():
    while True:
        await asyncio.sleep(0.250)
        if message_buffer:
            # Send the entire buffer as a single JSON array
            await produce_to_kafka(message_buffer.copy())
            message_buffer.clear()

async def produce_to_kafka(messages):
    # Convert the list of messages to a JSON array
    key = str(uuid4())
    producer.produce(topic, key=key, value=orjson.dumps(messages))
    producer.flush()

async def websocket_handler(url):
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    # Define your subscription message
                    subscribe_message = {
                        "APIKey": API_KEY,  # Required
                        "BoundingBoxes": BOUNDING_BOXES  # Required
                    }
                    print(subscribe_message)

                    # Send the subscription message
                    await ws.send_json(subscribe_message)

                    # Create the buffer_and_send task
                    buffer_task = asyncio.create_task(buffer_and_send())

                    while True:
                        msg = await ws.receive()
                        if msg.type == aiohttp.WSMsgType.BINARY:
                            message = orjson.loads(msg.data)
                            message_buffer.append(message)
                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            print("WebSocket connection closed. Reconnecting...")
                            break
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            print("WebSocket error:", msg.data)
                            break
        except aiohttp.ClientError as e:
            print("Error connecting to WebSocket:", e)
        except asyncio.CancelledError:
            print("WebSocket connection cancelled.")
        await asyncio.sleep(5)  # wait 5 seconds before reconnecting
        
        

url = 'wss://stream.aisstream.io/v0/stream'
asyncio.run(websocket_handler(url))
