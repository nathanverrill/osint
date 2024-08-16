#!/usr/bin/env python3
import sys
import time
from uuid import uuid4
from confluent_kafka import Consumer, KafkaError
from schemas.ais_streamio_pb2 import AISStreamIOMessage  # Adjust the import as needed
import zlib

# Kafka consumer configuration
consumer_config = {
    "bootstrap.servers": "localhost:19092",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "group.id": f"ais-streamio-group={uuid4()}",
    "group.instance.id": f"ais-streamio-consumer-{uuid4()}",
}

consumer = Consumer(consumer_config)

# Subscribe to the topic
consumer.subscribe(['ais_streamio'])

# Polling and message processing
poll_interval = 0.1  # Reduce polling interval for faster response
waiting_message_interval = 5  # Seconds to wait before printing "waiting for data"
last_waiting_message_time = time.time()

try:
    while True:
        msg = consumer.poll(poll_interval)

        if msg is None:
            if time.time() - last_waiting_message_time > waiting_message_interval:
                print("...waiting for data", file=sys.stderr)
                last_waiting_message_time = time.time()
            continue

        if msg.error():
            print(f"Consumer error: {msg.error()}", file=sys.stderr)
            continue

        key = msg.key()
        # Deserialize the Protobuf message directly
        ais_streamio_message = AISStreamIOMessage()
        ais_streamio_message.ParseFromString(msg.value())

        if ais_streamio_message:
            print(ais_streamio_message)
            
        print("Additional message data decompressed from full_message_zlib_compressed")
        decompressed_full_message = ais_streamio_message.full_message_zlib_compressed = zlib.decompress(ais_streamio_message.full_message_zlib_compressed)
        print(decompressed_full_message)
        print("-------------------")
        
except KeyboardInterrupt:
    print("Consumer interrupted by user.", file=sys.stderr)
except Exception as e:
    print(f"Unexpected error: {e}", file=sys.stderr)
finally:
    # Clean up and close the consumer gracefully
    consumer.close()
    print("Consumer closed gracefully.", file=sys.stderr)
