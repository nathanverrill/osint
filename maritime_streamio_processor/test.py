from confluent_kafka import Consumer, KafkaException, KafkaError, Producer
from uuid import uuid4
import orjson as json

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:19092', 
    'group.id': f'my_group_{uuid4()}',  
    'auto.offset.reset': 'latest'          
}

# Initialize the consumer
consumer = Consumer(conf)

producer = Producer({
    'bootstrap.servers': 'localhost:19092', 
    'compression.type': 'lz4'
})

# List of message types from the screenshot
message_types = [
    "BaseStationReport",
    "MultiSlotBinaryMessage",
    "ExtendedClassBPositionReport",
    "SafetyBroadcastMessage",
    "PositionReport",
    "ShipStaticData",
    "StandardClassBPositionReport",
    "StandardSearchAndRescueAircraftReport",
    "StaticDataReport",
    "SingleSlotBinaryMessage",
    "Interrogation",
    "LongRangeAisBroadcastMessage",
    "GnssBroadcastBinaryMessage",
    "DataLinkManagementMessage",
    "AddressedSafetyMessage",
    "AddressedBinaryMessage",
    "CoordinatedUTCInquiry",
    "BinaryAcknowledge",
    "ChannelManagement",
    "AssignedModeCommand",
    "AidsToNavigationReport"
]

def produce_to_kafka(topic, messages):
    for message in messages:
        producer.produce(topic, key=str(message["MetaData"]["MMSI"]), value=json.dumps(message))

def consume_from_kafka(topic_name):
    try:
        consumer.subscribe([topic_name])

        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for a message

            if msg is None:
                continue  # No message available, try again

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    continue
                else:
                    raise KafkaException(msg.error())

            messages = json.loads(msg.value())
            categorized_messages = {message_type: [] for message_type in message_types}

            # each kafka event is a collection of streamio messages
            # can go through and enrich and re-publish
            for m in messages:
                if m["MessageType"] in categorized_messages:
                    categorized_messages[m["MessageType"]].append(m)

            for message_type, msgs in categorized_messages.items():
                if msgs:
                    produce_to_kafka(f'test_maritime_streamio_{message_type.lower()}', msgs)
                    
            producer.flush()

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

# Replace 'raw_maritime_streamio' with the name of your Kafka topic
consume_from_kafka('raw_maritime_streamio')
