"""
AIS Streamio Pipeline

This Python script defines a Bytewax dataflow pipeline that processes raw AIS (Automatic Identification System) reports from a Kafka topic.
The pipeline enriches the metadata, bins the location, extracts features for analytics, and flattens the JSON data.
It then outputs the processed data to multiple Kafka topics, one for each AIS message type.

Configuration:
The pipeline configuration is loaded from a YAML file named `config.yaml`.
The configuration file should contain the following settings:
  - `kafka.brokers`: a list of Kafka broker URLs
  - `ais.message_types`: a list of AIS message types to process
  - `ais.topics.raw`: the name of the Kafka topic containing raw AIS reports

Dependencies:
  - Bytewax: a Python library for building data pipelines
  - Kafka: a distributed streaming platform
  - YAML: a human-readable serialization format
"""
from helpers.ais_bytewax import AISBytewaxOperations
from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from confluent_kafka import OFFSET_END
from uuid import uuid4

import os
import re
import yaml
import re
import os


# Load configuration from config.yaml
# Replaces references like ${MY_API_KEY} with the environmental variable of the same name
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
    config = yaml.safe_load(config_string)

BROKER = config['kafka']['broker']
AIS_MESSAGE_TYPES = config['ais']['message_types']
INPUT_TOPIC = config['ais']['topics']['raw']
OUTPUT_TOPIC = config['ais']['topics']['output']

# configuration needed to limit 
PRODUCER_CONFIG = {
    'queue.buffering.max.messages': 5000,
    'queue.buffering.max.kbytes': 212144,
    'linger.ms': 50,
    'batch.num.messages': 1000,
    'acks': 'all',
    'compression.type': 'lz4'   
}

# Stream processing for each Kafka message
flow = Dataflow("Process Raw AIS Streamio Reports")
stream = kop.input("kafka-in", flow, brokers=BROKER, topics=INPUT_TOPIC,starting_offset=OFFSET_END,batch_size=100)
errs = op.inspect("errors", stream.errs).then(op.raises, "crash-on-err")

# enriched = op.map("Enrich Metadata", stream, AISBytewaxOperations.enrich_metadata)
# binned = op.map("Bin Location", enriched, AISBytewaxOperations.bin_location)
# features = op.map("Calculate Features for Analytics", binned, AISBytewaxOperations.calculate_features)
# flattened = op.map("Flatten JSON", features, AISBytewaxOperations.flatten_json)
# op.output("Kafka Out - All Reports", flattened, KafkaSink(BROKER, OUTPUT_TOPIC))

# # Stream processing specific to message type
# keyed_on_mmsi = op.map("Assign MMSI Kafka Key", stream, AISBytewaxOperations.set_mmsi_key)

enriched = op.map("Enrich Metadata", stream.oks, AISBytewaxOperations.enrich_metadata)
kop.output("kafka-out", enriched, brokers=BROKER, topic="a_test_ais_streamio_all")

# op.output(f"Kafka Out - All", stream, KafkaSink(BROKER, "test_ais_streamio_all"))

# # # filter and route to topics for each message type
# for message_type in AIS_MESSAGE_TYPES:
#     filter_fn = lambda msg, mt=message_type: AISBytewaxOperations.filter_message_type(msg, mt)
#     filtered_messages = op.filter(f"Filter {message_type}", stream, filter_fn)
#     op.output(f"Kafka Out - {message_type}", filtered_messages, KafkaSink(BROKER, f"ais_{message_type.lower()}"))