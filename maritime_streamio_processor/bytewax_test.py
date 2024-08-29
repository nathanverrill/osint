from datetime import timedelta

from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from confluent_kafka import OFFSET_END
import orjson

def extract(kafka_msg):
    return orjson.loads(kafka_msg.value)

def flatten(json_list):
    
    for item in json_list:
        item.update(item.pop("MetaData"))
        
    return json_list

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["raw_maritime_streamio"],starting_offset=OFFSET_END,batch_size=4)
errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")

json_stream = op.map("extract", kinp.oks, extract)

flattened_stream = op.map("flatten", json_stream, flatten)

keyed_stream = op.key_on("key", flattened_stream, lambda _x: "ALL")

collected_stream = op.collect(
    "collect", keyed_stream, timeout=timedelta(seconds=5), max_size=12
)
op.output("out", collected_stream, StdOutSink())

# kop.output("kafka-out", enriched, brokers=brokers, topic="test_ais_streamio_all")