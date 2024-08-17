import orjson
from typing import Optional
from confluent_kafka.serialization import (
    Serializer,
    Deserializer,
    StringDeserializer,
    SerializationContext,
)
from bytewax import operators as op
from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage, KafkaSink
from bytewax.dataflow import Dataflow
from schemas.ais_streamio_pb2 import AISStreamIOMessage  # Import your Protobuf message

BROKERS = ["localhost:19092"]
IN_TOPICS = ["in_topic"]

class OrJSONSerializer(Serializer):
    def __call__(
        self, obj: Optional[object], ctx: Optional[SerializationContext] = None
    ) -> Optional[bytes]:
        if obj is None:
            return None
        return orjson.dumps(obj)

class ProtobufDeserializer(Deserializer):
    def __call__(
        self, value: Optional[bytes], ctx: Optional[SerializationContext] = None
    ) -> Optional[object]:
        if value is None:
            return None
        # Deserialize the Protobuf message
        message = AISStreamIOMessage()
        message.ParseFromString(value)
        # Convert Protobuf message to a dictionary
        return message

brokers = ["localhost:19092"]
val_de = ProtobufDeserializer()
key_de = StringDeserializer()
json_serializer = OrJSONSerializer()

flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["in-topic"])
protobuf_stream = kop.deserialize(
    "load_protobuf",
    kinp.oks,
    key_deserializer=key_de,
    val_deserializer=val_de,
)

json_stream = protobuf_stream.map(lambda msg: orjson.dumps(msg).decode('utf-8'))

op.inspect("inspect", json_stream.oks)
