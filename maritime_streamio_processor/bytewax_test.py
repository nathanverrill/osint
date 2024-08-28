from bytewax.connectors.kafka import operators as kop, KafkaSinkMessage
from bytewax import operators as op
from bytewax.dataflow import Dataflow
from confluent_kafka import OFFSET_END

brokers = ["localhost:19092"]
flow = Dataflow("example")
kinp = kop.input("kafka-in", flow, brokers=brokers, topics=["raw_maritime_streamio"],starting_offset=OFFSET_END,batch_size=100)
errs = op.inspect("errors", kinp.errs).then(op.raises, "crash-on-err")
processed = op.map("map", kinp.oks, lambda x: KafkaSinkMessage(x.key, x.value))
kop.output("kafka-out", processed, brokers=brokers, topic="test_ais_streamio_all")