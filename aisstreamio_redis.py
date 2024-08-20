import base64
import json

from bytewax.dataflow import Dataflow
from bytewax.inputs import ManualInputConfig, distribute


from websocket import create_connection

## input
ticker_list = ['AMZN', 'MSFT']

def aisstreamio_input(worker_tickers, state):
        ws = create_connection("wss://streamer.finance.yahoo.com/")
        ws_subscription = {
            "APIKey": api_key,
            "BoundingBoxes": bounding_boxes,            
        }
        ws.send(json.dumps({"subscribe": worker_tickers}))
        while True:
            yield state, ws.recv()
            
with websockets.connect("wss://stream.aisstream.io/v0/stream", ping_interval=30, ping_timeout=30) as websocket:
        # Subscribe to the AIS stream
        subscribe_message = {
            "APIKey": api_key,
            "BoundingBoxes": bounding_boxes,
        }
        await websocket.send(orjson.dumps(subscribe_message))            


def input_builder(worker_index, worker_count, resume_state):
    state = resume_state or None
    worker_tickers = list(distribute(ticker_list, worker_index, worker_count))
    print({"subscribing to": worker_tickers})
    return aisstreamio_input(worker_tickers, state)


flow = Dataflow()
flow.input("input", ManualInputConfig(input_builder))