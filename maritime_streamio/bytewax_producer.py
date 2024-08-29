from confluent_kafka import Producer
import websockets
from uuid import uuid4
import orjson


async def _ws_agen():
    url = "wss://stream.aisstream.io/v0/stream"
    async with websockets.connect(url) as websocket:
        msg = orjson.dumps(
            {
                "APIKey": "23f01fb154ef06c103887b7518f1482bc4b0f25d", 
                "BoundingBoxes": [[[25.835302, -80.207729], [25.602700, -79.879297]], [[33.772292, -118.356139], [33.673490, -118.095731]]]
                # "FiltersShipMMSI": ["368207620", "367719770", "211476060"],
                # "FilterMessageTypes": ["PositionReport"]
            }
        )
        
        await websocket.send(msg)
        
        # The first msg is just a confirmation that we have subscribed.
        await websocket.recv()

        while True:
            msg = await websocket.recv()
            yield (msg)
            
