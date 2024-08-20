# see https://docs.bytewax.io/stable/tutorials/orderbook-guide/index.html
import orjson as json
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Dict, List, Optional

import websockets
from bytewax import operators as op
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async


url = "wss://stream.aisstream.io/v0/stream"
key = "23f01fb154ef06c103887b7518f1482bc4b0f25d"
box = ["[[-90, -180], [90, 180]]"]

async def _ws_agen():
    """Connect to websocket and yield messages as they arrive.

    This function is a generator that connects to the  websocket and
    yields messages as they arrive. 

    Yields:
        _type_: A tuple of the product_id and the message as a dictionary.
    """

    async with websockets.connect(url) as websocket:
        msg = json.dumps(
            {
            "APIKey": key,
            "BoundingBoxes": box,
            }
        )
        await websocket.send(msg)
        # The first msg is just a confirmation that we have subscribed.
        await websocket.recv()

        while True:
            msg = await websocket.recv()
            yield (json.loads(msg))
            
    