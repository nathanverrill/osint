from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from kafka import KafkaConsumer
from collections import deque
from datetime import datetime, timedelta
from typing import Optional, List
import asyncio
from uuid import uuid4
import tenacity

app = FastAPI()

# allow requests from javascript / web browser
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Set up templates directory
templates = Jinja2Templates(directory="templates")

# Buffer to hold messages 
message_buffer = deque()

# Duration for buffering messages
BUFFER_DURATION = timedelta(seconds=5)

# API endpoint to get the most recent 5 minutes of messages
@app.get("/latest-ais-positions")
async def get_latest_ais_positions():
    # Remove expired messages
    cleanup_expired_messages()
    return {"messages": list(message_buffer)}

# Serve the map HTML page
@app.get("/map", response_class=HTMLResponse)
async def serve_map(request: Request):
    return templates.TemplateResponse("map.html", {"request": request})

# SSE endpoint for live data
@app.get("/events")
async def events():
    async def event_stream():
        while True:
            # Check if there are new messages in the buffer
            if message_buffer:
                message = message_buffer[-1]
                yield f"data: {message[1]}\n\n"
                print(f"Sending to map: {message[1]}")
            await asyncio.sleep(0.1)  # Send messages immediately (no delay)
    
    return StreamingResponse(event_stream(), media_type="text/event-stream")

# Kafka consumer task to be run as a background task
@tenacity.retry(wait=tenacity.wait_exponential(min=1, max=10), stop=tenacity.stop_after_attempt(5))
async def kafka_consumer_task():
    consumer = KafkaConsumer(
        'ais_positionreport',
        bootstrap_servers=['redpanda:9092'],
        auto_offset_reset='latest',  # Start with the latest messages
        enable_auto_commit=True,
        group_id=f'fastapi-consumer-group-{uuid4()}',
        value_deserializer=lambda x: x.decode('utf-8')
    )
    while True:
        try:
            msg = consumer.poll(1.0)  # Poll the broker every 1 second
            if msg is None:
                await asyncio.sleep(0.1)  # Yield control to the event loop
                continue
            for tp, messages in msg.items():
                for message in messages:
                    # Add the new message to the buffer and send it immediately
                    message_buffer.append((datetime.utcnow(), message.value))
                    print(f"Received from Kafka: {message.value}")
                    cleanup_expired_messages()
            await asyncio.sleep(0.1)  # Yield control to the event loop
        except Exception as e:
            # Log the error and retry
            print(f"Error consuming from Kafka: {e}")
            raise

# Remove messages older than 5 minutes
def cleanup_expired_messages():
    current_time = datetime.utcnow()
    while message_buffer and (current_time - message_buffer[0][0]) > BUFFER_DURATION:
        message_buffer.popleft()

# Background task to start the Kafka consumer
@app.on_event("startup")
async def start_kafka_consumer():
    loop = asyncio.get_event_loop()
    loop.create_task(kafka_consumer_task())

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
