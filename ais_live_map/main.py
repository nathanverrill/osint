from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
import redis
import asyncio
import json
from uuid import uuid4

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

# Serve the map HTML page
@app.get("/map", response_class=HTMLResponse)
async def serve_map(request: Request):
    return templates.TemplateResponse("map.html", {"request": request})

# Initialize Redis client
redis_client = redis.Redis(host='redis', port=6379, db=0)

# SSE endpoint to stream current MMSIs
@app.get("/events")
async def events():
    async def event_stream():
        last_seen_set = set()
        
        while True:
            current_mmsis = set(redis_client.keys("*"))  # Get all current MMSI keys from Redis
            new_mmsis = current_mmsis - last_seen_set   # Find new MMSIs
            if new_mmsis:
                last_seen_set = current_mmsis  # Update the last seen set
                mmsis_list = [mmsi.decode('utf-8') for mmsi in new_mmsis]
                yield f"data: {json.dumps(mmsis_list)}\n\n"
            await asyncio.sleep(1)  # Stream updates every second

    return StreamingResponse(event_stream(), media_type="text/event-stream")

# API endpoint to get the list of new or updated MMSIs
@app.get("/api/latest-mmsis", response_class=JSONResponse)
async def get_latest_mmsis():
    last_seen_set = set(redis_client.smembers('last_seen_mmsis'))  # Retrieve last seen MMSIs from Redis
    current_mmsis = set(redis_client.keys("*"))  # Get all current MMSI keys from Redis
    
    new_mmsis = current_mmsis - last_seen_set  # Find new MMSIs
    mmsis_list = [mmsi.decode('utf-8') for mmsi in new_mmsis]
    
    if new_mmsis:
        redis_client.delete('last_seen_mmsis')  # Clear the previous set
        for mmsi in current_mmsis:
            redis_client.sadd('last_seen_mmsis', mmsi)  # Store the current MMSIs in Redis for the next comparison
    
    return {"new_mmsis": mmsis_list}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
