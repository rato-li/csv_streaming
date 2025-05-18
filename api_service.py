from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis
import json
from typing import List, Dict

app = FastAPI()
r = redis.Redis(host='localhost', port=6379, db=0)

class ChunkRequest(BaseModel):
    records: List[Dict]

@app.post("/enqueue")
async def enqueue_chunk(request: ChunkRequest):
    try:
        # Update records to 'sent' state and add to queue
        updated_records = []
        for record in request.records:
            record["status"] = "sent"
            record["sent_at"] = datetime.utcnow().isoformat()
            updated_records.append(record)
        
        # Push to Redis stream
        r.lpush("processing_queue", json.dumps(updated_records))
        return {"message": f"Enqueued {len(updated_records)} records"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))