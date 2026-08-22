import uuid
import hashlib
import json
from typing import Dict, Any
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from redis.asyncio import Redis

from engine.redis_client import get_redis
from engine.queue import publish_task, setup_stream_topology
from engine.state import initialize_state, get_state
from engine.models import TaskMessage

app = FastAPI(title="Task Mesh Gateway")

class JobRequest(BaseModel):
    task_type: str
    payload: Dict[str, Any]

@app.on_event("startup")
async def startup_event():
    """Ensure the Redis streams and consumer groups exist before accepting traffic."""
    redis = await get_redis()
    await setup_stream_topology(redis)

@app.post("/jobs", response_model=dict)
async def submit_job(request: JobRequest, redis: Redis = Depends(get_redis)):
    """Ingests a job and enqueues it for the workers."""
    job_id = f"job_{uuid.uuid4().hex[:8]}"
    task_id = f"task_{uuid.uuid4().hex[:8]}"
    
    # Idempotency key: hash of the payload to prevent duplicate processing
    payload_str = json.dumps(request.payload, sort_keys=True)
    idempotency_key = hashlib.sha256(payload_str.encode()).hexdigest()

    task = TaskMessage(
        job_id=job_id,
        task_id=task_id,
        task_type=request.task_type,
        idempotency_key=idempotency_key,
        payload=request.payload
    )

    # 1. Initialize State in Redis Hash
    await initialize_state(redis, task_id)
    
    # 2. Push to the Stream
    await publish_task(redis, "stream:tasks:pending", task)

    return {"job_id": job_id, "task_id": task_id, "status": "PENDING"}

@app.get("/jobs/{task_id}", response_model=dict)
async def get_job_status(task_id: str, redis: Redis = Depends(get_redis)):
    """Polls the central Redis Hash for the current status."""
    state = await get_state(redis, task_id)
    if not state:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return state.model_dump()
