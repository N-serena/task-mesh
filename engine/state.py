import time
from typing import Optional
from redis.asyncio import Redis
from engine.models import TaskStatus, TaskStateRecord

def _get_key(task_id: str) -> str:
    return f"task:state:{task_id}"

async def initialize_state(redis: Redis, task_id: str) -> TaskStateRecord:
    """Creates the initial state tracking record when a task is ingested."""
    state = TaskStateRecord()
    # Redis cannot encode None, so unset fields are omitted and fall back to
    # model defaults when the record is read back.
    mapping = state.model_dump(mode="json", exclude_none=True)
    await redis.hset(_get_key(task_id), mapping=mapping)
    return state

async def update_state(
    redis: Redis, 
    task_id: str, 
    status: TaskStatus, 
    worker_id: Optional[str] = None,
    lock_expires_at: Optional[float] = None
) -> None:
    """Updates specific fields of the task's state."""
    mapping = {
        "status": status.value,
        "updated_at": time.time()
    }
    
    if worker_id is not None:
        mapping["worker_id"] = worker_id
    if lock_expires_at is not None:
        mapping["lock_expires_at"] = lock_expires_at
        
    await redis.hset(_get_key(task_id), mapping=mapping)

async def get_state(redis: Redis, task_id: str) -> Optional[TaskStateRecord]:
    """Fetches the current state of a task, or returns None if it doesn't exist."""
    data = await redis.hgetall(_get_key(task_id))
    if not data:
        return None
    return TaskStateRecord(**data)
