import json
from redis.asyncio import Redis
from engine.models import TaskMessage

# Define our topology
STREAMS = {
    "stream:tasks:pending": "cg_main_workers",
    "stream:tasks:retry": "cg_main_workers",
    "stream:tasks:dlq": "cg_recovery_agents",
    "stream:tasks:completed": None, # Append-only, no consumer group
    "stream:tasks:dead": None       # Append-only, no consumer group
}

async def setup_stream_topology(redis: Redis):
    """
    Idempotent function to ensure streams and consumer groups exist.
    Run this once when the application starts.
    """
    for stream_name, group_name in STREAMS.items():
        if group_name:
            try:
                # id="0" tells the group to read from the beginning of the stream.
                # mkstream=True creates the stream if it doesn't exist yet.
                await redis.xgroup_create(stream_name, group_name, id="0", mkstream=True)
                print(f"Created group '{group_name}' for stream '{stream_name}'")
            except Exception as e:
                if "BUSYGROUP" in str(e):
                    # The group already exists, which is expected on subsequent startups
                    pass
                else:
                    raise e

async def publish_task(redis: Redis, stream_name: str, task: TaskMessage) -> str:
    """
    Serializes a TaskMessage and pushes it to a Redis Stream.
    Returns the unique Redis-generated message ID.
    """
    # Redis Streams require flat string key-values.
    # We must serialize nested dicts (like 'payload') to JSON strings.
    flat_record = {
        "job_id": task.job_id,
        "task_id": task.task_id,
        "task_type": task.task_type,
        "idempotency_key": task.idempotency_key,
        "payload": json.dumps(task.payload),
        "retry_count": str(task.retry_count),
        "timestamp": str(task.timestamp)
    }
    
    # xadd appends the record to the stream
    message_id = await redis.xadd(stream_name, flat_record)
    return message_id