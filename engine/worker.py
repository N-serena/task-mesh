import json
import asyncio
import traceback
from redis.asyncio import Redis

from engine.models import TaskMessage, TaskStatus, DLQMessage
from engine.state import update_state
from engine.locks import acquire_lock, release_lock
from engine.classifier import classify_error, ErrorAction
from engine.queue import publish_dlq_message
from plugins.registry import tool_registry

MAX_RETRIES = 3

async def process_task(redis: Redis, worker_id: str, message_id: str, raw_data: dict):
    """Processes a single task safely."""
    # 1. Deserialize the flat Redis dictionary back into our TaskMessage
    task = TaskMessage(
        job_id=raw_data["job_id"],
        task_id=raw_data["task_id"],
        task_type=raw_data["task_type"],
        idempotency_key=raw_data["idempotency_key"],
        payload=json.loads(raw_data["payload"]),
        retry_count=int(raw_data.get("retry_count", 0)),
        timestamp=float(raw_data["timestamp"])
    )

    # 2. Acquire Distributed Lock (10 second TTL)
    lock_token = await acquire_lock(redis, task.task_id, ttl_ms=10000)
    if not lock_token:
        print(f"[{worker_id}] Task {task.task_id} is already locked by another worker.")
        return

    try:
        # 3. Update State to IN_PROGRESS
        await update_state(redis, task.task_id, TaskStatus.IN_PROGRESS, worker_id)
        
        # 4. Execute the Tool (This triggers Pydantic Validation dynamically)
        print(f"[{worker_id}] Executing '{task.task_type}' for task {task.task_id}")
        result = await tool_registry.execute(task.task_type, task.payload)
        
        # 5. Success Path
        await update_state(redis, task.task_id, TaskStatus.COMPLETED)
        # Note: We would also publish the result to stream:tasks:completed here
        print(f"[{worker_id}] Success! Result: {result}")

    except Exception as e:
        # 6. Failure Path (Classify and Route)
        action = classify_error(e)
        print(f"[{worker_id}] Task {task.task_id} failed. Reason: {type(e).__name__}. Action: {action.value}")

        if action == ErrorAction.ROUTE_TO_DLQ:
            # Package the context for the LLM
            dlq_msg = DLQMessage(
                task=task,
                error_type=type(e).__name__,
                error_message=str(e),
                stack_trace=traceback.format_exc(),
                target_schema=tool_registry.get_schema_definition(task.task_type)
            )
            await publish_dlq_message(redis, dlq_msg)
            await update_state(redis, task.task_id, TaskStatus.IN_DLQ)
            
        elif action == ErrorAction.RETRY_LOCAL:
            if task.retry_count < MAX_RETRIES:
                task.retry_count += 1
                # Re-queue the task logic would go here
            else:
                await update_state(redis, task.task_id, TaskStatus.DEAD)
                
        elif action == ErrorAction.ROUTE_TO_DEAD:
            await update_state(redis, task.task_id, TaskStatus.DEAD)

    finally:
        # 7. Acknowledge and Cleanup
        # XACK removes it from the Pending Entries List so no one else tries it
        await redis.xack("stream:tasks:pending", "cg_main_workers", message_id)
        await release_lock(redis, task.task_id, lock_token)

async def worker_loop(redis: Redis, worker_id: str):
    """The infinite blocking pull loop."""
    print(f"[{worker_id}] Starting Stream Worker...")
    
    # Ensure streams are set up (normally done at app startup, but safe to include)
    from engine.queue import setup_stream_topology
    await setup_stream_topology(redis)
    
    while True:
        try:
            # Blocking pull: Wait up to 2 seconds for a new task in the stream
            # ">" means "give me messages that haven't been delivered to any other worker yet"
            streams = await redis.xreadgroup(
                groupname="cg_main_workers",
                consumername=worker_id,
                streams={"stream:tasks:pending": ">"},
                count=1,
                block=2000 
            )

            if streams:
                # Parse the nested Redis stream response
                stream_name, messages = streams[0]
                message_id, raw_data = messages[0]
                
                # Process it
                await process_task(redis, worker_id, message_id, raw_data)
                
        except Exception as e:
            print(f"[{worker_id}] Worker loop error: {e}")
            await asyncio.sleep(1)
