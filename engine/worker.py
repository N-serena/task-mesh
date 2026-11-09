import json
import asyncio
import traceback
from redis.asyncio import Redis

from engine.models import TaskMessage, TaskStatus, DLQMessage
from engine.state import update_state
from engine.locks import LockLost, acquire_lock, release_lock, run_while_locked
from engine.classifier import classify_error, ErrorAction
from engine.queue import claim_stale_messages, publish_dlq_message
from plugins.registry import tool_registry

MAX_RETRIES = 3

# How long a worker holds a task exclusively without checking in. A live worker
# renews this for as long as the task runs, so the value bounds how long a crashed
# worker blocks the task and not how long a task may take.
# queue.STALE_MESSAGE_MS must stay above this so a live worker keeps its claim.
LOCK_TTL_MS = 10_000

# How often a worker checks the Pending Entries List for abandoned tasks.
SWEEP_INTERVAL_SECONDS = 10

async def process_task(redis: Redis, worker_id: str, message_id: str, raw_data: dict):
    """Processes a single task safely."""
    # 1. Deserialize the flat Redis dictionary back into our TaskMessage
    # An entry that cannot be read carries no task_id to record state against, and
    # it would be reclaimed from the Pending Entries List forever. Acknowledge it.
    try:
        task = TaskMessage(
            job_id=raw_data["job_id"],
            task_id=raw_data["task_id"],
            task_type=raw_data["task_type"],
            idempotency_key=raw_data["idempotency_key"],
            payload=json.loads(raw_data["payload"]),
            retry_count=int(raw_data.get("retry_count", 0)),
            timestamp=float(raw_data["timestamp"])
        )
    except Exception as e:
        print(f"[{worker_id}] Discarding unreadable stream entry {message_id}: "
              f"{type(e).__name__}: {e}")
        await redis.xack("stream:tasks:pending", "cg_main_workers", message_id)
        return

    # 2. Acquire Distributed Lock
    lock_token = await acquire_lock(redis, task.task_id, ttl_ms=LOCK_TTL_MS)
    if not lock_token:
        print(f"[{worker_id}] Task {task.task_id} is already locked by another worker.")
        return

    # Set to False where the message must stay in the Pending Entries List.
    acknowledge = True

    try:
        # 3. Update State to IN_PROGRESS
        await update_state(redis, task.task_id, TaskStatus.IN_PROGRESS, worker_id)
        
        # 4. Execute the Tool (This triggers Pydantic Validation dynamically)
        # The heartbeat holds the lock for the whole execution, so a task that runs
        # longer than LOCK_TTL_MS cannot be picked up and executed a second time.
        print(f"[{worker_id}] Executing '{task.task_type}' for task {task.task_id}")
        result = await run_while_locked(
            redis,
            task.task_id,
            lock_token,
            tool_registry.execute(task.task_type, task.payload),
            ttl_ms=LOCK_TTL_MS,
        )
        
        # 5. Success Path
        await update_state(redis, task.task_id, TaskStatus.COMPLETED)
        # Note: We would also publish the result to stream:tasks:completed here
        print(f"[{worker_id}] Success! Result: {result}")

    except LockLost:
        # Another worker may already be running this task, so leave its state alone
        # and leave the message pending for whoever holds the lock now.
        print(f"[{worker_id}] Lost the lock on task {task.task_id} mid-execution. "
              f"Abandoning it to the current holder.")
        acknowledge = False

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
        if acknowledge:
            await redis.xack("stream:tasks:pending", "cg_main_workers", message_id)
        # Compare-and-delete, so this is a no-op once the lock has changed hands.
        await release_lock(redis, task.task_id, lock_token)

async def sweep_stale_tasks(redis: Redis, worker_id: str) -> int:
    """Picks up tasks abandoned by crashed workers. Returns how many were claimed."""
    claimed = await claim_stale_messages(
        redis, "stream:tasks:pending", "cg_main_workers", worker_id
    )
    for message_id, raw_data in claimed:
        print(f"[{worker_id}] Reclaimed abandoned task {message_id} from the PEL.")
        await process_task(redis, worker_id, message_id, raw_data)
    return len(claimed)

async def worker_loop(redis: Redis, worker_id: str):
    """The infinite blocking pull loop."""
    print(f"[{worker_id}] Starting Stream Worker...")

    # Ensure streams are set up (normally done at app startup, but safe to include)
    from engine.queue import setup_stream_topology
    await setup_stream_topology(redis)

    next_sweep = 0.0

    while True:
        try:
            now = asyncio.get_running_loop().time()
            if now >= next_sweep:
                next_sweep = now + SWEEP_INTERVAL_SECONDS
                await sweep_stale_tasks(redis, worker_id)

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
