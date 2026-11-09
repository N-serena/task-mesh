import json
import time
from redis.asyncio import Redis
from engine.models import DLQMessage, TaskMessage

DLQ_STREAM = "stream:tasks:dlq"

# Tasks waiting out a transient failure, scored by the time they come due. Redis
# Streams deliver as soon as an entry is added, so a delayed retry needs a sorted
# set that consumers poll by due time.
RETRY_ZSET = "zset:tasks:retry"

# Define our topology
STREAMS = {
    "stream:tasks:pending": "cg_main_workers",
    DLQ_STREAM: "cg_recovery_agents",
    "stream:tasks:completed": None, # Append-only, no consumer group
    "stream:tasks:dead": None       # Append-only, no consumer group
}

# XACK clears the Pending Entries List but leaves the entry in the stream, so a
# length cap is the only thing that reclaims it. Every publish passes one, which
# is why all stream writes go through this module.
#
# Redis trims whole macro-nodes rather than exact counts, keeping XADD O(1), so
# the real length drifts slightly above these numbers.
TASK_STREAM_MAXLEN = 10_000

# DLQ entries carry a stack trace and a JSON schema, so they are far larger than
# a task record and get a tighter cap.
DLQ_STREAM_MAXLEN = 1_000

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

# A consumer that dies between XREADGROUP and XACK leaves its message in the
# group's Pending Entries List, where nothing else will ever pick it up. Anything
# idle longer than this is handed to a live consumer.
#
# This exceeds the 10s task lock TTL, so a slow-but-alive worker still holds its
# lock when the message is claimed and the claimant backs off.
STALE_MESSAGE_MS = 15_000

async def claim_stale_messages(
    redis: Redis,
    stream_name: str,
    group_name: str,
    consumer_name: str,
    min_idle_ms: int = STALE_MESSAGE_MS,
    count: int = 10,
) -> list:
    """
    Reassigns messages abandoned by dead consumers to this one.
    Returns the claimed (message_id, fields) pairs.
    """
    result = await redis.xautoclaim(
        stream_name,
        group_name,
        consumer_name,
        min_idle_time=min_idle_ms,
        count=count,
    )
    # returns (cursor, messages, deleted) tuple.
    return result[1]

# Pops the due members in one step. Two workers sweeping at the same moment
# would otherwise both read a task before either removed it, and re-queue it twice.
DUE_RETRIES_LUA_SCRIPT = """
local due = redis.call("zrangebyscore", KEYS[1], "-inf", ARGV[1], "LIMIT", 0, ARGV[2])
if #due > 0 then
    redis.call("zrem", KEYS[1], unpack(due))
end
return due
"""

async def schedule_retry(redis: Redis, task: TaskMessage, delay_seconds: float) -> float:
    """Parks a task until its backoff elapses. Returns the time it comes due.

    The task is held in Redis, so a worker that dies while tasks are parked
    strands none of them: whichever worker sweeps next re-queues them.
    """
    ready_at = time.time() + delay_seconds
    await redis.zadd(RETRY_ZSET, {task.model_dump_json(): ready_at})
    return ready_at

async def claim_due_retries(redis: Redis, count: int = 10) -> list[TaskMessage]:
    """Removes and returns the parked tasks whose backoff has elapsed."""
    members = await redis.eval(
        DUE_RETRIES_LUA_SCRIPT, 1, RETRY_ZSET, time.time(), count
    )

    tasks = []
    for member in members:
        # A member that cannot be read is already out of the sorted set, so it is
        # reported and dropped here.
        try:
            tasks.append(TaskMessage.model_validate_json(member))
        except Exception as e:
            print(f"Discarding unreadable parked retry: {type(e).__name__}: {e}")
    return tasks

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
    message_id = await redis.xadd(
        stream_name, flat_record, maxlen=TASK_STREAM_MAXLEN, approximate=True
    )
    return message_id

async def publish_dlq_message(redis: Redis, dlq_msg: DLQMessage) -> str:
    """
    Pushes a failed task and its repair context to the DLQ for the Recovery Agent.
    Returns the unique Redis-generated message ID.
    """
    return await redis.xadd(
        DLQ_STREAM,
        {"payload": dlq_msg.model_dump_json()},
        maxlen=DLQ_STREAM_MAXLEN,
        approximate=True,
    )