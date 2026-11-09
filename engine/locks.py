import asyncio
import uuid
from typing import Awaitable, TypeVar
from redis.asyncio import Redis

# Lua script to safely release a lock.
# It checks if the value at KEYS[1] matches ARGV[1]. 
# If it matches, it deletes the key. If it doesn't match, it returns 0.
RELEASE_LUA_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""

# Extends the deadline only for the token named in ARGV[1]. A worker whose lock
# already expired and was taken over cannot push the new holder's deadline out.
RENEW_LUA_SCRIPT = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("pexpire", KEYS[1], ARGV[2])
else
    return 0
end
"""

# Renewals attempted per TTL. Above 1 so a single lost round trip to Redis does
# not expire a lock the holder is still working under.
RENEWALS_PER_TTL = 3

T = TypeVar("T")


class LockLost(Exception):
    """The lock expired or changed hands while its holder was still working."""


async def acquire_lock(redis: Redis, task_id: str, ttl_ms: int = 10000) -> str | None:
    """
    Attempts to acquire an exclusive lock for a task.
    Returns a unique lock_token if successful, or None if the lock is currently held.
    """
    lock_key = f"lock:task:{task_id}"
    lock_token = str(uuid.uuid4())
    
    # nx=True: Only set if the key does NOT already exist.
    # px=ttl_ms: Automatically delete the lock after X milliseconds to prevent deadlocks.
    acquired = await redis.set(lock_key, lock_token, nx=True, px=ttl_ms)
    
    if acquired:
        return lock_token
    return None

async def release_lock(redis: Redis, task_id: str, lock_token: str) -> bool:
    """
    Safely releases the lock using a Lua script to ensure the caller actually owns it.
    """
    lock_key = f"lock:task:{task_id}"
    
    # Redis eval executes the Lua script atomically on the server
    result = await redis.eval(RELEASE_LUA_SCRIPT, 1, lock_key, lock_token)
    return bool(result)

async def renew_lock(redis: Redis, task_id: str, lock_token: str, ttl_ms: int) -> bool:
    """Resets the lock's deadline. False means the caller no longer holds it."""
    lock_key = f"lock:task:{task_id}"
    result = await redis.eval(RENEW_LUA_SCRIPT, 1, lock_key, lock_token, ttl_ms)
    return bool(result)

async def _renew_until_lost(
    redis: Redis, task_id: str, lock_token: str, ttl_ms: int, interval_seconds: float
):
    """Heartbeat for one lock. Returns as soon as the lock is no longer ours."""
    while True:
        await asyncio.sleep(interval_seconds)
        if not await renew_lock(redis, task_id, lock_token, ttl_ms):
            return

async def run_while_locked(
    redis: Redis,
    task_id: str,
    lock_token: str,
    coro: Awaitable[T],
    ttl_ms: int,
) -> T:
    """Runs coro while holding the lock open, however long the work takes.

    The lock TTL bounds how long a crashed holder blocks the task, so it stays
    short; a live holder keeps its claim by renewing. Raises LockLost after
    cancelling coro if the lock goes: work that no longer owns its lock must
    stop, because another holder may already have started the same task.
    """
    work = asyncio.ensure_future(coro)
    heartbeat = asyncio.ensure_future(
        _renew_until_lost(
            redis, task_id, lock_token, ttl_ms, ttl_ms / 1000 / RENEWALS_PER_TTL
        )
    )
    try:
        await asyncio.wait({work, heartbeat}, return_when=asyncio.FIRST_COMPLETED)
        if work.done():
            return work.result()
        raise LockLost(f"Lock on {task_id} expired or was taken over mid-execution")
    finally:
        for task in (work, heartbeat):
            task.cancel()
        # Settle the cancellations so neither task is collected while still running.
        await asyncio.gather(work, heartbeat, return_exceptions=True)
