import uuid
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
