import os
from redis.asyncio import Redis, ConnectionPool
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# decode_responses=True automatically converts Redis bytes to Python strings
pool = ConnectionPool.from_url(REDIS_URL, decode_responses=True)
redis_client = Redis(connection_pool=pool)

async def get_redis() -> Redis:
    """Dependency injection helper for FastAPI and Worker loops."""
    return redis_client
