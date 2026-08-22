import asyncio
from engine.redis_client import get_redis
from engine.worker import worker_loop
# Import the plugin so the registry populates
import plugins.financial.tools 

async def main():
    redis = await get_redis()
    await worker_loop(redis, "worker_01")

if __name__ == "__main__":
    asyncio.run(main())
