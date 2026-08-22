import asyncio
from engine.redis_client import get_redis
from engine.recovery import recovery_loop
# Import the plugin so the registry populates
import plugins.financial.tools 

async def main():
    redis = await get_redis()
    await recovery_loop(redis, "recovery_agent_01")

if __name__ == "__main__":
    asyncio.run(main())
