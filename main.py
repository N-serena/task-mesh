from fastapi import FastAPI

from api.routes import router as jobs_router
from engine.queue import TaskQueue
from engine.redis_client import create_redis_client
from engine.state import TaskStateStore
from plugins.registry import ToolRegistry


app = FastAPI(title="Task Mesh", version="0.1.0")
app.include_router(jobs_router)


@app.on_event("startup")
async def startup() -> None:
    redis = create_redis_client()
    app.state.redis = redis
    app.state.task_state_store = TaskStateStore(redis)
    app.state.task_queue = TaskQueue(redis)
    app.state.tool_registry = ToolRegistry()
    app.state.tool_registry.register_financial_tools()


@app.on_event("shutdown")
async def shutdown() -> None:
    await app.state.redis.aclose()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
