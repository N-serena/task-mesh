from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from engine.queue import TaskQueue
from engine.state import TaskRecord, TaskStateStore, TaskStatus
from plugins.financial.models import FetchMetricsArgs
from plugins.registry import ToolRegistry

router = APIRouter(prefix="/jobs", tags=["jobs"])


class JobSubmitRequest(BaseModel):
    job: str = Field(default="financial_analysis")
    ticker: str
    fiscal_year: int = Field(default_factory=lambda: datetime.now(tz=timezone.utc).year)


class JobSubmitResponse(BaseModel):
    job_id: str
    task_id: str
    status: TaskStatus


class JobStatusResponse(BaseModel):
    job_id: str
    task_id: str
    status: TaskStatus
    payload: dict[str, Any]


def get_state_store(request: Request) -> TaskStateStore:
    return request.app.state.task_state_store


def get_task_queue(request: Request) -> TaskQueue:
    return request.app.state.task_queue


def get_registry(request: Request) -> ToolRegistry:
    return request.app.state.tool_registry


@router.post("", response_model=JobSubmitResponse)
async def submit_job(
    payload: JobSubmitRequest,
    state_store: TaskStateStore = Depends(get_state_store),
    task_queue: TaskQueue = Depends(get_task_queue),
    registry: ToolRegistry = Depends(get_registry),
) -> JobSubmitResponse:
    if payload.job != "financial_analysis":
        raise HTTPException(status_code=400, detail="Unsupported job type")

    job_id = str(uuid4())
    task_id = f"{payload.ticker}_{job_id[:8]}"
    task_payload = FetchMetricsArgs(
        ticker=payload.ticker,
        fiscal_year=payload.fiscal_year,
    ).model_dump()

    record = TaskRecord(
        job_id=job_id,
        task_id=task_id,
        tool_name="fetch_metrics",
        payload=task_payload,
        status=TaskStatus.PENDING,
    )
    await state_store.initialize_task(record)
    await task_queue.enqueue_pending(record)

    return JobSubmitResponse(job_id=job_id, task_id=task_id, status=TaskStatus.PENDING)


@router.get("/{job_id}", response_model=JobStatusResponse)
async def get_job_status(
    job_id: str,
    state_store: TaskStateStore = Depends(get_state_store),
) -> JobStatusResponse:
    record = await state_store.get_by_job_id(job_id)
    if record is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatusResponse(
        job_id=record.job_id,
        task_id=record.task_id,
        status=record.status,
        payload=record.payload,
    )
