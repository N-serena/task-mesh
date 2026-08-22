import time
from enum import Enum
from typing import Dict, Any, Optional
from pydantic import BaseModel, Field

class TaskStatus(str, Enum):
    PENDING = "PENDING"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    IN_DLQ = "IN_DLQ"
    RECOVERING = "RECOVERING"
    DEAD = "DEAD"

class TaskMessage(BaseModel):
    job_id: str
    task_id: str
    task_type: str
    idempotency_key: str
    payload: Dict[str, Any]
    retry_count: int = 0
    timestamp: float = Field(default_factory=time.time)

class TaskStateRecord(BaseModel):
    status: TaskStatus = TaskStatus.PENDING
    worker_id: Optional[str] = None
    lock_expires_at: Optional[float] = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)

class DLQMessage(BaseModel):
    task: TaskMessage
    error_type: str
    error_message: str
    stack_trace: str
    target_schema: Dict[str, Any]
    recovery_attempts: int = 0
    #last_recovered_at: Optional[float] = None
    #last_recovered_by: Optional[str] = None
    