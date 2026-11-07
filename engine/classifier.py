import asyncio
from enum import Enum

import httpx
import redis.exceptions as redis_exc
from pydantic import ValidationError

class ErrorAction(Enum):
    RETRY_LOCAL = "RETRY_LOCAL"
    ROUTE_TO_DLQ = "ROUTE_TO_DLQ"
    ROUTE_TO_DEAD = "ROUTE_TO_DEAD"

# The exception types Redis and httpx raise on network trouble. Neither library's
# errors subclass the builtin ConnectionError or asyncio.TimeoutError.
TRANSIENT_ERRORS = (
    asyncio.TimeoutError,
    ConnectionError,
    redis_exc.ConnectionError,
    redis_exc.TimeoutError,
    httpx.TimeoutException,
    httpx.NetworkError,
)

# Payload-level failures the Recovery Agent can attempt to repair.
# ValidationError subclasses ValueError.
SCHEMA_ERRORS = (ValidationError, TypeError, KeyError, ValueError)

def classify_error(error: Exception) -> ErrorAction:
    """
    Evaluates an exception and routes the task accordingly.
    """
    # Transient infrastructure errors -> standard retry
    if isinstance(error, TRANSIENT_ERRORS):
        return ErrorAction.RETRY_LOCAL

    # Schema & data errors -> route to the LLM DLQ for repair
    if isinstance(error, SCHEMA_ERRORS):
        return ErrorAction.ROUTE_TO_DLQ

    # Fatal/unknown errors -> poison pill (drop it)
    # Examples: AuthenticationError, NotImplementedError, ZeroDivisionError
    return ErrorAction.ROUTE_TO_DEAD
