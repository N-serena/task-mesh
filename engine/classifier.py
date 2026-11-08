import asyncio
from enum import Enum

import httpx
import httpx2
import redis.exceptions as redis_exc
from google.genai import errors as genai_errors
from pydantic import ValidationError

class ErrorAction(Enum):
    RETRY_LOCAL = "RETRY_LOCAL"
    ROUTE_TO_DLQ = "ROUTE_TO_DLQ"
    ROUTE_TO_DEAD = "ROUTE_TO_DEAD"

# The exception types Redis and the HTTP clients raise on network trouble. None of
# them subclass the builtin ConnectionError or asyncio.TimeoutError.
#
# httpx and httpx2 are separate packages with separate class hierarchies and
# identically named exceptions, so both are listed. google-genai transports over
# httpx2; a client-side timeout there arrives as httpx2.ConnectTimeout.
#
# genai_errors.ServerError covers model-side 5xx, including capacity 503s.
TRANSIENT_ERRORS = (
    asyncio.TimeoutError,
    ConnectionError,
    redis_exc.ConnectionError,
    redis_exc.TimeoutError,
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx2.TimeoutException,
    httpx2.NetworkError,
    genai_errors.ServerError,
)

# 4xx responses that clear on their own. Every other ClientError is permanent.
TRANSIENT_API_CODES = frozenset({408, 429})

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

    if isinstance(error, genai_errors.APIError) and error.code in TRANSIENT_API_CODES:
        return ErrorAction.RETRY_LOCAL

    # Schema & data errors -> route to the LLM DLQ for repair
    if isinstance(error, SCHEMA_ERRORS):
        return ErrorAction.ROUTE_TO_DLQ

    # Fatal/unknown errors -> poison pill (drop it)
    # Examples: AuthenticationError, NotImplementedError, ZeroDivisionError
    return ErrorAction.ROUTE_TO_DEAD
