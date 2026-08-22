import asyncio
from enum import Enum
from pydantic import ValidationError

class ErrorAction(Enum):
    RETRY_LOCAL = "RETRY_LOCAL"
    ROUTE_TO_DLQ = "ROUTE_TO_DLQ"
    ROUTE_TO_DEAD = "ROUTE_TO_DEAD"

def classify_error(error: Exception) -> ErrorAction:
    """
    Evaluates an exception and routes the task accordingly.
    """
    # 1. Schema & Data Errors -> Route to the LLM DLQ for repair
    # If the payload was missing a key, had the wrong type, or failed a regex
    if isinstance(error, (ValidationError, TypeError, KeyError, ValueError)):
        return ErrorAction.ROUTE_TO_DLQ
    
    # 2. Transient Infrastructure Errors -> Standard retry
    # If a database was briefly unavailable or an API request timed out
    if isinstance(error, (asyncio.TimeoutError, ConnectionError)):
        return ErrorAction.RETRY_LOCAL
        
    # 3. Fatal/Unknown Errors -> Poison pill (Drop it)
    # Examples: AuthenticationError, NotImplementedError, ZeroDivisionError
    return ErrorAction.ROUTE_TO_DEAD
