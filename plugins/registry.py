from typing import Callable, Dict, Type, Tuple
from pydantic import BaseModel, ValidationError

class ToolRegistry:
    def __init__(self):
        # Maps tool_name -> (function_callable, Pydantic_Model)
        self._tools: Dict[str, Tuple[Callable, Type[BaseModel]]] = {}

    def register(self, name: str, schema: Type[BaseModel]):
        """Decorator to register a tool function and its strict Pydantic contract."""
        def decorator(func: Callable):
            self._tools[name] = (func, schema)
            return func
        return decorator

    def get_schema_definition(self, name: str) -> dict:
        """Exports the JSON Schema definition for the LLM prompt."""
        if name not in self._tools:
            raise ValueError(f"Tool '{name}' not found in registry.")
        _, schema = self._tools[name]
        return schema.model_json_schema()

    async def execute(self, name: str, raw_payload: dict) -> dict:
        """
        Validates the payload against the schema and executes the tool.
        If the payload is malformed, Pydantic raises a ValidationError here.
        """
        if name not in self._tools:
            raise ValueError(f"Tool '{name}' not registered.")
        
        func, schema = self._tools[name]
        
        # 1. Deterministic Validation Gate
        # If 'raw_payload' has bad keys/types, this line crashes and routes to DLQ.
        validated_args = schema(**raw_payload)
        
        # 2. Execution
        return await func(validated_args)

# Global singleton instance used by workers and the recovery agent
tool_registry = ToolRegistry()
