from redis.exceptions import DataError


class StubRedis:
    """In-memory stand-in for the Redis commands the engine uses.

    Values are checked against the types redis-py can encode, so a test fails if
    the engine tries to write None or another unsupported type.
    """

    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.streams: dict[str, list[dict]] = {}
        self.keys: dict[str, str] = {}
        self.acked: list[tuple[str, str]] = []

    @staticmethod
    def _encodable(value) -> None:
        if not isinstance(value, (bytes, str, int, float)):
            raise DataError(
                f"Invalid input of type: {type(value).__name__!r}. "
                "Convert to a bytes, string, int or float first."
            )

    async def hset(self, key: str, mapping: dict | None = None, **kwargs) -> int:
        fields = dict(mapping or {})
        for value in fields.values():
            self._encodable(value)
        self.hashes.setdefault(key, {}).update({k: str(v) for k, v in fields.items()})
        return len(fields)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def xadd(self, stream: str, record: dict) -> str:
        for value in record.values():
            self._encodable(value)
        entries = self.streams.setdefault(stream, [])
        entries.append(record)
        return f"{len(entries)}-0"

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.acked.append((stream, message_id))
        return 1

    async def set(self, key: str, value: str, nx: bool = False, px: int | None = None):
        if nx and key in self.keys:
            return None
        self.keys[key] = value
        return True

    async def eval(self, script: str, numkeys: int, key: str, arg: str) -> int:
        """Stands in for the compare-and-delete lock release in engine/locks.py."""
        if self.keys.get(key) == arg:
            del self.keys[key]
            return 1
        return 0
