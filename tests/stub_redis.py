import asyncio
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
        self.trim_requests: list[tuple[str, int | None, bool]] = []
        # Messages delivered to a consumer but not yet acknowledged, keyed by
        # (stream, message_id) — the Pending Entries List.
        self.pending: dict[tuple[str, str], dict] = {}
        self.claim_calls: list[tuple[str, str, str, int]] = []
        self.renew_calls: list[tuple[str, str, int]] = []
        # Sorted sets, as {name: {member: score}}.
        self.zsets: dict[str, dict[str, float]] = {}

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

    async def xadd(
        self,
        stream: str,
        record: dict,
        maxlen: int | None = None,
        approximate: bool = True,
    ) -> str:
        for value in record.values():
            self._encodable(value)
        entries = self.streams.setdefault(stream, [])
        entries.append(record)
        # Recorded so tests can assert a length cap was requested.
        self.trim_requests.append((stream, maxlen, approximate))
        return f"{len(entries)}-0"

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.acked.append((stream, message_id))
        self.pending.pop((stream, message_id), None)
        return 1

    def strand(self, stream: str, message_id: str, fields: dict) -> None:
        """Simulates a consumer that read a message and died before acknowledging."""
        self.pending[(stream, message_id)] = fields

    async def xautoclaim(
        self,
        name: str,
        groupname: str,
        consumername: str,
        min_idle_time: int,
        start_id: str = "0-0",
        count: int | None = None,
        justid: bool = False,
    ):
        claimed = [
            (message_id, fields)
            for (stream, message_id), fields in self.pending.items()
            if stream == name
        ]
        if count is not None:
            claimed = claimed[:count]
        self.claim_calls.append((name, groupname, consumername, min_idle_time))
        # Redis 7 shape: (next_cursor, messages, deleted)
        return "0-0", claimed, []

    async def set(self, key: str, value: str, nx: bool = False, px: int | None = None):
        if nx and key in self.keys:
            return None
        self.keys[key] = value
        return True

    async def xgroup_create(self, name, groupname, id="$", mkstream=False) -> bool:
        return True

    async def xreadgroup(self, groupname, consumername, streams, count=None, block=None):
        """No undelivered messages; the sweeper path is what these tests drive.

        Yields to the event loop, standing in for the real call's `block` wait. A
        consumer loop has no other await point, so returning synchronously here
        starves the scheduler.
        """
        await asyncio.sleep(0.001)
        return []

    async def zadd(self, name: str, mapping: dict) -> int:
        members = self.zsets.setdefault(name, {})
        added = sum(1 for member in mapping if member not in members)
        members.update(mapping)
        return added

    async def zcard(self, name: str) -> int:
        return len(self.zsets.get(name, {}))

    def steal_lock(self, resource: str, token: str = "token_of_another_worker") -> None:
        """Simulates a lock expiring and being re-acquired by a different worker."""
        self.keys[f"lock:task:{resource}"] = token

    async def eval(self, script: str, numkeys: int, key: str, *args) -> int:
        """Stands in for the Lua lock scripts in engine/locks.py.

        Dispatches on the script body and compares the stored token in Python, so
        the Lua itself is unverified here. lock_check.py covers that against a real
        server.
        """
        if "zrangebyscore" in script:
            members = self.zsets.setdefault(key, {})
            now, count = float(args[0]), int(args[1])
            due = [
                member
                for member, score in sorted(members.items(), key=lambda kv: kv[1])
                if score <= now
            ][:count]
            for member in due:
                del members[member]
            return due

        token = args[0]

        if "pexpire" in script:
            self.renew_calls.append((key, token, int(args[1])))
            return 1 if self.keys.get(key) == token else 0

        if self.keys.get(key) == token:
            del self.keys[key]
            return 1
        return 0
