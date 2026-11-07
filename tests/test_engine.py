import asyncio
import json

import httpx
import pytest
import redis.exceptions as redis_exc
from pydantic import ValidationError

import engine.recovery as recovery
import plugins.financial.tools  # noqa: F401  (registers the financial tools)
from engine.classifier import ErrorAction, classify_error
from engine.models import DLQMessage, TaskStatus
from engine.queue import STREAMS
from engine.state import get_state, initialize_state
from engine.worker import process_task
from plugins.financial.models import FetchMetricsArgs
from plugins.registry import tool_registry
from tests.stub_redis import StubRedis

PENDING_STREAM = "stream:tasks:pending"
DLQ_STREAM = "stream:tasks:dlq"

GOOD_PAYLOAD = {"ticker": "AAPL", "fiscal_year": 2025}
DRIFTED_PAYLOAD = {"company_ticker": "msft", "year": "25"}
REPAIRED_PAYLOAD = {"ticker": "MSFT", "fiscal_year": 2025}


@pytest.fixture
def redis() -> StubRedis:
    return StubRedis()


def stream_entry(task_id: str, payload: dict) -> dict:
    """Builds the flat record shape the engine reads off a Redis stream."""
    return {
        "job_id": "job_test",
        "task_id": task_id,
        "task_type": "fetch_sec_metrics",
        "idempotency_key": "key_test",
        "payload": json.dumps(payload),
        "retry_count": "0",
        "timestamp": "1700000000.0",
    }


def dlq_entry(task_id: str, payload: dict, recovery_attempts: int = 0) -> dict:
    message = DLQMessage(
        task={
            "job_id": "job_test",
            "task_id": task_id,
            "task_type": "fetch_sec_metrics",
            "idempotency_key": "key_test",
            "payload": payload,
        },
        error_type="ValidationError",
        error_message="payload failed schema validation",
        stack_trace="",
        target_schema=tool_registry.get_schema_definition("fetch_sec_metrics"),
        recovery_attempts=recovery_attempts,
    )
    return {"payload": message.model_dump_json()}


class TestStateStore:
    async def test_initialize_state_writes_an_encodable_record(self, redis):
        """Unset fields are None, which Redis cannot encode."""
        await initialize_state(redis, "task_1")
        assert redis.hashes["task:state:task_1"]["status"] == "PENDING"

    async def test_unset_fields_round_trip_as_defaults(self, redis):
        await initialize_state(redis, "task_1")
        record = await get_state(redis, "task_1")
        assert record.status == TaskStatus.PENDING
        assert record.worker_id is None
        assert record.lock_expires_at is None

    async def test_missing_task_returns_none(self, redis):
        assert await get_state(redis, "nope") is None


class TestHappyPath:
    async def test_valid_payload_completes(self, redis):
        await initialize_state(redis, "task_ok")
        await process_task(redis, "worker_1", "1-0", stream_entry("task_ok", GOOD_PAYLOAD))

        record = await get_state(redis, "task_ok")
        assert record.status == TaskStatus.COMPLETED

    async def test_message_is_acked(self, redis):
        await initialize_state(redis, "task_ok")
        await process_task(redis, "worker_1", "1-0", stream_entry("task_ok", GOOD_PAYLOAD))

        assert (PENDING_STREAM, "1-0") in redis.acked

    async def test_lock_is_released(self, redis):
        await initialize_state(redis, "task_ok")
        await process_task(redis, "worker_1", "1-0", stream_entry("task_ok", GOOD_PAYLOAD))

        assert redis.keys == {}

    async def test_a_locked_task_is_skipped(self, redis):
        await initialize_state(redis, "task_ok")
        redis.keys["lock:task:task_ok"] = "held-by-someone-else"

        await process_task(redis, "worker_1", "1-0", stream_entry("task_ok", GOOD_PAYLOAD))

        record = await get_state(redis, "task_ok")
        assert record.status == TaskStatus.PENDING


class TestChaosPath:
    async def test_schema_drift_routes_to_dlq(self, redis):
        await initialize_state(redis, "task_bad")
        await process_task(redis, "worker_1", "2-0", stream_entry("task_bad", DRIFTED_PAYLOAD))

        record = await get_state(redis, "task_bad")
        assert record.status == TaskStatus.IN_DLQ
        assert len(redis.streams[DLQ_STREAM]) == 1

    async def test_dlq_message_carries_the_target_schema(self, redis):
        await initialize_state(redis, "task_bad")
        await process_task(redis, "worker_1", "2-0", stream_entry("task_bad", DRIFTED_PAYLOAD))

        message = DLQMessage.model_validate_json(redis.streams[DLQ_STREAM][0]["payload"])
        assert set(message.target_schema["properties"]) == {"ticker", "fiscal_year"}

    async def test_dlq_message_preserves_the_broken_payload(self, redis):
        await initialize_state(redis, "task_bad")
        await process_task(redis, "worker_1", "2-0", stream_entry("task_bad", DRIFTED_PAYLOAD))

        message = DLQMessage.model_validate_json(redis.streams[DLQ_STREAM][0]["payload"])
        assert message.task.payload == DRIFTED_PAYLOAD


class TestRecoveryAgent:
    @pytest.fixture(autouse=True)
    def stub_llm(self, monkeypatch):
        """Replaces the LLM call so the suite needs no API key."""

        async def repair(dlq_msg):
            return REPAIRED_PAYLOAD

        monkeypatch.setattr(recovery, "repair_payload", repair)

    async def test_repair_reinjects_to_the_stream_workers_read(self, redis):
        await initialize_state(redis, "task_bad")
        await recovery.process_dlq_task(
            redis, "recovery_1", "3-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        assert len(redis.streams[PENDING_STREAM]) == 1

    async def test_no_write_lands_on_the_retired_retry_stream(self, redis):
        await initialize_state(redis, "task_bad")
        await recovery.process_dlq_task(
            redis, "recovery_1", "3-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        assert "stream:tasks:retry" not in redis.streams

    async def test_reinjected_payload_is_the_repaired_one(self, redis):
        await initialize_state(redis, "task_bad")
        await recovery.process_dlq_task(
            redis, "recovery_1", "3-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        entry = redis.streams[PENDING_STREAM][0]
        assert json.loads(entry["payload"]) == REPAIRED_PAYLOAD

    async def test_exhausted_recovery_budget_marks_the_task_dead(self, redis):
        await initialize_state(redis, "task_bad")
        await recovery.process_dlq_task(
            redis, "recovery_1", "3-0", dlq_entry("task_bad", DRIFTED_PAYLOAD, recovery_attempts=2)
        )

        record = await get_state(redis, "task_bad")
        assert record.status == TaskStatus.DEAD
        assert PENDING_STREAM not in redis.streams


class TestSelfHealingLoop:
    async def test_a_drifted_payload_eventually_completes(self, redis, monkeypatch):
        """The whole point of the system: drift in, COMPLETED out."""

        async def repair(dlq_msg):
            return REPAIRED_PAYLOAD

        monkeypatch.setattr(recovery, "repair_payload", repair)

        await initialize_state(redis, "task_heal")

        # 1. A worker fails on the drifted payload and routes it to the DLQ.
        await process_task(redis, "worker_1", "1-0", stream_entry("task_heal", DRIFTED_PAYLOAD))
        assert (await get_state(redis, "task_heal")).status == TaskStatus.IN_DLQ

        # 2. The recovery agent repairs it and re-injects.
        dlq_payload = redis.streams[DLQ_STREAM][0]
        await recovery.process_dlq_task(redis, "recovery_1", "2-0", dlq_payload)
        assert (await get_state(redis, "task_heal")).status == TaskStatus.PENDING

        # 3. A worker picks the repaired task up and it succeeds.
        reinjected = redis.streams[PENDING_STREAM][0]
        await process_task(redis, "worker_1", "3-0", reinjected)

        record = await get_state(redis, "task_heal")
        assert record.status == TaskStatus.COMPLETED


class TestErrorClassifier:
    @pytest.mark.parametrize(
        "error",
        [
            redis_exc.ConnectionError("redis unreachable"),
            redis_exc.TimeoutError("redis slow"),
            httpx.ConnectTimeout("api slow"),
            httpx.ReadTimeout("api slow"),
            httpx.ConnectError("dns failure"),
            asyncio.TimeoutError(),
            ConnectionError("socket dropped"),
        ],
        ids=lambda e: type(e).__name__,
    )
    def test_transient_errors_retry(self, error):
        assert classify_error(error) is ErrorAction.RETRY_LOCAL

    @pytest.mark.parametrize(
        "error",
        [KeyError("ticker"), TypeError("bad type"), ValueError("bad value")],
        ids=lambda e: type(e).__name__,
    )
    def test_payload_errors_route_to_the_dlq(self, error):
        assert classify_error(error) is ErrorAction.ROUTE_TO_DLQ

    def test_a_real_validation_error_routes_to_the_dlq(self):
        with pytest.raises(ValidationError) as exc:
            FetchMetricsArgs(**DRIFTED_PAYLOAD)
        assert classify_error(exc.value) is ErrorAction.ROUTE_TO_DLQ

    @pytest.mark.parametrize(
        "error",
        [NotImplementedError("fatal"), ZeroDivisionError("fatal"), PermissionError("auth")],
        ids=lambda e: type(e).__name__,
    )
    def test_fatal_errors_are_dropped(self, error):
        assert classify_error(error) is ErrorAction.ROUTE_TO_DEAD


class TestTopology:
    def test_every_declared_stream_has_a_reader_or_is_append_only(self):
        assert "stream:tasks:retry" not in STREAMS

    def test_the_worker_and_recovery_groups_are_declared(self):
        assert STREAMS[PENDING_STREAM] == "cg_main_workers"
        assert STREAMS[DLQ_STREAM] == "cg_recovery_agents"
