import asyncio
import json

import httpx
import httpx2
import pytest
import redis.exceptions as redis_exc
from google.genai import errors as genai_errors
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


def dlq_entry(
    task_id: str,
    payload: dict,
    recovery_attempts: int = 0,
    transient_attempts: int = 0,
) -> dict:
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
        transient_attempts=transient_attempts,
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
            # google-genai transports over httpx2, a separate package whose
            # exceptions share names with httpx but not class identity.
            httpx2.ConnectTimeout("api slow"),
            httpx2.ReadTimeout("api slow"),
            httpx2.ConnectError("dns failure"),
        ],
        ids=lambda e: f"{type(e).__module__}.{type(e).__name__}",
    )
    def test_transient_errors_retry(self, error):
        assert classify_error(error) is ErrorAction.RETRY_LOCAL

    def test_httpx_and_httpx2_are_distinct_hierarchies(self):
        """The reason both packages must be listed; a regression guard if they merge."""
        assert httpx.TimeoutException is not httpx2.TimeoutException
        assert not issubclass(httpx2.TimeoutException, httpx.TimeoutException)

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

    @pytest.mark.parametrize(
        ("code", "expected"),
        [
            (503, ErrorAction.RETRY_LOCAL),    # model at capacity
            (504, ErrorAction.RETRY_LOCAL),    # deadline expired
            (429, ErrorAction.RETRY_LOCAL),    # rate limited
            (404, ErrorAction.ROUTE_TO_DEAD),  # model retired
            (400, ErrorAction.ROUTE_TO_DEAD),  # malformed request
            (403, ErrorAction.ROUTE_TO_DEAD),  # bad credentials
        ],
        ids=lambda v: str(v),
    )
    def test_provider_errors_route_by_status_code(self, code, expected):
        cls = genai_errors.ServerError if code >= 500 else genai_errors.ClientError
        error = cls(code, {"error": {"message": "x"}})
        assert classify_error(error) is expected


class TestTransientFailureBudget:
    """A model outage must not consume a repairable task's remediation attempts."""

    @pytest.fixture(autouse=True)
    def no_backoff(self, monkeypatch):
        monkeypatch.setattr(recovery, "MAX_BACKOFF_SECONDS", 0)

    @staticmethod
    def fail_with(monkeypatch, error: Exception):
        async def failing_repair(dlq_msg):
            raise error

        monkeypatch.setattr(recovery, "repair_payload", failing_repair)

    @staticmethod
    def requeued(redis) -> DLQMessage:
        return DLQMessage.model_validate_json(redis.streams[DLQ_STREAM][-1]["payload"])

    async def test_an_unreachable_model_spares_the_recovery_budget(self, redis, monkeypatch):
        self.fail_with(monkeypatch, genai_errors.ServerError(503, {"error": {"message": "busy"}}))
        await initialize_state(redis, "task_bad")

        await recovery.process_dlq_task(
            redis, "recovery_1", "1-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        message = self.requeued(redis)
        assert message.transient_attempts == 1
        assert message.recovery_attempts == 0

    async def test_a_bad_repair_spends_the_recovery_budget(self, redis, monkeypatch):
        self.fail_with(monkeypatch, ValueError("the model returned the wrong shape"))
        await initialize_state(redis, "task_bad")

        await recovery.process_dlq_task(
            redis, "recovery_1", "1-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        message = self.requeued(redis)
        assert message.recovery_attempts == 1
        assert message.transient_attempts == 0

    async def test_a_task_survives_an_outage_longer_than_the_repair_budget(
        self, redis, monkeypatch
    ):
        """Three 503s would previously have killed the task. The budget is 2."""
        self.fail_with(monkeypatch, genai_errors.ServerError(503, {"error": {"message": "busy"}}))
        await initialize_state(redis, "task_bad")

        entry = dlq_entry("task_bad", DRIFTED_PAYLOAD)
        for _ in range(3):
            await recovery.process_dlq_task(redis, "recovery_1", "1-0", entry)
            entry = {"payload": self.requeued(redis).model_dump_json()}

        assert self.requeued(redis).transient_attempts == 3
        assert (await get_state(redis, "task_bad")).status != TaskStatus.DEAD

    async def test_a_sustained_outage_still_terminates(self, redis):
        await initialize_state(redis, "task_bad")

        await recovery.process_dlq_task(
            redis,
            "recovery_1",
            "1-0",
            dlq_entry("task_bad", DRIFTED_PAYLOAD, transient_attempts=5),
        )

        assert (await get_state(redis, "task_bad")).status == TaskStatus.DEAD

    async def test_repair_still_works_after_a_transient_blip(self, redis, monkeypatch):
        """A task that blipped keeps its full repair budget and completes."""
        self.fail_with(monkeypatch, genai_errors.ServerError(503, {"error": {"message": "busy"}}))
        await initialize_state(redis, "task_bad")
        await recovery.process_dlq_task(
            redis, "recovery_1", "1-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        async def working_repair(dlq_msg):
            return REPAIRED_PAYLOAD

        monkeypatch.setattr(recovery, "repair_payload", working_repair)
        await recovery.process_dlq_task(
            redis, "recovery_1", "2-0", {"payload": self.requeued(redis).model_dump_json()}
        )

        assert len(redis.streams[PENDING_STREAM]) == 1
        reinjected = redis.streams[PENDING_STREAM][0]
        await process_task(redis, "worker_1", "3-0", reinjected)
        assert (await get_state(redis, "task_bad")).status == TaskStatus.COMPLETED


class TestRecoveryModuleImport:
    def test_import_needs_no_credentials_and_builds_no_client(self, monkeypatch):
        """The agent module must import in CI, where no API key is present."""
        import importlib

        monkeypatch.delenv("GEMINI_API_KEY", raising=False)
        monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

        reloaded = importlib.reload(recovery)
        assert reloaded._client is None, "the client must be built on first use"


class TestTopology:
    def test_every_declared_stream_has_a_reader_or_is_append_only(self):
        assert "stream:tasks:retry" not in STREAMS

    def test_the_worker_and_recovery_groups_are_declared(self):
        assert STREAMS[PENDING_STREAM] == "cg_main_workers"
        assert STREAMS[DLQ_STREAM] == "cg_recovery_agents"
