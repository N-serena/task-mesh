import asyncio
import contextlib
import json
import time

import httpx
import httpx2
import pytest
import redis.exceptions as redis_exc
from google.genai import errors as genai_errors
from pydantic import ValidationError

import engine.recovery as recovery
import plugins.financial.tools  # noqa: F401  (registers the financial tools)
from engine.classifier import ErrorAction, classify_error
import engine.queue as queue
import engine.worker as worker
from engine.models import DLQMessage, TaskMessage, TaskStatus
from engine.queue import (
    RETRY_ZSET,
    STREAMS,
    claim_due_retries,
    publish_dlq_message,
    publish_task,
    schedule_retry,
)
from engine.locks import LockLost, acquire_lock, renew_lock, run_while_locked
from engine.locks import RENEWALS_PER_TTL
from engine.worker import LOCK_TTL_MS
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


def task_message(task_id: str, payload: dict) -> TaskMessage:
    return TaskMessage(
        job_id="job_test",
        task_id=task_id,
        task_type="fetch_sec_metrics",
        idempotency_key="key_test",
        payload=payload,
    )


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


class TestStreamTrimming:
    """XACK clears the PEL but leaves the entry, so every publish must cap length."""

    async def test_task_publishes_request_a_length_cap(self, redis):
        await publish_task(redis, PENDING_STREAM, task_message("task_1", GOOD_PAYLOAD))

        stream, maxlen, approximate = redis.trim_requests[-1]
        assert stream == PENDING_STREAM
        assert maxlen == queue.TASK_STREAM_MAXLEN
        assert approximate is True

    async def test_dlq_publishes_request_a_tighter_cap(self, redis):
        message = DLQMessage.model_validate_json(
            dlq_entry("task_1", DRIFTED_PAYLOAD)["payload"]
        )
        await publish_dlq_message(redis, message)

        stream, maxlen, approximate = redis.trim_requests[-1]
        assert stream == DLQ_STREAM
        assert maxlen == queue.DLQ_STREAM_MAXLEN
        assert approximate is True

    async def test_dlq_entries_are_capped_lower_than_task_entries(self):
        """DLQ records carry a stack trace and schema, so they are much larger."""
        assert queue.DLQ_STREAM_MAXLEN < queue.TASK_STREAM_MAXLEN

    async def test_every_worker_and_recovery_write_is_capped(self, redis, monkeypatch):
        """Fails if a new code path writes to a stream without going through queue.py."""

        async def repair(dlq_msg):
            return REPAIRED_PAYLOAD

        monkeypatch.setattr(recovery, "repair_payload", repair)
        await initialize_state(redis, "task_heal")

        await process_task(redis, "worker_1", "1-0", stream_entry("task_heal", DRIFTED_PAYLOAD))
        await recovery.process_dlq_task(
            redis, "recovery_1", "2-0", redis.streams[DLQ_STREAM][0]
        )

        assert redis.trim_requests, "no stream writes were recorded"
        uncapped = [(s, m) for s, m, _ in redis.trim_requests if m is None]
        assert not uncapped, f"these writes bypassed the length cap: {uncapped}"

    async def test_a_failed_repair_requeue_is_also_capped(self, redis, monkeypatch):
        async def failing(dlq_msg):
            raise ValueError("wrong shape")

        monkeypatch.setattr(recovery, "repair_payload", failing)
        await initialize_state(redis, "task_bad")

        await recovery.process_dlq_task(
            redis, "recovery_1", "1-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        stream, maxlen, _ = redis.trim_requests[-1]
        assert stream == DLQ_STREAM
        assert maxlen == queue.DLQ_STREAM_MAXLEN


class TestStaleMessageSweeper:
    """A consumer that dies before XACK leaves its message in the PEL forever."""

    async def test_an_abandoned_task_is_reclaimed_and_completed(self, redis):
        await initialize_state(redis, "task_orphan")
        redis.strand(PENDING_STREAM, "9-0", stream_entry("task_orphan", GOOD_PAYLOAD))

        claimed = await worker.sweep_stale_tasks(redis, "worker_rescuer")

        assert claimed == 1
        assert (await get_state(redis, "task_orphan")).status == TaskStatus.COMPLETED

    async def test_a_reclaimed_task_is_acknowledged(self, redis):
        await initialize_state(redis, "task_orphan")
        redis.strand(PENDING_STREAM, "9-0", stream_entry("task_orphan", GOOD_PAYLOAD))

        await worker.sweep_stale_tasks(redis, "worker_rescuer")

        assert (PENDING_STREAM, "9-0") in redis.acked
        assert not redis.pending, "the PEL must be empty once the task is handled"

    async def test_nothing_to_reclaim_is_a_no_op(self, redis):
        assert await worker.sweep_stale_tasks(redis, "worker_rescuer") == 0

    async def test_the_claim_threshold_exceeds_the_task_lock_ttl(self, redis):
        """A slow-but-alive worker must still hold its lock when a claim happens."""
        await worker.sweep_stale_tasks(redis, "worker_rescuer")

        _, _, _, min_idle = redis.claim_calls[-1]
        assert min_idle == queue.STALE_MESSAGE_MS
        assert min_idle > LOCK_TTL_MS

    async def test_a_reclaimed_task_held_by_a_live_worker_stays_pending(self, redis):
        """The claimant backs off and leaves the message for the next sweep."""
        await initialize_state(redis, "task_orphan")
        redis.strand(PENDING_STREAM, "9-0", stream_entry("task_orphan", GOOD_PAYLOAD))
        redis.keys["lock:task:task_orphan"] = "held-by-the-original-worker"

        await worker.sweep_stale_tasks(redis, "worker_rescuer")

        assert (await get_state(redis, "task_orphan")).status == TaskStatus.PENDING
        assert (PENDING_STREAM, "9-0") not in redis.acked

    async def test_the_worker_loop_actually_sweeps(self, redis):
        """Guards the wiring, not just sweep_stale_tasks in isolation."""
        loop_task = asyncio.create_task(worker.worker_loop(redis, "worker_1"))
        await asyncio.sleep(0.05)
        loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop_task

        assert redis.claim_calls, "worker_loop must sweep the PEL"

    async def test_the_recovery_loop_actually_sweeps(self, redis):
        loop_task = asyncio.create_task(recovery.recovery_loop(redis, "recovery_1"))
        await asyncio.sleep(0.05)
        loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop_task

        assert redis.claim_calls, "recovery_loop must sweep the PEL"

    async def test_an_abandoned_dlq_message_is_reclaimed(self, redis, monkeypatch):
        async def repair(dlq_msg):
            return REPAIRED_PAYLOAD

        monkeypatch.setattr(recovery, "repair_payload", repair)
        await initialize_state(redis, "task_orphan")
        redis.strand(DLQ_STREAM, "9-0", dlq_entry("task_orphan", DRIFTED_PAYLOAD))

        claimed = await recovery.sweep_stale_dlq_messages(redis, "recovery_rescuer")

        assert claimed == 1
        assert len(redis.streams[PENDING_STREAM]) == 1


class TestUnreadableStreamEntries:
    """Without this, the sweeper would reclaim a malformed entry forever."""

    @pytest.mark.parametrize(
        ("entry", "label"),
        [
            ({"job_id": "j"}, "missing fields"),
            (
                {
                    "job_id": "j", "task_id": "t", "task_type": "fetch_sec_metrics",
                    "idempotency_key": "k", "payload": "not json",
                    "retry_count": "0", "timestamp": "0",
                },
                "payload is not json",
            ),
        ],
        ids=["missing-fields", "bad-json"],
    )
    async def test_an_unreadable_entry_is_acknowledged_not_retried(
        self, redis, entry, label
    ):
        await process_task(redis, "worker_1", "9-0", entry)

        assert (PENDING_STREAM, "9-0") in redis.acked, label

    async def test_an_unreadable_entry_does_not_survive_a_sweep(self, redis):
        redis.strand(PENDING_STREAM, "9-0", {"job_id": "incomplete"})

        await worker.sweep_stale_tasks(redis, "worker_rescuer")

        assert not redis.pending, "a malformed entry must not be reclaimed forever"


class TestLockRenewal:
    """A task slower than its lock TTL must not be handed to a second worker."""

    # Short enough that a test can outlive a TTL; the heartbeat fires every third.
    TTL_MS = 90

    async def slow_work(self, seconds: float = 0.25) -> str:
        await asyncio.sleep(seconds)
        return "finished"

    async def test_work_outlasting_the_ttl_still_finishes(self, redis):
        token = await acquire_lock(redis, "task_slow", ttl_ms=self.TTL_MS)

        result = await run_while_locked(
            redis, "task_slow", token, self.slow_work(), ttl_ms=self.TTL_MS
        )

        assert result == "finished"
        assert len(redis.renew_calls) >= 2, "the heartbeat must renew more than once"
        assert all(
            key == "lock:task:task_slow" and renewed == token and ttl == self.TTL_MS
            for key, renewed, ttl in redis.renew_calls
        )

    async def test_renewal_extends_only_the_holders_own_lock(self, redis):
        token = await acquire_lock(redis, "task_a", ttl_ms=self.TTL_MS)
        assert await renew_lock(redis, "task_a", token, self.TTL_MS) is True

        redis.steal_lock("task_a")
        assert await renew_lock(redis, "task_a", token, self.TTL_MS) is False

        assert await renew_lock(redis, "task_never_locked", token, self.TTL_MS) is False

    async def test_losing_the_lock_cancels_the_work(self, redis):
        token = await acquire_lock(redis, "task_slow", ttl_ms=self.TTL_MS)
        reached_the_end = False

        async def work():
            nonlocal reached_the_end
            await asyncio.sleep(0.25)
            reached_the_end = True

        redis.steal_lock("task_slow")

        with pytest.raises(LockLost):
            await run_while_locked(
                redis, "task_slow", token, work(), ttl_ms=self.TTL_MS
            )

        assert not reached_the_end, "work must stop once another worker holds the lock"

    async def test_an_error_in_the_work_is_not_masked_by_the_heartbeat(self, redis):
        token = await acquire_lock(redis, "task_slow", ttl_ms=self.TTL_MS)

        async def work():
            raise ValueError("the payload drifted")

        with pytest.raises(ValueError, match="drifted"):
            await run_while_locked(
                redis, "task_slow", token, work(), ttl_ms=self.TTL_MS
            )

    def test_the_heartbeat_fires_more_than_once_per_ttl(self):
        """One lost round trip to Redis must not cost the lock."""
        assert RENEWALS_PER_TTL > 1


class TestWorkerLockRenewal:
    TTL_MS = 90

    @pytest.fixture(autouse=True)
    def short_ttl(self, monkeypatch):
        monkeypatch.setattr(worker, "LOCK_TTL_MS", self.TTL_MS)

    @staticmethod
    def executing(monkeypatch, body):
        monkeypatch.setattr(tool_registry, "execute", body)

    async def test_a_slow_task_renews_its_lock_while_running(self, redis, monkeypatch):
        async def slow_tool(task_type, payload):
            await asyncio.sleep(0.25)
            return {"ok": True}

        self.executing(monkeypatch, slow_tool)
        await initialize_state(redis, "task_slow")

        await process_task(redis, "worker_1", "1-0", stream_entry("task_slow", GOOD_PAYLOAD))

        assert redis.renew_calls, "a task outliving its TTL must renew its lock"
        assert (await get_state(redis, "task_slow")).status == TaskStatus.COMPLETED

    async def test_a_stolen_lock_leaves_the_message_for_its_new_holder(
        self, redis, monkeypatch
    ):
        async def tool_that_loses_the_lock(task_type, payload):
            redis.steal_lock("task_slow")
            await asyncio.sleep(0.25)
            return {"ok": True}

        self.executing(monkeypatch, tool_that_loses_the_lock)
        await initialize_state(redis, "task_slow")

        await process_task(redis, "worker_1", "1-0", stream_entry("task_slow", GOOD_PAYLOAD))

        assert (PENDING_STREAM, "1-0") not in redis.acked, (
            "the new holder is responsible for acknowledging the message"
        )
        state = await get_state(redis, "task_slow")
        assert state.status not in (TaskStatus.DEAD, TaskStatus.COMPLETED), (
            f"a lost lock must not settle the task; it is {state.status.value}"
        )
        assert redis.keys["lock:task:task_slow"] == "token_of_another_worker", (
            "the new holder's lock must survive the old holder's release"
        )

    async def test_a_lost_lock_does_not_reach_the_dlq(self, redis, monkeypatch):
        """LockLost would otherwise classify as fatal and kill a running task."""
        async def tool_that_loses_the_lock(task_type, payload):
            redis.steal_lock("task_slow")
            await asyncio.sleep(0.25)
            return {"ok": True}

        self.executing(monkeypatch, tool_that_loses_the_lock)
        await initialize_state(redis, "task_slow")

        await process_task(redis, "worker_1", "1-0", stream_entry("task_slow", GOOD_PAYLOAD))

        assert DLQ_STREAM not in redis.streams


class TestRecoveryLockRenewal:
    """LLM_TIMEOUT_MS allows a repair longer than the DLQ lock TTL."""

    TTL_MS = 90

    @pytest.fixture(autouse=True)
    def short_ttl(self, monkeypatch):
        monkeypatch.setattr(recovery, "DLQ_LOCK_TTL_MS", self.TTL_MS)
        monkeypatch.setattr(recovery, "MAX_BACKOFF_SECONDS", 0)

    def test_the_llm_timeout_can_outlast_the_lock_ttl(self):
        """The reason the heartbeat is needed here, stated as a check."""
        assert recovery.LLM_TIMEOUT_MS > recovery.DLQ_LOCK_TTL_MS

    async def test_a_slow_repair_renews_its_lock(self, redis, monkeypatch):
        async def slow_repair(dlq_msg):
            await asyncio.sleep(0.25)
            return REPAIRED_PAYLOAD

        monkeypatch.setattr(recovery, "repair_payload", slow_repair)
        await initialize_state(redis, "task_bad")

        await recovery.process_dlq_task(
            redis, "recovery_1", "1-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        assert redis.renew_calls, "a slow repair must renew its DLQ lock"
        assert len(redis.streams[PENDING_STREAM]) == 1

    async def test_a_stolen_dlq_lock_spends_no_budget(self, redis, monkeypatch):
        async def repair_that_loses_the_lock(dlq_msg):
            redis.steal_lock("dlq_task_bad")
            await asyncio.sleep(0.25)
            return REPAIRED_PAYLOAD

        monkeypatch.setattr(recovery, "repair_payload", repair_that_loses_the_lock)
        await initialize_state(redis, "task_bad")

        await recovery.process_dlq_task(
            redis, "recovery_1", "1-0", dlq_entry("task_bad", DRIFTED_PAYLOAD)
        )

        assert (DLQ_STREAM, "1-0") not in redis.acked
        assert DLQ_STREAM not in redis.streams, "no re-queue: the holder still has it"
        assert PENDING_STREAM not in redis.streams, "an abandoned repair re-injects nothing"


class TestTransientTaskRetry:
    """RETRY_LOCAL used to discard the task: ACKed, never re-queued, stuck IN_PROGRESS."""

    TRANSIENT = redis_exc.ConnectionError("redis blipped mid-tool")

    @pytest.fixture(autouse=True)
    def flaky_tool(self, monkeypatch):
        """Every execution raises a transient error unless a test replaces this."""
        async def always_transient(task_type, payload):
            raise TestTransientTaskRetry.TRANSIENT

        monkeypatch.setattr(tool_registry, "execute", always_transient)

    @staticmethod
    def entry(task_id: str, retry_count: int = 0) -> dict:
        record = stream_entry(task_id, GOOD_PAYLOAD)
        record["retry_count"] = str(retry_count)
        return record

    @staticmethod
    def parked(redis) -> list[TaskMessage]:
        return [
            TaskMessage.model_validate_json(member)
            for member in redis.zsets.get(RETRY_ZSET, {})
        ]

    async def test_the_classifier_still_calls_this_transient(self):
        """The fix is pointless if the error routes elsewhere."""
        assert classify_error(self.TRANSIENT) == ErrorAction.RETRY_LOCAL

    async def test_a_transient_failure_parks_the_task_rather_than_dropping_it(self, redis):
        await initialize_state(redis, "task_blip")

        await process_task(redis, "worker_1", "1-0", self.entry("task_blip"))

        assert len(self.parked(redis)) == 1, "the task must survive a transient failure"
        assert (await get_state(redis, "task_blip")).status == TaskStatus.PENDING

    async def test_the_message_is_acknowledged_once_the_task_is_parked(self, redis):
        """The retry copy lives in the sorted set, so the stream entry is finished."""
        await initialize_state(redis, "task_blip")

        await process_task(redis, "worker_1", "1-0", self.entry("task_blip"))

        assert (PENDING_STREAM, "1-0") in redis.acked

    async def test_a_parked_task_carries_an_incremented_retry_count(self, redis):
        await initialize_state(redis, "task_blip")

        await process_task(redis, "worker_1", "1-0", self.entry("task_blip", retry_count=1))

        assert self.parked(redis)[0].retry_count == 2

    @pytest.mark.parametrize(
        ("retry_count", "expected_delay"), [(0, 2), (1, 4), (2, 8)]
    )
    async def test_the_backoff_grows_with_each_attempt(
        self, redis, retry_count, expected_delay
    ):
        await initialize_state(redis, "task_blip")

        await process_task(
            redis, "worker_1", "1-0", self.entry("task_blip", retry_count=retry_count)
        )

        ready_at = next(iter(redis.zsets[RETRY_ZSET].values()))
        assert ready_at - time.time() == pytest.approx(expected_delay, abs=1.0)

    async def test_the_backoff_is_capped(self, redis, monkeypatch):
        monkeypatch.setattr(worker, "MAX_RETRIES", 40)
        monkeypatch.setattr(worker, "MAX_RETRY_BACKOFF_SECONDS", 60)
        await initialize_state(redis, "task_blip")

        await process_task(redis, "worker_1", "1-0", self.entry("task_blip", retry_count=30))

        ready_at = next(iter(redis.zsets[RETRY_ZSET].values()))
        assert ready_at - time.time() == pytest.approx(60, abs=1.0)

    async def test_an_exhausted_retry_budget_marks_the_task_dead(self, redis):
        await initialize_state(redis, "task_blip")

        await process_task(
            redis, "worker_1", "1-0", self.entry("task_blip", retry_count=worker.MAX_RETRIES)
        )

        assert (await get_state(redis, "task_blip")).status == TaskStatus.DEAD
        assert not self.parked(redis), "a dead task must not also be parked"

    async def test_a_parked_task_is_not_requeued_before_it_is_due(self, redis):
        await schedule_retry(redis, task_message("task_blip", GOOD_PAYLOAD), 30)

        assert await worker.requeue_due_retries(redis, "worker_1") == 0
        assert len(self.parked(redis)) == 1, "it must stay parked until due"

    async def test_a_due_task_is_requeued_to_the_pending_stream(self, redis):
        await schedule_retry(redis, task_message("task_blip", GOOD_PAYLOAD), delay_seconds=0)

        assert await worker.requeue_due_retries(redis, "worker_1") == 1
        assert len(redis.streams[PENDING_STREAM]) == 1
        assert not self.parked(redis), "a re-queued task must leave the sorted set"

    async def test_a_claimed_retry_is_not_handed_out_twice(self, redis):
        """Two workers sweeping at once must not both re-queue the same task."""
        await schedule_retry(redis, task_message("task_blip", GOOD_PAYLOAD), delay_seconds=0)

        first = await claim_due_retries(redis)
        second = await claim_due_retries(redis)

        assert len(first) == 1
        assert second == []

    async def test_an_unreadable_parked_retry_is_dropped(self, redis):
        redis.zsets[RETRY_ZSET] = {"{not a task}": 0.0}

        assert await claim_due_retries(redis) == []
        assert not redis.zsets[RETRY_ZSET], "it must not be retried forever"

    async def test_the_worker_loop_actually_requeues_due_retries(self, redis):
        """Guards the wiring, not just requeue_due_retries in isolation."""
        await schedule_retry(redis, task_message("task_blip", GOOD_PAYLOAD), delay_seconds=0)

        loop_task = asyncio.create_task(worker.worker_loop(redis, "worker_1"))
        await asyncio.sleep(0.05)
        loop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await loop_task

        assert not self.parked(redis), "worker_loop must sweep due retries"

    async def test_a_task_that_blips_once_then_succeeds_completes(self, redis, monkeypatch):
        """The whole point: a transient failure costs a delay, not the task."""
        attempts = []

        async def fails_once(task_type, payload):
            attempts.append(payload)
            if len(attempts) == 1:
                raise TestTransientTaskRetry.TRANSIENT
            return {"ok": True}

        monkeypatch.setattr(tool_registry, "execute", fails_once)
        await initialize_state(redis, "task_blip")

        await process_task(redis, "worker_1", "1-0", self.entry("task_blip"))
        assert (await get_state(redis, "task_blip")).status == TaskStatus.PENDING

        # Fast-forward past the backoff.
        redis.zsets[RETRY_ZSET] = {m: 0.0 for m in redis.zsets[RETRY_ZSET]}
        assert await worker.requeue_due_retries(redis, "worker_1") == 1

        await process_task(redis, "worker_1", "2-0", redis.streams[PENDING_STREAM][-1])

        assert len(attempts) == 2
        assert (await get_state(redis, "task_blip")).status == TaskStatus.COMPLETED


class TestTopology:
    def test_every_declared_stream_has_a_reader_or_is_append_only(self):
        assert "stream:tasks:retry" not in STREAMS

    def test_the_worker_and_recovery_groups_are_declared(self):
        assert STREAMS[PENDING_STREAM] == "cg_main_workers"
        assert STREAMS[DLQ_STREAM] == "cg_recovery_agents"
