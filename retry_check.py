"""Live retry-scheduling check against a real Redis.

tests/stub_redis.py reimplements the retry sorted-set script in Python, so the unit
suite cannot verify the Lua in engine/queue.py. These checks run it on a real
server, then drive a worker through a transient failure end to end.

Needs Redis running (docker compose up -d redis). Exits non-zero if any check fails.
"""
import asyncio
import time

from engine import queue, worker
from engine.models import TaskMessage, TaskStatus
from engine.queue import claim_due_retries, publish_task, schedule_retry
from engine.redis_client import get_redis
from engine.state import get_state, initialize_state
from plugins.registry import tool_registry
import plugins.financial.tools  # noqa: F401

# A key of its own, so a check never pops tasks a running worker parked.
queue.RETRY_ZSET = f"zset:tasks:retry:check_{int(time.time())}"

GOOD = {"ticker": "AAPL", "fiscal_year": 2025}
results = []


def check(label: str, passed: bool, detail: str = "") -> None:
    results.append(passed)
    print(f"  [{'PASS' if passed else 'FAIL'}] {label}" + (f" -- {detail}" if detail else ""))


def task(task_id: str, retry_count: int = 0) -> TaskMessage:
    return TaskMessage(
        job_id="job_check", task_id=task_id, task_type="fetch_sec_metrics",
        idempotency_key="k_check", payload=GOOD, retry_count=retry_count,
    )


async def main():
    r = await get_redis()
    stamp = int(time.time())

    # 1. A task parked with a delay must not come back early.
    await schedule_retry(r, task(f"check_early_{stamp}"), delay_seconds=30)
    due = await claim_due_retries(r)
    check("a task parked for 30s is not claimed now", due == [],
          f"claimed {len(due)}")
    check("it is still parked", await r.zcard(queue.RETRY_ZSET) == 1)
    await r.delete(queue.RETRY_ZSET)

    # 2. A due task is claimed and removed in the same step.
    await schedule_retry(r, task(f"check_due_{stamp}"), delay_seconds=0)
    due = await claim_due_retries(r)
    check("a due task is claimed", len(due) == 1, f"claimed {len(due)}")
    check("claiming removes it from the sorted set",
          await r.zcard(queue.RETRY_ZSET) == 0,
          f"{await r.zcard(queue.RETRY_ZSET)} left")

    # 3. The reason the script is Lua: concurrent sweeps must not both get it.
    await schedule_retry(r, task(f"check_race_{stamp}"), delay_seconds=0)
    claims = await asyncio.gather(*(claim_due_retries(r) for _ in range(8)))
    total = sum(len(c) for c in claims)
    check("eight concurrent sweeps claim it exactly once", total == 1,
          f"claimed {total} times")
    await r.delete(queue.RETRY_ZSET)

    # 4. End to end against real Redis: a transient failure costs a delay, not
    #    the task. Driven through process_task directly, so a worker that happens
    #    to be running on this machine cannot claim the task first.
    executions = []
    real_execute = tool_registry.execute

    async def fails_once(task_type, payload):
        executions.append(time.time())
        if len(executions) == 1:
            raise ConnectionError("simulated transient failure")
        return {"ok": True}

    def flat(message):
        import json
        return {
            "job_id": message.job_id, "task_id": message.task_id,
            "task_type": message.task_type,
            "idempotency_key": message.idempotency_key,
            "payload": json.dumps(message.payload),
            "retry_count": str(message.retry_count),
            "timestamp": str(message.timestamp),
        }

    tool_registry.execute = fails_once
    try:
        tid = f"check_blip_{stamp}"
        await initialize_state(r, tid)

        await worker.process_task(r, "worker_retry_check", "1-0", flat(task(tid)))

        state = await get_state(r, tid)
        check("a transient failure leaves the task parked, not lost",
              await r.zcard(queue.RETRY_ZSET) == 1 and state.status == TaskStatus.PENDING,
              f"parked={await r.zcard(queue.RETRY_ZSET)} state={state.status.value}")

        # Real server-side scoring decides when it comes due.
        started = time.time()
        due = []
        while time.time() - started < 20:
            due = await claim_due_retries(r)
            if due:
                break
            await asyncio.sleep(0.2)
        waited = time.time() - started
        check("the backoff really delayed it", bool(due) and waited >= 2,
              f"became due after {waited:.1f}s")

        if due:
            retried = due[0]
            check("the parked copy carries retry_count 1", retried.retry_count == 1,
                  str(retried.retry_count))
            await worker.process_task(r, "worker_retry_check", "2-0", flat(retried))

        state = await get_state(r, tid)
        check("the retry completed the task", state.status == TaskStatus.COMPLETED,
              state.status.value)
        check("it executed twice: the failure and the retry", len(executions) == 2,
              f"{len(executions)} execution(s)")
        check("nothing is left parked", await r.zcard(queue.RETRY_ZSET) == 0)
    finally:
        tool_registry.execute = real_execute
        await r.delete(queue.RETRY_ZSET)

    print()
    ok = all(results)
    print("RESULT:", f"PASS - {len(results)}/{len(results)} checks" if ok
          else f"FAIL - {results.count(False)} of {len(results)} checks failed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
