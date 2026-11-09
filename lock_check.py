"""Live lock-renewal check against a real Redis.

tests/stub_redis.py compares lock tokens in Python, so the unit suite cannot verify
the Lua in engine/locks.py. These checks run the real scripts on a real server,
including the token comparison and real TTL expiry, and then prove that a task
slower than its lock TTL is executed exactly once.

Needs Redis running (docker compose up -d). Exits non-zero if any check fails.
"""
import asyncio
import time

from engine import locks, worker
from engine.locks import acquire_lock, renew_lock, run_while_locked, LockLost
from engine.redis_client import get_redis
from engine.state import initialize_state, get_state
from engine.worker import process_task
from plugins.registry import tool_registry
import plugins.financial.tools  # noqa: F401

TTL_MS = 300
GOOD = {"ticker": "AAPL", "fiscal_year": 2025}
results = []


def check(label: str, passed: bool, detail: str = "") -> None:
    results.append(passed)
    print(f"  [{'PASS' if passed else 'FAIL'}] {label}" + (f" -- {detail}" if detail else ""))


def entry(task_id: str) -> dict:
    import json
    return {
        "job_id": "job_live", "task_id": task_id, "task_type": "fetch_sec_metrics",
        "idempotency_key": "k_live", "payload": json.dumps(GOOD),
        "retry_count": "0", "timestamp": str(time.time()),
    }


async def main():
    r = await get_redis()

    # 1. Without renewal the lock really does lapse. This is the defect.
    tid = f"live_lapse_{int(time.time())}"
    await acquire_lock(r, tid, ttl_ms=TTL_MS)
    await asyncio.sleep(TTL_MS / 1000 * 2)
    check("an un-renewed lock expires on its own",
          await r.exists(f"lock:task:{tid}") == 0)

    # 2. The heartbeat keeps a lock alive past its TTL.
    tid = f"live_renew_{int(time.time())}"
    token = await acquire_lock(r, tid, ttl_ms=TTL_MS)

    async def slow_work():
        await asyncio.sleep(TTL_MS / 1000 * 4)
        return "finished"

    out = await run_while_locked(r, tid, token, slow_work(), ttl_ms=TTL_MS)
    still_held = await r.get(f"lock:task:{tid}") == token
    check("work 4x longer than the TTL finishes and keeps the lock",
          out == "finished" and still_held, f"result={out!r} held={still_held}")
    await r.delete(f"lock:task:{tid}")

    # 3. The Lua token check: a stale holder must not extend someone else's lock.
    #    This is the property the stub cannot verify.
    tid = f"live_steal_{int(time.time())}"
    stale_token = await acquire_lock(r, tid, ttl_ms=TTL_MS)
    await r.set(f"lock:task:{tid}", "token_of_the_new_holder", px=400)
    refused = await renew_lock(r, tid, stale_token, 60_000)
    pttl = await r.pttl(f"lock:task:{tid}")
    check("a stale token cannot renew", refused is False, f"returned {refused}")
    check("the new holder's deadline was not pushed out", pttl <= 400, f"pttl={pttl}ms")
    await r.delete(f"lock:task:{tid}")

    # 4. Losing the lock mid-flight cancels the work.
    tid = f"live_lost_{int(time.time())}"
    token = await acquire_lock(r, tid, ttl_ms=TTL_MS)
    reached_the_end = False

    async def work():
        nonlocal reached_the_end
        await asyncio.sleep(TTL_MS / 1000 * 4)
        reached_the_end = True

    await r.set(f"lock:task:{tid}", "token_of_the_new_holder", px=5000)
    raised = False
    try:
        await run_while_locked(r, tid, token, work(), ttl_ms=TTL_MS)
    except LockLost:
        raised = True
    check("a lost lock raises LockLost and cancels the work",
          raised and not reached_the_end, f"raised={raised} finished={reached_the_end}")
    await r.delete(f"lock:task:{tid}")

    # 5. The point of all of it: a task slower than its TTL runs exactly once.
    #    A second worker starts after the original lock would have lapsed.
    worker.LOCK_TTL_MS = TTL_MS
    executions = []
    real_execute = tool_registry.execute

    async def slow_tool(task_type, payload):
        executions.append(time.time())
        await asyncio.sleep(TTL_MS / 1000 * 5)
        return {"ok": True}

    tool_registry.execute = slow_tool
    try:
        tid = f"live_once_{int(time.time())}"
        await initialize_state(r, tid)

        first = asyncio.create_task(process_task(r, "worker_a", "1-0", entry(tid)))
        # Long after the TTL would have expired, had nothing renewed it.
        await asyncio.sleep(TTL_MS / 1000 * 2)
        lock_alive = await r.exists(f"lock:task:{tid}") == 1
        second = asyncio.create_task(process_task(r, "worker_b", "2-0", entry(tid)))
        await asyncio.gather(first, second)

        state = await get_state(r, tid)
        check("the lock was still held well past its TTL", lock_alive)
        check("a slow task is executed exactly once",
              len(executions) == 1, f"{len(executions)} execution(s)")
        check("the slow task completed", state.status.value == "COMPLETED",
              state.status.value)
    finally:
        tool_registry.execute = real_execute
        worker.LOCK_TTL_MS = 10_000

    print()
    ok = all(results)
    print("RESULT:", f"PASS - {len(results)}/{len(results)} checks"
          if ok else f"FAIL - {results.count(False)} of {len(results)} checks failed")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
