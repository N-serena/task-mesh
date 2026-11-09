import httpx
import sys
import asyncio

API_URL = "http://localhost:8000"

# Give a task this long to reach a terminal state before calling it stalled.
MAX_WAIT_SECONDS = 60

TERMINAL_STATES = ("COMPLETED", "DEAD")

# A task parked in one of these waits on a consumer that is not running, and which
# state it stalls in says which process is missing.
STALL_HINTS = {
    "PENDING": ("nothing is consuming stream:tasks:pending", "python run_worker.py"),
    "IN_DLQ": ("nothing is consuming stream:tasks:dlq", "python run_recovery.py"),
}

# Shown when the API cannot be reached. This script drives the engine over HTTP
# only, so an unreachable API says nothing about Redis or the consumers.
STARTUP_HELP = f"""
Cannot reach the API at {API_URL}.

This script drives the engine over HTTP, so the stack has to be up first. Run each
of these in its own terminal:

    docker compose up -d redis
    python -m uvicorn api.main:app --port 8000
    python run_worker.py
    python run_recovery.py
"""


async def api_is_reachable(client: httpx.AsyncClient) -> bool:
    """Any HTTP answer proves the API is listening; the status itself is irrelevant."""
    try:
        await client.get(f"{API_URL}/docs")
    except httpx.RequestError:
        print(STARTUP_HELP)
        return False
    return True

# The Perfect Payload
HAPPY_PAYLOAD = {
    "task_type": "fetch_sec_metrics",
    "payload": {
        "ticker": "AAPL",
        "fiscal_year": 2025
    }
}

# The Poison Payload (Wrong keys, wrong types, bad casing)
CHAOS_PAYLOAD = {
    "task_type": "fetch_sec_metrics",
    "payload": {
        "company_ticker": "msft",
        "year": "25"
    }
}

async def track_job(client: httpx.AsyncClient, name: str, payload: dict) -> str | None:
    """Submits a job and tracks its state machine transitions.

    Returns the terminal status, or None if the task stalled or ingestion failed.
    """
    print(f"\n--- Submitting {name} ---")
    try:
        response = await client.post(f"{API_URL}/jobs", json=payload)
    except httpx.RequestError as e:
        print(f"[{name}] Ingestion FAILED: {API_URL} is unreachable "
              f"({type(e).__name__}).")
        return None

    if response.status_code != 200:
        print(f"[{name}] Ingestion FAILED: HTTP {response.status_code} {response.text}")
        return None

    task_id = response.json()["task_id"]
    print(f"[{name}] Ingested. Task ID: {task_id}")

    # Poll the status
    previous_status = None
    deadline = asyncio.get_running_loop().time() + MAX_WAIT_SECONDS

    while asyncio.get_running_loop().time() < deadline:
        try:
            status_res = await client.get(f"{API_URL}/jobs/{task_id}")
        except httpx.RequestError as e:
            print(f"[{name}] Lost contact with the API mid-run "
                  f"({type(e).__name__}). Did it stop?")
            return None

        if status_res.status_code != 200:
            print(f"[{name}] Status check FAILED: HTTP {status_res.status_code} "
                  f"{status_res.text}")
            return None

        current_status = status_res.json()["status"]

        if current_status != previous_status:
            print(f"[{name}] State Transition: {previous_status} -> {current_status}")
            previous_status = current_status

        if current_status in TERMINAL_STATES:
            print(f"[{name}] Reached terminal state.")
            return current_status

        await asyncio.sleep(1)

    print(f"[{name}] STALLED in '{previous_status}' after {MAX_WAIT_SECONDS}s.")
    if previous_status in STALL_HINTS:
        reason, command = STALL_HINTS[previous_status]
        print(f"[{name}] Probably {reason}. Start it with:")
        print(f"[{name}]     {command}")
    return None

async def main() -> int:
    async with httpx.AsyncClient(timeout=10.0) as client:
        if not await api_is_reachable(client):
            return 1

        # Launch both tasks concurrently
        happy, chaos = await asyncio.gather(
            track_job(client, "Happy Path", HAPPY_PAYLOAD),
            track_job(client, "Chaos Path", CHAOS_PAYLOAD)
        )

    print("\n=== Verdict ===")
    failures = 0
    for name, status in (("Happy Path", happy), ("Chaos Path", chaos)):
        ok = status == "COMPLETED"
        failures += not ok
        print(f"  {'PASS' if ok else 'FAIL'}  {name}: {status or 'no terminal state'}")

    return 1 if failures else 0

if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
