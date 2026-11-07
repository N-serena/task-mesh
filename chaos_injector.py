import httpx
import sys
import asyncio

API_URL = "http://localhost:8000"

# Give a task this long to reach a terminal state before calling it stalled.
MAX_WAIT_SECONDS = 60

TERMINAL_STATES = ("COMPLETED", "DEAD")

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
    response = await client.post(f"{API_URL}/jobs", json=payload)

    if response.status_code != 200:
        print(f"[{name}] Ingestion FAILED: HTTP {response.status_code} {response.text}")
        return None

    task_id = response.json()["task_id"]
    print(f"[{name}] Ingested. Task ID: {task_id}")

    # Poll the status
    previous_status = None
    deadline = asyncio.get_running_loop().time() + MAX_WAIT_SECONDS

    while asyncio.get_running_loop().time() < deadline:
        status_res = await client.get(f"{API_URL}/jobs/{task_id}")
        current_status = status_res.json()["status"]

        if current_status != previous_status:
            print(f"[{name}] State Transition: {previous_status} -> {current_status}")
            previous_status = current_status

        if current_status in TERMINAL_STATES:
            print(f"[{name}] Reached terminal state.")
            return current_status

        await asyncio.sleep(1)

    print(f"[{name}] STALLED in '{previous_status}' after {MAX_WAIT_SECONDS}s.")
    return None

async def main() -> int:
    async with httpx.AsyncClient(timeout=10.0) as client:
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
