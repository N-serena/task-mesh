import httpx
import time
import asyncio

API_URL = "http://localhost:8000"

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

async def track_job(client: httpx.AsyncClient, name: str, payload: dict):
    """Submits a job and tracks its state machine transitions."""
    print(f"\n--- Submitting {name} ---")
    response = await client.post(f"{API_URL}/jobs", json=payload)
    task_id = response.json()["task_id"]
    print(f"[{name}] Ingested. Task ID: {task_id}")

    # Poll the status
    previous_status = None
    while True:
        status_res = await client.get(f"{API_URL}/jobs/{task_id}")
        current_status = status_res.json()["status"]
        
        if current_status != previous_status:
            print(f"[{name}] State Transition: {previous_status} -> {current_status}")
            previous_status = current_status
            
        if current_status in ["COMPLETED", "DEAD"]:
            print(f"[{name}] Reached terminal state.")
            break
            
        await asyncio.sleep(1)

async def main():
    async with httpx.AsyncClient() as client:
        # Launch both tasks concurrently
        await asyncio.gather(
            track_job(client, "Happy Path", HAPPY_PAYLOAD),
            track_job(client, "Chaos Path", CHAOS_PAYLOAD)
        )

if __name__ == "__main__":
    asyncio.run(main())
