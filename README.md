# Task Mesh

**An autonomous distributed task processing engine with an LLM-powered Dead-Letter Queue.**

---

## The problem

In asynchronous distributed pipelines, worker tasks fail when inputs or tool parameters
suffer from **schema drift**, malformed payloads, or bad data types. Standard systems
retry endlessly or dump the task in a Dead-Letter Queue for manual triage, where it sits
indefinitely until a human intervenes.

Standard retry mechanisms (exponential backoff) only solve transient network glitches.
They cannot resolve payload-level schema mismatches, missing required arguments, or tool
contract violations.

## The approach

Task Mesh pairs conventional distributed-systems primitives — Redis Streams consumer
groups, atomic state hashes, distributed locks — with an **autonomous Recovery Agent**
that repairs failed payloads instead of parking them.

When a task fails schema validation, the Recovery Agent receives the original payload, the
error trace, and the tool's JSON Schema. It asks an LLM to synthesize a corrected payload,
then **validates that correction against the real Pydantic model before re-injecting it**.

That validation gate is the core safety property: the LLM's output is never executed
blindly. A bad repair is rejected, not executed — so model quality affects the *success
rate*, never *correctness*.

---

## Architecture

<!-- Drop your architecture diagram at docs/architecture.png -->
![Task Mesh architecture](docs/architecture.png)

Redis does triple duty: **Streams** as the message broker, **Hashes** as the central state
machine, and **Lua** for distributed locking.

### Stream topology

| Stream | Consumer group | Who reads it | Purpose |
|---|---|---|---|
| `stream:tasks:pending` | `cg_main_workers` | All StreamWorkers | Fresh and repaired tasks awaiting execution |
| `stream:tasks:dlq` | `cg_recovery_agents` | All RecoveryAgents | Tasks flagged for LLM schema repair |
| `stream:tasks:completed` | *(none)* | Reporting / UI | Append-only audit log of successes |
| `stream:tasks:dead` | *(none)* | Alerting | Append-only log of fatal failures |

Transient failures are held in a sorted set, `zset:tasks:retry`, scored by the time each
task comes due. Streams deliver as soon as an entry is added, so a delayed retry needs a
structure consumers poll by due time. Workers check it every 2 seconds.

### Components

| Component | Responsibility |
|---|---|
| `IngestionAPI` | Accepts jobs, generates IDs, initializes state |
| `ToolRegistry` | Maps tool names to their function **and** Pydantic contract |
| `StreamWorker` | Pulls tasks, locks them, validates, executes |
| `ErrorClassifier` | Routes a failure: retry, repair, or kill |
| `RecoveryAgent` | Diagnoses, repairs, re-validates, re-injects |

---

## How it works

### The happy path

1. Client submits a job; the API writes initial state to a Redis Hash and publishes to
   `stream:tasks:pending`.
2. A worker claims it via `XREADGROUP` and acquires a distributed lock (10s TTL),
   renewing it every few seconds for as long as the task runs.
3. The payload is validated against the tool's Pydantic schema, then executed.
4. The worker marks the task `COMPLETED`, `XACK`s, and releases the lock.

### The chaos path — self-healing

1. A payload arrives with schema drift: `{"company_ticker": "msft", "year": "25"}`
   instead of `{"ticker": "MSFT", "fiscal_year": 2025}`.
2. Pydantic raises `ValidationError`. The classifier recognizes this as a *repairable*
   failure and routes it to `stream:tasks:dlq` with the stack trace and target schema.
3. The Recovery Agent prompts an LLM with the schema, the broken payload, and the error.
4. **The deterministic gate:** the agent instantiates the real Pydantic model with the
   LLM's output. A hallucinated shape raises and the repair is rejected.
5. On success, the repaired task is re-injected into `stream:tasks:pending` and executes
   normally.
6. After 2 failed recovery attempts, the task is marked `DEAD`.

### State machine

| From | To | Trigger |
|---|---|---|
| *(none)* | `PENDING` | Client submits a job |
| `PENDING` | `IN_PROGRESS` | Worker pulls the task and acquires the lock |
| `IN_PROGRESS` | `COMPLETED` | Tool executes successfully |
| `IN_PROGRESS` | `IN_DLQ` | Schema failure — repairable |
| `IN_PROGRESS` | `DEAD` | Fatal error (auth, 404) |
| `IN_DLQ` | `RECOVERING` | Recovery Agent claims it |
| `RECOVERING` | `PENDING` | LLM repair passed the validation gate |
| `RECOVERING` | `DEAD` | Repair failed, or recovery budget exhausted |

---

## Quick start

**Requirements:** Python 3.11+, Docker, and a `GEMINI_API_KEY` (only needed for the
recovery agent).

```bash
pip install -r requirements.txt
```

Create a `.env`:

```
REDIS_URL=redis://localhost:6379/0
GEMINI_API_KEY=your_key_here
```

Start Redis:

```bash
docker compose up -d redis
```

Then run each of these in its own terminal:

```bash
python -m uvicorn api.main:app --port 8000
```

```bash
python run_worker.py
```

```bash
python run_recovery.py
```

### Run the tests

The suite stubs Redis and the LLM, so it needs no Docker and no API key:

```bash
python -m pytest
```

### Try it

Run the chaos test, which submits one valid and one malformed payload concurrently, prints
every state transition, and exits non-zero if either fails to complete:

```bash
python chaos_injector.py
```

Expected: the happy path reaches `COMPLETED` directly; the chaos path walks
`IN_DLQ → RECOVERING → PENDING → COMPLETED` after the agent repairs it.

The locking rules depend on Lua that the stubbed suite cannot execute, so they have
their own live check:

```bash
python lock_check.py
```

```bash
python retry_check.py
```

To submit a job by hand, the API is also browsable at `http://localhost:8000/docs`.

---

## Project structure

The execution engine is completely agnostic to the financial domain logic.

```
task-mesh/
├── docker-compose.yml      # Redis
├── chaos_injector.py       # End-to-end test: happy + malformed payloads
├── lock_check.py           # Live check: the Lua lock scripts and renewal
├── retry_check.py          # Live check: the Lua retry scheduling
│
├── api/
│   └── main.py             # POST /jobs, GET /jobs/{task_id}
│
├── engine/                 # Domain-agnostic core
│   ├── redis_client.py     # Connection pool
│   ├── models.py           # TaskMessage, TaskStateRecord, DLQMessage
│   ├── state.py            # Redis Hash state CRUD
│   ├── queue.py            # Stream topology and publishing
│   ├── locks.py            # Distributed lock (SET NX PX + Lua release)
│   ├── classifier.py       # Transient vs. repairable vs. fatal
│   ├── worker.py           # Main consumer loop
│   └── recovery.py         # LLM recovery agent
│
└── plugins/                # Business logic
    ├── registry.py         # tool name -> (function, Pydantic schema)
    └── financial/
        ├── models.py       # FetchMetricsArgs, ComputeDebtRatioArgs
        └── tools.py        # Tool implementations
```

Adding a domain means adding a `plugins/<domain>/` package. The engine never imports it.

---

## Design notes

**Why Redis for everything?** Streams replace Kafka/RabbitMQ, Hashes replace Postgres for
state, and Lua gives us atomic lock release. One dependency instead of three keeps the
infrastructure footprint small for an MVP.

**Why the lock, not the PEL, prevents double execution.** `XAUTOCLAIM` reassigns a
message on an idle timer, which cannot distinguish a dead worker from a slow one. The
claimant therefore has to ask for the lock, and a live worker keeps its lock by
renewing it. A long task does get its message reassigned every 15s while it runs, but
each claimant backs off, and the original worker's `XACK` clears the entry when it
finishes.

**Why Pydantic field descriptions are written for an LLM.** The `Field(description=...)`
text in `plugins/financial/models.py` is exported via `model_json_schema()` and fed
directly to the recovery agent's prompt. Those descriptions are prompt engineering.

**Why a hosted LLM over a local one.** Local inference was the original plan, but running a
model server on the dev machine added ops overhead for no gain on public financial
identifiers. Worth revisiting if the pipeline ever carries sensitive data.

---

## Status

Verified end-to-end against a real Redis and a live model: a drifted payload
(`{"company_ticker": "msft", "year": "25"}`) walks
`IN_PROGRESS -> IN_DLQ -> RECOVERING -> COMPLETED` and executes with the repaired
arguments, in roughly two seconds.

The recovery budget is split: repairs the model actually produced are capped at 2, while
attempts lost to the model being unreachable get their own budget of 5 with exponential
backoff. An outage therefore cannot consume a repairable task's remediation attempts.

Streams are length-capped on every publish, so they cannot grow without bound. All stream
writes go through `engine/queue.py`, which is where the cap lives.

Workers and recovery agents sweep their consumer group's Pending Entries List every 10
seconds, claiming anything idle for more than 15 seconds. A consumer that dies between
`XREADGROUP` and `XACK` therefore has its work picked up by a live one.

Both hold their lock open with a heartbeat for as long as the work takes, so the lock
TTL bounds how long a crashed holder blocks a task and not how long a task may run. A
holder that loses its lock anyway has its work cancelled and leaves the message
pending for the new holder. This matters most in the recovery agent, where a model
call is allowed 30s against a 15s lock.

A tool that fails transiently no longer loses its task. The task is parked in
`zset:tasks:retry` with an exponential backoff (2s, 4s, 8s, capped at 60s) and re-queued by
whichever worker sweeps next, so a worker that dies while tasks are parked strands none of
them. After `MAX_RETRIES` it is marked `DEAD`. Claiming due tasks is a single Lua script,
because two workers sweeping at the same moment would otherwise both re-queue the same task.

Known gaps, in rough priority order:

- A worker whose event loop stalls past its TTL still loses its lock, and the heartbeat
  cannot renew what it cannot reach. Cancellation keeps that from double-executing,
  but a task stalled mid-side-effect has already done part of its work;
  `idempotency_key` enforcement is what would make that safe.
- `idempotency_key` is computed and carried but not yet enforced for deduplication.
- `stream:tasks:completed` and `stream:tasks:dead` are declared but not yet written to.
- Logging is `print()`-based; no structured, task-correlated logs.
- The test suite stubs Redis and the LLM, so real stream semantics (`XREADGROUP`, the
  Pending Entries List) and the live LLM call are covered only by running
  `chaos_injector.py`, `lock_check.py` and `retry_check.py` against a real Redis.
- The two live checks duplicate some setup. If more accumulate they are worth folding
  into one suite that skips itself when Redis is unreachable.
