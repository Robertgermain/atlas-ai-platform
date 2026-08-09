# Atlas Technical Design

## Status

Local foundation decisions are recorded as they are validated. The comprehensive local-to-AWS system architecture remains incomplete until working local components exist and the Visio diagrams are reviewed.

## Validated local decisions

### Application package and API entrypoint

- Python 3.12 is the only supported runtime (`requires-python = ">=3.12,<3.13"`), managed with `uv` and a committed lockfile.
- Application code lives under `src/atlas` and is installed as the `atlas` package.
- The FastAPI application entrypoint is `atlas.main:app`.
- `GET /health` is process liveness and does not open database connections.
- `GET /ready` lazily checks PostgreSQL connectivity; SQLAlchemy database errors return `503 {"status":"not_ready"}` without exposing credentials, while unexpected programming errors propagate.
- Quality gates are Ruff format, Ruff lint, mypy (strict, `src` and `tests`), and Pytest.
- Ruff owns formatting and import sorting; black and isort are not dependencies.
- Runtime dependencies (FastAPI, Uvicorn, SQLAlchemy, psycopg, Alembic, pydantic-settings, langgraph, langgraph-checkpoint-postgres, langchain-core, langchain-openai, langchain-anthropic) are separated from development dependencies (Pytest, httpx2, Ruff, mypy).

### Continuous integration

- One GitHub Actions workflow (`.github/workflows/ci.yml`) runs on pull requests and pushes to `main`.
- The workflow uses `permissions: contents: read` only.
- Actions are pinned to full commit SHAs (`actions/checkout`, `astral-sh/setup-uv`) with version comments.
- `setup-uv` is configured with `version: "0.11.8"`, `python-version: "3.12"`, and `enable-cache: true`.
- Dependencies install with `uv sync --frozen`.
- CI runs the same local gates: `ruff format --check .`, `ruff check .`, `mypy src tests`, and `pytest`.
- CI provides a Postgres 16 service and sets `ATLAS_DATABASE_URL` to the dedicated `atlas_test` database; Pytest owns empty-schema reset and Alembic upgrade.

### ResearchJob domain model

- Domain code lives under `atlas.domain` and depends only on the Python standard library.
- `ResearchJob` is a slotted entity with read-only properties; callers change lifecycle state through `start()`, `complete(result)`, and `fail(reason)`.
- Construction always creates `PENDING` jobs with stripped `id` and `question`; result and failure fields start empty.
- Allowed transitions are `PENDING → RUNNING → COMPLETED` and `PENDING → RUNNING → FAILED`.
- Terminal states (`COMPLETED`, `FAILED`) reject further lifecycle transitions.
- Optional timezone-aware timestamps make creation and transitions deterministic; omitted timestamps default to current UTC. Supplied timezone-aware timestamps are normalized to UTC.
- Timestamps must be timezone-aware and must not move earlier than the job's `updated_at`.
- Domain errors are `InvalidResearchJobError` for field/timestamp invariants and `InvalidTransitionError` for illegal status changes.
- Durable state is rebuilt with `ResearchJob.reconstitute(...)`, which validates status/field consistency without applying transitions.

### PostgreSQL persistence

- Local Postgres 16 runs via Docker Compose; host port `5433` maps to container `5432`; databases `atlas` (app) and `atlas_test` (tests).
- Settings load `ATLAS_DATABASE_URL` through `pydantic-settings`; engines and sessions are created lazily.
- ORM model `research_jobs` uses `TIMESTAMPTZ` columns and CHECK constraints for status/field combinations as defense in depth.
- Nullable `idempotency_key` / `request_fingerprint` columns support API idempotency; a CHECK requires both null or both set; a unique constraint applies to non-null keys (PostgreSQL treats NULLs as distinct).
- `SqlAlchemyResearchJobRepository` implements the application `ResearchJobRepository` Protocol (`add` with required idempotency metadata, `get`, `get_by_idempotency_key` → `ResearchJobIdempotencyRecord`, `save`) with caller-owned `session_scope` transactions.
- Duplicate primary keys raise `ResearchJobAlreadyExistsError`; duplicate idempotency keys raise `IdempotencyKeyConflictError`; unrelated integrity failures are re-raised unchanged. `session_scope` performs rollback when exceptions escape.
- Job ids are capped at 128 characters in the domain (`MAX_RESEARCH_JOB_ID_LENGTH`), matching the persistence column.
- Status-specific timestamp orderings are enforced in domain reconstitution and mirrored by database CHECK constraints.
- Integration-test helpers live only under `tests/integration/`; they parse URLs with SQLAlchemy, require `atlas_test` or `*_test`, reset once per suite with AUTOCOMMIT `DROP SCHEMA public CASCADE` / `CREATE SCHEMA public`, run `alembic upgrade head`, initialize LangGraph checkpoint tables via `PostgresSaver.setup()`, and truncate Atlas job/audit/model-ledger rows plus checkpoint data between tests.

### Research-job HTTP API

- Versioned routes live under `/v1`; `/health` and `/ready` remain unversioned ops endpoints.
- `POST /v1/research-jobs` accepts a trimmed question (1–8000 chars) and required `Idempotency-Key` (max 128), creates a server-side UUID4 id via `ResearchJobService`, persists a `PENDING` job, and returns `202` with `ResearchJobResponse`.
- Matching idempotent replay returns the original job with `202`; key reuse with a different canonical payload returns structured `409`.
- `GET /v1/research-jobs/{job_id}` returns `200` or structured `404`.
- Request fingerprints hash deterministic canonical JSON of the normalized create request (currently `{"question": ...}` only).
- Structured `ErrorResponse` covers application errors and `RequestValidationError` (`422`). Only `sqlalchemy.exc.OperationalError` maps to research-job API `503`; other failures are not hidden as unavailable.
- Idempotency key values are not returned in bodies, error details, or logs.
- The application service is FastAPI-independent but coordinates SQLAlchemy `sessionmaker`/`session_scope` transactions; no Unit of Work abstraction.
- Verified on `main` through Pull Request #7 (pull-request CI and resulting `main` CI green).

### Background worker

- Separate process entrypoint: `python -m atlas.worker`.
- Claims use `SELECT … FOR UPDATE SKIP LOCKED` for `PENDING` or lease-expired `RUNNING` jobs; every claim/reclaim sets a new `secrets.token_hex(32)` token and `lease_expires_at`.
- `ClaimedResearchJob` carries the domain job plus claim token; lease/token columns are not on the domain entity or public API schemas.
- Domain `start()` / `complete()` / `fail()` remain authoritative; repository claim/finalize applies those transitions then persists.
- Fenced finalize requires matching job id, `RUNNING` status, and claim token in one transaction; clears token/lease atomically on success; returns `False` without modifying the row on ownership loss.
- `ResearchJobProcessor` is a Protocol: `(question: str, *, job_id: str) -> str`. The worker owns claim/finalize and does not import LangGraph.
- Defaults: poll 1s, processing timeout 60s, lease 90s (Milestone 8 locally validated defaults; may be tuned later); no heartbeat renewal.
- The 60-second worker processing timeout is orchestration-only (`Future.result(timeout=…)` on a single-thread executor). It does not cancel or interrupt the processor thread. Late results are ignored permanently and cannot finalize. New claims are refused while an abandoned processor still occupies the pool thread.
- Bounded shutdown: stop claiming, wait at most `shutdown_grace_seconds` (default equals the processing timeout), then `shutdown(wait=False)`. Python cannot kill processor threads and does not guarantee process exit if a callable remains blocked; operators may need SIGKILL. Hard termination of arbitrary LLM/tool/graph work requires process isolation or an external worker later. Milestone 8 does not add unsafe thread-based cancellation to claim a harder timeout.
- Processing is at-least-once: claim tokens fence stale database finalization but cannot undo duplicate in-process work.
- Verified on `main` through Pull Request #8 for the worker foundation and Pull Request #10 for the Milestone 7 LangGraph processor. Milestone 8 adds model-provider adapters behind the same worker boundary and is not yet on `main`.

### LangGraph research workflow

- Package `atlas.workflow` holds typed state, five nodes (`validate` → `plan` → `research` → `draft` → `complete`), sync `PostgresSaver` runtime, and `LangGraphResearchProcessor`.
- Plan and draft are model-backed through Atlas `ResearchPlanner` / `ResearchDrafter` Protocols. Validate, research (fake tool), and complete remain deterministic in Milestone 8.
- Model capabilities are injected via LangGraph typed runtime context: `ModelRuntimeContext`, `StateGraph(..., context_schema=ModelRuntimeContext)`, `graph.invoke(..., context=...)`, and `Runtime[ModelRuntimeContext]` inside nodes. No module-level `ContextVar` for model wiring.
- Stable LangGraph `thread_id` is the research `job_id`. Resume of unfinished work uses `graph.invoke(None, config, context=...)` and must not resend the original input. Runtime context is not checkpointed.
- Checkpoint tables are LangGraph-owned and created only through an explicit worker-startup `PostgresSaver.setup()` path (not per job). Atlas Alembic migration `20260809_0004` owns `workflow_executions` / `workflow_node_executions`; `20260809_0005` owns the model invocation ledger.
- Checkpoint writes, Atlas audit writes, and model-ledger writes use separate connections/transactions and are not atomic together; after a crash they may briefly disagree. Checkpoints are the resume source of truth; audit and ledger rows are operational history.
- One `workflow_executions` row per worker processing attempt; attempts for a job share `thread_id`. Reclaim creates a new execution and marks prior `RUNNING` executions `ABANDONED` when practical. Node attempts are append-only with unique `(workflow_execution_id, node_name, attempt)`.
- Node wrappers and workflow-level processor handling catch `Exception` only so `KeyboardInterrupt` / `SystemExit` propagate; ordinary node failures mark audit rows failed. Persisted node errors are class-only (`<ExceptionClass>: node execution failed`) and never store raw exception messages.
- Graph nodes never call `finalize_*` or mutate `ResearchJob` lifecycle.
- Final result is a report with sections `Question`, `Plan`, `Findings`, and `Draft`.

### Model providers and invocation ledger (Milestone 8)

- LangChain `BaseChatModel` is Atlas’s model boundary. `ChatOpenAI` and `ChatAnthropic` are instantiated only in the composition/configuration layer (`atlas.models.langchain` / `atlas.models.composition`). Workflow and domain code do not import provider SDKs.
- Default provider is `fake`. Deterministic fakes implement Atlas planner/drafter Protocols directly and do not require LangChain fake model classes. Real OpenAI/Anthropic calls require explicit provider selection and credentials.
- OpenAI uses langchain-openai with `use_responses_api=True` (verified against the pinned package). `ATLAS_MODEL_CALL_TIMEOUT_SECONDS` (default 25) is the provider HTTP/SDK request timeout and the ledger attempt `deadline_at` duration. Atlas does not enforce a separate hard wall-clock around the entire structured-invoke path (validation, ledger I/O, etc.).
- Provider exceptions are translated into Atlas-owned categories at the adapter boundary (`ModelTimeoutError`, `ModelRateLimitedError`, etc.). Provider-specific exception types must not leak into workflow, domain, API, or persistence code.
- Two-table invocation ledger: `model_invocations` (logical idempotent invocation + cached validated output) and `model_invocation_attempts` (every physical provider attempt with explicit `deadline_at`).
- Atlas prevents another provider call when a validated success is committed for the same invocation key. Atlas cannot guarantee exactly-once provider billing if the process dies after the provider completes a request but before the ledger commit.
- Concurrent calls with the same invocation key fail fast with `ModelInvocationInProgressError` (no wait/poll while holding a worker claim).
- A stale in-progress invocation may be reclaimed only after its attempt deadline has expired and the owning research-job claim is no longer valid.
- Attempt finalization is fenced with conditional SQL updates: a physical attempt may leave `STARTED` only while still `STARTED`, and the logical invocation is updated only when that attempt remains the latest active attempt for the invocation. A late result from a superseded attempt raises `ModelAttemptOwnershipLostError` and must not overwrite newer status, output, metadata, or error fields.
- Ledger captures provider, model, prompt version, token usage, latency, status, error class/retry class, and estimated cost. It never stores API keys, raw exception messages, complete prompts, or complete model responses.
- Estimated cost uses a versioned pricing catalog (`PRICING_VERSION`); values are labeled estimates; unknown models leave estimated cost null.
- Opt-in live OpenAI and Anthropic structured-plan verification passed locally (2026-08-09) using gitignored credentials. Pull-request CI and resulting `main` CI remain required before Milestone 8 is Complete.

These decisions cover the verified foundation through the Milestone 8 model-provider slice. They do not imply live search, agents, messaging, or cloud topology choices.

## Why the full diagram comes later

A Visio session will make the request path, trust boundaries, state stores, asynchronous workflows, agent/tool interactions, observability flow, deployment topology, and local-to-cloud mapping explicit once local components provide credible evidence. The approved diagram will then expand this written design so the prose and visual architecture cannot drift.

## Sections to complete after Visio

1. Design goals, constraints, and assumptions.
2. System context and user request flow.
3. Components and responsibility boundaries.
4. Agent orchestration, state, retry, and recovery model.
5. Data ownership, events, caching, retrieval, and provenance.
6. Remaining contracts for future events, tools, retrieval, agents, and expanded APIs (health, readiness, and research-job HTTP endpoints already exist).
7. Security, identity, networking, and trust boundaries.
8. Logs, metrics, traces, dashboards, alerts, and SLOs.
9. Local Docker-based topology and AWS/Kubernetes topology.
10. CI/CD, infrastructure as code, rollout, rollback, backup, and disaster recovery.
11. Scalability, reliability, cost, and failure-mode analysis.
12. Alternatives, tradeoffs, and unresolved decisions.

## Diagram deliverables

- Editable Visio source file.
- Reviewable PDF and/or PNG export.
- A legend and numbered primary request flow.
- Clear separation of local development and AWS production views.

No cloud or deployment architecture described here should be treated as accepted until those deliverables are reviewed.
