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
- Runtime dependencies (FastAPI, Uvicorn, SQLAlchemy, psycopg, Alembic, pydantic-settings) are separated from development dependencies (Pytest, httpx2, Ruff, mypy).

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
- Integration-test helpers live only under `tests/integration/`; they parse URLs with SQLAlchemy, require `atlas_test` or `*_test`, reset once per suite with AUTOCOMMIT `DROP SCHEMA public CASCADE` / `CREATE SCHEMA public`, run `alembic upgrade head`, and truncate between tests.

### Research-job HTTP API (Milestone 5, locally verified)

- Versioned routes live under `/v1`; `/health` and `/ready` remain unversioned ops endpoints.
- `POST /v1/research-jobs` accepts a trimmed question (1–8000 chars) and required `Idempotency-Key` (max 128), creates a server-side UUID4 id via `ResearchJobService`, persists a `PENDING` job, and returns `202` with `ResearchJobResponse`.
- Matching idempotent replay returns the original job with `202`; key reuse with a different canonical payload returns structured `409`.
- `GET /v1/research-jobs/{job_id}` returns `200` or structured `404`.
- Request fingerprints hash deterministic canonical JSON of the normalized create request (currently `{"question": ...}` only).
- Structured `ErrorResponse` covers application errors and `RequestValidationError` (`422`). Only `sqlalchemy.exc.OperationalError` maps to research-job API `503`; other failures are not hidden as unavailable.
- Idempotency key values are not returned in bodies, error details, or logs.
- The application service is FastAPI-independent but coordinates SQLAlchemy `sessionmaker`/`session_scope` transactions; no Unit of Work abstraction.

These decisions cover the verified foundation through the locally validated research-job API slice. They do not imply background workers, agents, messaging, or cloud topology choices.

## Why the full diagram comes later

A Visio session will make the request path, trust boundaries, state stores, asynchronous workflows, agent/tool interactions, observability flow, deployment topology, and local-to-cloud mapping explicit once local components provide credible evidence. The approved diagram will then expand this written design so the prose and visual architecture cannot drift.

## Sections to complete after Visio

1. Design goals, constraints, and assumptions.
2. System context and user request flow.
3. Components and responsibility boundaries.
4. Agent orchestration, state, retry, and recovery model.
5. Data ownership, events, caching, retrieval, and provenance.
6. API, event, and tool contracts beyond the health endpoint.
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
