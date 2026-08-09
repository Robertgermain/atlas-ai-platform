# Atlas AI Platform — Project State

- Last updated: 2026-08-09
- Phase: Local implementation foundation
- Milestone: Deterministic LangGraph workflow (Milestone 7)
- Implementation status: Milestone 6 complete on `main`; Milestone 7 is Current but paused until the Milestones 1–6 architecture and code-ownership walkthrough is complete

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with validated local foundation through the research-job worker slice.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files; committed `.env.example` (no secrets).
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with FastAPI `GET /health` (liveness) and `GET /ready` (Postgres readiness).
- Pytest, Ruff (format + lint), and mypy configuration; domain, API, worker, and PostgreSQL integration tests.
- GitHub Actions CI with Postgres 16 service targeting `atlas_test`, green on `main` through Pull Request #8.
- `atlas.domain` package with slotted `ResearchJob`, `reconstitute(...)`, and lifecycle transitions.
- Docker Compose Postgres 16 on host port `5433` with databases `atlas` and `atlas_test`.
- SQLAlchemy 2.x + psycopg3 + Alembic persistence for `research_jobs`, including idempotency metadata (`20260808_0002`) and claim lease/token columns (`20260809_0003`).
- `POST /v1/research-jobs` and `GET /v1/research-jobs/{job_id}` with Pydantic contracts, `ResearchJobService`, required `Idempotency-Key`, and structured API errors (merged via Pull Request #7).
- Background worker (`python -m atlas.worker`) with PostgreSQL `FOR UPDATE SKIP LOCKED` claiming, claim-token fencing, deterministic processing, orchestration timeout, and bounded shutdown (does not hard-kill processor threads), merged via Pull Request #8.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- Agents, LangGraph, LLM calls, brokers, Redis, Kafka, pgvector, application/worker Docker images, Kubernetes, Terraform, or AWS resources.
- Heartbeat lease renewal or hard cancellation of in-flight processor threads.
- Validated quality, latency, reliability, or cost benchmarks.
- A completed Milestones 1–6 architecture and code-ownership walkthrough (required before Milestone 7 implementation).

## Decisions

- Keep documentation intentionally small: Research, PRD, Technical Design, Testing Strategy, Local Build Plan, AGENTS, and Project State.
- Build locally through small, tested vertical slices before producing the comprehensive AWS design.
- Update the technical design incrementally as local architectural decisions are validated.
- Create the Visio system and AWS deployment diagrams once working local components provide credible design evidence.
- Add code folders and files incrementally, with an explainable purpose for each.
- Use the technology portfolio through justified capabilities and experiments, not decorative dependencies.
- Track the complete roadmap in `docs/LOCAL_BUILD_PLAN.md`; keep this file limited to current truth and the immediate handoff.
- Runtime dependencies stay in `[project].dependencies`; development tools stay in `[dependency-groups].dev`.
- `requires-python = ">=3.12,<3.13"`; mypy checks both `src` and `tests`.
- Ruff owns formatting and import sorting; black and isort are not used.
- CI installs from the committed lockfile (`uv sync --frozen`) and runs Ruff format, Ruff lint, mypy, and Pytest.
- GitHub Actions are pinned to full commit SHAs with version comments; the workflow has `contents: read` only.
- `astral-sh/setup-uv` uses `version` and `python-version` (not `python-version-file`) for the pinned action.
- Research-job identity is a caller-supplied stripped string; the domain does not generate UUIDs.
- Domain creation always starts in `PENDING`; lifecycle changes go through `start()`, `complete()`, and `fail()` only.
- Durable jobs are rebuilt with `ResearchJob.reconstitute(...)`; persistence mapping does not bypass domain validation.
- Timestamps are timezone-aware, deterministic when supplied, normalized to UTC, and must not move earlier than `updated_at`.
- PostgreSQL is the authoritative store for research jobs; settings use `pydantic-settings` (`ATLAS_DATABASE_URL`).
- Persistence uses sync SQLAlchemy 2.x and psycopg3; the repository implements the application `ResearchJobRepository` Protocol.
- Integration tests guard destructive operations with SQLAlchemy URL parsing (`atlas_test` or `*_test` only), reset once per suite via `DROP SCHEMA public CASCADE` + `CREATE SCHEMA public` (AUTOCOMMIT), then `alembic upgrade head`, and truncate between tests.
- `/health` is process liveness without DB I/O; `/ready` lazily checks Postgres, maps SQLAlchemy database errors to controlled `503`, and does not hide unexpected programming errors or expose credentials.
- Research-job HTTP create uses server-generated UUID4 ids; the domain still receives caller-supplied ids from the application service.
- API create requires `Idempotency-Key` (max 128); same key and payload replays with `202`; same key and different payload returns `409`.
- Idempotency metadata lives on `research_jobs` as nullable `idempotency_key` / `request_fingerprint` with a both-null-or-both-set CHECK; PostgreSQL unique allows multiple NULL keys.
- Request fingerprints hash a deterministic canonical JSON create representation (currently only normalized `question`).
- `ResearchJobRepository` Protocol and `ResearchJobIdempotencyRecord` / `ClaimedResearchJob` are application ports; claim/lease metadata is not on the domain entity or public responses.
- Research-job API maps only `OperationalError` to structured `503`; validation uses a shared `ErrorResponse` envelope at `422`.
- Raw idempotency key values are not echoed in API responses, structured error details, or logs.
- Worker claims use `secrets.token_hex(32)` on every claim/reclaim; fenced finalize requires matching job id, `RUNNING`, and claim token; zero-row updates mean ownership loss.
- Processing is at-least-once: the claim token fences stale DB finalization but cannot undo duplicate in-process work or future external side effects.
- Worker defaults: poll 1s, processing timeout 5s, lease 30s; no heartbeat renewal.
- Processing timeout is an orchestration timeout via `Future.result(timeout=...)` on a single-thread executor; late results are ignored permanently and cannot finalize.
- Shutdown stops new claims and waits at most `shutdown_grace_seconds` (default = processing timeout) before `ThreadPoolExecutor.shutdown(wait=False)`. Milestone 6 does not kill blocked processor threads; a hung non-daemon thread may keep the process alive until the callable returns or the process is force-killed. Hard termination of arbitrary LLM/tool work requires process isolation later.
- Milestone 7 implementation is paused until a complete Milestones 1–6 architecture and code-ownership walkthrough is finished.

## Verification (Milestone 1)

- `uv run python --version` → Python 3.12.13
- `uv sync --frozen` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (3 source files)
- `uv run pytest` → 1 passed (`GET /health` → `200`, `{"status": "ok"}`)

## Verification (Milestone 2)

### Local

- Removed black and isort from runtime dependencies; regenerated `uv.lock` so Ruff alone owns format and import sorting.
- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success
- `uv run pytest` → 1 passed
- Workflow file created with pinned `actions/checkout@v7.0.1` and `astral-sh/setup-uv@v9.0.0` commit SHAs.

### Remote (Pull Request #1)

- Initial CI run passed (green).
- Intentional failure commit `21587a6` caused CI to fail (red).
- Revert commit `2a8190c` restored the correct test and CI passed again (green).
- Pull Request #1 merged to `main`.

### Remote (`main` push after PR #1)

- The push workflow on `main` failed during the `setup-uv` step.
- Cause: unsupported input `python-version-file` for `astral-sh/setup-uv`.

### Remote (Pull Request #2 and `main`)

- Repair commit `9968478` set `version: "0.11.8"`, `python-version: "3.12"`, and `enable-cache: true`.
- Pull Request #2 merged successfully.
- The resulting `main` push workflow passed (green).
- Milestone 2 completion gate is satisfied.

## Verification (Milestone 3)

### Local

- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success
- `uv run pytest` → 50 passed (health + ResearchJob lifecycle)
- Domain package uses standard-library imports only; no FastAPI, database, or agent dependencies.

### Remote (Pull Request #3 and `main`)

- Pull Request #3, `feat: add ResearchJob domain lifecycle`, merged into `main`.
- Pull request CI passed (green).
- The resulting `main` push CI passed (green).
- Milestone 3 completion gate is satisfied.

## Verification (Milestone 4)

### Local

- Docker Compose Postgres 16 healthy with `atlas` and `atlas_test`.
- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (31 source files)
- `ATLAS_DATABASE_URL=.../atlas_test uv run pytest` → 80 passed
- Empty test schema migrates to Alembic head; repository persists across sessions; duplicate-key errors preserve `IntegrityError` cause; test-DB guard rejects non-test URLs without SQL.

### Remote (Pull Request #5 and `main`)

- Pull Request #5, `Milestone 4 postgres`, merged into `main`.
- Pull request CI passed (green).
- The resulting `main` push CI passed (green).
- Milestone 4 completion gate is satisfied.

## Verification (Milestone 5)

### Local

- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (46 source files)
- `ATLAS_DATABASE_URL=.../atlas_test uv run pytest` → 109 passed
- `git diff --check` → clean

### Remote (Pull Request #7 and `main`)

- Pull Request #7, `Milestone 5 research job api`, merged into `main` as commit `743f0bb`.
- Pull request CI passed (green).
- The resulting `main` push CI passed (green).
- Milestone 5 completion gate is satisfied; Milestone 5 is **Complete**.

## Verification (Milestone 6)

### Local

- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (54 source files)
- `ATLAS_DATABASE_URL=.../atlas_test uv run pytest` → 125 passed
- `git diff --check` → clean
- Covers concurrent `SKIP LOCKED` claiming, two-job concurrent claims, stale-token fencing after reclaim, API→worker→GET success/failure/timeout, bounded shutdown with a still-blocked processor, and migration head `20260809_0003`.

### Remote (Pull Request #8 and `main`)

- Pull Request #8, `feat: add background worker and job recovery`, merged into `main` as commit `a656cea`.
- Pull request CI passed (green).
- The resulting `main` push CI passed (green).
- Milestone 6 completion gate is satisfied; Milestone 6 is **Complete** and Milestone 7 is **Current** but paused pending the ownership walkthrough.

## Next steps

1. Complete an architecture and code-ownership walkthrough covering Milestones 1–6 (foundation, CI, domain, persistence, research-job API, and background worker).
2. After that walkthrough, begin Milestone 7: deterministic LangGraph workflow.
3. Do not implement Milestone 7 graph nodes, checkpoints, or fake-model adapters before the walkthrough is done.

## Active blockers

None for Milestone 6. Milestone 7 implementation is paused until the Milestones 1–6 ownership walkthrough is completed.
