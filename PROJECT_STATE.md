# Atlas AI Platform — Project State

- Last updated: 2026-08-09
- Phase: Local implementation foundation
- Milestone: Real model-provider adapters (Milestone 8)
- Implementation status: Milestone 7 is complete. Milestone 8 is Current; local automated gates and opt-in live OpenAI/Anthropic verification passed on `milestone-8-model-providers`. Pull-request CI and resulting `main` CI remain outstanding before Complete.

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with validated local foundation through the Milestone 8 model-provider slice.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files; committed `.env.example` (no secrets).
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with FastAPI `GET /health` (liveness) and `GET /ready` (Postgres readiness).
- Pytest, Ruff (format + lint), and mypy configuration; domain, API, worker, workflow, model, and PostgreSQL integration tests.
- GitHub Actions CI with Postgres 16 service targeting `atlas_test`; `main` is green through Pull Request #10 (Milestone 7 LangGraph workflow). Milestone 8 remote verification is still pending its pull request and the resulting `main` workflow.
- `atlas.domain` package with slotted `ResearchJob`, `reconstitute(...)`, and lifecycle transitions.
- Docker Compose Postgres 16 on host port `5433` with databases `atlas` and `atlas_test`.
- SQLAlchemy 2.x + psycopg3 + Alembic persistence for `research_jobs`, including idempotency metadata (`20260808_0002`), claim lease/token columns (`20260809_0003`), Atlas-owned workflow execution history (`20260809_0004`), and model invocation ledger tables (`20260809_0005`).
- `POST /v1/research-jobs` and `GET /v1/research-jobs/{job_id}` with Pydantic contracts, `ResearchJobService`, required `Idempotency-Key`, and structured API errors (merged via Pull Request #7).
- Background worker (`python -m atlas.worker`) with PostgreSQL `FOR UPDATE SKIP LOCKED` claiming, claim-token fencing, orchestration timeout, and bounded shutdown (does not hard-kill processor threads), merged via Pull Request #8 and extended in Milestones 7–8.
- LangGraph research workflow behind `ResearchJobProcessor` (`job_id` keyword; `thread_id = job_id`): validate → plan → research → draft → complete; sync `PostgresSaver` checkpoints; Atlas audit tables for per-attempt workflow/node history.
- Model-provider adapters (`atlas.models`): LangChain `BaseChatModel` boundary; OpenAI (`ChatOpenAI` + Responses API) and Anthropic (`ChatAnthropic`) composed only in configuration; default `fake` deterministic planner/drafter Protocols; LangGraph `ModelRuntimeContext`; two-table invocation ledger with fail-fast in-progress conflicts and deadline-based stale reclaim.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- Live web search, RAG/pgvector, MCP, specialist agents, grading, Redis, Kafka, application/worker Docker images, Kubernetes, Terraform, or AWS resources.
- Heartbeat lease renewal or hard cancellation of in-flight processor threads.
- Exactly-once provider billing guarantees after crash between provider success and ledger commit.
- Validated quality, latency, reliability, or cost benchmarks.
- Remote CI verification for Milestone 8 (pending PR and main).

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
- Integration tests guard destructive operations with SQLAlchemy URL parsing (`atlas_test` or `*_test` only), reset once per suite via `DROP SCHEMA public CASCADE` + `CREATE SCHEMA public` (AUTOCOMMIT), then `alembic upgrade head`, initialize LangGraph checkpoint schema via `PostgresSaver.setup()`, and truncate Atlas + checkpoint data between tests.
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
- Worker defaults (Milestone 8): poll 1s, processing timeout 60s (orchestration-only `Future.result`; does not kill threads), lease 90s; provider request timeout / attempt deadline 25s (not a hard whole-invoke wall clock); no heartbeat renewal.
- Processing timeout is an orchestration timeout via `Future.result(timeout=...)` on a single-thread executor; late results are ignored permanently and cannot finalize. Python cannot forcibly stop an already-running processor thread.
- Shutdown stops new claims and waits at most `shutdown_grace_seconds` (default = processing timeout) before `ThreadPoolExecutor.shutdown(wait=False)`. A hung non-daemon thread may keep the process alive until the callable returns or the process is force-killed.
- `ResearchJobProcessor` requires `question` and keyword-only `job_id`; the worker injects `LangGraphResearchProcessor` and must not import LangGraph.
- LangGraph owns node progression and durable checkpoints (`PostgresSaver`, tables via one-time worker-startup `setup()`). Atlas Alembic owns `workflow_executions` / `workflow_node_executions` / model invocation ledger. Checkpoint, audit, and ledger writes are not one atomic transaction and may briefly disagree after a crash.
- One `workflow_executions` row per worker processing attempt; all attempts share `thread_id = research_job_id`. Reclaim creates a new execution and marks prior `RUNNING` rows `ABANDONED` when practical. Node history stores one row per `(workflow_execution_id, node_name, attempt)`.
- Workflow/node failure handling catches `Exception` only; process-control exceptions propagate. Persisted node errors are class-only (`<ExceptionClass>: node execution failed`) with no raw exception text.
- Graph nodes never finalize `ResearchJob` rows.
- Report sections: `Question`, `Plan`, `Findings`, `Draft`.
- LangChain `BaseChatModel` is the model boundary; provider SDKs and `ChatOpenAI`/`ChatAnthropic` stay in composition only.
- Model wiring uses LangGraph `ModelRuntimeContext` + `context_schema` + `invoke(..., context=...)` + `Runtime[...]`; not a module-level model `ContextVar`.
- Default provider is `fake`; real providers require explicit selection and credentials. Plan/draft are model-backed; validate/research/complete stay deterministic for Milestone 8.
- Invocation ledger is two tables (`model_invocations`, `model_invocation_attempts`). Concurrent same-key calls fail fast with `ModelInvocationInProgressError`. Stale reclaim requires expired attempt deadline and invalid research-job claim. Attempt finalization uses conditional `STARTED`→terminal updates and active-attempt checks so a late superseded attempt cannot overwrite newer ledger state (`ModelAttemptOwnershipLostError`). Exactly-once provider billing is not claimed.
- Ledger never stores API keys, raw exception messages, complete prompts, or complete model responses. Estimated cost is versioned and may be null.

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
- Milestone 6 completion gate is satisfied; Milestone 6 is **Complete**.

## Verification (Milestone 7)

### Local

- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (64 source files)
- `ATLAS_DATABASE_URL=.../atlas_test uv run pytest` → 135 passed
- `git diff --check` → clean
- Covers deterministic graph output, `interrupt_after=["plan"]` restart recovery with full runtime disposal and `invoke(None, …)`, API→worker→LangGraph→GET, per-attempt workflow history / abandon-on-reclaim, safe class-only node failure persistence, and Alembic head `20260809_0004`.

### Remote

- Pull Request #10, `feat: add deterministic LangGraph workflow`, merged into `main` as commit `5a6d19c`.
- GitHub Actions pull-request and resulting `main` CI for that merge are the source of truth for Milestone 7 remote verification.

## Verification (Milestone 8)

### Local

- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (79 source files)
- `ATLAS_DATABASE_URL=.../atlas_test uv run pytest` → 156 passed, 2 skipped (live tests skipped in the default suite)
- `git diff --check` → clean
- Default suite makes no live provider network calls.
- Covers mocked OpenAI/Anthropic adapters, ledger success/replay, fail-fast in-progress conflicts, stale reclaim after deadline + invalid claim, late superseded-attempt fencing, Atlas-only error classes in ledger rows, and Alembic head `20260809_0005`.

### Opt-in live providers

- `.env` is gitignored (confirmed via `git check-ignore`; contents not inspected in documentation).
- `ATLAS_ENABLE_LIVE_MODEL_TESTS=1 uv run pytest tests/models/test_live_providers.py -v` → 2 passed:
  - `test_live_openai_structured_plan` (LangChain → OpenAI, provider-default model)
  - `test_live_anthropic_structured_plan` (LangChain → Anthropic, provider-default model)
- No global `ATLAS_MODEL_NAME` override was used for the live run.
- Live verification used local ignored credentials only; keys were not logged, committed, or copied into docs/tests/examples.

### Remote

- Pending: pull-request CI and resulting `main` CI must pass before Milestone 8 is marked Complete.
- Milestone 8 remains **Current**.

## Next steps

1. Open the Milestone 8 pull request from `milestone-8-model-providers` and confirm PR CI + resulting `main` CI are green.
2. Only then mark Milestone 8 Complete and advance Milestone 9 to Current.
3. Do not expand into later roadmap capabilities ahead of their milestones.

## Active blockers

None.
