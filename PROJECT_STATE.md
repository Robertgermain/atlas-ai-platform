# Atlas AI Platform — Project State

- Last updated: 2026-08-09
- Phase: Local implementation foundation
- Milestone: Specialist agents and report synthesis (Milestone 11) — **Current** (Slices 11A and 11B implemented locally; PR/`main` CI outstanding)
- Implementation status: Milestone 10 is **Complete** through Pull Request #13 (`bfabd59`; PR CI and resulting `main` CI green). Milestone 11 Slices 11A–11B are implemented and locally verified on `milestone-11-specialist-agents`. Final Milestone 11 PR CI and resulting `main` CI remain outstanding. Do not mark Milestone 11 Complete yet.

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with validated local foundation through Milestone 10.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files; committed `.env.example` (no secrets).
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with FastAPI `GET /health` (liveness) and `GET /ready` (Postgres readiness).
- Pytest, Ruff (format + lint), and mypy configuration; domain, API, worker, workflow, model, tool, MCP, evidence, embedding, and PostgreSQL/pgvector integration tests.
- GitHub Actions CI with Postgres 16 + pgvector (`pgvector/pgvector:pg16`) targeting `atlas_test`; `main` is green through Pull Request #13 (Milestone 10).
- `atlas.domain` package with slotted `ResearchJob`, `reconstitute(...)`, and lifecycle transitions.
- Docker Compose PostgreSQL 16 + pgvector on host port `5433` with databases `atlas` and `atlas_test`.
- SQLAlchemy 2.x + psycopg3 + Alembic persistence through head `20260809_0008` (pgvector embeddings). Prior: evidence foundation `20260809_0007`; jobs, claims/leases, workflow audit, model ledger, tool ledger.
- Research-job HTTP APIs plus evidence APIs: `POST /v1/evidence/documents`, `GET /v1/evidence/items/{id}`, `GET /v1/research-jobs/{id}/citations`.
- Background worker (`python -m atlas.worker`) with PostgreSQL claiming, fencing, and LangGraph orchestration (Milestones 6–9).
- LangGraph research workflow: validate → plan → research → draft → verify_citations → complete. Specialists in `atlas.specialists` own planner, research/retrieval, synthesizer (behind node name `draft`), and deterministic citation verification. Final `persist_final` retains defense-in-depth `CitationValidator`.
- Bounded specialist package (`atlas.specialists`): typed handoffs, fail-closed citation verification against durable job links + provenance, synthesizer pack-scope validation (no silent ID stripping). Graph/model-ledger node names remain `plan`/`research`/`draft`; verifier node is `verify_citations` (no model-ledger row; no migration — `workflow_node_executions.node_name` accepts arbitrary non-empty strings). Capability isolation is composition/spy-proven (no separate permission framework). Slice 11B adds ledger/audit attribution proofs, boundary/ablation evidence, and full API→worker→citations E2E with resume idempotency.
- Model-provider adapters (`atlas.models`): LangChain boundary; default `fake`; draft schema includes optional claims; prompt version `draft.v2`.
- Embeddings (`atlas.embeddings`): profile `embeddings.v1`, 1536-d, default `fake` deterministic embedder, optional LangChain OpenAI `text-embedding-3-small`.
- Governed research tools (`atlas.tools`): search results persist as sources/documents/evidence and job links (with nullable FK to `tool_invocations`); live fetch still unavailable.
- FastMCP stdio server unchanged in role (Milestone 9).
- Evidence package (`atlas.evidence`): contracts, URL canonicalization, normalize/chunk (Unicode code points), ingest, embedding service, retriever, citation validator, report artifact service, offline retrieval metrics.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- Final Milestone 11 pull-request CI and resulting `main` CI (Slices 11A–11B implemented locally).
- Grading, repair routing, autonomous retry policy, Redis, Kafka, application/worker Docker images, Kubernetes, Terraform, or AWS resources.
- HTTP MCP listeners, remote MCP consumption, or MCP authentication.
- Heartbeat lease renewal or hard cancellation of in-flight processor threads.
- Exactly-once provider/tool billing guarantees after crash between provider success and ledger commit.
- Validated real-world semantic retrieval quality (offline fake-embedding metrics validate pipeline/fixture geometry only).
- Live arbitrary-URL fetch, HTML extraction, PDF ingest, multipart upload, and object storage.

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
- LangGraph owns node progression and durable checkpoints (`PostgresSaver`, tables via one-time worker-startup `setup()`). Atlas Alembic owns `workflow_executions` / `workflow_node_executions` / model and tool invocation ledgers / evidence tables / embeddings. Checkpoint, audit, ledger, evidence, and embedding writes are not one atomic transaction and may briefly disagree after a crash.
- One `workflow_executions` row per worker processing attempt; all attempts share `thread_id = research_job_id`. Reclaim creates a new execution and marks prior `RUNNING` rows `ABANDONED` when practical. Node history stores one row per `(workflow_execution_id, node_name, attempt)`.
- Workflow/node failure handling catches `Exception` only; process-control exceptions propagate. Persisted node errors are class-only (`<ExceptionClass>: node execution failed`) with no raw exception text.
- Graph nodes never finalize `ResearchJob` rows.
- Report sections: `Question`, `Plan`, `Findings`, `Draft`, and `Citations` when claims exist.
- LangChain `BaseChatModel` is the model boundary; provider SDKs and `ChatOpenAI`/`ChatAnthropic` stay in composition only.
- Model/tool wiring uses LangGraph `WorkflowRuntimeContext` + `context_schema` + `invoke(..., context=...)` + `Runtime[...]`; not a module-level `ContextVar`.
- Default model and tool providers are `fake`; real providers require explicit selection and credentials. Plan/draft are model-backed; research uses governed tools; findings remain `list[str]` (max 4 KiB each, `[untrusted_source]` labeled) plus durable `evidence_item_ids`.
- Model invocation ledger is two tables (`model_invocations`, `model_invocation_attempts`) with fencing as in Milestone 8. Model ledger `node_name` CHECK currently allows only `plan` and `draft`.
- Tool invocation ledger is two tables (`tool_invocations`, `tool_invocation_attempts`) with origin `WORKFLOW|MCP`, replay, stale reclaim, and conditional finalization fencing. Logical keys include `tool_policy_version`. MCP rows keep workflow FKs NULL and stamp a per-process `actor_id` UUID (never from tool args). Tool allowlist is research-node only.
- Tool budgets (orchestration only): 6 logical calls / research node, 2 attempts / call, 8s attempt timeout, 45s research-node deadline, remaining-budget checks. Budget exhaustion raises `ToolBudgetExhaustedError` (not silent partial success).
- Live search uses Tavily via direct streaming `httpx` with `Content-Length` pre-checks, streamed byte caps before JSON deserialize, and required JSON content-type. Live arbitrary-URL fetch is not enabled. `ATLAS_TOOL_FETCH_ENABLED=true` fails closed at composition. Under `tool_provider=tavily`, `fetch_url` is omitted from the tool registry. HTML extraction and full SSRF-safe live fetch are deferred.
- Fake fetch under `tool_provider=fake` validates scheme/port/userinfo only (no DNS resolution).
- FastMCP stdio is real (`python -m atlas.mcp`); production LangGraph research does not route through MCP. Controlled Atlas `ToolError`s are raised as FastMCP `ToolError`s (sanitized class-only messages; `mask_error_details=True`).
- Workflow tool ledger rows stamp `workflow_node_attempt` from the research-node audit hook attempt (no module globals / `ContextVar`).
- Milestone 10 is Complete through Pull Request #13 (`bfabd59`). Document identity, hash semantics, citation composite FK, idempotent report artifacts, embeddings profile `embeddings.v1`, exact vs HNSW retrieval, and offline fake-embedding eval remain as validated in Milestone 10.
- Embedding identity is `(evidence_item_id, embedding_profile)`; existing embeddings are never overwritten silently. Profile is settings-restricted to `Literal["embeddings.v1"]` at 1536 dimensions with a partial HNSW cosine index for that profile.
- Retrieval: exact cosine for offline CI metric gate; HNSW-eligible candidate path for production-oriented local default. Offline thresholds Recall@5 ≥ 0.80 and MRR@5 ≥ 0.70 validate fixture geometry, not real semantic quality. Opt-in live embedding tests require `ATLAS_ENABLE_LIVE_EMBEDDING_TESTS=1`.

## Verification (Milestone 9)

### Remote

- Pull Request #12, `feat: add governed research tools and MCP (#12)`, merged into `main` as commit `0db343d`.
- Pull-request CI and resulting `main` CI passed (user-verified). Milestone 9 is **Complete**.

## Verification (Milestone 10)

### Remote

- Pull Request #13, `feat: add evidence-grounded RAG with pgvector (#13)`, merged into `main` as commit `bfabd59`.
- Pull-request CI and resulting `main` CI passed (user-verified). Milestone 10 is **Complete**.

### Local (pre-merge branch verification)

- `uv sync --frozen`, Ruff format/lint, mypy, full Pytest against `atlas_test` (221 passed, 4 skipped), Alembic head `20260809_0008`, offline Recall@5/MRR@5 = 1.00/1.00 (pipeline geometry), opt-in live OpenAI embedding dimensions 1536, and `git diff --check` were verified on the Milestone 10 branch before merge.

## Verification (Milestone 11 — Slices 11A–11B local)

### Local

- Branch `milestone-11-specialist-agents` at `bfabd59` base; Milestone 11 changes uncommitted pending final review.
- Slice 11A: specialist contracts, linear topology with `verify_citations`, checkpoint resume, fail-closed unlinked citations.
- Slice 11B: capability-isolation composition/spy tests; model/tool ledger attribution (`plan`/`draft` models, `research` tools only; `verify_citations` audit without model/tool rows); deterministic boundary/ablation suite; full API→worker→citations E2E with provenance and resume idempotency; draft-failure blocks complete; bounded execution confirmations.
- Quality gates: `uv sync --frozen`, Ruff format/lint, mypy, full Pytest against `atlas_test` → **258 passed, 4 skipped**; `git diff --check` clean; Alembic head `20260809_0008`; default suite makes no live provider calls.

### Outstanding before Milestone 11 Complete

- Final Milestone 11 pull-request CI and resulting `main` CI

## Next steps

1. Stop for final Milestone 11 review (Slices 11A–11B).
2. Open one Milestone 11 PR after approval; do not create a documentation-only PR.
3. Do not mark Milestone 11 Complete until PR/`main` CI pass.

## Active blockers

None for Milestone 11 local review.
