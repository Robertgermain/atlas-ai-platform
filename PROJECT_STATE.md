# Atlas AI Platform — Project State

- Last updated: 2026-08-09
- Phase: Local implementation foundation
- Milestone: Evidence, provenance, RAG, and pgvector (Milestone 10) — **Current** (Slices 10A and 10B implemented and locally verified; final PR/`main` CI outstanding)
- Implementation status: Milestone 9 is **Complete** through Pull Request #12 (`0db343d`; PR CI and resulting `main` CI green). Milestone 10 Slice 10A (evidence/provenance) is approved and locally verified. Slice 10B (pgvector, embeddings, retrieval, offline eval) is implemented and locally verified on `milestone-10-evidence-rag`. Final Milestone 10 PR CI and resulting `main` CI remain outstanding. Do not mark Milestone 10 Complete yet.

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with validated local foundation through Milestone 10 Slice 10B.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files; committed `.env.example` (no secrets).
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with FastAPI `GET /health` (liveness) and `GET /ready` (Postgres readiness).
- Pytest, Ruff (format + lint), and mypy configuration; domain, API, worker, workflow, model, tool, MCP, evidence, embedding, and PostgreSQL/pgvector integration tests.
- GitHub Actions CI with Postgres 16 + pgvector (`pgvector/pgvector:pg16`) targeting `atlas_test`; `main` is green through Pull Request #12 (Milestone 9).
- `atlas.domain` package with slotted `ResearchJob`, `reconstitute(...)`, and lifecycle transitions.
- Docker Compose PostgreSQL 16 + pgvector on host port `5433` with databases `atlas` and `atlas_test`.
- SQLAlchemy 2.x + psycopg3 + Alembic persistence through head `20260809_0008` (pgvector embeddings). Prior: evidence foundation `20260809_0007`; jobs, claims/leases, workflow audit, model ledger, tool ledger.
- Research-job HTTP APIs plus evidence APIs: `POST /v1/evidence/documents`, `GET /v1/evidence/items/{id}`, `GET /v1/research-jobs/{id}/citations`.
- Background worker (`python -m atlas.worker`) with PostgreSQL claiming, fencing, and LangGraph orchestration (Milestones 6–9).
- LangGraph research workflow: validate → plan → research → draft → complete; after search evidence, bounded semantic retrieval links operator-corpus hits to the job, merges/dedupes IDs, packs evidence, drafts structured claims, and validates citations.
- Model-provider adapters (`atlas.models`): LangChain boundary; default `fake`; draft schema includes optional claims; prompt version `draft.v2`.
- Embeddings (`atlas.embeddings`): profile `embeddings.v1`, 1536-d, default `fake` deterministic embedder, optional LangChain OpenAI `text-embedding-3-small`.
- Governed research tools (`atlas.tools`): search results persist as sources/documents/evidence and job links (with nullable FK to `tool_invocations`); live fetch still unavailable.
- FastMCP stdio server unchanged in role (Milestone 9).
- Evidence package (`atlas.evidence`): contracts, URL canonicalization, normalize/chunk (Unicode code points), ingest, embedding service, retriever, citation validator, report artifact service, offline retrieval metrics.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- Specialist agents, grading, Redis, Kafka, application/worker Docker images, Kubernetes, Terraform, or AWS resources.
- HTTP MCP listeners, remote MCP consumption, or MCP authentication.
- Heartbeat lease renewal or hard cancellation of in-flight processor threads.
- Exactly-once provider/tool billing guarantees after crash between provider success and ledger commit.
- Validated real-world semantic retrieval quality (offline fake-embedding metrics validate pipeline/fixture geometry only).
- Live arbitrary-URL fetch, HTML extraction, PDF ingest, multipart upload, and object storage.
- Milestone 10 Complete (final PR/`main` CI still outstanding).

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
- Model invocation ledger is two tables (`model_invocations`, `model_invocation_attempts`) with fencing as in Milestone 8.
- Tool invocation ledger is two tables (`tool_invocations`, `tool_invocation_attempts`) with origin `WORKFLOW|MCP`, replay, stale reclaim, and conditional finalization fencing. Logical keys include `tool_policy_version`. MCP rows keep workflow FKs NULL and stamp a per-process `actor_id` UUID (never from tool args).
- Tool budgets (orchestration only): 6 logical calls / research node, 2 attempts / call, 8s attempt timeout, 45s research-node deadline, remaining-budget checks. Budget exhaustion raises `ToolBudgetExhaustedError` (not silent partial success).
- Live search uses Tavily via direct streaming `httpx` with `Content-Length` pre-checks, streamed byte caps before JSON deserialize, and required JSON content-type. Live arbitrary-URL fetch is not enabled. `ATLAS_TOOL_FETCH_ENABLED=true` fails closed at composition. Under `tool_provider=tavily`, `fetch_url` is omitted from the tool registry. HTML extraction and full SSRF-safe live fetch are deferred.
- Fake fetch under `tool_provider=fake` validates scheme/port/userinfo only (no DNS resolution).
- FastMCP stdio is real (`python -m atlas.mcp`); production LangGraph research does not route through MCP. Controlled Atlas `ToolError`s are raised as FastMCP `ToolError`s (sanitized class-only messages; `mask_error_details=True`).
- Workflow tool ledger rows stamp `workflow_node_attempt` from the research-node audit hook attempt (no module globals / `ContextVar`).
- Milestone 10 process: one final PR with two mandatory review stops (10A then 10B). Slice 10A is approved; Slice 10B is implemented locally pending final milestone review.
- Document identity is `UNIQUE(source_id, content_sha256, parser_version)` — not global content hash uniqueness.
- Hash semantics: document = SHA-256 of accepted raw UTF-8 bytes; evidence item = SHA-256 of normalized UTF-8 text; report artifact = SHA-256 of rendered report UTF-8.
- Citation scope is enforced by composite FK from `citations` to `evidence_job_links` plus application fail-closed validation. Every persisted claim requires ≥1 job-linked evidence id; otherwise `claims=[]` and no invented citations.
- Final report persistence is idempotent by `workflow_execution_id` with canonical body hash + citation-mapping comparison; conflicts fail closed.
- Evidence job links may reference `tool_invocations.id` with `ON DELETE SET NULL`.
- Ingest format for 10A/10B: UTF-8 plain text / Markdown only via JSON API (no PDF/multipart).
- Markdown that normalizes to whitespace-only content is rejected with `EvidenceValidationError`.
- Embedding identity is `(evidence_item_id, embedding_profile)`; existing embeddings are never overwritten silently. Profile is settings-restricted to `Literal["embeddings.v1"]` at 1536 dimensions with a **partial** HNSW cosine index for that profile. Default embedder is deterministic fake (pipeline tests only); optional live OpenAI via LangChain. Provider calls occur outside long DB transactions; missing embeddings can be backfilled. Partial failure: evidence may exist without an embedding when the provider fails after evidence commit; API maps typed embedding errors to sanitized `ErrorResponse` codes (422/409/503).
- Retrieval: exact cosine (transaction-local indexscan/bitmapscan off) for offline CI metric gate; HNSW-eligible candidate query (`ORDER BY embedding <=> query LIMIT`, profile-scoped via partial index) plus post-filter deterministic ordering for production-oriented local default (`ATLAS_RETRIEVAL_USE_HNSW`). HNSW index eligibility is proven by EXPLAIN under test-only planner settings; production planner decisions remain PostgreSQL-owned (not every production query is guaranteed to choose HNSW). Offline thresholds Recall@5 ≥ 0.80 and MRR@5 ≥ 0.70 validate fixture geometry, not real semantic quality. Opt-in live embedding tests require `ATLAS_ENABLE_LIVE_EMBEDDING_TESTS=1`.

## Verification (Milestone 9)

### Remote

- Pull Request #12, `feat: add governed research tools and MCP (#12)`, merged into `main` as commit `0db343d`.
- Pull-request CI and resulting `main` CI passed (user-verified). Milestone 9 is **Complete**.

## Verification (Milestone 10 — Slices 10A + 10B local)

### Local

- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success
- `ATLAS_DATABASE_URL=.../atlas_test uv run pytest` → 221 passed, 4 skipped (live model/tool/embedding tests skipped; default suite makes no live provider calls)
- `git diff --check` → clean
- Alembic: downgrade `20260809_0008` → `20260809_0007` and upgrade to head `20260809_0008` verified; historical migrations `0001`–`0006` unchanged; `0007`/`0008` are new on this branch
- Offline fake-embedding eval: Recall@5 = 1.00, MRR@5 = 1.00 (pipeline/fixture geometry only — not real semantic quality)
- Opt-in live LangChain OpenAI embedding test passed (`ATLAS_ENABLE_LIVE_EMBEDDING_TESTS=1`): provider/model OpenAI `text-embedding-3-small`, returned dimensions 1536; credentials from ignored local `.env` only; remains opt-in and skipped in normal CI
- Covers whitespace-only Markdown rejection, fake embedding determinism/dimensions, embedding persistence/idempotent replay/backfill, partial embedding failure (service + structured API 503), pgvector + HNSW index with EXPLAIN eligibility, exact cosine retrieval, metadata include/exclude filters, job linking of retrieved corpus evidence, provenance on retrieval results, workflow retrieval→draft→citation, metric thresholds, embedding-profile settings validation, live-test isolation, and Milestone 1–10A regressions

### Outstanding before Milestone 10 Complete

- Final Milestone 10 pull-request CI and resulting `main` CI
- Documentation remains Milestone 10 **Current** until those remote gates pass; Complete is reconciled on the next milestone branch after merge CI

## Next steps

1. Stop for final Milestone 10 review (Slices 10A + 10B).
2. After approval: commit, push, and open one Milestone 10 PR (do not create a documentation-only PR).
3. Do not mark Milestone 10 Complete until PR/`main` CI pass.

## Active blockers

None for final Milestone 10 review.
