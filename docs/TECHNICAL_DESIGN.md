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

- Local Postgres 16 runs via Docker Compose; host port `127.0.0.1:5433` maps to container `5432` (localhost-only publish); databases `atlas` (app) and `atlas_test` (tests). Compose credentials are development-only.
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
- Verified on `main` through Pull Request #8 for the worker foundation, Pull Request #10 for the Milestone 7 LangGraph processor, and Pull Request #11 for Milestone 8 model providers.

### LangGraph research workflow

- Package `atlas.workflow` holds typed state, five nodes (`validate` → `plan` → `research` → `draft` → `complete`), sync `PostgresSaver` runtime, and `LangGraphResearchProcessor`.
- Plan and draft are model-backed through Atlas `ResearchPlanner` / `ResearchDrafter` Protocols. Research uses governed `ResearchPlanExecutor` tools and persists search hits as durable evidence. Graph state carries `findings: list[str]` plus `evidence_item_ids` and structured `claims` (Milestone 10A).
- Capabilities are injected via LangGraph typed runtime context: `WorkflowRuntimeContext` (renamed from `ModelRuntimeContext`), `StateGraph(..., context_schema=WorkflowRuntimeContext)`, `graph.invoke(..., context=...)`, and `Runtime[WorkflowRuntimeContext]` inside nodes. No module-level `ContextVar` for model/tool wiring.
- Stable LangGraph `thread_id` is the research `job_id`. Resume of unfinished work uses `graph.invoke(None, config, context=...)` and must not resend the original input. Runtime context is not checkpointed. Missing `evidence_item_ids` / `claims` on older checkpoints are treated as empty.
- Checkpoint tables are LangGraph-owned and created only through an explicit worker-startup `PostgresSaver.setup()` path (not per job). Atlas Alembic migration `20260809_0004` owns `workflow_executions` / `workflow_node_executions`; `20260809_0005` owns the model invocation ledger; `20260809_0006` owns the tool invocation ledger; `20260809_0007` owns evidence/provenance/report tables.
- Checkpoint writes, Atlas audit writes, ledger writes, and evidence writes use separate connections/transactions and are not atomic together; after a crash they may briefly disagree. Checkpoints are the resume source of truth; audit/ledger/evidence rows are operational history. Report artifact persistence is idempotent by `workflow_execution_id` so a commit-before-job-finalize window can safely retry.
- One `workflow_executions` row per worker processing attempt; attempts for a job share `thread_id`. Reclaim creates a new execution and marks prior `RUNNING` executions `ABANDONED` when practical. Node attempts are append-only with unique `(workflow_execution_id, node_name, attempt)`.
- Node wrappers and workflow-level processor handling catch `Exception` only so `KeyboardInterrupt` / `SystemExit` propagate; ordinary node failures mark audit rows failed. Persisted node errors are class-only (`<ExceptionClass>: node execution failed`) and never store raw exception messages.
- Graph nodes never call `finalize_*` or mutate `ResearchJob` lifecycle.
- Final result is a report with sections `Question`, `Plan`, `Findings`, `Draft`, and `Citations` when claims exist.

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
- Opt-in live OpenAI and Anthropic structured-plan verification passed locally (2026-08-09). Milestone 8 is Complete through Pull Request #11 (PR CI and resulting `main` CI green).

### Governed research tools and MCP (Milestone 9)

- Atlas-owned Pydantic contracts (`WebSearchInput`/`Output`, `FetchUrlInput`/`Output`) and a typed `ResearchTool` Protocol live under `atlas.tools`. Domain/application/workflow import only Atlas ports/contracts — never FastMCP, Tavily SDK, or raw httpx clients.
- `ToolRegistry` + `NodePermissionPolicy` allow `web_search`/`fetch_url` only on the research node (workflow) and on MCP origin; other workflow nodes have empty allowlists.
- Default `ATLAS_TOOL_PROVIDER=fake` uses deterministic fake search/fetch. Live search uses Tavily’s HTTP API via direct streaming `httpx` (`POST https://api.tavily.com/search`, Bearer auth) with required JSON content-type, optional `Content-Length` pre-rejection, and streamed byte caps before JSON deserialize.
- Live arbitrary-URL fetch is **unavailable** in Milestone 9. A concurrency-safe request-scoped transport that connects to a validated public IP while preserving TLS SNI/certificate verification was required; process-global `socket.getaddrinfo` monkeypatching is explicitly rejected as unsafe. Until such a transport or a controlled egress boundary exists, `ATLAS_TOOL_FETCH_ENABLED=true` fails at composition with `ToolAuthConfigError`. Under `tool_provider=tavily`, `fetch_url` is not registered (never substituted with fake fetch). HTML extraction (e.g. BeautifulSoup/lxml) and full SSRF-safe live fetch are deferred, not implemented. Fake fetch validates scheme/port/userinfo only.
- Research-node budgets (orchestration only, not thread kill): max 6 logical calls, max 2 attempts/call, 8s attempt timeout, 45s node deadline, remaining-budget checks before start/retry. Exhaustion raises `ToolBudgetExhaustedError` rather than returning incomplete findings as success.
- Projected findings are capped at 4 KiB and prefixed with `[untrusted_source]` before draft prompts; draft system prompts instruct that findings are untrusted external data, not instructions.
- Two-table tool ledger (`tool_invocations`, `tool_invocation_attempts`) mirrors model-ledger fencing. Logical identity includes job/node/tool/provider/input fingerprint/`tool_policy_version` (workflow) or origin+actor/tool/provider/input/`tool_policy_version` (MCP). Workflow execution id is physical attribution, not part of the logical key. Workflow rows also store `workflow_node_attempt` from the research-node audit hook.
- FastMCP stdio server (`python -m atlas.mcp`) delegates to the same `ToolInvocationService`. MCP stamps `origin=MCP` and a per-process UUID `actor_id` generated at server import/start — never accepted from tool arguments; workflow attribution fields are not MCP inputs; workflow FKs are NULL for MCP rows. Atlas `ToolError` failures are raised as FastMCP `ToolError` (sanitized; `mask_error_details=True`) so clients observe MCP errors rather than successful payloads containing error dicts. Production LangGraph research does not route through MCP.
- Default CI uses fake tools and makes no live network calls. Opt-in live tool tests require `ATLAS_ENABLE_LIVE_TOOL_TESTS=1` and credentials.

### Evidence and provenance (Milestone 10A)

- `atlas.evidence` owns ingest/normalize/chunk contracts, URL canonicalization, citation validation, and idempotent final report persistence. Persistence tables: `sources`, `documents`, `evidence_items`, `evidence_job_links`, `report_artifacts`, `claims`, `citations` (Alembic `20260809_0007`).
- Document identity is `UNIQUE(source_id, content_sha256, parser_version)`. Identical bytes at different sources remain distinct documents.
- Hash meanings: `documents.content_sha256` = SHA-256 of accepted raw UTF-8 bytes; `evidence_items.content_sha256` = SHA-256 of normalized item UTF-8 text; `report_artifacts.content_sha256` = SHA-256 of the canonical rendered report UTF-8.
- Citations require job linkage via `evidence_job_links`. Composite FK `(research_job_id, evidence_item_id)` prevents cross-job citation at the database; application validation fails closed first. Every persisted claim cites ≥1 linked evidence item; otherwise claims are empty and no unsupported claim rows are written.
- `ReportArtifactService.persist_final` is idempotent by `workflow_execution_id` (unique). Matching body hash + canonical citation mapping replays; mismatch raises `ReportArtifactConflictError`.
- HTTP: `POST /v1/evidence/documents` (UTF-8 text/Markdown JSON), `GET /v1/evidence/items/{id}`, `GET /v1/research-jobs/{id}/citations`.
- Drafter evidence packs enforce max 8 items, max 1,500 Unicode code points per item display text, and max 12,000 UTF-8 bytes across those display texts only (IDs/URIs/labels/prompt framing are outside the byte budget).
- Markdown normalization (`markdown.normalize.v1`) preserves structural whitespace (fenced/indented code, nested lists, tables, internal spaces); it is not plain-text whitespace collapsing. Normalized Markdown that contains only whitespace is rejected. IPv6 URL literals are rejected in Milestone 10A.

### Embeddings and semantic retrieval (Milestone 10B)

- Local Postgres uses `pgvector/pgvector:pg16` (Compose + CI). One database continues to hold relational and vector data.
- Alembic `20260809_0008` enables `vector`, creates `evidence_embeddings` with PK `(evidence_item_id, embedding_profile)`, FK to `evidence_items` `ON DELETE CASCADE`, fixed `vector(1536)`, `dimensions = 1536` check, and partial HNSW cosine index `ix_evidence_embeddings_hnsw_cosine` for `embedding_profile = 'embeddings.v1'` (keeps the profile-scoped ANN candidate query index-eligible).
- Profile `embeddings.v1`: OpenAI live model `text-embedding-3-small` at 1536 dimensions via LangChain only. Default provider is deterministic `fake` (stable digest-based token features; tests pipeline behavior, not real semantic quality). Optional `openai` requires credentials; composition constructs providers.
- Embedding identity is `(evidence_item_id, embedding_profile)`. New evidence is embedded after evidence commit; idempotent document replay does not duplicate embeddings; missing embeddings can be backfilled; existing rows are never overwritten silently. Provider calls run outside long DB transactions with bounded batch/item caps. Provider/config/timeout failures become typed `atlas.embeddings` errors. Partial failure: evidence can exist without an embedding when the provider fails after evidence persistence; backfill recovers.
- `EvidenceRetriever` embeds the query, then retrieves with cosine distance, deterministic `(distance, evidence_item_id)` ordering, `k` default 5 / hard max 8, optional source-kind / strength / job-link filters, and full evidence→document→source provenance on every hit.
  - **Exact mode** (offline CI metric gate): transaction-local `SET LOCAL enable_indexscan/bitmapscan = off` forces a non-approximate scan for deterministic cosine ranking. Settings are transaction-scoped and do not leak across pooled connections.
  - **HNSW mode** (production-oriented local default via `ATLAS_RETRIEVAL_USE_HNSW`): an inner candidate query orders solely by `embedding <=> query` with `LIMIT` (optionally over-fetching up to `HNSW_CANDIDATE_MULTIPLIER`, capped by `MAX_HNSW_CANDIDATES` when filters apply), then metadata filters run on candidates, then final deterministic `(distance, evidence_item_id)` ordering capped to `k`. This shape is eligible for `ix_evidence_embeddings_hnsw_cosine` and is verified by an EXPLAIN integration test under controlled test-only planner settings (`enable_seqscan = off`). PostgreSQL still owns normal production planner decisions; Atlas does **not** claim every production query is guaranteed to use HNSW. HNSW filtered retrieval uses a bounded over-fetch candidate pool, currently capped at 64. Highly selective metadata filters may return fewer than `k` results when matching evidence falls outside that approximate candidate pool. Exact mode remains available when deterministic exhaustive behavior is required.
- Typed embedding failures from document ingest map to the shared `ErrorResponse` envelope (`embedding_invalid_request`→422, `embedding_conflict`→409, auth/timeout/rate-limit/provider→503) with sanitized messages only. Evidence may already be committed before the embedding error (partial failure); replay/backfill does not duplicate rows.
- Implemented embedding profile is restricted to `Literal["embeddings.v1"]` in settings (plus adapter defense-in-depth).
- Research node: after search evidence persists, retrieve operator-corpus (and appropriate filters), link retrieved IDs to the job, merge/dedupe with search IDs preserving rank order, apply evidence-pack caps, draft, and validate citations. No invented citations when retrieval yields nothing usable. Checkpoints lacking Slice 10B fields remain backward-compatible (runtime context carries retriever; not checkpointed).
- Offline eval fixture + Recall@K / MRR@K gate (Recall@5 ≥ 0.80, MRR@5 ≥ 0.70) validates deterministic fake embeddings and **exact** cosine pipeline geometry — not real-world semantic quality. Opt-in live embedding checks use `ATLAS_ENABLE_LIVE_EMBEDDING_TESTS=1` and never run in default CI.

### Recovery, repair, and retry policy (Milestone 12 Slice 12B)

- Graph topology (Slice 12B): `validate → plan → research → draft → verify_citations → evaluate → policy → {complete | repair | await_review | terminal}`. `repair → draft` (re-enter draft only; never plan/research). `await_review → complete`. Compiled with `interrupt_after=["await_review"]` by default; passing policy → complete is NOT interrupted because `await_review` is not on that path (proven by spike).
- Recovery policy in `atlas.recovery.policy`: `decide_for_evaluation` routes based on hard/soft dimension failures and attempt caps (max repairs=1, max retries=2, max eval attempts=4). `decide_for_exception` categorizes transient vs permanent exceptions for retry/terminal. Exception categorization uses an `isinstance` registry with typed categories (TRANSIENT, PERMANENT, TERMINAL ownership/conflict) and string-name fallback for third-party exceptions only.
- Structure repair policy: `QUALITY_STRUCTURE` with `repair_count==0` triggers repair (`STRUCTURE_REPAIR`); `repair_count>=1` is terminal. `QUALITY_CITATION_INTEGRITY` and `QUALITY_TOOL_POLICY` are immediate terminal. Ownership/conflict exceptions (`ClaimOwnershipError`, evaluation ownership/conflict) are always terminal — never retried.
- Exponential backoff: `delay = min(max_backoff, base * 2^(attempt-1)) + bounded_jitter`. Settings: `retry_base_seconds` (5.0), `retry_max_backoff_seconds` (60.0), `retry_jitter_max_seconds` (0.0 for deterministic local/CI).
- Processor bind logic: REVIEW_COMPLETE resumes active execution with `thread_id=execution_id`, verifies checkpoint `next==("complete",)`, invokes complete only. Lease reclaim (`NONE` + active RUNNING) resumes that execution. Policy `JOB_RETRY` abandons unfinished and creates a new execution with `thread_id=execution_id`. Fresh NONE without a resumable active execution does the same.
- `ProcessingOutcome` variants: `CompletedProcessing` (worker finalizes COMPLETED), `PausedForReview` (processor transitioned to AWAITING_REVIEW; worker does not finalize), `RetryScheduled` (processor transitioned to delayed PENDING with JOB_RETRY; worker does not finalize), `TerminalFailed` (processor failed execution; worker finalizes FAILED).
- Operator review API: `POST /v1/research-jobs/{job_id}/review-decisions` with required `Idempotency-Key`. Returns 404 when `ATLAS_REVIEW_API_ENABLED=false` (default). Approve transitions AWAITING_REVIEW → delayed PENDING with REVIEW_COMPLETE mode and active execution binding. Reject transitions AWAITING_REVIEW → FAILED. Same key + same fingerprint replays with 202; different fingerprint returns 409.
- Claim-fenced report persistence: `ReportArtifactService.persist_final` requires `claim_token` and verifies the job is RUNNING with matching claim_token, valid lease (`lease_expires_at IS NOT NULL` and `lease_expires_at > now`), in the same transaction as the artifact insert. `EvidenceOwnershipError` on failure. Accepts explicit `at: datetime | None` for testability.
- `DispositionHint` expanded to include `repair`, `await_review`, `retry` in addition to `complete` and `terminal`. Evaluation runner disposition stays `complete`/`terminal`; policy engine writes graph state disposition. Policy decisions are persisted in `policy_decisions` table with `decision_fingerprint CHAR(64)` unique constraint `(research_job_id, decision_fingerprint)` for idempotent replay. Inserts use transaction-safe `INSERT … ON CONFLICT DO NOTHING RETURNING id` (never `session.rollback()` inside the caller TX); callers use the returned authoritative id. Identical replay reuses the row; same fingerprint with inconsistent fields raises `PolicyDecisionConflictError`. Fingerprint is SHA-256 of canonical JSON inputs (job_id, execution_id, eval_run_id, decision, category, reason, repair/retry/eval counts).
- Repository: `schedule_retry` increments `job_retry_count` by 1. `increment_repair_count` and `increment_evaluation_attempt_count` are claim-fenced with hard caps (1 and 4 respectively). All claim-fenced methods accept explicit `at: datetime` for lease-valid fencing.
- Claim ownership validation (`_owns_running_claim`): requires status==RUNNING, exact `claim_token` match, `lease_expires_at IS NOT NULL`, and `lease_expires_at > at` (strictly later). All claim-fenced repository mutations pass explicit `at`. Processor-owned workflow execution `complete`/`fail`/`abandon` use claim-aware methods that also require the job’s active execution binding and matching execution ownership in the same statement. Retry scheduling verifies the claim before abandoning an execution and transitioning the job in one transaction. Operator rejection fails the paused execution under the AWAITING_REVIEW job lock (no worker claim).
- Fail-closed mutations: processor raises `ClaimOwnershipError` (sanitized, no tokens) on `False` from any claim-fenced mutation. Worker catches `ClaimOwnershipError` and calls `finalize_failure` which becomes a safe no-op when the claim is already lost. Processor never returns `PausedForReview`, `RetryScheduled`, or `CompletedProcessing` after ownership loss.
- Evaluation attempt accounting: job-global `evaluation_attempt_count` increments in the same transaction as a **new** `evaluation_runs` insert only (`EvaluationService.begin_or_resume`). Reclaim/replay of an existing attempt does not increment. Cap of 4 is claim-fenced; exceeding raises `EvaluationAttemptCapError`. Crash before commit rolls back both.
- Lease reclaim resumes the same active RUNNING execution when safe. Policy job retry creates a new workflow execution and checkpoint thread — do not conflate these behaviors.
- Structure failures: first `report_structure` failure with `repair_count=0` → repair; after one repair → terminal. Citation-integrity and tool-policy remain immediately terminal.
- Continuation modes: scheduling persists `JOB_RETRY` / review approval persists `REVIEW_COMPLETE`; `claim_next` copies pending mode into durable `claimed_continuation_mode`, clears pending mode, and returns the consumed mode on `ClaimedResearchJob`. Lease reclaim reads `claimed_continuation_mode`. `ContinuationMode` is a `StrEnum` (values: NONE, JOB_RETRY, REVIEW_COMPLETE).
- Composite FK `(active_workflow_execution_id, id) → workflow_executions(id, research_job_id)` with `ON DELETE NO ACTION`; pointer cleared on terminal transitions.
- Complete node authorization: passing evaluation **or** durable human-review approve override matching `fingerprint_grading_snapshot` for that exact execution/candidate. Disposition `await_review` alone never authorizes persistence.
- Worker orchestration timeout (`FuturesTimeoutError`) remains terminal for this milestone (no auto-retry while the processor thread may still run).
- Alembic head `20260809_0010` (0009 unchanged).

These decisions cover the verified foundation through Milestone 11 Complete on `main` (`c5d4749`), Milestone 12 Slices 12A and 12B merged to `main` through Pull Request #17 (`e3412c3`), and the local, not-yet-merged Milestone 12 human calibration closeout. They do not imply production semantic retrieval quality, messaging, or cloud topology choices.

### Specialist agents (Milestone 11)

- Package `atlas.specialists` owns typed handoffs for planner, research/retrieval, synthesizer, and deterministic citation verifier.
- LangGraph topology (pre-12A): `validate → plan → research → draft → verify_citations → complete`. The synthesizer runs behind the existing `draft` node name; model-ledger identity remains `plan`/`draft`.
- Capability isolation is composition-proven: planner holds only the model planner port; research alone receives the governed executor and retriever; synthesizer receives drafter + evidence-pack ingest; citation verifier receives only deterministic citation/evidence services; `complete` formats and persists only.
- Planner retains the established exactly-three-task contract. Research findings are bounded (`MAX_RESEARCH_FINDINGS=6`) and are never padded/fabricated. Research evidence IDs are merged with order-preserving first-seen deduplication (tool/search first, then retrieval). A retriever may not be composed without evidence ingest (fail-closed configuration); ingest-only or neither is allowed for isolated tests.
- Synthesizer rejects claims citing evidence IDs outside the supplied pack (no silent stripping). Citation verifier fail-closes against durable `evidence_job_links` plus evidence→document→source resolution and does not write model- or tool-ledger rows. It is deterministic architecture enforcement, not a Milestone 12 model grader.
- `ReportArtifactService.persist_final` continues to invoke `CitationValidator` as defense in depth after the verifier node.
- Ledger/audit reuse only: existing `workflow_node_executions`, `model_invocations`, and `tool_invocations`. No specialist ledger table. Milestone 11 required no migration (`workflow_node_executions.node_name` accepts arbitrary non-empty strings).
- Slice 11B proves attribution (`plan`/`draft` model rows; `research` tool rows; `verify_citations` audit without model/tool rows), labeled boundary/ablation evidence, and full API→worker→citations E2E with resume idempotency of report artifacts, claims, citations, and evidence-job links.
- Milestone 11 is Complete through Pull Request #16 (`c5d4749`) after PR #14 merge / PR #15 process revert / restoration.

### Candidate evaluation (Milestone 12 Slice 12A)

- Package `atlas.evaluation` owns candidate grading contracts, deterministic graders, typed evaluation/provenance ports, Fake semantic grader, deferred live semantic scaffold, fenced persistence, and the evaluation runner.
- Topology: `validate → plan → research → draft → verify_citations → evaluate → policy → complete|terminal`. Accepted `report_artifacts` persist **only** in `complete` after a passing evaluation. Failed candidates raise `EvaluationTerminalError` via `terminal` and must not create accepted artifacts.
- Profile `evaluation.candidate.v1` is provisional (not frozen `evaluation.v1`). Expanded `candidate_goldens.v1` fixtures are calibration-development / regression examples — **not** an independent validation set and **not** human-reviewed production calibration.
- Hard gates (block pass): `citation_integrity`, `tool_use`, `report_structure` (empty/missing candidate output).
- Provisional soft gates (also block Slice 12A pass pending human golden approval): `coverage`, `completeness`, `lexical_id_groundedness` at threshold ≥ 0.70. Lexical ID overlap is **not** semantic groundedness.
- Optional `semantic_groundedness` is skipped in default composition. Offline Fake grader is used in tests. Live LangChain semantic evaluation is **deferred** until later in Milestone 12 (`DeferredSemanticGroundednessPort`); a skipped placeholder is not live-verification evidence.
- Tool-use grader loads logical `tool_invocations` for the current research job **and** current workflow execution only. It rejects non-`research` WORKFLOW rows, rejects unknown origins, enforces logical-call budget (`tool_max_logical_calls_per_research_node`, default 6), distinguishes logical calls from physical attempts, and allows a legitimate zero-tool path.
- Complete grading fingerprints are computed **after** loading durable inputs: linked evidence ids, execution-scoped tool summary rows, logical-call budget summary, provenance outcome, and candidate plan/findings/draft/claims (hashed/canonical ids only — no evidence bodies or raw prompts). `input_fingerprint` is internal to persistence/replay diagnostics and is **not** exposed on public evaluation API responses.
- Provenance checks use typed ports (no `getattr` discovery). When claims exist, missing citation validator or evidence resolver yields `provenance_ok=false`; unexpected resolver failures raise sanitized `EvaluationTerminalError`.
- Evaluation ownership fencing: cryptographically random 64-hex `ownership_token`; finalize requires `IN_PROGRESS` + matching token. Create/reclaim require proof of the current valid research-job claim token (presented in-memory only). Persisted `job_claim_fingerprint` is SHA-256 of the originating claim — never the raw token. Stale reclaim: unexpired → in progress; expired + same live originating claim blocks competing owners (they cannot prove the current claim); expired + new valid processing claim may reclaim; anonymous reclaim is rejected. Same attempt + different input fingerprint conflicts. After claim, typed/unexpected grader failures finalize `FAILED` with sanitized error classes; never finalize after `EvaluationOwnershipLostError`. Evaluation finalization still requires the independent evaluation ownership token.
- Job/execution consistency: service validates `workflow_executions.research_job_id` matches the evaluation job; Alembic `20260809_0009` also adds composite FK `(workflow_execution_id, research_job_id)` via unique `(id, research_job_id)` on `workflow_executions`.
- Uniqueness: `(workflow_execution_id, evaluation_profile, evaluation_attempt)`. Model ledger node_name CHECK allows `evaluate` for future LLM attribution.
- Read API: `GET /v1/research-jobs/{id}/evaluation` (run id, profile, attempt, status, passed, aggregate score, disposition, dimensions, safe grader versions); job GET may include sanitized `evaluation_summary`.
- Slice 12A and Slice 12B (including the correction pass) merged into `main` through Pull Request #17 (`e3412c3`; PR CI and resulting `main` CI passed).
- Human calibration closeout (2026-08-10): the project owner reviewed `candidate_goldens.v1` and approved the judgment that a well-supported report fulfilling the plan via semantically equivalent paraphrase is acceptable, even though the provisional lexical `completeness` heuristic scores it as a failure. The fixture and test harness now separate `grader_expected` (deterministic-grader regression expectation) from `human_expected` (separate human quality judgment) for every graded case; `tests/evaluation/test_golden_candidates.py` runs a grader-regression check and a separate human-calibration check that reports TP/FP/FN/TN/precision/recall/F1 against `human_expected` (F1 is intentionally not forced to `1.0`). Three requirement-driven fixtures were added to close observed coverage gaps: `fail_incomplete_provenance` (`provenance_ok=false`), `fail_empty_plan` (`STRUCTURE_EMPTY_PLAN`), and `fail_missing_required_section` (`STRUCTURE_MISSING_SECTION` via a narrowly-scoped test-only `preview_report_override`; production formatting always emits all required labels). The `golden_facets_hit` override case is explicitly labeled fixture-only scaffolding because `atlas.workflow.graph` never populates that field in production. This closeout changed no production grader thresholds, policy behavior, database schema, or Alembic migration.
- Milestone 12 remains Current: the evaluation/recovery implementation is merged to `main`, but the calibration-closeout change is local-only pending its own PR CI and resulting `main` CI. Do not freeze `evaluation.v1`; freezing remains deferred pending an independent held-out human-labeled set and/or a live semantic grader.

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
