# Atlas AI Platform — Project State

- Last updated: 2026-08-10
- Phase: Local implementation foundation
- Milestone: Evaluation, grading, repair, and retry policy (Milestone 12) — **Current** (implementation merged to `main` via Pull Request #17; human calibration review approved 2026-08-10; calibration-closeout implemented locally on `milestone-12-calibration-closeout` and pending its own PR CI / resulting `main` CI)
- Implementation status: Milestone 11 is **Complete** through Pull Request #16 (`c5d4749`; PR CI and resulting `main` CI green). Milestone 12 Slice 12A + Slice 12B (evaluation, repair, job-level retry, human review, operator review API) merged to `main` through Pull Request #17 (`e3412c3`; PR CI and resulting `main` CI passed). The project owner completed the human calibration review on 2026-08-10 and approved the paraphrase-completeness judgment (see Decisions). The calibration closeout implementing that judgment (grader/human label separation, three new requirement-driven fixtures, documentation) is implemented locally and verified; it remains **pending its own PR CI and resulting `main` CI** before Milestone 12 is marked Complete. Do not freeze `evaluation.v1` yet. Do not mark Milestone 12 Complete yet. Do not mark Milestone 13 Current yet.

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with validated local foundation through Milestone 11 on `main`.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files; committed `.env.example` (no secrets).
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with FastAPI `GET /health` (liveness) and `GET /ready` (Postgres readiness).
- Pytest, Ruff (format + lint), and mypy configuration; domain, API, worker, workflow, model, tool, MCP, evidence, embedding, specialist, and PostgreSQL/pgvector integration tests.
- GitHub Actions CI with Postgres 16 + pgvector (`pgvector/pgvector:pg16`) targeting `atlas_test`; `main` is green through Pull Request #17 (Milestone 12 evaluation/recovery implementation merged as `e3412c3`).
- `atlas.domain` package with slotted `ResearchJob`, `reconstitute(...)`, and lifecycle transitions.
- Docker Compose PostgreSQL 16 + pgvector published on `127.0.0.1:5433` only, with databases `atlas` and `atlas_test`. Development credentials are local-only.
- SQLAlchemy 2.x + psycopg3 + Alembic persistence through head `20260809_0010` (recovery/review continuation fields + policy/recovery/review tables; prior `20260809_0009` evaluation foundation).
- Research-job HTTP APIs plus evidence APIs: `POST /v1/evidence/documents`, `GET /v1/evidence/items/{id}`, `GET /v1/research-jobs/{id}/citations`, `GET /v1/research-jobs/{id}/evaluation` (no public `input_fingerprint`). Local-only operator review: `POST /v1/research-jobs/{id}/review-decisions` (404 when `ATLAS_REVIEW_API_ENABLED=false`, the default).
- Background worker (`python -m atlas.worker`) with PostgreSQL claiming, fencing, and LangGraph orchestration (Milestones 6–9).
- LangGraph research workflow (Milestone 12): validate → plan → research → draft → verify_citations → evaluate → policy → complete|repair|await_review|terminal. Accepted reports persist only after a passing evaluation or fingerprint-bound human-review override. Checkpoint `thread_id = workflow_execution_id`.
- Recovery package (`atlas.recovery`): deterministic failure categories, policy decisions, bounded repair (≤1), job-level transient retries (≤2), evaluation attempts (≤4), exponential backoff with injectable jitter.
- Typed `ProcessingOutcome` contract: `CompletedProcessing`, `PausedForReview`, `RetryScheduled`, `TerminalFailed` with exhaustive worker handling.
- Bounded specialist package (`atlas.specialists`): typed handoffs, fail-closed citation verification, synthesizer pack-scope validation, capability isolation, ledger/audit attribution, boundary/ablation evidence, full cited-report E2E.
- Model-provider adapters (`atlas.models`): LangChain boundary; default `fake`; draft schema includes optional claims; prompt version `draft.v2`.
- Embeddings (`atlas.embeddings`): profile `embeddings.v1`, 1536-d, default `fake` deterministic embedder, optional LangChain OpenAI `text-embedding-3-small`.
- Governed research tools (`atlas.tools`): search results persist as sources/documents/evidence and job links; live fetch still unavailable.
- FastMCP stdio server unchanged in role (Milestone 9).
- Evidence package (`atlas.evidence`): contracts, URL canonicalization, normalize/chunk, ingest, embedding service, retriever, citation validator, report artifact service, offline retrieval metrics.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- Frozen/calibrated `evaluation.v1` production thresholds. `candidate_goldens.v1` is human-reviewed (project owner, 2026-08-10) as a small regression + calibration baseline, but it is explicitly **not** a held-out validation set, **not** independent statistical validation, and **not** proof of production semantic quality (see fixture `_meta` and `docs/TESTING.md`).
- Live LangChain semantic groundedness grader (typed deferred scaffold only; Fake offline grader for tests; dimension skipped in default composition).
- Redis, Kafka, application/worker Docker images, Kubernetes, Terraform, or AWS resources.
- Heartbeat lease renewal or hard cancellation of in-flight processor threads (orchestration timeout remains terminal for this milestone; no auto-retry while the thread may still run).
- Exactly-once provider/tool billing guarantees after crash between provider success and ledger commit.
- Review API authentication/RBAC (local flag-gated endpoint only).
- Frozen `evaluation.v1` (profile remains provisional `evaluation.candidate.v1`).
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
- LangGraph owns node progression and durable checkpoints (`PostgresSaver`, tables via one-time worker-startup `setup()`). Atlas Alembic owns `workflow_executions` / `workflow_node_executions` / model and tool invocation ledgers / evidence tables / embeddings / recovery tables. Checkpoint, audit, ledger, evidence, and embedding writes are not one atomic transaction and may briefly disagree after a crash.
- One `workflow_executions` row per worker processing attempt; Slice 12B uses `thread_id = execution_id` (not `job_id`) for fresh executions and resumes active executions by their own execution_id. Lease reclaim resumes the same active RUNNING execution when safe. Policy job retry clears `active_workflow_execution_id` and creates a new execution/checkpoint thread after abandoning prior RUNNING rows. Node history stores one row per `(workflow_execution_id, node_name, attempt)`.
- Workflow/node failure handling catches `Exception` only; process-control exceptions propagate. Persisted node errors are class-only (`<ExceptionClass>: node execution failed`) with no raw exception text.
- Graph nodes never finalize `ResearchJob` rows.
- LangGraph topology (Slice 12B): `validate → plan → research → draft → verify_citations → evaluate → policy → {complete | repair | await_review | terminal}`. `repair → draft` (re-enter draft only). `await_review → complete`. `interrupt_after=["await_review"]` by default. Passing policy → complete is NOT interrupted.
- Processor bind logic: REVIEW_COMPLETE resumes active execution, verifies checkpoint next==("complete",), invokes complete only. Lease reclaim (`NONE` + active RUNNING) resumes that execution. Policy `JOB_RETRY` always creates a new execution after abandoning unfinished. Fresh NONE without resumable active creates new execution after abandoning unfinished.
- Recovery policy: `decide_for_evaluation` routes to complete/repair/await_review/terminal based on hard/soft dimension failures and attempt caps. `decide_for_exception` routes to retry (transient within cap) or terminal. Backoff: `retry_base_seconds * 2^(attempt-1)` capped at `retry_max_backoff_seconds` plus bounded jitter.
- Exception categorization uses `isinstance` registry (transient/permanent/terminal-ownership) with string-name fallback for third-party exceptions only. `ClaimOwnershipError` and evaluation ownership/conflict exceptions are always terminal — never retried. Worker `Future.result` orchestration timeout is terminal in the worker (not via policy `isinstance` on `TimeoutError`, which is aliased to `concurrent.futures.TimeoutError` on modern Python).
- Structure repair: `QUALITY_STRUCTURE` with `repair_count==0` triggers repair; `repair_count>=1` is terminal. `QUALITY_CITATION_INTEGRITY` and `QUALITY_TOOL_POLICY` are immediate terminal.
- Policy decision idempotency: `decision_fingerprint CHAR(64)` with `UNIQUE(research_job_id, decision_fingerprint)` on `policy_decisions`. Inserts use `ON CONFLICT DO NOTHING RETURNING id` (no inner `session.rollback()`). Callers capture the returned authoritative id for recovery-attempt FKs. Same fingerprint with inconsistent fields fails closed via `PolicyDecisionConflictError`.
- `ContinuationMode` is a `StrEnum` (not a plain class with string constants). `ClaimedResearchJob` carries typed `continuation_mode: ContinuationMode`.
- Claim-fenced mutations are fail-closed: all return `bool`; processor raises `ClaimOwnershipError` on `False` (never ignores). Worker catches `ClaimOwnershipError` and calls `finalize_failure` which becomes a safe no-op when the claim is already lost.
- `_owns_running_claim` validates status==RUNNING, exact claim_token, `lease_expires_at IS NOT NULL`, and `lease_expires_at > at`. All claim-fenced repo methods accept explicit `at: datetime`. Processor-owned execution terminalization is claim-fenced (`complete_execution_for_claim` / `fail_execution_for_claim` / `abandon_execution_for_claim`). `schedule_retry` verifies claim then optionally abandons the active execution before the job transition in one TX.
- Evaluation attempt accounting: job-global `evaluation_attempt_count` is incremented in the same Postgres transaction as a **new** `evaluation_runs` insert only (inside `EvaluationService.begin_or_resume`). Reclaim/replay of an existing attempt does not increment. Cap of 4 is claim-fenced; exceeding raises `EvaluationAttemptCapError`. Crash before commit rolls back both the counter and the run.
- Lease reclaim resumes the same active RUNNING workflow execution/checkpoint when safe. Policy job retry creates a **new** workflow execution and checkpoint thread after abandoning the prior attempt — do not conflate these as “reclaim.”
- Processor bind: REVIEW_COMPLETE resumes the interrupted execution; JOB_RETRY/fresh NONE create new executions after abandoning unfinished; mid-run lease reclaim with active RUNNING execution resumes that execution.
- Operator review API: `POST /v1/research-jobs/{job_id}/review-decisions` with Idempotency-Key. Returns 404 when `ATLAS_REVIEW_API_ENABLED=false` (default). Approve → AWAITING_REVIEW → delayed PENDING with REVIEW_COMPLETE. Reject → FAILED.
- Report sections: `Question`, `Plan`, `Findings`, `Draft`, and `Citations` when claims exist.
- LangChain `BaseChatModel` is the model boundary; provider SDKs and `ChatOpenAI`/`ChatAnthropic` stay in composition only.
- Model/tool wiring uses LangGraph `WorkflowRuntimeContext` + `context_schema` + `invoke(..., context=...)` + `Runtime[...]`; not a module-level `ContextVar`.
- Default model and tool providers are `fake`; real providers require explicit selection and credentials. Plan/draft are model-backed; research uses governed tools; findings remain `list[str]` (max 4 KiB each, `[untrusted_source]` labeled) plus durable `evidence_item_ids`.
- Model invocation ledger is two tables (`model_invocations`, `model_invocation_attempts`) with fencing as in Milestone 8. Model ledger `node_name` CHECK allows `plan`, `draft`, and (Slice 12A) `evaluate` when an LLM grader is attributed.
- Tool invocation ledger is two tables (`tool_invocations`, `tool_invocation_attempts`) with origin `WORKFLOW|MCP`, replay, stale reclaim, and conditional finalization fencing. Logical keys include `tool_policy_version`. MCP rows keep workflow FKs NULL and stamp a per-process `actor_id` UUID (never from tool args). Tool allowlist is research-node only.
- Tool budgets (orchestration only): 6 logical calls / research node, 2 attempts / call, 8s attempt timeout, 45s research-node deadline, remaining-budget checks. Budget exhaustion raises `ToolBudgetExhaustedError` (not silent partial success).
- Live search uses Tavily via direct streaming `httpx` with `Content-Length` pre-checks, streamed byte caps before JSON deserialize, and required JSON content-type. Live arbitrary-URL fetch is not enabled. `ATLAS_TOOL_FETCH_ENABLED=true` fails closed at composition. Under `tool_provider=tavily`, `fetch_url` is omitted from the tool registry. HTML extraction and full SSRF-safe live fetch are deferred.
- Fake fetch under `tool_provider=fake` validates scheme/port/userinfo only (no DNS resolution).
- FastMCP stdio is real (`python -m atlas.mcp`); production LangGraph research does not route through MCP. Controlled Atlas `ToolError`s are raised as FastMCP `ToolError`s (sanitized class-only messages; `mask_error_details=True`).
- Workflow tool ledger rows stamp `workflow_node_attempt` from the research-node audit hook attempt (no module globals / `ContextVar`).
- Milestone 10 is Complete through Pull Request #13 (`bfabd59`). Document identity, hash semantics, citation composite FK, idempotent report artifacts, embeddings profile `embeddings.v1`, exact vs HNSW retrieval, and offline fake-embedding eval remain as validated in Milestone 10.
- Embedding identity is `(evidence_item_id, embedding_profile)`; existing embeddings are never overwritten silently. Profile is settings-restricted to `Literal["embeddings.v1"]` at 1536 dimensions with a partial HNSW cosine index for that profile.
- Retrieval: exact cosine for offline CI metric gate; HNSW-eligible candidate path for production-oriented local default. Offline thresholds Recall@5 ≥ 0.80 and MRR@5 ≥ 0.70 validate fixture geometry, not real semantic quality. Opt-in live embedding tests require `ATLAS_ENABLE_LIVE_EMBEDDING_TESTS=1`.
- Local Compose Postgres binds to `127.0.0.1:5433` only so the development database is not published on all interfaces.
- Milestone 11 is Complete through Pull Request #16 (`c5d4749`) after an accidental PR #14 merge / PR #15 revert / restoration sequence; restoration was not due to a code defect.
- Milestone 12 evaluates candidates before accepted report persistence; Slice 12A is pass/terminal only (no repair, job-level retry, or human review yet). Evaluation reclaim is job-claim-aware; fingerprints cover the complete durable grading snapshot; tool grading is execution-scoped with logical-call budgets; owned evaluation failures finalize `FAILED` with sanitized error classes.
- Pull Request #17, `feat: add evaluation grading and recovery workflows (#17)`, merged Milestone 12 Slice 12A + Slice 12B into `main` as commit `e3412c3`. PR CI and resulting `main` CI passed.
- Human calibration review of `candidate_goldens.v1` was completed by the project owner on 2026-08-10. Approved judgment: a well-supported report that fulfills the plan via semantically equivalent paraphrase is acceptable, even though the provisional lexical `completeness` heuristic scores it as a failure. This is recorded as one approved known false negative, not a grader defect to fix in this closeout.
- `candidate_goldens.v1` fixtures now separate `grader_expected` (deterministic-grader regression expectation) from `human_expected` (separate human quality judgment) for every graded case. `tests/evaluation/test_golden_candidates.py` runs two separate checks: grader regression (actual output vs. `grader_expected`) and human calibration (actual grader output vs. `human_expected`, reporting TP/FP/FN/TN/precision/recall/F1; F1 is intentionally not 1.0).
- Three requirement-driven graded fixtures were added to close known coverage gaps found during the calibration review: `fail_incomplete_provenance` (`provenance_ok=false` → `CITATION_PROVENANCE_INCOMPLETE`), `fail_empty_plan` (`STRUCTURE_EMPTY_PLAN`), and `fail_missing_required_section` (`STRUCTURE_MISSING_SECTION` via a narrowly-scoped test-only `preview_report_override`; production formatting always emits all required labels, so this is formatter/structure-gate regression coverage, not an observed production defect). The fixture harness also gained an optional case-level `provenance_ok` field (default `true`).
- The `golden_facets_hit` coverage-override fixture is explicitly labeled fixture-only scaffolding: `atlas.workflow.graph` never populates `golden_facets_hit`/`golden_completeness_ratio` in production, so that branch and its human-calibration case do not validate live production coverage quality. Focused unit tests for the `golden_completeness_ratio` override branch and the `STRUCTURE_EMPTY_PLAN`/`STRUCTURE_MISSING_SECTION` codes were added to `tests/evaluation/test_graders.py`.
- Calibration-closeout final counts: 25 total fixture cases — 23 graded, 2 fingerprint-only. Human-calibration confusion matrix (actual grader output vs. `human_expected`): TP=8, FP=0, FN=1, TN=14; human-positive=9, human-negative=14; precision=1.0, recall=8/9, F1=16/17. The single FN is the approved paraphrase case; the test fails if any other disagreement appears.
- `evaluation.candidate.v1` remains provisional. Frozen `evaluation.v1` remains deferred and unfrozen pending an independent held-out human-labeled set and/or a live semantic grader.

## Verification (Milestone 9)

### Remote

- Pull Request #12, `feat: add governed research tools and MCP (#12)`, merged into `main` as commit `0db343d`.
- Pull-request CI and resulting `main` CI passed (user-verified). Milestone 9 is **Complete**.

## Verification (Milestone 10)

### Remote

- Pull Request #13, `feat: add evidence-grounded RAG with pgvector (#13)`, merged into `main` as commit `bfabd59`.
- Pull-request CI and resulting `main` CI passed (user-verified). Milestone 10 is **Complete**.

## Verification (Milestone 11)

### Remote

- Pull Request #14 originally merged Milestone 11 as `005ea58` (PR CI and resulting `main` CI passed), then Pull Request #15 reverted it as `e675f43` due to a workflow/process error (not an identified code defect).
- Pull Request #16, `Restore milestone 11 specialists (#16)`, merged into `main` as commit `c5d4749`. Pull-request CI and resulting `main` CI passed (user-verified). Milestone 11 is **Complete**.

## Verification (Milestone 12)

### Remote

- Pull Request #17, `feat: add evaluation grading and recovery workflows (#17)`, merged Slice 12A + Slice 12B into `main` as commit `e3412c3`. PR CI and resulting `main` CI passed (user-verified).

### Local — Slice 12A + 12B (through Pull Request #17)

- Slice 12A: evaluate-before-complete; fenced evaluation; claim-aware reclaim; complete grading fingerprints; execution-scoped tool grader; provisional `evaluation.candidate.v1`.
- Slice 12B: typed `ProcessingOutcome`; per-execution checkpoint identity; durable continuation modes + `claimed_continuation_mode`; bounded repair/retry/review; claim-fenced persist; fingerprint-bound human override; review API 404 when disabled; LangGraph `await_review` interrupt spike proven on installed LangGraph 1.2.10; migration `20260809_0010`.
- Slice 12B correction pass: `ClaimOwnershipError` + fail-closed mutations; `_owns_running_claim` validates lease_expires_at > at; all claim-fenced methods accept explicit `at`; `ContinuationMode` is `StrEnum`; typed isinstance exception registry (fallback string matching for third-party only); structure failures repair once then terminal; `increment_evaluation_attempt_count` wired into evaluation create path; policy decision idempotent fingerprint via `ON CONFLICT DO NOTHING RETURNING id` (no inner rollback); claim-fenced workflow execution complete/fail/abandon; worker catches `ClaimOwnershipError` as safe no-op finalization.

### Local — human calibration closeout (2026-08-10, branch `milestone-12-calibration-closeout`, based at `e3412c3`)

- Project-owner human calibration review completed and approved 2026-08-10, including the paraphrase-completeness judgment (see Decisions).
- `candidate_goldens.v1.json` and `tests/evaluation/test_golden_candidates.py` updated: `grader_expected`/`human_expected` separation for every graded case; three new requirement-driven fixtures (`fail_incomplete_provenance`, `fail_empty_plan`, `fail_missing_required_section`); `provenance_ok` and `preview_report_override` case-level fixture fields; fixture metadata records `human_reviewed: true`, `human_reviewer: "project_owner"`, `reviewed_at: "2026-08-10"`, `frozen_profile: false`, and explicit not-held-out / not-independent-statistical-validation / not-production-semantic-quality-proof statements.
- Test harness split into `test_candidate_goldens_grader_regression` (deterministic grader vs. `grader_expected`) and `test_candidate_goldens_human_calibration` (actual grader output vs. `human_expected`, with TP/FP/FN/TN/precision/recall/F1 and an explicit assertion that the paraphrase case is the sole approved false negative).
- Added unit coverage in `tests/evaluation/test_graders.py` for `STRUCTURE_EMPTY_PLAN`, `STRUCTURE_MISSING_SECTION`, and the `golden_completeness_ratio` override branch (both below- and at-threshold).
- Final fixture counts: 25 total cases — 23 graded, 2 fingerprint-only. Human-calibration confusion matrix: TP=8, FP=0, FN=1, TN=14; human-positive=9, human-negative=14; precision=1.0, recall=8/9, F1=16/17.
- No production grader thresholds, policy behavior, database schema, or Alembic migrations changed. No live semantic grader, Redis, Kafka, or Milestone 13+ work added.
- Quality gates (2026-08-10): `uv sync --frozen`; Ruff format/lint clean; mypy clean; isolated non-integration Pytest and full Pytest against `atlas_test` both green (see exact counts recorded after the verification run below); Alembic head unchanged at `20260809_0010` with `0010`↔`0009` round-trip still covered; `git diff --check` clean. Nothing committed or pushed.
- Milestone 12 remains **Current**: implementation is merged to `main` (Pull Request #17), but this calibration-closeout change is local-only and pending its own PR CI and resulting `main` CI. Do not freeze `evaluation.v1` yet. Do not mark Milestone 12 Complete until this closeout's PR/`main` CI pass.

## Next steps

1. Open a pull request for the `milestone-12-calibration-closeout` branch.
2. Do not mark Milestone 12 Complete until this closeout's PR CI and resulting `main` CI pass.
3. After Milestone 12 Complete, mark Milestone 13 Current — not before.
4. Do not freeze `evaluation.v1`; freezing requires an independent held-out human-labeled set and/or a live semantic grader, both deferred beyond this closeout.

## Active blockers

None. The calibration-closeout implementation is locally complete and verified; it is pending PR CI and resulting `main` CI before Milestone 12 can be marked Complete.
