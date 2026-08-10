# Atlas Testing and Evaluation Strategy

## Purpose

Atlas must verify conventional software behavior and probabilistic AI quality. Tests should arrive with each vertical slice, not after the platform is assembled.

## Test layers

- **Unit tests:** domain rules, state transitions, validation, budgets, retry decisions, and deterministic transformations.
- **Integration tests:** PostgreSQL/pgvector, Redis, Kafka, model/tool adapters, checkpoints, and migrations using realistic disposable dependencies.
- **Contract tests:** API schemas, events, tools, model-provider adapters, and compatibility between producers and consumers.
- **End-to-end tests:** submit, execute, retrieve, evaluate, fail, retry, resume, and complete representative research jobs.
- **AI evaluations:** golden tasks for citation support, evidence coverage, answer completeness, groundedness, tool selection, and policy compliance.
- **Resilience tests:** timeouts, duplicate events, unavailable dependencies, partial agent failure, poison messages, restart/recovery, and replay.
- **Performance tests:** API latency, concurrent jobs, worker throughput, queue lag, database behavior, and cost under controlled load.
- **Security tests:** authentication/authorization, secrets exposure, dependency/container/IaC scanning, prompt injection, unsafe tool use, and data leakage.
- **Infrastructure tests:** Terraform validation, policy checks, Kubernetes manifest/chart checks, deployment health, rollback, backup, and restore.

## Evaluation principles

- Begin with a small human-reviewed golden dataset before tuning prompts or agents.
- Version prompts, models, datasets, evaluators, and thresholds with results.
- Use deterministic assertions where possible and calibrated graders where judgment is necessary.
- Do not let a model grade itself as the only quality signal.
- Track quality together with latency, token usage, cost, and failure/recovery behavior.
- Review evaluator disagreements and prevent benchmark leakage.

## CI/CD quality gates

Pull requests should eventually run formatting/linting, type checks, unit tests, relevant integration and contract tests, migration checks, security scans, and fast AI regression tests. Main-branch or release workflows may run broader end-to-end, evaluation, container, infrastructure, and deployment verification.

Thresholds will be introduced from measured baselines. A failing required gate blocks promotion; production rollout must support health verification and rollback.

## First testing deliverable

When the first vertical slice is chosen, define its acceptance criteria, fixtures, failure cases, and a minimal golden example before implementing it.

## Research-job API testing (Milestone 5)

- Fast API/contract tests override the application service dependency and cover `202`/`200`/`404`/`409`/`422`, structured validation errors, narrow `OperationalError`→`503`, and non-hiding of unexpected failures.
- Application service unit tests use an in-memory repository fake implementing the Protocol, including idempotent replay via `ResearchJobIdempotencyRecord`.
- PostgreSQL integration tests cover durable create/get, idempotent replay/conflict, concurrent duplicate submissions, Alembic head through `20260808_0002`, and legacy-row survival from revision `0001`.

## Background worker testing (Milestone 6)

- Unit tests cover ordinary processor exceptions (safe failure reason, no secret leakage), orchestration timeout with late results ignored, bounded `close()` while a processor remains blocked, shutdown preventing new claims, and in-flight shutdown within grace.
- PostgreSQL integration tests cover concurrent `FOR UPDATE SKIP LOCKED` exclusivity (lock held across sessions), concurrent claims of two pending jobs, stale-token fencing after reclaim, API→worker→GET for success/failure/timeout, reclaim-then-stale-finalize rejection, and Alembic head through claim-lease migration `20260809_0003` (superseded locally by `20260809_0004` after Milestone 7).

## Deterministic LangGraph workflow testing (Milestone 7)

- Unit tests cover deterministic fake planner/tool output and end-to-end graph completion with an in-memory checkpointer for pure logic.
- Node-failure tests cover ordinary exceptions producing FAILED attempt records, class-only sanitized persisted errors (no raw exception text such as `sk-secret-value`), and process-control exceptions (`KeyboardInterrupt`) propagating without fail-audit handling.
- PostgreSQL resume test uses LangGraph `interrupt_after=["plan"]`, confirms the plan checkpoint, disposes processor/graph/checkpointer/connections, builds fresh B instances, resumes with `graph.invoke(None, config, context=...)` for the same `thread_id`, and proves `validate`/`plan` are not executed again.
- Integration tests cover API→worker→LangGraph→GET completion, report structure (`Question`/`Plan`/`Findings`/`Draft`, plus `Citations` when claims exist), per-attempt workflow/node history, abandon of prior `RUNNING` executions on a new attempt, and Alembic head through `20260809_0004` (superseded locally by later migrations; current head is `20260809_0007`).
- Suite isolation truncates Atlas audit/ledger tables and LangGraph checkpoint data between tests after one-time `PostgresSaver.setup()` at session start.

## Model-provider testing (Milestone 8)

- Default provider remains `fake`; normal CI makes no live provider network calls.
- Unit tests cover structured-output contracts, deterministic planner/drafter Protocols, versioned pricing estimates (null for unknown models), Atlas error categories, and mocked OpenAI/Anthropic adapter construction (`use_responses_api=True` for OpenAI) plus structured-invoke success/failure translation.
- Ledger integration tests cover success + replay without a second provider call, fail-fast `ModelInvocationInProgressError` for concurrent in-flight keys, stale reclaim after attempt deadline expiry with invalid job claim, late superseded-attempt fencing (attempt 1 cannot overwrite attempt 2), failure rows that store Atlas error classes only, and Alembic migrations through model ledger `20260809_0005`.
- Opt-in live tests (`ATLAS_ENABLE_LIVE_MODEL_TESTS=1` plus provider credentials) are skipped by default in normal CI.
- Local live verification (2026-08-09): both `test_live_openai_structured_plan` and `test_live_anthropic_structured_plan` passed with provider-specific default models and no global `ATLAS_MODEL_NAME` override. Credentials came from a gitignored `.env` only; keys were not recorded in docs, tests, or examples. Milestone 8 is Complete through Pull Request #11.

## Governed tools and MCP testing (Milestone 9)

- Default tool provider remains `fake`; normal CI makes no live tool network calls. Deterministic fake fetch exists; live arbitrary-URL fetch is unavailable (`ATLAS_TOOL_FETCH_ENABLED=true` fails at composition).
- Unit tests cover contracts, fake search/fetch, permission deny, budget exhaustion (`ToolBudgetExhaustedError`), finding byte cap + untrusted label, basic fake-fetch URL scheme/port/userinfo validation, streaming Tavily response bounds (`Content-Length`, streamed oversize, missing/non-JSON content-type, valid within-limit success), live-registry omit-fetch / fetch-enabled composition failure, and mocked Tavily error mapping.
- MCP contract test uses an in-memory FastMCP `Client` against the real server object: list tools, invoke `web_search` through the MCP protocol path, assert ledger `origin=MCP` with null workflow FKs and process `actor_id`, assert MCP tool schemas omit workflow attribution fields, and assert a disabled/unavailable `fetch_url` under live config raises an MCP `ToolError` (not a successful error-dict payload).
- Tool ledger integration tests cover success + replay, fail-fast in-progress conflicts, stale reclaim after deadline + invalid claim, transient retry (one), late-attempt fencing on logical invocation ownership, and Alembic migrations through tool ledger `20260809_0006` (superseded locally by evidence `20260809_0007` and embeddings `20260809_0008` after Milestone 10).
- Workflow regression asserts tool-ledger `workflow_node_attempt` matches the research-node audit attempt.
- Opt-in live tool tests (`ATLAS_ENABLE_LIVE_TOOL_TESTS=1`) are skipped by default in normal CI.
- Local live verification (2026-08-09): `test_live_tavily_search` passed with Tavily via direct streaming `httpx` and `ATLAS_TOOL_FETCH_ENABLED=false`. Credentials came from a gitignored `.env` only; keys were not recorded in docs, tests, or examples.
- HTML extraction and request-scoped SSRF-safe live fetching are deferred (not implemented / not tested as live behavior).
- Milestone 9 is Complete through Pull Request #12.

## Evidence and provenance testing (Milestone 10A)

- Unit tests cover URL canonicalization, raw-vs-normalized hash distinction, Unicode code-point chunking, size/chunk caps, whitespace-only Markdown rejection, and fake embedder determinism/dimensions.
- Integration tests cover corpus ingest idempotency, evidence HTTP APIs, search-hit provenance with nullable tool-invocation FK, application + composite-FK citation integrity, idempotent report artifact replay/conflict (no duplicate claims/citations), citations API chain (`claim → evidence → document → source`), workflow API→worker→citations, embedding persistence/idempotent replay/backfill, partial embedding failure, pgvector extension + `vector(1536)` + HNSW index, exact cosine retrieval, HNSW smoke path, metadata filters, job linking of retrieved corpus evidence, retrieval provenance, workflow retrieval→draft→citation, and Alembic head `20260809_0008` (upgrade/downgrade `0008`↔`0007` verified).
- Offline retrieval evaluation (`tests/integration/test_retrieval_eval.py` + `retrieval_dataset.json`) uses deterministic fake embeddings and **exact** cosine retrieval (approximate indexes disabled via transaction-local planner settings). Thresholds: Recall@5 ≥ 0.80, MRR@5 ≥ 0.70. These scores validate pipeline/fixture geometry only — not real semantic model quality. HNSW is tested separately (index existence, index-eligible candidate query EXPLAIN referencing `ix_evidence_embeddings_hnsw_cosine`, provenance, and filter effectiveness) and does **not** gate the metric thresholds. PostgreSQL owns production planner decisions unless Atlas deliberately applies a transaction-local setting.
- Embedding ingest API failures return structured `ErrorResponse` codes (`embedding_provider_failed`, etc.) without leaking provider text, vectors, or secrets. Partial-failure integration coverage asserts evidence remains, replay/backfill succeeds, and rows are not duplicated.
- Settings reject unsupported `embedding_profile` values; only `embeddings.v1` is accepted.
- Default suite remains offline (no live model/tool/embedding provider calls). Opt-in live embedding verification requires `ATLAS_ENABLE_LIVE_EMBEDDING_TESTS=1` plus credentials from the ignored local environment; keys and raw sensitive content are never printed.
- Local live embedding verification (2026-08-09): `test_live_openai_embedding_dimensions` passed via LangChain OpenAI `text-embedding-3-small` and returned 1536 dimensions. Credentials came from a gitignored `.env` only. The live test remains opt-in and is skipped in normal CI. Milestone 10 is Complete through Pull Request #13 (`bfabd59`; PR CI and resulting `main` CI green).

## Specialist agents testing (Milestone 11)

- Unit tests cover planner three-task enforcement (including rejection of non-three/empty tasks), research findings without padding, order-preserving tool+retrieval evidence ID deduplication, retriever/ingest composition fail-closed rules, synthesizer pack-scope rejection (including empty-pack + claims), empty-claim success without a pack, capability-isolation composition/spy proofs (planner/research/synthesizer/verifier/complete), and linear graph topology (no specialist back-edges to plan/research).
- Deterministic boundary/ablation suite (architecture evidence, not semantic grading): planner boundary; research order/dedupe/no fabrication; synthesizer pack-scope; verifier unlinked catch; defense-in-depth `persist_final` when verifier is bypassed; empty pack → no claims/citations; valid linked evidence through synth→verify→persist→citations API.
- Integration tests cover interrupt-after-`draft` resume through verify/evaluate/policy/complete, completed-workflow short-circuit, durable citation verifier fail-closed on unlinked evidence, graph verify/draft failure blocking complete/report persistence, processor specialist wiring isolation, and full API→worker→citations E2E with provenance, audit stages, model ledger `plan`/`draft` attribution, tool ledger `research`-only attribution, and resume idempotency.
- Milestone 11 is Complete through Pull Request #16 (`c5d4749`; PR CI and resulting `main` CI green).

## Candidate evaluation testing (Milestone 12 Slice 12A)

- Unit tests cover deterministic graders (including tool-budget / unknown-origin / zero-tool / provenance gate / empty-plan / missing-section / `golden_completeness_ratio` override), aggregation hard/soft gates, complete grading-snapshot fingerprints (evidence/tool mutation deltas), Fake semantic grader, deferred live scaffold fail-closed behavior, and provenance fail-closed ports.
- `candidate_goldens.v1` is human-reviewed (project owner, 2026-08-10) as a small regression + calibration baseline. It is explicitly **not** a held-out validation set, **not** independent statistical validation, and **not** proof of production semantic quality. See "Human calibration closeout" below for the full separation of grader-regression expectations from human quality labels.
- Integration tests cover parent job-claim fingerprint attribution (Worker A/B reclaim sequence; no anonymous create/reclaim; raw tokens absent from evaluation rows), job-aware stale reclaim, FAILED/reclaim fingerprint conflicts, job/execution mismatch validation, owned failure finalization, execution-scoped tool loading, Alembic head `20260809_0009` with upgrade/downgrade `0009`↔`0008`, evaluation API 200/404 without public `input_fingerprint` or `job_claim_fingerprint`, evaluate-node audit stages, pass→one accepted artifact, fail→no accepted artifact, completed replay without duplicate evaluation/artifact.
- Live LangChain semantic evaluation is deferred; skipped placeholders are not live-verification evidence.
- Default suite remains offline. Slice 12A is locally approved. Milestone 12 remains Current.

## Recovery, repair, and retry policy testing (Milestone 12 Slice 12B)

- Unit tests cover policy decision engine (backoff formula attempt=1 equals base, doubling, max cap, bounded jitter), `decide_for_evaluation` routing (pass→complete, hard→terminal, soft→repair/await_review with cap awareness, eval-attempt-cap→terminal), `decide_for_exception` routing (transient→retry/terminal cap, permanent→terminal, unknown→terminal), and exception categorization.
- Worker outcome matrix tests cover `PausedForReview` (no finalization), `RetryScheduled` (no finalization), `TerminalFailed` (finalizes FAILED), unrecognized outcome (finalizes FAILED), and orchestration timeout remains terminal.
- LangGraph spike proves `interrupt_after=["await_review"]` → `next==("complete",)`, resume-only-complete, idempotent completed re-invoke, new graph instance resume, pass path uninterrupted, no report before continuation.
- Claim-fence report race: stale claim cannot persist after ownership loss.
- Review API tests cover 404 when `ATLAS_REVIEW_API_ENABLED=false` (default).
- Alembic covers `0010`↔`0009` upgrade/downgrade; integration truncate lists include recovery tables.

### Slice 12B correction pass tests

- `tests/recovery/test_claim_ownership.py`: `ClaimOwnershipError` message/inheritance, `ContinuationMode` StrEnum identity/membership, and `_owns_running_claim` lease validation (expired/valid/null lease, wrong token, non-RUNNING status, None model).
- `tests/recovery/test_exception_registry.py`: `isinstance`-based exception categorization matrix covering concrete Atlas model/tool/embedding/evaluation errors (including structured-output, refusal, SSRF/content rejection, ownership/in-progress, not-found/stale, embedding conflict); ownership/conflict always terminal; third-party name fallback only.
- `tests/recovery/test_policy.py` (additions): structure repair first-time triggers repair, structure terminal after repair, citation integrity always terminal, tool policy always terminal.
- `tests/recovery/test_fingerprint.py`: deterministic fingerprint, different decision/job produces different fingerprint, null execution_id handled, replay same fingerprint on increment.
- `tests/integration/test_stale_claim_mutations.py`: stale worker A cannot bind, increment repair/eval counters, enter review, schedule retry, or finalize after B owns the claim; matching token with expired lease fails all protected mutations.
- `tests/integration/test_evaluation_attempt_accounting.py`: new evaluation increments; repair-style second attempt increments; reclaim/replay does not double-increment; crash before create commit rolls back counter; attempt four allowed; attempt five blocked with `EvaluationAttemptCapError`.
- `tests/integration/test_policy_decision_replay.py`: transaction-safe policy insert/replay, outer-TX survival, retry recovery-attempt FK to authoritative id, no duplicate recovery on replay, forced exception after replay rolls back caller mutations while preserving committed policy, inconsistent fields fail closed.
- `tests/integration/test_claim_fenced_execution_transitions.py`: after B reclaims, A cannot complete/fail/abandon; B can finalize; A exception path leaves B’s execution/job intact; operator reject fails without worker claim; retry abandon only when scheduling claim is valid.
- Default suite results recorded in `PROJECT_STATE.md` after the correction-pass verification gate. Milestone 12 Slice 12A/12B implementation is merged to `main` through Pull Request #17.

## Human calibration closeout (Milestone 12, 2026-08-10)

- The project owner completed human calibration review of `candidate_goldens.v1` and approved one judgment that diverges from the current deterministic grader: a well-supported report that fulfills the plan through semantically equivalent paraphrase is acceptable, even though the provisional lexical `completeness` heuristic scores it as a failure. This is recorded as exactly one approved known false negative, not a defect requiring a grader-threshold change.
- `candidate_goldens.v1.json` now records, for every graded case, a `grader_expected` block (deterministic-grader regression expectation: overall pass/fail, per-dimension behavior, regression rationale, known heuristic limitation) separate from a `human_expected` block (separate human overall judgment + rationale). Fingerprint-only (`kind: "fingerprint_delta"`) cases are unchanged and carry no human label — they validate replay/fingerprint sensitivity only, not report quality.
- Fixture metadata (`_meta`) now records `human_reviewed: true`, `human_reviewer: "project_owner"`, `reviewed_at: "2026-08-10"`, `frozen_profile: false`, and explicit statements that this is not a held-out validation set, not independent statistical validation, and not proof of production semantic quality. `evaluation.candidate.v1` remains provisional; frozen `evaluation.v1` remains deferred.
- Three requirement-driven graded fixtures were added: `fail_incomplete_provenance` (case-level `provenance_ok: false` → hard `CITATION_PROVENANCE_INCOMPLETE`, otherwise-linked claim), `fail_empty_plan` (empty `plan` → hard `STRUCTURE_EMPTY_PLAN` even though the draft is otherwise valid), and `fail_missing_required_section` (a narrowly-scoped case-level `preview_report_override` synthesizes a preview report missing the `Findings:` label → hard `STRUCTURE_MISSING_SECTION`; production formatting via `format_research_report` always emits all four required labels, so this is formatter/structure-gate regression coverage, not an observed production defect). The fixture harness gained matching optional case-level `provenance_ok` (default `true`) and `preview_report_override` (default unset) fields.
- The pre-existing `golden_facets_hit` coverage-override case is explicitly labeled fixture-only scaffolding (`fixture_mechanism_note`): `atlas.workflow.graph` never populates `golden_facets_hit` or `golden_completeness_ratio` in production, so that branch and case do not validate live production coverage quality.
- `tests/evaluation/test_golden_candidates.py` is split into three tests: `test_candidate_goldens_meta_records_human_calibration_scope` (honesty metadata present), `test_candidate_goldens_grader_regression` (actual deterministic-grader output, including fingerprint-delta cases, matches `grader_expected`/expected fingerprint-sensitivity — independent of human judgment), and `test_candidate_goldens_human_calibration` (actual grader output vs. `human_expected`, computing TP/FP/FN/TN/precision/recall/F1; asserts the paraphrase case is the sole approved disagreement and fails if any other disagreement appears; does **not** assert F1 == 1.0).
- Final counts (verified by direct execution, not hard-coded independent of the fixture): 25 total cases — 23 graded, 2 fingerprint-only. Human-calibration confusion matrix: TP=8, FP=0, FN=1, TN=14; human-positive=9, human-negative=14; precision=1.0, recall=8/9, F1=16/17.
- `tests/evaluation/test_graders.py` gained focused unit coverage for `STRUCTURE_EMPTY_PLAN`, `STRUCTURE_MISSING_SECTION`, and the `golden_completeness_ratio` override branch (below- and at-threshold), closing gaps found during the calibration review. `CITATION_PROVENANCE_INCOMPLETE` already had unit coverage (`test_grade_citation_integrity_provenance_gate`).
- No production grader thresholds, policy behavior, database schema, or Alembic migration changed by this closeout. No live semantic grader, Redis, Kafka, or Milestone 13+ work was added.
- Milestone 12 remains Current: this calibration-closeout change is local-only and pending its own PR CI and resulting `main` CI.
