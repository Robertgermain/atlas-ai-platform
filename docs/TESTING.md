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
- PostgreSQL resume test uses LangGraph `interrupt_after=["plan"]`, confirms the plan checkpoint, disposes processor/graph/checkpointer/connections, builds fresh B instances, resumes with `graph.invoke(None, config)` for the same `thread_id`, and proves `validate`/`plan` are not executed again.
- Integration tests cover API→worker→LangGraph→GET completion, report structure (`Question`/`Plan`/`Findings`/`Draft`), per-attempt workflow/node history, abandon of prior `RUNNING` executions on a new attempt, and Alembic head `20260809_0004`.
- Suite isolation truncates Atlas audit tables and LangGraph checkpoint data between tests after one-time `PostgresSaver.setup()` at session start.
