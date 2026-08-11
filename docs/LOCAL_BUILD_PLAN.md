# Atlas Local Build Plan

## Purpose

This is the ordered roadmap for building Atlas completely on a local machine before designing or deploying its AWS architecture. It is shared by Robert, Cursor, Codex, and any other contributor.

This file answers **what comes next and why**. `PROJECT_STATE.md` answers **what is true right now**.

## How to use this plan

1. Work on only the milestone marked **Current** in this document and `PROJECT_STATE.md`.
2. Before implementation, agree on the milestone's scope, files, tests, and completion gate.
3. Implement the smallest complete vertical slice.
4. Run every required verification command and review the diff.
5. Update this plan only after the completion gate passes.
6. Update `PROJECT_STATE.md` with implemented behavior, verification evidence, decisions, and the next milestone.
7. Commit the completed milestone before beginning the next one.

Statuses:

- **Pending** — not started.
- **Current** — the only approved implementation milestone.
- **Complete** — implemented, verified, documented, and committed.
- **Blocked** — cannot proceed until the stated blocker is resolved.

Do not mark a milestone complete because files were generated. Its completion gate must pass.

## Local definition of success

The local platform is complete when a new developer can clone Atlas, configure safe local settings, start its dependencies, apply migrations, submit a research request, observe a durable multi-agent workflow, retrieve a cited and evaluated report, inspect logs/metrics/traces, exercise controlled failure recovery, and run the automated test suite.

---

## Milestone 1 — Python and FastAPI foundation

**Status:** Complete

**Goal:** Establish a reproducible Python application and its first tested endpoint.

**Build:**

- Python 3.12 pinned through `uv`.
- `pyproject.toml` and committed `uv.lock`.
- Minimal `src/atlas` package.
- FastAPI application with `GET /health`.
- Pytest, Ruff, and mypy configuration.

**Why now:** Every later capability needs a consistent runtime, dependency definition, package, and quality gates. The first slice deliberately excludes databases and AI so foundation problems are easy to isolate.

**Completion gate:**

- `uv run python --version` reports Python 3.12.x.
- `uv sync --frozen` succeeds.
- `uv run ruff check .` succeeds.
- `uv run mypy src tests` succeeds.
- `uv run pytest` succeeds and verifies `/health` returns the expected response.

---

## Milestone 2 — Continuous integration

**Status:** Complete

**Goal:** Make the local quality commands mandatory on GitHub.

**Build:** A minimal GitHub Actions workflow running Python 3.12, frozen dependency installation, Ruff, mypy, and Pytest.

**Why second:** CI should automate commands already proven locally. Adding it now prevents unverified code from accumulating.

**Completion gate:** The workflow passes on the repository; an intentionally broken check is proven to fail before the breakage is reverted.

---

## Milestone 3 — ResearchJob domain model

**Status:** Complete

**Goal:** Define a research job and its lifecycle independently of HTTP and storage.

**Build:** Typed job identity, question, status, timestamps, result/failure information, and tested transitions beginning with `PENDING → RUNNING → COMPLETED | FAILED`.

**Why before the schema:** Business invariants should determine storage design. Database tables should not accidentally become the domain model.

**Completion gate:** Unit tests prove valid transitions, reject invalid transitions, and show the domain has no FastAPI or database dependency.

---

## Milestone 4 — PostgreSQL persistence

**Status:** Complete

**Goal:** Make PostgreSQL the authoritative store for research jobs.

**Build:** PostgreSQL in Docker Compose, SQLAlchemy, Alembic, settings, connection/session ownership, first migration, repository implementation, readiness check, and isolated integration tests.

**Why now:** The job model and invariants exist, so the minimum schema and transaction boundaries can be designed from real behavior.

**Completion gate:** A clean database can migrate, jobs persist across application restarts, and repository integration tests pass.

---

## Milestone 5 — Database-backed research-job API

**Status:** Complete

**Goal:** Deliver the first durable product workflow.

**Build:**

- `POST /v1/research-jobs` returning `202` and a stable job ID.
- `GET /v1/research-jobs/{job_id}` returning status and result.
- Pydantic request/response contracts.
- Service/use-case boundary, repository use, structured errors, and idempotency-key handling.

**Why now:** This connects HTTP, domain behavior, transactions, and persistence as one demonstrable vertical slice.

**Completion gate:** Submission, retrieval, restart persistence, duplicate-request safety, validation, and not-found behavior pass API and integration tests.

---

## Milestone 6 — Background execution and recovery foundation

**Status:** Complete

**Goal:** Run long-lived work outside the HTTP request.

**Build:** A separate worker, PostgreSQL-backed safe job claiming, deterministic processing, graceful shutdown, timeouts, failure recording, and restart behavior.

**Why before Redis or Kafka:** PostgreSQL can prove asynchronous execution, concurrency, and recovery without adding a second distributed system.

**Completion gate:** The API returns immediately, multiple workers cannot process one claim concurrently, and interruption does not corrupt or lose the job.

---

## Milestone 7 — Deterministic LangGraph workflow

**Status:** Complete

**Goal:** Introduce explicit, checkpointed AI workflow orchestration without live models.

**Build:** Typed graph state; validate, plan, fake-research, draft, and complete nodes; Postgres checkpoints; workflow/node execution records; deterministic fake model and tool.

**Why fake AI first:** Workflow correctness and recovery must be testable without API keys, model variability, network failures, or cost.

**Completion gate:** The graph executes deterministically, persists progress, resumes after interruption, and maps failures to controlled job state.

**Verified locally:** 135 pytest tests green against `atlas_test`, including official `interrupt_after=["plan"]` restart recovery after disposing processor/graph/checkpointer connections and resuming with `graph.invoke(None, config)`, plus safe class-only node-failure persistence.

**Verified remotely:** Merged to `main` via Pull Request #10 (`5a6d19c`).

---

## Milestone 8 — Real model-provider adapters

**Status:** Complete

**Goal:** Add real LLM behavior without coupling Atlas to one provider.

**Build:** OpenAI and Anthropic adapters behind LangChain `BaseChatModel`, configuration, prompt/model versioning, timeouts, retry classification, token/latency/cost capture, two-table invocation ledger, mocked contract tests, and opt-in live tests.

**Why now:** The surrounding workflow is already deterministic and verified, allowing provider failures to be isolated.

**Completion gate:** Normal tests use fakes; an opt-in live run succeeds; secrets are protected; model metadata, latency, tokens, and cost are recorded. Do not mark Complete until pull-request CI and resulting `main` CI pass.

**Local implementation status:** Complete through Pull Request #11. Local automated gates and opt-in live OpenAI/Anthropic verification passed; PR CI and resulting `main` CI succeeded.

---

## Milestone 9 — Governed research tools and MCP

**Status:** Complete

**Goal:** Give agents controlled access to research capabilities.

**Build:** Typed tool interface and registry, allowlists, search/content tools, FastMCP boundary where justified, validation, permissions, timeouts, audit records, and untrusted-content handling.

**Why now:** MCP should expose or consume real tools, not exist as an empty protocol layer.

**Completion gate:** Only approved tools execute, inputs/outputs validate, calls are attributable to workflow nodes, and failures are controlled. Do not mark Complete until pull-request CI and resulting `main` CI pass.

**Local implementation status:** Complete through Pull Request #12 (`0db343d`). PR CI and resulting `main` CI passed.

---

## Milestone 10 — Evidence, provenance, RAG, and pgvector

**Status:** Complete

**Goal:** Produce traceable evidence and semantic retrieval rather than ungrounded chat responses.

**Build in two reviewed slices (one final PR, two mandatory review stops):**

1. **Slice 10A (Complete):** Sources, documents, evidence items, claims, citations, hashes, provenance, report artifacts, JSON text/Markdown ingest, structured draft claims, citation integrity, idempotent report persistence.
2. **Slice 10B (Complete):** pgvector, versioned embeddings (`embeddings.v1` / 1536-d), fake + optional OpenAI LangChain adapters, metadata filters, HNSW cosine index, exact vs HNSW retrieval, workflow retrieval→link→pack→draft→citation, and offline retrieval evaluation (fake embeddings; pipeline geometry only).

**Why evidence comes first:** Embeddings are useful only when retrieved material retains identity, provenance, and a relationship to report claims.

**Completion gate:** A citation maps to stored evidence and its source; ingestion is safely repeatable; retrieval preserves provenance and passes a small measured relevance baseline.

**Local implementation status:** Complete through Pull Request #13 (`bfabd59`). PR CI and resulting `main` CI passed.

---

## Milestone 11 — Specialist agents and report synthesis

**Status:** Complete

**Goal:** Expand the proven workflow into bounded specialists that improve measurable outcomes.

**Build:** Planner, research/retrieval specialist, synthesizer, and citation verifier initially. Every agent receives typed inputs/outputs, scoped tools, budgets, termination conditions, and failure policy.

**Why not earlier:** Multi-agent systems increase handoffs, cost, latency, and failure modes. A single workflow provides the baseline needed to justify each specialist.

**Completion gate:** A complete cited report is produced; agent loops are bounded; handoffs are typed; each specialist has test or evaluation evidence supporting its role.

**Local implementation status:** Complete through Pull Request #16 (`c5d4749`). PR #14 originally merged Milestone 11 (`005ea58`) with green CI; PR #15 reverted it (`e675f43`) for process reasons; PR #16 restored it with green PR CI and resulting `main` CI.

---

## Milestone 12 — Evaluation, grading, repair, and retry policy

**Status:** Complete

**Goal:** Measure AI quality and make controlled recovery decisions.

**Build:** Human-reviewed golden tasks; citation, coverage, groundedness, completeness, structure, and tool-use checks; grader calibration; versioned results; deterministic failure categories; bounded retry/backoff/jitter; repair, human-review, and terminal-failure routes.

**Why evaluation precedes autonomous recovery:** Atlas needs trustworthy signals before deciding whether work deserves retry or repair. Deterministic policy—not an unconstrained agent—controls execution.

**Completion gate:** Regressions are measurable; grader results are compared with human judgment; transient failures retry safely; permanent failures do not; all loops terminate. Candidate reports are evaluated before accepted report persistence; failed candidates are never exposed as accepted finals. Do not mark Complete until pull-request CI and resulting `main` CI pass.

**Completed through:** Pull Request #17 (`e3412c3`; evaluation/recovery implementation) and Pull Request #18 (`9d5abde`; human calibration closeout). Both PR CI and resulting `main` CI passed. `evaluation.candidate.v1` remains provisional; frozen `evaluation.v1`, held-out validation, and live semantic grading remain deferred.

---

## Milestone 13 — Redis and Kafka

**Status:** Current

**Goal:** Add ephemeral coordination and durable event distribution only after their workloads exist.

**Redis responsibilities:** Rate limiting, quota tracking, short-lived caching, worker heartbeats, and temporary coordination. Redis is not authoritative storage.

**Kafka responsibilities:** Versioned domain events, independent evaluation/audit/notification/analytics consumers, consumer groups, replay, dead-letter topics, and lag monitoring. Use a PostgreSQL transactional outbox and idempotent consumers.

**Why this late:** Atlas must first prove the core workflow. Redis and Kafka then solve observed concurrency and distribution needs rather than becoming decorative dependencies.

**Completion gate:** Redis loss does not lose jobs; rate limits work concurrently; database updates and outbox events stay consistent; duplicate Kafka delivery is safe; replay and poison-message handling are tested.

**Local implementation status:** Slice 13A is **Complete** through Pull Request #19 (`dc19714`; PR CI run #40 and resulting `main` CI run #41 passed): Redis-backed fixed-window rate limiting for `POST /v1/research-jobs` by direct peer IP; dedicated worker heartbeat thread with TTL keys; `noop` default coordination provider; pinned Redis 8.8.1 in Compose/CI. Slice 13B is **Complete** through Pull Request #20 (`48ce40a`; corrected PR CI run #43 and resulting `main` CI run #44 passed): typed research-job domain events + PostgreSQL transactional outbox + singleton advisory-lock relay; global `outbox_position` head-of-line claiming; fresh post-producer lease fencing; strict stop-on-failure ordering. Slice 13C1 (real Kafka 4.3.1 broker; typed `confluent-kafka` producer adapter with delivery-callback-confirmed publish and a narrow, fail-closed Kafka error classification — fatal, or retriable/`_MSG_TIMED_OUT` recoverable, or else fatal; `AdminClient`-based topic administration/verification; Kafka-only executable `python -m atlas.outbox`) is implemented locally on `milestone-13-kafka` (based at `48ce40a`) and pending its own PR CI and resulting `main` CI. Kafka consumers, inbox/deduplication, replay, DLQ, and Redis caching remain deferred to Slice 13C2. Celery remains excluded. Do not mark Milestone 13 Complete until Slice 13C1 and Slice 13C2 (and their PR/`main` CI) pass.

## Milestone 14 — Observability and security

**Status:** Pending

**Goal:** Make the distributed local platform explainable, measurable, and defensible.

**Build:** Structured logs and correlation IDs throughout; OpenTelemetry traces; Prometheus metrics; Grafana dashboards; Alertmanager; optional justified log/trace backends; authentication/authorization; scoped tools; prompt-injection defenses; auditability; dependency, source, secret, and container scanning.

**Measure:** API/job latency, throughput, failures, recovery, worker utilization, database behavior, Redis metrics, Kafka lag, model tokens/cost/latency, tool failures, retrieval quality, and evaluation scores.

**Why now:** Basic instrumentation grows throughout earlier milestones; the complete stack becomes useful when multiple components must be correlated and secured together.

**Completion gate:** One job can be followed end to end; dashboards answer operational questions; alerts are actionable; secrets and sensitive content are excluded; security tests and scans pass or document accepted risk.

---

## Milestone 15 — Reproducible local release and validation

**Status:** Pending

**Goal:** Prove Atlas works as a complete local platform before any cloud design.

**Build:** Complete Docker Compose environment for the API, worker, PostgreSQL/pgvector, Redis, Kafka, Prometheus, Grafana, Alertmanager, and justified trace/log services; setup/runbook documentation; load, resilience, backup/restore, replay, and cost testing.

**Why last locally:** Containers and operational tests should package and validate known processes and dependencies, not speculative services.

**Completion gate:** A clean checkout can start the stack, migrate, complete and observe a cited research job, recover from tested failures, restore data, and pass the full automated suite. Bottlenecks, resource usage, quality, latency, and local limitations are documented.

---

## After local completion — Cloud architecture

Cloud work is intentionally out of scope until Milestone 15 passes. Then Atlas will use measured local behavior to create:

- Logical system and workflow diagram.
- Microsoft Visio AWS deployment and network diagram.
- Completed cloud sections in `docs/TECHNICAL_DESIGN.md`.
- Local-to-AWS service mapping and trade-off analysis.
- Terraform, Kubernetes/EKS, security, observability, CI/CD, rollout/rollback, disaster-recovery, and cost plans.

## Planned technology coverage

The roadmap provides justified entry points for Python, AsyncIO, FastAPI, Pydantic, Pytest, Ruff, mypy, GitHub Actions, PostgreSQL, SQLAlchemy, Alembic, Docker Compose, LangChain/LangGraph, OpenAI/Anthropic, RAG, pgvector, MCP/FastMCP, specialist agents, grading/evaluations, retries and recovery, Redis, Kafka, OpenTelemetry, Prometheus, Grafana, Alertmanager, structured logging, security scanning, and complete local containerization.

Kubernetes, Helm, Terraform, and AWS begin only after the local release gate passes.
