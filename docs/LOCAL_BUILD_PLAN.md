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

**Local implementation status:** Slice 13A is **Complete** through Pull Request #19 (`dc19714`; PR CI run #40 and resulting `main` CI run #41 passed): Redis-backed fixed-window rate limiting for `POST /v1/research-jobs` by direct peer IP; dedicated worker heartbeat thread with TTL keys; `noop` default coordination provider; pinned Redis 8.8.1 in Compose/CI. Slice 13B is **Complete** through Pull Request #20 (`48ce40a`; corrected PR CI run #43 and resulting `main` CI run #44 passed): typed research-job domain events + PostgreSQL transactional outbox + singleton advisory-lock relay; global `outbox_position` head-of-line claiming; fresh post-producer lease fencing; strict stop-on-failure ordering. Slice 13C1 is **Complete** through Pull Request #21 (`cd5b25e`; PR CI and resulting `main` CI run #47 passed): real Kafka 4.3.1 broker; typed `confluent-kafka` producer adapter with delivery-callback-confirmed publish and a narrow, fail-closed Kafka error classification — fatal, or retriable/`_MSG_TIMED_OUT` recoverable, or else fatal; `AdminClient`-based topic administration/verification; Kafka-only executable `python -m atlas.outbox`. Slice 13C2A (PostgreSQL-backed consumer inbox with `(consumer_id, event_id)` deduplication; the research-job lifecycle projection as the first business consumer; typed `KafkaEventConsumer` with manual offset commit only after the PostgreSQL transaction commits; non-HTTP executable `python -m atlas.consumer`; migration `20260809_0012`) is implemented and locally verified on branch `milestone-13-kafka-consumers` (based at `cd5b25e`); it has not yet opened a Pull Request. Slice 13C2B (retry/backoff, poison-event/dead-letter handling, and replay tooling) remains pending. Celery remains excluded. Do not mark Milestone 13 Complete until Slice 13C2A and Slice 13C2B (and their PR/`main` CI) pass.

## Milestone 14 — Observability

**Status:** Pending

**Goal:** Make the distributed local platform explainable and measurable before defending it.

**Build:** Structured logs and correlation/trace IDs throughout the API, worker, outbox relay, and consumer; OpenTelemetry traces across the HTTP → worker → LangGraph → model/tool/Kafka boundary; Prometheus metrics; Grafana dashboards; Alertmanager routing; optional justified log/trace backends. No authentication, RBAC, or scanning work — that is Milestone 15's scope.

**Measure:** API/job latency, throughput, failures, recovery, worker utilization, database behavior, Redis metrics, Kafka consumer lag/offset behavior, outbox publish latency, model tokens/cost/latency, tool failures, retrieval quality, and evaluation scores.

**Why now:** Basic instrumentation grows throughout earlier milestones; a dedicated slice is needed once enough distributed components (API, worker, PostgreSQL, Redis, Kafka producer + consumer) exist that correlating one job across all of them provides real value.

**Completion gate:** One job's full lifecycle can be followed end to end via correlated logs/traces; dashboards answer real operational questions (latency, failure rate, Kafka lag, recovery counts); alerts are actionable and not noisy; secrets and sensitive content remain excluded from all telemetry.

---

## Milestone 15 — Security and software-supply-chain CI

**Status:** Pending

**Goal:** Make the CI pipeline itself defensible: authenticated APIs, scoped tools, and automated supply-chain scanning, not just green tests.

**Build:** API authentication/authorization for previously local-only/flag-gated endpoints (e.g. the operator review API); scoped-tool and prompt-injection defenses hardened for the now-larger tool/consumer surface; dependency vulnerability scanning (e.g. `pip-audit`/`uv`-aware scanning); source/secret scanning (e.g. gitleaks-style); container image scanning once application images exist (Milestone 17); auditability of security-relevant decisions (auth failures, scan suppressions).

**Why after observability:** Security review is more effective once logs/traces/metrics exist to show what a security control actually observes and blocks; this also keeps Milestone 14 focused on visibility rather than mixing in an unrelated authn/authz surface.

**Completion gate:** CI fails on a newly introduced known-vulnerable dependency or a detected secret; authenticated endpoints reject unauthenticated/unauthorized requests with structured errors; scan results are either clean or have a documented, reviewed suppression; no security tests are skipped silently.

---

## Milestone 16 — Next.js/TypeScript/Tailwind frontend and document upload

**Status:** Pending

**Goal:** Give Atlas a real user-facing surface instead of only an HTTP API, and close the "live document ingest" gap left open since Milestone 10.

**Build:** A Next.js + TypeScript + Tailwind frontend (functional components, typed props/hooks, no `any`) for submitting research jobs, tracking status, viewing cited reports and evaluation results, and the local-only operator review flow; a document-upload endpoint (multipart) feeding the existing evidence/embedding pipeline, replacing the current JSON/text-only ingest path; browser-safe error handling (no raw backend exception text reaching the UI).

**Why here:** A frontend is more valuable once the backend has observability and baseline security (Milestones 14–15) to build on, and document upload is the one Milestone 10 gap (live arbitrary-URL fetch/HTML/PDF ingest/object storage) most directly unblocked by adding a real upload surface.

**Completion gate:** A user can submit a job, upload a document, and see a cited, evaluated report in the browser without directly calling the API; frontend and backend integration tests pass; no secrets or raw exception text are exposed client-side.

---

## Milestone 17 — Complete containerized local release

**Status:** Pending

**Goal:** Package every proven local component into one reproducible Docker Compose environment.

**Build:** Application Docker images for the API, worker, outbox relay, and Kafka consumer (replacing the current bare-`uv run` local processes); the frontend's own container; Compose wiring for PostgreSQL/pgvector, Redis, Kafka, Prometheus, Grafana, Alertmanager, and any justified trace/log backend; setup/runbook documentation for a clean checkout.

**Why here:** Containerizing is meaningful only once the set of services is actually stable (through Milestone 16); packaging earlier would mean repeatedly re-packaging as new services appear.

**Completion gate:** A clean checkout can `docker compose up` the entire stack (including the frontend) from published/build images, migrate, and complete an observed, cited research job without any host-installed Python/Node tooling beyond Docker itself.

---

## Milestone 18 — Full local E2E, load, failure, recovery, and backup/restore validation

**Status:** Pending

**Goal:** Prove the fully containerized platform behaves correctly under realistic load and failure, not just under unit/integration tests.

**Build:** End-to-end test scenarios driving the real containerized stack (frontend → API → worker → Kafka → consumer); load testing (concurrent job submission, rate-limit behavior under load); deliberate failure injection (broker/database/Redis restarts, worker/consumer crashes) validating the recovery, outbox, and consumer-inbox guarantees already proven in isolation; backup/restore drills for PostgreSQL; poison-event and DLQ replay drills (Slice 13C2B) under the full stack.

**Why last locally:** This is the local platform's final gate — it validates the integration of every earlier milestone together, which is only possible once Milestone 17's single environment exists.

**Completion gate:** The full E2E suite passes against the containerized stack; a documented load profile with measured latency/throughput/failure rates exists; each tested failure (broker/DB/Redis/worker/consumer restart) recovers without data loss or duplicate business effects; a backup can be restored and validated; bottlenecks and local limitations are documented.

---

## After local completion — Cloud architecture

Cloud design work is intentionally out of scope until Milestone 18 passes. Milestones 19–23 then use measured local behavior to design, provision, deploy, and validate Atlas on AWS.

## Milestone 19 — Local and AWS Visio/system-design diagrams

**Status:** Pending

**Goal:** Turn validated local architecture into credible design artifacts before any cloud provisioning.

**Build:** A logical local system/workflow diagram reflecting what Milestone 18 actually validated (not aspirational architecture); a Microsoft Visio AWS deployment and network diagram mapping each local component (API, worker, outbox relay, consumer, PostgreSQL, Redis, Kafka, frontend, observability stack) to a proposed AWS service; completed cloud-architecture sections in `docs/TECHNICAL_DESIGN.md`; an explicit local-to-AWS service mapping and trade-off analysis (e.g. self-managed Kafka vs. MSK, self-managed Postgres vs. RDS/Aurora).

**Why now:** Diagrams produced before Milestone 18 would describe an unvalidated system; producing them immediately after gives the cloud milestones a concrete, defensible target instead of a speculative one.

**Completion gate:** Diagrams and trade-off analysis are reviewed and approved before any Terraform code is written; every local component has an explicit AWS mapping or an explicit, justified decision to omit/replace it.

---

## Milestone 20 — Terraform AWS infrastructure

**Status:** Pending

**Goal:** Provision the AWS infrastructure designed in Milestone 19 as reviewable, versioned code.

**Build:** Terraform modules for networking (VPC, subnets, security groups), managed data services chosen in Milestone 19's trade-off analysis, an EKS cluster (consumed by Milestone 21), IAM roles/policies scoped to least privilege, and remote state management. Provisional scope — the exact managed-service choices depend on Milestone 19's approved mapping.

**Why after the diagrams:** Infrastructure-as-code should implement an already-approved design, not drive architecture decisions ad hoc.

**Completion gate:** `terraform plan`/`apply` provisions a working environment from a clean state; a destroy/recreate cycle succeeds; no secrets are committed to Terraform state or source; least-privilege IAM is documented and reviewed.

---

## Milestone 21 — Kubernetes/EKS and Helm deployment

**Status:** Pending

**Goal:** Run the containerized platform (Milestone 17's images) on the EKS cluster provisioned in Milestone 20.

**Build:** Helm charts for the API, worker, outbox relay, consumer, and frontend; Kubernetes-native configuration for the observability stack; readiness/liveness probes appropriate to each workload (including the explicit caveat already carried locally that a healthy process does not imply every partition/business handler is healthy); horizontal scaling policy for stateless components; secrets management via a Kubernetes-native or AWS-native mechanism (never plain ConfigMaps for credentials).

**Why after Terraform:** Kubernetes workloads need the cluster and networking Milestone 20 provisions; deploying before that would have nowhere to run.

**Completion gate:** A clean `helm install` against the provisioned EKS cluster brings up a working platform reachable end to end; probes correctly reflect real health; a rolling update/rollback succeeds without downtime for stateless components.

---

## Milestone 22 — Cloud CI/CD, promotion, verification, and rollback

**Status:** Pending

**Goal:** Automate build, promotion, and safe rollback for the deployed cloud platform.

**Build:** CI/CD pipelines building and publishing the Milestone 17 container images; environment promotion (e.g. staging → production) gated on automated verification; automated smoke/health verification post-deploy; a documented, tested rollback procedure; integration with Milestone 15's supply-chain scanning gates in the cloud pipeline.

**Why after the deployment exists:** Automating promotion and rollback requires a real deployed target (Milestone 21) to promote to and roll back against.

**Completion gate:** A merged change is built, scanned, deployed to a non-production environment, verified, and promotable to production through the pipeline without manual server access; a deliberately bad deploy is caught and rolled back automatically or via a documented one-command procedure.

---

## Milestone 23 — Cloud validation, runbooks, cost analysis, portfolio demo, and interview narrative

**Status:** Pending

**Goal:** Close out the project as a defensible, explainable, portfolio-ready system.

**Build:** Cloud-environment load/failure/recovery validation mirroring Milestone 18's local drills; operational runbooks for on-call scenarios (broker down, database failover, consumer stuck/poison event); a cost analysis of the running cloud environment with identified optimization opportunities; a recorded or scripted portfolio demonstration; a written interview narrative connecting design decisions, trade-offs, and validated evidence across all 23 milestones.

**Why last:** This milestone only synthesizes and validates what every prior milestone already proved; it adds no new architecture.

**Completion gate:** Cloud failure drills recover without data loss; runbooks are reviewed and actionable; a cost figure with at least one identified optimization is documented; the demo and interview narrative are reviewed as accurate representations of what was actually built and verified (not aspirational claims).

---

## Planned technology coverage

The roadmap provides justified entry points for Python, AsyncIO, FastAPI, Pydantic, Pytest, Ruff, mypy, GitHub Actions, PostgreSQL, SQLAlchemy, Alembic, Docker Compose, LangChain/LangGraph, OpenAI/Anthropic, RAG, pgvector, MCP/FastMCP, specialist agents, grading/evaluations, retries and recovery, Redis, Kafka, OpenTelemetry, Prometheus, Grafana, Alertmanager, structured logging, security/supply-chain scanning, Next.js/TypeScript/Tailwind, and complete local containerization.

Kubernetes, Helm, Terraform, and AWS begin only after the local release gate (Milestone 18) passes, per Milestones 19–23 above.
