# Atlas Local Build Plan

## Purpose

This is the ordered roadmap for building Atlas completely on a local machine before designing or deploying its AWS architecture. It is shared by Robert, Cursor, Codex, and any other contributor.

This file answers **what comes next and why**. `PROJECT_STATE.md` answers **what is true right now**.

## Governing architecture rule: local-first, cloud-portable

Every cloud capability must first have a working local equivalent whenever technically practical. AWS hosts and operationalizes an already-validated system; AWS must not become the first environment where Atlas components are integrated. This rule shapes every milestone below, not just the cloud ones.

Required implications:

- Application/domain behavior must not directly depend on AWS-specific APIs.
- The same application contracts and Docker images flow through: local processes → Docker Compose → local Kubernetes with `kind` → AWS EKS.
- Helm charts must be validated on `kind` (Milestone 18) before EKS (Milestone 22).
- PostgreSQL, Redis, Kafka, storage, ingress, secrets, telemetry, and workload boundaries each require an explicit local-to-AWS mapping (produced in Milestone 20).
- AWS-only capabilities that cannot be reproduced faithfully — IAM, WAF, Route 53, managed-service failover, AWS networking — still require: a local contract or configuration boundary where practical; automated configuration/contract tests; an explicit local-to-AWS mapping and trade-off analysis; and final integration validation in AWS.
- Do not claim full behavioral equivalence where local emulation is incomplete.
- This rule does not authorize working ahead of the milestone marked **Current**.

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

**Status:** Complete

**Goal:** Add ephemeral coordination and durable event distribution only after their workloads exist.

**Redis responsibilities:** Rate limiting, quota tracking, short-lived caching, worker heartbeats, and temporary coordination. Redis is not authoritative storage.

**Kafka responsibilities:** Versioned domain events, independent evaluation/audit/notification/analytics consumers, consumer groups, replay, dead-letter topics, and lag monitoring. Use a PostgreSQL transactional outbox and idempotent consumers.

**Why this late:** Atlas must first prove the core workflow. Redis and Kafka then solve observed concurrency and distribution needs rather than becoming decorative dependencies.

**Completion gate:** Redis loss does not lose jobs; rate limits work concurrently; database updates and outbox events stay consistent; duplicate Kafka delivery is safe; replay and poison-message handling are tested.

**Local implementation status:** Slice 13A is **Complete** through Pull Request #19 (`dc19714`; PR CI run #40 and resulting `main` CI run #41 passed): Redis-backed fixed-window rate limiting for `POST /v1/research-jobs` by direct peer IP; dedicated worker heartbeat thread with TTL keys; `noop` default coordination provider; pinned Redis 8.8.1 in Compose/CI. Slice 13B is **Complete** through Pull Request #20 (`48ce40a`; corrected PR CI run #43 and resulting `main` CI run #44 passed): typed research-job domain events + PostgreSQL transactional outbox + singleton advisory-lock relay; global `outbox_position` head-of-line claiming; fresh post-producer lease fencing; strict stop-on-failure ordering. Slice 13C1 is **Complete** through Pull Request #21 (`cd5b25e`; PR CI and resulting `main` CI run #47 passed): real Kafka 4.3.1 broker; typed `confluent-kafka` producer adapter with delivery-callback-confirmed publish and a narrow, fail-closed Kafka error classification — fatal, or retriable/`_MSG_TIMED_OUT` recoverable, or else fatal; `AdminClient`-based topic administration/verification; Kafka-only executable `python -m atlas.outbox`. Slice 13C2A (PostgreSQL-backed consumer inbox with `(consumer_id, event_id)` deduplication; the research-job lifecycle projection as the first business consumer; typed `KafkaEventConsumer` with manual offset commit only after the PostgreSQL transaction commits; non-HTTP executable `python -m atlas.consumer`; migration `20260809_0012`) is **Complete** through Pull Request #22 (`9f2b7af`; PR CI run #48 and resulting `main` CI run #49 passed). Slice 13C2B (bounded, deterministic in-process retry with a runtime processing deadline; permanent-poison classification into a PostgreSQL-backed dead-letter store, migration `20260809_0013`; offset commit only after durable DLQ persistence; a local-only operator replay CLI, `python -m atlas.consumer.replay`, with durable replay ownership fencing and idempotency) is **Complete** through Pull Request #25 (`865023b`, confirmed as the current tip of `origin/main`; the resulting `main` CI run number is not recorded because it was not independently re-verified against the GitHub API this session). Celery remains excluded. **Milestone 13 is Complete.**

## Milestone 14 — Backend container foundation and build/runtime CI

**Status:** Complete through Pull Request #26 (`f5c421f`, "Milestone 14 backend containers (#26)"), confirmed as the current tip of `origin/main`; both resulting `main` GitHub Actions checks (`quality`, `build-and-verify`) confirmed `completed`/`success` against the public GitHub API for commit `f5c421f`.

**Goal:** Package the backend services proven through Milestone 13 (API, worker, outbox relay, Kafka consumer) into reproducible, scanned container images and prove the built images actually run — not just that they build — before observability, security, and frontend work continues.

**Build in three reviewable slices:**

### Slice 14A — Shared backend image and local startup verification

One immutable image (`Dockerfile`, multi-stage, digest-pinned `python:3.12-slim-bookworm` base and `ghcr.io/astral-sh/uv:0.11.8` build tool, frozen `uv sync` install, fixed non-root UID/GID 10001, no dev dependencies, no source tree at runtime — `atlas` is installed non-editable into the copied venv) runs all four long-running roles (API, worker, outbox relay, Kafka consumer) by overriding the container command, plus `alembic upgrade head` as a one-shot job. The image's `ENTRYPOINT` is a checksum-verified, digest-pinned Tini binary (`ADD --checksum=sha256:... /tini` from the upstream `krallin/tini` v0.19.0 GitHub release, selected per-architecture via the automatic `TARGETARCH` build arg) — a portable PID 1/init boundary built into the image itself, not a Compose-only `docker run --init`/`init: true` setting, so it is identical in Compose, `kind`, and EKS. Verified locally, without any host-run PostgreSQL/Redis/Kafka dependency: image build; all four role modules import; dev dependencies (Pytest/Ruff/mypy) absent; non-root UID/GID; a plain import succeeds under `--read-only` with a `/tmp` tmpfs; Tini is PID 1 for every role with `CMD` overrides still resolving correctly; a double-fork test proved Tini adopts and fully reaps an orphaned grandchild process (no lingering zombie); `/health` returns 200, `/ready` returns a sanitized 503 with PostgreSQL unavailable; and all four roles now emit only sanitized (fixed message + exception class) output on an unavailable-dependency startup failure, terminating promptly and controllably on SIGTERM without any `docker run --init` flag. Topic administration (`python -m atlas.outbox.topic_admin`) has no executable entry point yet — deferred to Slice 14B, not duplicated here.

Local verification's Tini-based fix directly addressed a genuine, pre-existing (non-container-specific) finding from the image's first implementation pass: Linux gives PID 1 special signal semantics — a default-disposition SIGTERM sent to PID 1 is silently ignored, not deferred, by the kernel. The worker and outbox relay each install their own SIGINT/SIGTERM handler only *after* a blocking, PostgreSQL-dependent startup step (`initialize_checkpointer_schema()`; the advisory-lock `acquire()`), so a SIGTERM sent during that window used to be ignored entirely when the role ran as bare container PID 1. Rather than relying on Compose's `docker run --init` (which has no Kubernetes Pod-spec equivalent, so a Compose-only fix would leave `kind`/EKS deployments without correct signal semantics), Tini is now baked into the image's own `ENTRYPOINT`, so all three non-HTTP roles terminate promptly on SIGTERM during that window in every environment that runs the image, with no `--init` flag needed anywhere. Separately, the worker's `initialize_checkpointer_schema()` failure and the outbox relay's advisory-lock `acquire()` failure are now each caught at their executable startup boundary and logged as a fixed sanitized message plus exception class only (narrowly scoped — no domain/application-level exception handling was broadened). A related, narrower, still-open gap remains, unchanged: `psycopg_pool`'s own background per-connection-attempt `WARNING` logging (a different, lower-level logging channel than the app's own failure-path logging) still emits a configured host:port (never credentials) while a connection attempt is in flight; likewise the consumer's underlying librdkafka client still emits its own native `FAIL|...Connection refused` line (host:port only) alongside the consumer's own already-sanitized log line. Neither is the "raw traceback on ultimate failure" this slice's requirement targets.

### Slice 14B — Compose topology and one-shot administration jobs

**Complete** (Pull Request #26, `f5c421f`). Full Docker Compose topology on one internal (default Compose) network: PostgreSQL/pgvector, Redis, Kafka (unchanged infrastructure services) plus six Atlas services all sharing the one Slice 14A image, built once and referenced everywhere as `atlas-backend:${GIT_SHA:-dev}` via a shared YAML anchor — `db-migrate` (`alembic upgrade head`, one-shot), `kafka-topic-init` (one-shot, now the typed `python -m atlas.outbox.topic_admin` executable rather than the earlier `kafka-topics.sh` shell script), `api` (default `CMD`, published `127.0.0.1:8000:8000`), `worker`, `outbox-relay`, and `consumer`. `python -m atlas.outbox.topic_admin` is a thin `main()` added directly to the existing `atlas.outbox.topic_admin` module — it delegates to the existing `verify_broker_connectivity`/`ensure_topic_exists`/`verify_topic_partitioning` functions in order and duplicates no Kafka admin logic. Compose never overrides the image's `ENTRYPOINT` (Tini remains PID 1 for every role, proven in Slice 14A) and does not set `init: true` (no test found a concrete need for double-init). Every Atlas service's `ATLAS_DATABASE_URL`/`ATLAS_REDIS_URL`/`ATLAS_KAFKA_BOOTSTRAP_SERVERS` is a hardcoded internal-DNS literal (`postgres:5432`/`redis:6379`/`kafka:9092`) rather than a `${...}`-interpolated value, so a developer's host-oriented `.env` (ports `5433`/`6380`/`9094`) can never leak into a container; the three optional third-party credentials are each individually mapped as `${VAR:-}` on `api`/`worker` only — no service uses `env_file: .env`. Every Atlas service is hardened identically via a shared `x-atlas-hardening` anchor: `read_only: true`, `tmpfs: [/tmp]`, `cap_drop: [ALL]`, `security_opt: [no-new-privileges:true]`, the image's non-root user preserved, no source/socket mounts, no `privileged`. Dependency ordering uses only health/completion `depends_on` conditions (`db-migrate`/`kafka-topic-init` gate the long-running services; `api` and `worker` additionally wait on `redis: {condition: service_healthy}`, matching their explicit `ATLAS_COORDINATION_PROVIDER=redis` — Compose startup sequencing only, not a change to Atlas's own runtime fail-open Redis behavior; API readiness is never treated as proof that worker/outbox-relay/consumer are healthy). Compose shutdown budgets (`stop_grace_period`): API `15s` (Uvicorn's own `--timeout-graceful-shutdown 10` bound, Slice 14A, plus headroom), worker `75s`, outbox relay `30s` (bounded by `kafka_delivery_timeout_seconds` for an in-flight publish, that same setting again for the producer's own `close()`/flush, plus headroom — not derived from the publish lease, which is a fencing/crash-recovery constraint, not an additive shutdown step), consumer `300s` — Kubernetes independently rederives its own termination budgets in Milestone 18. Local end-to-end verification against the real Compose-started PostgreSQL/Redis/Kafka (fake model/tool/embedding providers throughout, no live network calls) proved: `/health`/`/ready` both `200`; the worker's Redis heartbeat key exists and its TTL refreshes; a submitted research job reaches `COMPLETED` and its event is observed end-to-end through the PostgreSQL outbox, Kafka, the consumer inbox, and the lifecycle projection; `Idempotency-Key` replay/conflict behavior is unchanged; and all four long-running services stop promptly and cleanly on `SIGTERM`, well inside their configured budgets.

### Slice 14C — GitHub Actions build, runtime, and security gates

**Complete** (Pull Request #26, `f5c421f`; PR CI and resulting `main` CI's `build-and-verify` check both passed). A separate single-job `.github/workflows/containers.yml` workflow (alongside the unchanged `.github/workflows/ci.yml`), triggered on pull requests and pushes to `main`, with `contents: read`, concurrency cancellation, and a 25-minute job timeout. Derives `GIT_SHA` via `git rev-parse HEAD` on the actual checked-out tree — not `github.event.pull_request.head.sha`/`github.sha`, since a `pull_request` trigger's checked-out ref is GitHub's own PR merge commit, a different revision than `head.sha` — and uses that exact revision for the image tag, the OCI revision label, `BUILD_DATE`, image-identity evidence, artifact names, and the E2E idempotency key. Builds the shared Slice 14A/14B image exactly once with a pinned Buildx invocation (`docker buildx build --load --provenance=false --sbom=false`, `--cache-from type=gha`/`--cache-to type=gha,mode=max,ignore-error=true` — cache export failure is explicitly non-authoritative and can only degrade build performance, never fail an otherwise valid cold build). Beyond each Action's own full-commit-SHA pin, `docker/setup-buildx-action` (`version: v0.36.1`), `anchore/sbom-action` (`syft-version: v1.42.3`), and all three `aquasecurity/trivy-action` invocations (`version: v0.70.0`) each explicitly pin the specific underlying tool release as an action input. Records the built image's `docker image inspect --format '{{.Id}}'` and re-verifies before structural checks, before infrastructure startup, before application-service startup, and before Syft/Trivy that the tag still resolves to that same ID; additionally verifies, immediately after Compose starts each of the six Atlas services (`db-migrate`, `kafka-topic-init`, `api`, `worker`, `outbox-relay`, `consumer`), that the specific container Compose created for that service resolves to that same recorded image ID. No step after the single build ever runs `docker compose build` or passes `--build`; every later Compose command consumes the already-loaded image. Pinned inputs and the frozen `uv.lock` make the build functionally/content-equivalent on every run of the same commit — strict byte-for-byte image-digest reproducibility is not claimed, and the `type=gha` cache affects build time only, never build output. Runs structural checks against the built image (Python version, package import of all four role modules, non-root UID/GID, absence of Pytest/Ruff/mypy/a `uv` binary/the source tree, OCI labels, `linux/amd64`, and static pinned-base-image/Tini-checksum checks against the `Dockerfile` source). Reuses the Slice 14B Compose topology to run the same end-to-end flow in CI: infrastructure health, `db-migrate`/`kafka-topic-init` one-shot jobs, `api`/`worker`/`outbox-relay`/`consumer` startup, `/health`/`/ready`, the worker Redis heartbeat, one fake-provider research job whose response job id is validated as a well-formed UUID before any SQL use and observed through PostgreSQL outbox → Kafka → consumer inbox → lifecycle projection. Before any replay, records and asserts baseline database invariants for that job: exactly one `research_jobs` row; exactly one published `research_job.created` and exactly one published `research_job.completed` outbox event (an explicit exactly-once check per event type, not a numeric lower bound alone, which is what rules out a vacuous `0 == 0` pass); exactly one lifecycle-projection row; zero duplicate consumer-inbox `(consumer_id, event_id)` keys; and the projection consumer's distinct inbox `event_id` count equal to the distinct published `outbox_events.event_id` count. An identical idempotent replay must return `202` with the same job id and leave all recorded counts unchanged, and a changed-body replay must return `409` and again leave all counts unchanged. Also exercises bounded idle SIGTERM shutdown for all four long-running roles (each service's real Compose `stop_grace_period` still governs Compose's own internal shutdown escalation; a uniform 20-second outer `timeout` wraps the CI-side observation of that idle-case shutdown so a hung service fails the step — and still reaches the unconditional `if: always()` teardown — instead of exhausting the whole job's timeout). Generates a CycloneDX JSON SBOM with a pinned Syft action and uploads it as a 30-day artifact; scans the exact image with a pinned Trivy action for `os,library` HIGH/CRITICAL findings, emitting one readable table (always exit 0, informational), one JSON artifact uploaded with `if: always()` (always exit 0, retained evidence), and a final gate invocation with `ignore-unfixed: true` and no `.trivyignore` that fails the job on any fixable HIGH/CRITICAL finding. Every third-party action is pinned to a full commit SHA with a version comment. Compose containers and volumes are always torn down (`if: always()`). No registry publication, signing, attestation, SARIF upload, Kubernetes, Terraform/AWS, or broader Milestone 16 security scanning is included.

**Why now:** Containerizing the backend is meaningful once its set of services is actually stable (through Milestone 13); doing so immediately, before observability/security/frontend work, means every later milestone builds and tests against real containers instead of host-installed Python, and keeps the frontend's own container (added in Milestone 17) additive rather than requiring the backend to be re-packaged later. Container scanning begins here and remains enforced in every later milestone that ships an image.

**Completion gate:** A clean checkout can build and run the backend platform (API, worker, outbox relay, Kafka consumer, and their PostgreSQL/Redis/Kafka dependencies) through Docker Compose with no host-installed Python tooling beyond Docker, and PR/`main` container CI (Slice 14C) passes. **Gate passed:** Pull Request #26 (merge commit `f5c421f`) — both the PR CI run and the resulting `main` CI run's `build-and-verify` check completed with conclusion `success`, independently confirmed against the public GitHub API for commit `f5c421f`.

---

## Milestone 15 — Observability, LangSmith, semantic grading, and advisory operations analysis

**Status:** Current (working branch `milestone-15-semantic-evaluation`). Slice 15A is **Complete** through Pull Request #27 / `8a25935`. Slice 15B (mandatory LangSmith AI observability) is **Complete** through Pull Request #28 / `062fb92`, with green PR CI and resulting `main` CI. Slice 15C is **Current**. Slice 15C1 Phase 1 is checkpointed/frozen for calibration at commit `936a74a`. Slice 15C1 held-out labels were approved on 2026-08-13 before predictions and were not modified after predictions. Live held-out calibration ran on 2026-08-13 and the frozen calibration gate passed. Slice 15C1 is recorded locally and is not marked Complete pending review. Slice 15C2 has not started. `evaluation.candidate.v1` remains provisional.

**Goal:** Make the distributed local platform explainable and measurable — for both infrastructure and AI behavior — before defending it, and provide a bounded advisory analyst over that telemetry without granting it any control-plane power.

**Existing agent, grading, repair, retry, and monitoring truth (for context, not new work in this milestone):**

- Existing specialists (Milestone 11): planner specialist, governed research/retrieval specialist, report synthesizer, deterministic citation verifier.
- Existing evaluation/grading (Milestone 12): citation-integrity grader, tool-use grader, report-structure grader, coverage grader, completeness grader, lexical groundedness grader, durable evaluation runs/results, evaluation ownership fencing, human-review routing, and provisional `evaluation.candidate.v1` (frozen `evaluation.v1` remains deferred).
- Existing repair/retry (Milestone 12): a deterministic recovery policy controls all loops; one bounded report-repair attempt is allowed for eligible structural failures and re-enters drafting/synthesis only (never planning, research, or tools automatically); transient failed jobs may receive at most two job-level retries with exponential backoff and bounded jitter; permanent, ownership, citation-integrity, and tool-policy failures fail closed; an AI component may generate a repaired draft, but deterministic policy — not an unconstrained retry agent — decides whether repair/retry is allowed.
- Existing monitoring (Milestone 13 Slice 13A): a Redis-backed worker heartbeat exists, plus durable job, workflow, model, tool, evaluation, outbox, and consumer records. A dedicated monitoring agent does **not** exist yet. The heartbeat alone is not full monitoring or job health — full observability is this milestone.
- Existing consumer retry/DLQ/replay (Milestone 13 Slice 13C2B): bounded, deterministic Kafka-consumer retry with a runtime processing deadline; permanent-poison classification into a PostgreSQL-backed dead-letter store; offset commit only after durable DLQ persistence; a local operator replay CLI with durable ownership fencing. **Complete** through Pull Request #25, merge commit `865023b` — this milestone adds telemetry/observability around that behavior (e.g. DLQ/retry metrics), not the retry/DLQ/replay mechanism itself.
- Milestone 14 (backend container foundation) now precedes this milestone in the approved roadmap order; this milestone's build/runtime CI work assumes the backend already runs as containers.

**Build in three reviewable slices:**

### Slice 15A — Operational telemetry foundation

**Complete** through Pull Request #27 / `8a25935`. Divided into three independently approved sub-slices so structured logging, metric production, and the complete dashboards/tracing/alerting stack each got their own review boundary: 15A1 (structured logging + correlation context), 15A2 (Prometheus metric production and scrape endpoints — the local metric contract application code emits, without a Prometheus server), and 15A3 (Prometheus server, Grafana, Alertmanager, OpenTelemetry tracing, dashboards, alerts, and the complete local observability Compose profile that consumes 15A1/15A2's output). Historical local checkpoints (`a5b1b0c` / `1685f50` / `83b82b7`) were superseded by the merged Slice 15A PR.

#### Slice 15A1 — Structured JSON logging and correlation context

**Complete** through Pull Request #27 / `8a25935`. Centralized, structured, sanitized JSON logging (`src/atlas/observability/logging.py`: `configure_logging`, `AtlasJSONFormatter`, `log_event`, `log_exception_boundary`) built on the Python standard library only (no new third-party dependency). A fixed, closed event-name set (`src/atlas/observability/events.py`, `Event(StrEnum)`) — no f-string/free-text event names anywhere. A fixed, approved JSON field allowlist (`timestamp`/`severity`/`service`/`event` always present; `trace_id`/`span_id`/`research_job_id`/`workflow_execution_id`/`node_name`/`model_invocation_id`/`tool_invocation_id`/`evaluation_run_id`/`outbox_event_id`/`consumer_event_id`/`error_class`/`duration_ms`/`outcome` present as `null` when unset, never omitted); `trace_id`/`span_id` are always `null` in this sub-slice and remain so through Slice 15A2 — Slice 15A3 (OpenTelemetry tracing) is what populates them. Only `exc.__class__.__name__` may ever represent an exception; no `str(exc)`/`repr(exc)`/`exc.args`/`exc_info`/`stack_info`/`logger.exception` path exists anywhere in the approved helpers. A legacy/third-party (non-Atlas-structured) line renders as exactly `timestamp`/`severity`/`service`/`event`(=`unstructured_log_suppressed`)/`logger_category` — a normalized, fixed-allowlist category, never the raw logger name, message, arguments, `exc_info`, `stack_info`, or `extra`. `AtlasJSONFormatter` serializes with `json.dumps(..., allow_nan=False)`, and `log_event` itself rejects `NaN`/`Infinity`/a negative `duration_ms` before anything is serialized. `contextvars`-based Atlas business correlation context (`src/atlas/observability/context.py`: `bind_context`, `current_context`) with proven nested-restoration, `asyncio`-task isolation, `threading` non-propagation, unsupported-field/oversized-value handling, and external immutability (`current_context()` returns a `types.MappingProxyType` view — mutation raises `TypeError`) — bound at no production call site yet (no per-job/per-message identifier is available at the entrypoint boundaries this sub-slice converts). An explicit, tested third-party logger policy (`uvicorn.access` suppressed; `uvicorn`/`uvicorn.error` rerouted through Atlas's own JSON envelope; `psycopg.pool`/`httpx`/`httpcore`/`redis`/`sqlalchemy.engine` level-adjusted; `confluent_kafka` never touches Python `logging` at all) controls level/routing only — every record still renders through the same fixed, sanitized shape. Converted call sites: the API's logging setup and `/ready` failure path; the worker, outbox-relay, Kafka-consumer, and Kafka-topic-admin entrypoints' startup/shutdown/signal/poll-loop boundaries. Not a repository-wide logging rewrite — deeper call sites remain unconverted, documented accurately rather than silently left inconsistent. See `PROJECT_STATE.md`'s "Verification (Milestone 15 Slice 15A1)" for full evidence.

#### Slice 15A2 — Prometheus metric production and scrape endpoints

**Complete** through Pull Request #27 / `8a25935`. Scope is metric *production* and scrape *contracts* only — no Prometheus server, Grafana, Alertmanager, or OpenTelemetry existed at the time this sub-slice was implemented (all Slice 15A3, now also locally implemented — see below). `prometheus-client>=0.21.0,<1` (resolved `0.26.0`) is the only new third-party dependency; both `pyproject.toml` and `uv.lock` are part of commit `1685f50`. `atlas.observability.metrics` (`catalog.py`, `normalize.py`, `exposition.py`) is the single place any Prometheus object is constructed: one `AtlasMetrics` class owns one `CollectorRegistry` and every metric family (HTTP request count/duration; research-job submissions/terminal outcomes; worker claim/processing outcomes and duration; workflow node executions/duration; model logical invocations, physical attempts, token/cost; governed tool invocations/attempts/duration; evaluation run/dimension outcomes; human-review decisions; recovery-policy decisions by action/failure-category; outbox relay-run outcomes and published-event counts as two distinct counters, the relay-lock-held gauge, and backlog size/age/collection-staleness gauges; Kafka consumer message outcomes, per-stage retry attempts, offset-commit outcomes, and dead-letter events by failure code; Redis rate-limit decisions; worker heartbeat writes and last-success timestamp; database readiness failures). A focused correction pass fixed several post-commit-ordering and lifecycle issues after the initial implementation: evaluation/review/recovery-decision metrics now emit strictly after their own durable-transaction commit and only for a freshly-persisted (non-replayed) row; `atlas_outbox_publications_total` was renamed to `atlas_outbox_relay_runs_total` (counts `run_once()` calls, not events) alongside a new `atlas_outbox_published_events_total` (counts actual published events); `MetricsServerHandle.close()` is now thread-safe and genuinely bounded even if the underlying server's `shutdown()` blocks; and a `generate_latest()` failure at scrape time now returns a sanitized `503` (API and each role's internal server) instead of propagating. Every label is drawn from a bounded, reviewed allowlist (an unrecognized value degrades to a fixed `"other"`, never grows cardinality); HTTP route/status normalization (`normalize.py`) uses an explicit `path_format`-to-canonical-label allowlist (`"unmatched"` when no route matched, `"other"` when a route matched but its template is not approved) and an exact-status-code allowlist with bounded `NxxCode_other`/`"other"` buckets. The API exposes `/metrics` unauthenticated on its existing ASGI port via a pure ASGI middleware (not `BaseHTTPMiddleware`, to preserve streaming/exception behavior and read the post-routing route template); the worker, outbox relay, and Kafka consumer each start a minimal internal-only `wsgiref`-based HTTP server on a fixed container-internal port (`metrics_port`, default `9464`, never published by `docker-compose.yml`) that fails open (continues without a metrics endpoint) on a bind or thread-start failure. Every `observe_*`/`set_*` method is wrapped in the same failure-containment boundary as Slice 15A1's structured logger — a metrics-library exception is logged and suppressed, never allowed to fail a job, request, provider call, publication, or consumer transaction. Prometheus multiprocess mode is not used (current one-process-per-container runtime does not require it). Kafka consumer lag and model/tool ownership-lost attempt observations are explicitly deferred (documented in `docs/TECHNICAL_DESIGN.md`). A focused cross-process integration test (`tests/integration/test_cross_process_metrics.py`, opt-in against a running Compose stack) proves each of the four roles exposes its own reachable, independent registry and that a real end-to-end research-job submission advances the expected counter on each role. See `PROJECT_STATE.md`'s "Verification (Milestone 15 Slice 15A2)" for full evidence.

#### Slice 15A3 — Prometheus server, Grafana, Alertmanager, OpenTelemetry tracing, dashboards, and alerting

**Complete** through Pull Request #27 / `8a25935`. Manual OpenTelemetry distributed tracing (`src/atlas/observability/tracing/`: `configure_tracing`/`TracingProviderHandle`, strict W3C version-`00` `traceparent` parsing/formatting, `run_in_span` for cross-thread propagation, fixed per-role `Resource` identity) spans API → worker → LangGraph nodes → model/tool attempts → outbox enqueue → Kafka → consumer, finally populating the `trace_id`/`span_id` fields Slice 15A1 reserved as always-`null` through Slice 15A2. A new migration (`20260812_0014`) adds nullable `traceparent` columns to `research_jobs`/`outbox_events` and a nullable, persistence-only `research_jobs.initial_traceparent_consumed_at` marker: the marker is set atomically in the successful first-claim transaction, and only that claimant may treat the stored API `traceparent` as a direct parent — any later claim, including an immediate crash/lease reclaim, starts a new root trace with a Span Link instead. The full local observability Compose profile — digest-pinned Prometheus, Grafana, Alertmanager, Tempo (monolithic, no Kafka backend), and the core OpenTelemetry Collector distribution, plus a new stdlib-only Atlas-owned internal Alertmanager webhook receiver (`src/atlas/observability/alert_receiver.py`, reusing the shared backend image, no host port published) — scrapes/collects what Slices 15A1/15A2/15A3 emit: Prometheus scrape/recording/alert rules (globally-aggregated, denominator-guarded HTTP error ratio; `AtlasWorkerHeartbeatStale` scoped to `job="atlas-worker"` so default-zero gauges from other roles cannot fire it, with a missing worker series producing no alert; outbox-backlog-growing and scrape-target-down alerts, all validated with `promtool`); Grafana with anonymous Viewer-only access (default admin account and login form both disabled, bound to `127.0.0.1`, no credential committed) and file-provisioned dashboards/datasources; Alertmanager routed to the internal receiver (validated with `amtool`, demonstrated end-to-end fire→route→resolve against a real Alertmanager process). See `PROJECT_STATE.md`'s "Verification (Milestone 15 Slice 15A3)" for full evidence, including the Collector-distribution trade-off (core, not contrib — core does bundle the `health_check` extension per the upstream manifest, configured on `0.0.0.0:13133`, but the official core image is a `FROM scratch` build with no shell/curl/wget, so no Docker-level `healthcheck:` is defined for that service), the SDK/Collector bounds, and — from a later correction/runtime-verification pass — a completed, fully containerized Compose acceptance test (all images pulled, full topology started, live end-to-end Tempo trace, Grafana anonymous-Viewer-only access, a real Prometheus-alert fire→route→resolve cycle, a Collector-outage fail-open demonstration, and a clean graceful shutdown/teardown) plus alert-receiver startup/shutdown hardening for parity with the worker/outbox-relay/consumer executable boundaries.

### Slice 15B — Mandatory LangSmith AI observability

**Complete** through Pull Request #28 / `062fb92`, with green PR CI and resulting `main` CI. LangSmith is **mandatory** for Atlas AI observability — it must never be described as optional. Distinction to preserve: LangSmith is mandatory for LangGraph/LLM/RAG/tool/evaluation observability; OpenTelemetry, Prometheus, Grafana, Alertmanager, and structured logging remain mandatory for infrastructure and distributed-system observability; LangSmith does not replace operational monitoring; and LangSmith must never become an availability dependency — an export outage must not fail a research job.

Demonstrated in this working tree (see `PROJECT_STATE.md` "Verification (Milestone 15 Slice 15B)"):

- Focused hierarchy spike first: native LangGraph/LangChain runs under `tracing_context` already provide `atlas.research_job` → `atlas.research_graph` → node names (`validate`, `plan`, `research`, `draft`, `verify_citations`, `evaluate`, `policy`, `complete`). Those native runs are retained. Specialists and `_wrap_node` are not wrapped.
- Explicit Atlas traces only where the spike proved a gap: `model.plan` / `model.draft` (fake ports emit `run_type="llm"`; LangChain-backed ports emit a `chain` parent with the native provider LLM run beneath), governed `tool.{tool_id}`, `retrieval`, `evaluation.run` + `dimension.{name}`.
- Worker-owned LangSmith Client (`langsmith>=0.10.17,<0.11`, resolved `0.10.17`) with `hide_inputs=True` / `hide_outputs=True`, closed metadata allowlist, `tracing_mode="langsmith"`. OTel correlation is metadata-only (`atlas.otel_trace_id` / `atlas.otel_span_id`).
- Settings validates LangSmith field types and URL syntax only. The mandatory key for live model/tool/embedding providers is enforced at worker AI composition/startup, not in global `Settings`, so the API can construct Settings with live-provider fields and no LangSmith key.
- Compose maps `ATLAS_LANGSMITH_API_KEY`, `ATLAS_LANGSMITH_PROJECT`, `ATLAS_LANGSMITH_API_URL`, and `ATLAS_LANGSMITH_TIMEOUT_MS` onto `worker` only. An unset URL preserves the SDK hosted default.
- Fail-open enqueue/flush after successful config; fake/offline without a key stays network-free. Bounded `Client.flush(timeout=5)`.
- Dataset/experiment orchestration is test-only (`tests/observability/langsmith_dataset_support.py` over `tests/evaluation/candidate_goldens.v1.json`). Production code does not import tests or golden fixture paths. Boolean compare against `grader_expected`. Unique live experiments are retained for manual cleanup.
- Metric: `atlas_langsmith_operations_total{operation,outcome}` with closed allowlists. `operation=export` is error/timeout-only.
- Offline tests require no key and must not inherit a developer `.env`: Settings/containment tests `delenv` `ATLAS_LANGSMITH_*` and `chdir` into `tmp_path` before constructing `Settings()`. Opt-in live tests require `ATLAS_ENABLE_LIVE_LANGSMITH_TESTS=1` and `ATLAS_LANGSMITH_API_KEY`; skipped in CI; fake Atlas providers only. The workflow live test polls `list_runs(trace_id=...)` until the required root/graph/native-node/explicit-boundary runs and in-trace parents are present rather than accepting the first partial snapshot. Local live verification (2026-08-13) passed in LangSmith project `atlas-slice-15b-live-20260813160447` (`7 passed, 0 skipped` on the two live modules). Local complete-suite gate (2026-08-13): isolated Pytest `1126 passed, 7 skipped`; full Pytest `1341 passed, 11 skipped`. Slice 15B is Complete through Pull Request #28 / `062fb92`.

Not in this slice: live semantic grader, held-out calibration, `evaluation.v1` freeze, advisory analyst (all Slice 15C). No Alembic migration.

- Mandatory LangSmith integration through LangChain/LangGraph.
- Traces for planner, researcher, synthesizer, citation verification, grading, repair, retries, model calls, tool calls, and retrieval.
- Safe correlation with durable Atlas identifiers (job, workflow execution, node execution, model invocation, tool invocation, evaluation).
- Dataset-based LangSmith evaluation tracing (traces of existing deterministic/Fake grading runs; the live semantic grader itself is Slice 15C work).
- An explicit prompt/response/evidence redaction and sampling policy defining exactly which prompts, responses, evidence, and metadata may be sent.
- Cost, token, latency, error, and retry metadata.
- Local/offline tests must not require a real LangSmith API key: contract tests use fakes/mocks; at least one explicit opt-in live integration test proves traces appear in a real LangSmith project; a simulated LangSmith outage proves research continues and the failure is logged/metriced safely.
- No API keys, raw secrets, unrestricted evidence, or unsanitized exception text may ever be exported to LangSmith.

### Slice 15C — Live semantic grader, held-out calibration, and the bounded advisory analyst

**Current** (working branch `milestone-15-semantic-evaluation`). Slice 15C1 Phase 1 (live semantic-grader foundation) is checkpointed and frozen for calibration at commit `936a74a08e3e5d20fc0e93e55cee4fbc0102f4b8`. Slice 15C1 Phase 2 held-out labels were approved on 2026-08-13 before predictions and were not modified after predictions. Slice 15C1 live held-out calibration ran on 2026-08-13 (`openai`/`gpt-4o-mini`; LangSmith experiment `atlas.15c1.heldout.67ff260be9b8-53559ce0`); automated criteria passed; human systematic-failure review passed; the frozen calibration gate passed. Slice 15C1 is recorded locally and is not marked Complete pending review. Slice 15C2 and the bounded advisory analyst have not started. `evaluation.candidate.v1` remains provisional. Passing calibration does not automatically create `evaluation.v1`. This 20-case / 23-claim run is a bounded calibration, not statistical proof.

Slice 15C1 Phase 1 checkpoint (`936a74a`; see `PROJECT_STATE.md` "Verification (Milestone 15 Slice 15C1)"):

- Explicit `ATLAS_SEMANTIC_GRADER_MODE=skipped|fake|live`, default `skipped`. Live is never inferred from provider selection. Worker startup rejects `live` plus a fake model provider. Not a global API availability requirement.
- Bounded typed semantic input (claims + job-linked excerpts only), deterministic assembly, fingerprint extension, `LangChainSemanticGroundednessGrader` via existing model composition, malformed-only two-attempt ledger cap, and quality-versus-availability evaluation outcomes.
- Pass threshold remains exactly `0.70`. Prompt version remains `semantic_groundedness.v1`. No `evaluation.v1` freeze. No Alembic migration. No per-claim semantic SQL tables.

Slice 15C1 Phase 2 demonstrated in this working tree:

- Independent held-out dataset `tests/evaluation/held_out_semantic.v1.json`, distinct from `candidate_goldens.v1`. Proposed labels were authored before predictions against the frozen Phase 1 contract. The project owner approved those labels on 2026-08-13 (`human_reviewed: true`). Approval occurred before predictions. Labels were not modified after predictions. The grader remains frozen at checkpoint `936a74a08e3e5d20fc0e93e55cee4fbc0102f4b8`.
- Test-only calibration harness. Frozen promotion criteria (supported precision/recall ≥ 0.80, macro-F1 ≥ 0.75, report F1 ≥ 0.80, no safety or availability failure, plus explicit human `no_unexplained_systematic_failure`) do not automatically create `evaluation.v1`. `summarize_predictions` reports `automated_criteria_met` only; the final `promotion_criteria_met` stays pending until `finalize_promotion_gate`.

Slice 15C1 Phase 3 live calibration recorded (2026-08-13; not a live rerun):

- Provider/model `openai`/`gpt-4o-mini`. LangSmith project `atlas-local`, experiment `atlas.15c1.heldout.67ff260be9b8-53559ce0`. 20/20 quality outcomes; availability 0; safety-boundary false; supported P/R/F1 1.000; unclear 0.500; unsupported 0.917; macro-F1 0.806; score MAE 0.0804; report F1 1.000. Automated criteria passed. Human systematic-failure review passed. Final calibration gate passed. Fingerprint `0bd236a522847cc9f0996fbe3be71d389ca4af15ed48c8990054cf301e34433b` unchanged. Bounded 20-case / 23-claim set; unclear class has two human labels; one OpenAI configuration.

Remaining Slice 15C work (not started):

- Bounded advisory analyst (Slice 15C2).
- `evaluation.candidate.v1` remains provisional. Freezing `evaluation.v1` is a separate, explicitly reviewed decision — passing this held-out/live-semantic calibration gate does not automatically freeze `evaluation.v1`.

An advisory AI analyst that consumes sanitized, bounded telemetry summaries (never unrestricted raw production data) to: summarize incidents; cluster recurring failures; suggest likely causes and remediation; and explain job, agent, model, tool, retrieval, Kafka, and evaluation failures.

It must **not**: restart workloads; retry jobs; change configuration; modify prompts; deploy code; acknowledge alerts; mutate database state; or invoke infrastructure APIs. Deterministic Prometheus/Alertmanager rules remain authoritative — the advisory analyst is not the monitoring control plane.

**Why now:** Basic instrumentation grows throughout earlier milestones; a dedicated slice is needed once enough distributed components (API, worker, PostgreSQL, Redis, Kafka producer + consumer) exist that correlating one job across all of them provides real value, and AI-specific observability/grading is the natural next step once infrastructure telemetry exists to correlate it against.

**Completion gate:**

- One complete job is traceable end to end in both LangSmith and the local operational stack.
- A live semantic grade is persisted.
- A LangSmith export failure does not fail the job.
- Tested deterministic alerts fire for meaningful failure conditions.
- Dashboards answer latency, throughput, failure, retry, backlog, lag, cost, and quality questions.
- Sensitive information is excluded from all telemetry (metrics, traces, logs, LangSmith exports).
- The advisory analyst explains a sanitized test incident without any mutation capability.

---

## Milestone 16 — Security, authentication, and supply-chain GitHub Actions

**Status:** Pending

**Goal:** Make the CI pipeline and the API itself defensible: authenticated APIs, scoped tools, and automated supply-chain scanning, not just green tests.

**Build:**

- API authentication and authorization.
- RBAC for operator/review/replay endpoints.
- Prompt-injection and governed-tool hardening.
- Dependency vulnerability scanning with a `uv`-compatible scanner.
- Static security analysis using Bandit and/or Semgrep.
- Secret scanning using Gitleaks or equivalent.
- GitHub dependency review on pull requests.
- CodeQL.
- SBOM generation.
- License-policy checks.
- GitHub Actions workflow validation.
- Verification that actions remain pinned to full commit SHAs.
- Documented vulnerability suppression with justification, owner, and expiration.
- Sanitized scanner output.

**Why after observability:** Security review is more effective once logs/traces/metrics exist to show what a security control actually observes and blocks; this also keeps Milestone 15 focused on visibility rather than mixing in an unrelated authn/authz surface.

**Completion gate:**

- A deliberately vulnerable test dependency or fixture is detected.
- A test secret is detected.
- Unauthorized API requests fail with structured responses.
- No finding is silently ignored.
- CI remains green only when findings are fixed or explicitly reviewed and time-bounded.

---

## Milestone 17 — Next.js, TypeScript, Tailwind frontend, document upload, and frontend container

**Status:** Pending

**Goal:** Give Atlas a real user-facing surface instead of only an HTTP API, close the "live document ingest" gap left open since Milestone 10, and complete the container release (started for the backend in Milestone 14) by adding the frontend's own image.

**Build:**

- Next.js, TypeScript, Tailwind.
- Typed client contracts.
- Research submission; job progress/status.
- Reports, citations, provenance, and evaluation results.
- Operator review flow.
- Multipart document uploads feeding the evidence/embedding pipeline (replacing the current JSON/text-only ingest path).
- Browser-safe errors (no raw backend exception text reaching the UI).
- Frontend lint, type checks, unit/component tests, and backend integration tests.
- No `any` without an explicit reviewed justification.
- A frontend container image: multi-stage build, non-root runtime user, minimal runtime contents, immutable Git-SHA tag, and appropriate health/readiness behavior — matching the standard already set for the backend images in Milestone 14. Docker Compose is extended to run the frontend alongside the already-containerized backend. GitHub Actions builds, starts, and smoke-tests the frontend image the same way it already does for the backend images, and scans it (vulnerability scan + SBOM) the same way.

**Why here:** A frontend is more valuable once the backend has observability and baseline security (Milestones 15–16) to build on, and document upload is the one Milestone 10 gap (live arbitrary-URL fetch/HTML/PDF ingest/object storage) most directly unblocked by adding a real upload surface. The frontend's container is added here, once the frontend itself exists, rather than in Milestone 14 — the backend does not need to wait for the frontend to be containerized, and the frontend does not need a placeholder image before there is anything to package.

**Completion gate:** A user can upload a document, submit research, follow progress, and view a cited and evaluated report without directly calling the API. A clean checkout can build and run the complete platform (backend, from Milestone 14, plus this milestone's frontend image) through Docker Compose with no host-installed Python or Node tooling beyond Docker.

---

## Milestone 18 — Local Kubernetes and Helm

**Status:** Pending

**Goal:** Run the backend images from Milestone 14 and the frontend image from Milestone 17 on Kubernetes locally, on `kind`, before Kubernetes is ever attempted in AWS — per the local-first, cloud-portable rule.

**Build:**

- A reproducible `kind` cluster (the default local Kubernetes distribution).
- Helm charts for the API, worker, outbox relay, Kafka consumer, and frontend.
- Database migration and Kafka topic initialization Jobs/hooks.
- Services and local ingress; ConfigMaps; Kubernetes Secrets without committed credentials.
- Resource requests/limits; workload-specific liveness/readiness/startup probes (including the caveat already carried locally that a healthy process does not imply every partition/business handler is healthy).
- Persistent-volume behavior where required.
- A local observability stack running on `kind`.
- Upgrade, rollback, teardown, and recreation workflow.
- CI Helm lint/template validation; CI `kind` deployment and smoke testing where practical.

**Why here:** Kubernetes and Helm must first prove themselves locally on `kind`, the same way every other cloud-bound capability does; EKS (Milestone 22) reuses these exact charts rather than being the first place they ever run.

**Completion gate:** The same images proven in Compose install through Helm on `kind`, complete an end-to-end research job, survive tested pod restarts, upgrade and roll back correctly, and recreate cleanly.

---

## After local completion — Cloud architecture

Cloud design work is intentionally out of scope until Milestone 19 passes. Milestones 20–24 then use measured local behavior to design, provision, deploy, and validate Atlas on AWS, per the local-first, cloud-portable governing rule above.

## Milestone 19 — Local E2E, load, failure, recovery, and backup/restore validation

**Status:** Pending

**Goal:** Prove the fully containerized platform — both Docker Compose and `kind`/Helm — behaves correctly under realistic load and failure, not just under unit/integration tests. This is the final local release gate.

**Build**, validating both Docker Compose and `kind`/Helm:

- Full browser-to-report end-to-end tests.
- Concurrent load; rate limiting under load.
- Broker, database, Redis, worker, relay, consumer, and pod failures.
- Kafka retry/DLQ/replay drills (Slice 13C2B).
- Stuck-job and stale-worker behavior.
- Backup and restore; restart/rescheduling; scaling; recovery-time measurements.
- Security-control verification.
- Observability and LangSmith trace continuity.
- Local limitations documented honestly.

**Why last locally:** This is the local platform's final gate — it validates the integration of every earlier milestone together, across both deployment surfaces (Compose and `kind`/Helm), which is only possible once Milestones 14, 17, and 18 exist.

**Completion gate:** The full E2E suite passes against both the Compose and `kind`/Helm stacks; a documented load profile with measured latency/throughput/failure rates exists; each tested failure recovers without data loss or duplicate business effects; a backup can be restored and validated; bottlenecks and local limitations are documented.

---

## Milestone 20 — Local and AWS Visio/system-design diagrams

**Status:** Pending

**Goal:** Turn validated local architecture into credible design artifacts before any cloud provisioning.

**Build:**

- Validated local logical architecture.
- Docker Compose deployment view; `kind`/Kubernetes deployment view.
- AWS network/deployment view.
- Trust boundaries; data flows; failure/recovery flows; observability flows; CI/CD flow.
- An explicit local-to-AWS service mapping and trade-off analysis.

Include Route 53, WAF, ALB/API routing, VPC/subnets, EKS, ECR, RDS/Aurora PostgreSQL/pgvector, ElastiCache, MSK, S3, Secrets Manager, IAM, and selected AWS observability services only after design review.

**Why now:** Diagrams produced before Milestone 19 would describe an unvalidated system; producing them immediately after gives the cloud milestones a concrete, defensible target instead of a speculative one.

**Completion gate:** Diagrams and trade-off analysis are reviewed and approved before any Terraform code is written; every local component has an explicit AWS mapping or an explicit, justified decision to omit/replace it.

---

## Milestone 21 — Terraform AWS infrastructure

**Status:** Pending

**Goal:** Provision the AWS infrastructure designed in Milestone 20 as reviewable, versioned code.

**Build:**

- Networking; EKS; managed data services selected in Milestone 20.
- ECR; IAM; Secrets; remote Terraform state and locking.
- Least privilege; environment separation; drift detection.

GitHub Actions: `terraform fmt -check`; `terraform validate`; TFLint; Checkov or tfsec; Terraform plan on PR; protected apply only after review/approval (no automatic apply from an unreviewed PR); controlled destroy/recreate validation.

**Why after the diagrams:** Infrastructure-as-code should implement an already-approved design, not drive architecture decisions ad hoc.

**Completion gate:** `terraform plan`/`apply` provisions a working environment from a clean state; a destroy/recreate cycle succeeds; no secrets are committed to Terraform state or source; least-privilege IAM is documented and reviewed.

---

## Milestone 22 — Kubernetes/EKS deployment

**Status:** Pending

**Goal:** Run the containerized platform on the EKS cluster provisioned in Milestone 21, reusing the Helm charts already validated on `kind` in Milestone 18 — EKS must not be the first place the Helm workloads run.

**Build:**

- ECR image pull; AWS-native workload identity; AWS secrets integration.
- Ingress/load balancer; network policies; autoscaling; pod disruption budgets.
- Safe migrations; rolling update; rollback; managed-service connectivity.

**Why after Terraform:** Kubernetes workloads need the cluster and networking Milestone 21 provisions; deploying before that would have nowhere to run.

**Completion gate:** A clean `helm install` (the same charts proven on `kind`) against the provisioned EKS cluster brings up a working platform reachable end to end; probes correctly reflect real health; a rolling update/rollback succeeds without downtime for stateless components.

---

## Milestone 23 — Cloud CI/CD, promotion, verification, and rollback

**Status:** Pending

**Goal:** Automate build, promotion, and safe rollback for the deployed cloud platform.

**Required pipeline:** pull request → formatting/lint/type/unit/integration/evaluation tests → security and secret scans → image builds → image scans and SBOMs → container smoke tests → `kind`/Helm tests → Terraform plan → review/merge → immutable image publication to ECR → staging deployment → post-deploy smoke/E2E tests → approval/promotion → production deployment → health verification → automatic or controlled rollback on failure. No production deployment directly from an unreviewed pull request.

**Why after the deployment exists:** Automating promotion and rollback requires a real deployed target (Milestone 22) to promote to and roll back against.

**Completion gate:** A merged change is built, scanned, deployed to a non-production environment, verified, and promotable to production through the pipeline without manual server access; a deliberately bad deploy is caught and rolled back automatically or via a documented one-command procedure.

---

## Milestone 24 — Cloud validation, runbooks, cost analysis, portfolio demonstration, and interview narrative

**Status:** Pending

**Goal:** Close out the project as a defensible, explainable, portfolio-ready system.

**Build:**

- Cloud load/failure/recovery validation; backup and disaster-recovery drills.
- Operational and security runbooks.
- Cost tracking and optimization analysis.
- LangSmith and operational observability demonstration.
- Architecture diagrams; recorded/scripted demo; written interview narrative.
- Evidence-based explanation of trade-offs and limitations across all 24 milestones.

**Why last:** This milestone only synthesizes and validates what every prior milestone already proved; it adds no new architecture.

**Completion gate:** Cloud failure drills recover without data loss; runbooks are reviewed and actionable; a cost figure with at least one identified optimization is documented; the demo and interview narrative are reviewed as accurate representations of what was actually built and verified (not aspirational claims).

---

## Planned technology coverage

The roadmap provides justified entry points for Python, AsyncIO, FastAPI, Pydantic, Pytest, Ruff, mypy, GitHub Actions, PostgreSQL, SQLAlchemy, Alembic, Docker Compose, LangChain/LangGraph, OpenAI/Anthropic, RAG, pgvector, MCP/FastMCP, specialist agents, grading/evaluations, retries and recovery, Redis, Kafka, OpenTelemetry, Prometheus, Grafana, Alertmanager, structured logging, mandatory LangSmith AI observability and live semantic grading, a bounded advisory operations analyst, security/supply-chain scanning (Bandit/Semgrep, Gitleaks, CodeQL, SBOM), Next.js/TypeScript/Tailwind, complete local containerization, local Kubernetes/Helm via `kind`, Terraform, EKS, and cloud CI/CD.

Kubernetes and Helm begin locally on `kind` at Milestone 18 — not for the first time in AWS. Container scanning begins when the first (backend) images exist at Milestone 14 and remains enforced thereafter, including the frontend image added at Milestone 17. Security GitHub Actions begin at Milestone 16. Mandatory LangSmith begins at Milestone 15. Local work completes after Milestone 19; cloud architecture starts at Milestone 20 (Terraform is Milestone 21, EKS is Milestone 22, cloud CI/CD is Milestone 23, and final cloud validation is Milestone 24) — 24 milestones in total.
