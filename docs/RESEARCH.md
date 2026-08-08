# Atlas Research and Technology Requirements

## Purpose

This document captures why Atlas is being built, the engineering capabilities it should demonstrate, and the technologies under consideration. It is a decision input—not a promise that every named technology must appear in the production path.

## Candidate and market alignment

Robert's résumé and LinkedIn emphasize backend and applied-AI engineering: Python, FastAPI, REST APIs, PostgreSQL, Redis, Kafka, LangChain, LangGraph, pgvector, RAG, multi-agent systems, LLM integrations, AWS, Terraform, Kubernetes, Docker, GitHub Actions, and CI/CD.

Current higher-paying NYC AI/backend roles repeatedly emphasize:

- Reliable agents, RAG, tool use, recovery paths, evaluation and regression suites.
- Python APIs and durable distributed systems with PostgreSQL, Redis, queues, Kafka, and asynchronous workflows.
- Production observability across quality, latency, cost, traces, metrics, logs, and alerts.
- Containers, Kubernetes, cloud infrastructure, security, identity, networking, and CI/CD.
- Idempotency, auditability, retries, background jobs, failure handling, and operational ownership.

Representative market references:

- [General Intelligence — Applied AI Engineer](https://jobs.ashbyhq.com/generalintelligencecompany/4bc5d479-3bba-432d-887f-423847aa650a)
- [Ramp — Applied AI Engineer](https://jobs.ashbyhq.com/ramp/d204e136-2749-42de-82b4-88a0dd352090/)
- [Overtone — AI Engineer](https://jobs.ashbyhq.com/overtone/bb959943-94cc-4f2e-b9a6-cb99ce3aa5e4)
- [AWS AgentCore — Sr. Software Development Engineer](https://www.amazon.jobs/en/jobs/10478272/sr-software-development-engineer-agentcore-aws-agentic-ai)
- [Hinge — Backend Engineer](https://jobs.lever.co/matchgroup/eee3f501-72ed-4d28-a25b-8a18288a60d6)

Job listings change; these links are evidence for recurring capabilities, not permanent requirements.

## Core implementation technologies

Each technology must receive a defined responsibility in the technical design before adoption.

| Area | Planned technologies | Intended learning or system responsibility |
|---|---|---|
| Language and API | Python, AsyncIO, FastAPI, Pydantic, REST, webhooks | Typed asynchronous service boundaries and API contracts |
| Persistence and retrieval | PostgreSQL, SQLAlchemy, Alembic, pgvector | Durable workflow state, relational data, migrations, metadata, and vector retrieval |
| Coordination and events | Redis, Kafka | Caching/rate limits/short-lived coordination and durable event streaming/replay |
| AI orchestration | LangChain, LangGraph, OpenAI API, Anthropic API | Provider abstraction, stateful workflows, agents, tools, and model calls |
| Knowledge and interoperability | RAG, embeddings, MCP, FastMCP | Evidence retrieval and governed tool/resource access |
| Testing and evaluation | Pytest, Testcontainers, golden datasets, grading agents | Software correctness, integration testing, and repeatable AI-quality evaluation |
| Observability | OpenTelemetry, Prometheus, Grafana, Alertmanager, CloudWatch | Correlated traces, metrics, dashboards, alerting, logs, and cloud operations |
| Packaging and runtime | Docker, Docker Compose, Kubernetes, Helm | Reproducible local services and portable orchestration |
| Cloud and infrastructure | AWS, Terraform | Infrastructure as code, identity, networking, compute, storage, and managed services |
| Delivery and security | Git, GitHub, GitHub Actions | Version control, CI/CD, image publishing, deployments, tests, and security scanning |

For AWS, the likely observability path is OpenTelemetry plus Prometheus-compatible metrics, Amazon Managed Service for Prometheus, Amazon Managed Grafana, and CloudWatch. This remains a design proposal until the Visio architecture is approved.

## Required production capabilities

- Structured logging with correlation, trace, user, job, agent, and tool identifiers.
- Metrics, distributed tracing, dashboards, actionable alerts, and SLO thinking.
- Timeouts, bounded retries with exponential backoff and jitter, circuit breaking, and rate limiting.
- Idempotent commands, deduplication, transactional event publication, dead-letter handling, and replay.
- Durable workflow state, checkpoints, human approval where risk warrants it, and controlled recovery.
- Offline evaluations, golden tasks, regression gates, grader calibration, and online quality/latency/cost monitoring.
- Authentication, authorization, secrets management, encryption, audit trails, prompt-injection defenses, and tool permissions.
- Unit, integration, contract, end-to-end, load, resilience, security, and infrastructure tests.
- Automated CI/CD with linting, type checks, tests, dependency/container/IaC scanning, image publishing, deployment verification, and rollback.
- Backups, restore testing, cost controls, and environment teardown procedures.

## Technologies requiring a separate justification

The résumé also includes JavaScript, TypeScript, MongoDB, Azure, Microsoft Graph, Google Workspace, and automation tooling. They should appear only when Atlas has a real requirement or a bounded comparison/experiment. Optional market-aligned experiments may include fine-tuning, PyTorch, self-hosted inference, or vLLM. None should be forced into the core architecture merely for keyword coverage.

## Research questions for system design

- What is the smallest flagship research workflow that proves Atlas is more than a chatbot?
- Which state belongs in PostgreSQL, Redis, Kafka, object storage, and the vector index?
- Where do LangGraph checkpoints end and platform-level job orchestration begin?
- Which workloads run synchronously versus asynchronously?
- What are the trust boundaries for users, agents, tools, models, and retrieved content?
- Which services run locally, and what are their exact AWS equivalents?
- What quality, latency, reliability, and cost signals determine success?
