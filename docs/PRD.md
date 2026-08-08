# Atlas AI Platform — Product Requirements Document

## Product vision

Atlas turns a complex research request into a durable, traceable, evidence-backed report through coordinated specialist agents. It is an AI system and operations platform, not a single-turn chatbot.

## Problem

Complex research requires planning, source discovery, evidence management, synthesis, verification, and recovery from partial failures. Basic chat interfaces hide this work inside one model call, making quality difficult to measure and failures difficult to diagnose or resume.

## Primary user

The initial user is a professional researcher or knowledge worker who needs a cited answer and visibility into how it was produced. Robert is also an operator/developer persona who must inspect, test, deploy, and explain the system.

## Flagship workflow

1. A user submits a bounded research question and receives a job identifier.
2. Atlas validates the request and creates durable workflow state.
3. A planner decomposes the request into bounded tasks.
4. Specialist agents use approved tools and retrieval sources to gather evidence.
5. Atlas stores source provenance and intermediate results.
6. A synthesis step creates a cited report.
7. Evaluation checks evidence coverage, citation support, completeness, and policy compliance.
8. Failed or low-quality steps receive controlled retry, repair, escalation, or termination.
9. The user can view status, final output, and useful operational diagnostics.

## Goals

- Produce evidence-backed reports with traceable citations.
- Make long-running work resumable and failures observable.
- Demonstrate bounded multi-agent orchestration and governed tool use.
- Evaluate AI quality systematically rather than relying on manual impressions.
- Run locally and retain a credible, documented path to AWS.
- Provide concrete experience with production backend, reliability, security, observability, and delivery practices.

## Non-goals for the first release

- General autonomous operation without bounds or approval controls.
- Training a foundation model from scratch.
- Supporting every model, data source, cloud, or user interface.
- Adding technologies without a defined responsibility and verification method.
- Claiming production scale before measurement supports it.

## Functional requirements

- Accept, validate, identify, and persist research jobs.
- Expose job status and final results through an API; a simple browser interface may follow.
- Plan and execute a bounded agent workflow with explicit states and transitions.
- Retrieve documents and external evidence through governed adapters/tools.
- Preserve source provenance and produce citations.
- Checkpoint progress and safely resume interrupted jobs.
- Enforce time, token, cost, retry, and tool-call budgets.
- Evaluate outputs and route failures to retry, repair, human review, or terminal failure.
- Record an auditable history of significant workflow actions.
- Allow model and prompt behavior to be versioned and evaluated.

## Non-functional requirements

- Reliability: duplicate requests and events must not produce uncontrolled duplicate work.
- Observability: logs, metrics, and traces must correlate requests, jobs, agents, tools, and model calls.
- Security: least privilege, protected secrets, safe input/tool boundaries, and auditable access.
- Portability: the local design must map clearly to AWS services without changing core domain behavior.
- Testability: deterministic logic and external integrations must be independently verifiable.
- Operability: failed work must be diagnosable, recoverable, and safe to replay.
- Cost awareness: token, model, storage, compute, and data-transfer costs must be measurable and bounded.

## Success measures

Before implementation, establish a small golden research dataset and define baselines for:

- Citation correctness and evidence coverage.
- Task completion and recovery rates.
- End-to-end latency and time spent per workflow stage.
- Model/token and infrastructure cost per job.
- Duplicate-work prevention and successful replay/resume.
- Regression-suite pass rate and operator time to diagnose failures.

Exact targets will be set after the architecture and first baseline measurements exist.

## Data, safety, and AI guardrails

- Store only necessary user and research data with explicit retention rules.
- Treat retrieved content as untrusted and defend against prompt injection.
- Restrict agents to allow-listed tools with scoped credentials and validated inputs/outputs.
- Preserve evidence provenance and distinguish model-generated claims from source material.
- Require human approval for materially risky or externally mutating actions.
- Never expose secrets, private prompts, or sensitive data in logs or evaluation artifacts.

## Open decisions

- The exact first research domain and golden dataset.
- Supported source types and external tool integrations for the first release.
- Local service boundaries and their AWS equivalents.
- Human-approval points and user-facing diagnostic detail.
- Initial measurable quality, reliability, latency, and cost targets.
