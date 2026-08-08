# Atlas Technical Design

## Status

Local foundation decisions are recorded as they are validated. The comprehensive local-to-AWS system architecture remains incomplete until working local components exist and the Visio diagrams are reviewed.

## Validated local decisions

### Application package and API entrypoint

- Python 3.12 is the only supported runtime (`requires-python = ">=3.12,<3.13"`), managed with `uv` and a committed lockfile.
- Application code lives under `src/atlas` and is installed as the `atlas` package.
- The FastAPI application entrypoint is `atlas.main:app`.
- `GET /health` is the first HTTP contract and returns `{"status": "ok"}` for liveness checks.
- Quality gates are Ruff (lint), mypy (strict, `src` and `tests`), and Pytest.
- Runtime dependencies (FastAPI, Uvicorn) are separated from development dependencies (Pytest, httpx, Ruff, mypy).

These decisions are limited to the verified foundation slice. They do not imply database, agent, messaging, container, or cloud topology choices.

## Why the full diagram comes later

A Visio session will make the request path, trust boundaries, state stores, asynchronous workflows, agent/tool interactions, observability flow, deployment topology, and local-to-cloud mapping explicit once local components provide credible evidence. The approved diagram will then expand this written design so the prose and visual architecture cannot drift.

## Sections to complete after Visio

1. Design goals, constraints, and assumptions.
2. System context and user request flow.
3. Components and responsibility boundaries.
4. Agent orchestration, state, retry, and recovery model.
5. Data ownership, events, caching, retrieval, and provenance.
6. API, event, and tool contracts beyond the health endpoint.
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
