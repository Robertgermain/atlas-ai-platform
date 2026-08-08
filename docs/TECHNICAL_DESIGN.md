# Atlas Technical Design

## Status

Not yet designed. This document will be completed after the local-to-AWS system architecture is created and reviewed in Microsoft Visio.

## Why the diagram comes first

The Visio session will make the request path, trust boundaries, state stores, asynchronous workflows, agent/tool interactions, observability flow, deployment topology, and local-to-cloud mapping explicit. The approved diagram will then anchor this written design so the prose and visual architecture cannot drift.

## Sections to complete after Visio

1. Design goals, constraints, and assumptions.
2. System context and user request flow.
3. Components and responsibility boundaries.
4. Agent orchestration, state, retry, and recovery model.
5. Data ownership, events, caching, retrieval, and provenance.
6. API, event, and tool contracts.
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

No architecture described here should be treated as accepted until those deliverables are reviewed.
