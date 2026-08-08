# Atlas AI Platform

Atlas is a planned production-oriented, multi-agent deep-research platform and a hands-on AI/backend/cloud engineering portfolio project.

The repository has a verified Python 3.12 / FastAPI local foundation (`GET /health`) and continues through small, tested vertical slices so every folder and file has a clear purpose.

## Read in this order

1. `PROJECT_STATE.md` — what is true now and what happens next
2. `docs/RESEARCH.md` — candidate, market, and technology context
3. `docs/LOCAL_BUILD_PLAN.md` — complete local roadmap, milestone status, rationale, and completion gates
4. `docs/PRD.md` — what Atlas must accomplish
5. `docs/TESTING.md` — how software and AI behavior will be verified
6. `docs/TECHNICAL_DESIGN.md` — architectural decisions validated during implementation, followed later by the complete local-to-AWS design
7. `AGENTS.md` — rules for AI assistants working in the repository

## Current milestone

Define the typed `ResearchJob` domain model and tested lifecycle transitions, independent of HTTP and storage. PostgreSQL-backed APIs and AI workflow capabilities follow later. The comprehensive Visio and AWS design remains deferred until working local components can be mapped to the cloud with evidence.
