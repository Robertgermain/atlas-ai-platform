# Atlas AI Platform — Project State

- Last updated: 2026-08-08
- Phase: Local implementation foundation
- Milestone: Establish a tested Python/FastAPI development foundation
- Implementation status: No application code or infrastructure exists

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- Research, product requirements, testing strategy, and a technical-design placeholder.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- Application code, tests, APIs, agents, databases, brokers, containers, CI/CD, Kubernetes, Terraform, or AWS resources.
- Validated quality, latency, reliability, or cost benchmarks.

## Decisions

- Keep documentation intentionally small: Research, PRD, Technical Design, Testing Strategy, AGENTS, and Project State.
- Build locally through small, tested vertical slices before producing the comprehensive AWS design.
- Update the technical design incrementally as local architectural decisions are validated.
- Create the Visio system and AWS deployment diagrams once working local components provide credible design evidence.
- Add code folders and files incrementally, with an explainable purpose for each.
- Use the technology portfolio through justified capabilities and experiments, not decorative dependencies.

## Next steps

1. Initialize Git and establish the Python 3.12 project with `uv`.
2. Add the smallest FastAPI application with a `/health` endpoint.
3. Add Pytest, Ruff, and mypy configuration and verify the first slice.
4. Update this file and commit the working foundation.
5. Next, define the initial `ResearchJob` state model and build its PostgreSQL-backed API slice.

## Active blockers

None. The first implementation change should remain deliberately small and must be reviewed before adding database or AI infrastructure.
