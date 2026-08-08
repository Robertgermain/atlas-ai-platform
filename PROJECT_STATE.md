# Atlas AI Platform — Project State

- Last updated: 2026-08-08
- Phase: Local implementation foundation
- Milestone: Continuous integration (Milestone 2)
- Implementation status: Python 3.12 / FastAPI foundation verified; CI not yet added

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with an initial local foundation section.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files.
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with a FastAPI app exposing `GET /health`.
- Pytest, Ruff, and mypy configuration; health contract test.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- Continuous integration, PostgreSQL, agents, brokers, containers, Kubernetes, Terraform, or AWS resources.
- Validated quality, latency, reliability, or cost benchmarks.

## Decisions

- Keep documentation intentionally small: Research, PRD, Technical Design, Testing Strategy, Local Build Plan, AGENTS, and Project State.
- Build locally through small, tested vertical slices before producing the comprehensive AWS design.
- Update the technical design incrementally as local architectural decisions are validated.
- Create the Visio system and AWS deployment diagrams once working local components provide credible design evidence.
- Add code folders and files incrementally, with an explainable purpose for each.
- Use the technology portfolio through justified capabilities and experiments, not decorative dependencies.
- Track the complete roadmap in `docs/LOCAL_BUILD_PLAN.md`; keep this file limited to current truth and the immediate handoff.
- Runtime dependencies stay in `[project].dependencies`; development tools stay in `[dependency-groups].dev`.
- `requires-python = ">=3.12,<3.13"`; mypy checks both `src` and `tests`.

## Verification (Milestone 1)

- `uv run python --version` → Python 3.12.13
- `uv sync --frozen` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (3 source files)
- `uv run pytest` → 1 passed (`GET /health` → `200`, `{"status": "ok"}`)

## Next steps

1. Commit the Milestone 1 foundation after review.
2. Implement Milestone 2: minimal GitHub Actions workflow for frozen sync, Ruff, mypy, and Pytest.
3. Do not begin database or AI work until Milestone 2 is complete and reviewed.

## Active blockers

None. Stop for review before beginning Milestone 2.
