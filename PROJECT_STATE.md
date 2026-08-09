# Atlas AI Platform — Project State

- Last updated: 2026-08-08
- Phase: Local implementation foundation
- Milestone: PostgreSQL persistence (Milestone 4)
- Implementation status: Milestone 3 complete on `main`; typed `ResearchJob` domain lifecycle verified locally and on GitHub Actions

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with validated local foundation, CI, and domain decisions.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files.
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with a FastAPI app exposing `GET /health`.
- Pytest, Ruff (format + lint), and mypy configuration; health and ResearchJob domain tests.
- Minimal GitHub Actions workflow at `.github/workflows/ci.yml` (PR and `main` push; `contents: read`), green on `main` through Pull Request #3.
- `atlas.domain` package with slotted `ResearchJob`, status enum, domain exceptions, and lifecycle transitions `PENDING → RUNNING → COMPLETED | FAILED`, merged via Pull Request #3.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- PostgreSQL persistence, Docker Compose database topology, SQLAlchemy/Alembic, settings/session ownership, research-job repositories, research-job HTTP API, agents, brokers, containers, Kubernetes, Terraform, or AWS resources.
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
- Ruff owns formatting and import sorting; black and isort are not used.
- CI installs from the committed lockfile (`uv sync --frozen`) and runs Ruff format, Ruff lint, mypy, and Pytest.
- GitHub Actions are pinned to full commit SHAs with version comments; the workflow has `contents: read` only.
- `astral-sh/setup-uv` uses `version` and `python-version` (not `python-version-file`) for the pinned action.
- Research-job identity is a caller-supplied stripped string; the domain does not generate UUIDs.
- Domain creation always starts in `PENDING`; lifecycle changes go through `start()`, `complete()`, and `fail()` only.
- Timestamps are timezone-aware, deterministic when supplied, normalized to UTC, and must not move earlier than `updated_at`.

## Verification (Milestone 1)

- `uv run python --version` → Python 3.12.13
- `uv sync --frozen` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success (3 source files)
- `uv run pytest` → 1 passed (`GET /health` → `200`, `{"status": "ok"}`)

## Verification (Milestone 2)

### Local

- Removed black and isort from runtime dependencies; regenerated `uv.lock` so Ruff alone owns format and import sorting.
- `uv sync --frozen` → success
- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success
- `uv run pytest` → 1 passed
- Workflow file created with pinned `actions/checkout@v7.0.1` and `astral-sh/setup-uv@v9.0.0` commit SHAs.

### Remote (Pull Request #1)

- Initial CI run passed (green).
- Intentional failure commit `21587a6` caused CI to fail (red).
- Revert commit `2a8190c` restored the correct test and CI passed again (green).
- Pull Request #1 merged to `main`.

### Remote (`main` push after PR #1)

- The push workflow on `main` failed during the `setup-uv` step.
- Cause: unsupported input `python-version-file` for `astral-sh/setup-uv`.

### Remote (Pull Request #2 and `main`)

- Repair commit `9968478` set `version: "0.11.8"`, `python-version: "3.12"`, and `enable-cache: true`.
- Pull Request #2 merged successfully.
- The resulting `main` push workflow passed (green).
- Milestone 2 completion gate is satisfied.

## Verification (Milestone 3)

### Local

- `uv run ruff format --check .` → success
- `uv run ruff check .` → all checks passed
- `uv run mypy src tests` → success
- `uv run pytest` → 50 passed (health + ResearchJob lifecycle)
- Domain package uses standard-library imports only; no FastAPI, database, or agent dependencies.

### Remote (Pull Request #3 and `main`)

- Pull Request #3, `feat: add ResearchJob domain lifecycle`, merged into `main`.
- Pull request CI passed (green).
- The resulting `main` push CI passed (green).
- Milestone 3 completion gate is satisfied; Milestone 3 is **Complete** and Milestone 4 is **Current**.

## Next steps

1. Agree on the Milestone 4 PostgreSQL persistence proposal, then implement it as a reviewed vertical slice.
2. Do not begin the research-job HTTP API (Milestone 5) or AI workflow work until Milestone 4 is complete and reviewed.

## Active blockers

None.
