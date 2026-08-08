# Atlas AI Platform — Project State

- Last updated: 2026-08-08
- Phase: Local implementation foundation
- Milestone: Continuous integration (Milestone 2)
- Implementation status: PR #1 merged; main push CI failed on unsupported `setup-uv` input; repair committed as `9968478` and pushed on `fix/ci-setup-uv`

## Objective

Build a production-oriented deep-research platform that provides interview-defensible experience in applied AI, backend/distributed systems, reliability, observability, delivery, and AWS infrastructure.

## Current direction

A user submits a complex research request. Atlas creates a durable job, plans bounded work, coordinates specialist agents and governed tools, gathers evidence, produces a cited report, grades the result, applies controlled recovery, and exposes progress, quality, cost, and operational diagnostics.

## What exists

- A minimal repository baseline and one flat `docs/` folder.
- `docs/LOCAL_BUILD_PLAN.md` as the ordered local roadmap and milestone checklist.
- Research, product requirements, testing strategy, and a technical-design document with validated local foundation and CI decisions.
- Root instructions for AI assistants and this current-state handoff.
- Local environment and ignore files.
- Python 3.12 project managed with `uv` (`pyproject.toml`, committed `uv.lock`, `.python-version`).
- `src/atlas` package with a FastAPI app exposing `GET /health`.
- Pytest, Ruff (format + lint), and mypy configuration; health contract test.
- Minimal GitHub Actions workflow at `.github/workflows/ci.yml` (PR and `main` push; `contents: read`), merged via Pull Request #1.

## What does not exist

- A comprehensive Visio system-design diagram or approved AWS deployment architecture.
- A green GitHub Actions push run on `main` after the PR #1 merge.
- PostgreSQL, agents, brokers, containers, Kubernetes, Terraform, or AWS resources.
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
- `astral-sh/setup-uv` must use supported inputs (`version`, `python-version`); `python-version-file` is not valid for the pinned action.

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
- After the intentional failure was reverted, the branch returned to the expected code state and CI passed.
- Pull Request #1 merged to `main`.

### Remote (`main` push after merge)

- The push workflow on `main` failed during the `setup-uv` step.
- Cause: unsupported input `python-version-file` for `astral-sh/setup-uv` (valid inputs include `version` and `python-version`).
- Repair commit `9968478` replaces that input with `version: "0.11.8"`, `python-version: "3.12"`, and keeps `enable-cache: true` while retaining existing action commit SHA pins.
- Branch `fix/ci-setup-uv` is pushed to GitHub with that repair.
- Milestone 2 remains **Current** and Milestone 3 remains **Pending** until `main` CI is green.

## Next steps

1. Open the repair pull request for `fix/ci-setup-uv` and confirm its CI is green.
2. Merge the repair PR; confirm the resulting `main` push workflow is green.
3. After `main` CI is green, mark Milestone 2 **Complete** and Milestone 3 **Current**.
4. Then begin Milestone 3: typed `ResearchJob` domain model and tested transitions (no HTTP or storage).
5. Do not begin PostgreSQL or AI workflow work until Milestone 3 is complete and reviewed.

## Active blockers

Main branch CI is red because of the unsupported `setup-uv` input used in the merged workflow. The repair is committed and pushed on `fix/ci-setup-uv`; the blocker remains until the repair PR is merged and the resulting `main` push workflow passes.
