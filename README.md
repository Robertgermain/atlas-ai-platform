# Atlas AI Platform

Atlas is an in-progress production-oriented multi-agent deep-research platform and a hands-on AI/backend/cloud engineering portfolio project.

The repository already has a working local backend and workflow foundation: Python 3.12 / FastAPI (`GET /health`, `GET /ready`, research-job APIs), PostgreSQL persistence, a background worker, a LangGraph research workflow, and Milestone 8 model-provider adapters (default `fake`, optional OpenAI/Anthropic). Work continues through small, tested vertical slices so every folder and file has a clear purpose.

## Read in this order

1. `PROJECT_STATE.md` — what is true now and what happens next
2. `docs/RESEARCH.md` — candidate, market, and technology context
3. `docs/LOCAL_BUILD_PLAN.md` — complete local roadmap, milestone status, rationale, and completion gates
4. `docs/PRD.md` — what Atlas must accomplish
5. `docs/TESTING.md` — how software and AI behavior will be verified
6. `docs/TECHNICAL_DESIGN.md` — architectural decisions validated during implementation, followed later by the complete local-to-AWS design
7. `AGENTS.md` — rules for AI assistants working in the repository

## Current milestone

Milestone 8 adds real model-provider adapters (OpenAI and Anthropic) behind LangChain `BaseChatModel`, with a durable invocation ledger and `fake` as the default provider. Plan and draft are model-backed; validate, research, and complete remain deterministic. Local automated gates and opt-in live OpenAI/Anthropic verification have passed. Do not mark Milestone 8 Complete until pull-request CI and resulting `main` CI pass. The comprehensive Visio and AWS design remains deferred.

### Run locally

Development-only Compose credentials are `atlas` / `atlas` on host port `5433` (see `.env.example` and `docker-compose.yml`). Do not use these values outside local development.

```bash
# 1. Install locked dependencies
uv sync --frozen

# 2. Start PostgreSQL (creates databases atlas and atlas_test)
docker compose up -d

# 3. Apply migrations to the local application database
export ATLAS_DATABASE_URL=postgresql+psycopg://atlas:atlas@127.0.0.1:5433/atlas
uv run alembic upgrade head

# 4. Start the API (terminal 1)
uv run uvicorn atlas.main:app --reload

# 5. Start the worker (terminal 2)
# Default ATLAS_MODEL_PROVIDER=fake (no live LLM calls).
# For real providers: set ATLAS_MODEL_PROVIDER=openai|anthropic and the matching API key.
uv run python -m atlas.worker
```
