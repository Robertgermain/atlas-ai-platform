# Atlas AI Platform

Atlas is an in-progress production-oriented multi-agent deep-research platform and a hands-on AI/backend/cloud engineering portfolio project.

The repository already has a working local backend and workflow foundation: Python 3.12 / FastAPI (`GET /health`, `GET /ready`, research-job APIs), PostgreSQL persistence, a background worker, a LangGraph research workflow, Milestone 8 model-provider adapters (default `fake`, optional OpenAI/Anthropic), and Milestone 9 governed research tools with an optional FastMCP stdio boundary. Work continues through small, tested vertical slices so every folder and file has a clear purpose.

## Read in this order

1. `PROJECT_STATE.md` — what is true now and what happens next
2. `docs/RESEARCH.md` — candidate, market, and technology context
3. `docs/LOCAL_BUILD_PLAN.md` — complete local roadmap, milestone status, rationale, and completion gates
4. `docs/PRD.md` — what Atlas must accomplish
5. `docs/TESTING.md` — how software and AI behavior will be verified
6. `docs/TECHNICAL_DESIGN.md` — architectural decisions validated during implementation, followed later by the complete local-to-AWS design
7. `AGENTS.md` — rules for AI assistants working in the repository

## Current milestone

Milestone 9 adds governed research tools (typed contracts, registry/permissions, deterministic fake search/fetch, optional Tavily search via streaming `httpx`), a two-table tool invocation ledger, research-node budgets, `[untrusted_source]` labeling on findings, and a real FastMCP stdio server (`python -m atlas.mcp`). Live arbitrary-URL fetch remains unavailable (fail-closed); HTML extraction and request-scoped SSRF-safe live fetching are deferred. LangGraph uses `WorkflowRuntimeContext` and does not route through MCP. Milestone 8 is Complete through Pull Request #11. Local automated gates for Milestone 9 have passed. Do not mark Milestone 9 Complete until pull-request CI and resulting `main` CI pass. The comprehensive Visio and AWS design remains deferred.

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
# Default ATLAS_MODEL_PROVIDER=fake and ATLAS_TOOL_PROVIDER=fake (no live network).
# For real models: set ATLAS_MODEL_PROVIDER=openai|anthropic and the matching API key.
# For live search: set ATLAS_TOOL_PROVIDER=tavily and ATLAS_TAVILY_API_KEY.
# Live fetch is unavailable in Milestone 9 (ATLAS_TOOL_FETCH_ENABLED=true fails closed).
uv run python -m atlas.worker

# Optional: FastMCP stdio server for the same governed tools
# uv run python -m atlas.mcp
```
