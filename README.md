# Atlas AI Platform

Atlas is an in-progress production-oriented multi-agent deep-research platform and a hands-on AI/backend/cloud engineering portfolio project.

The repository already has a working local backend and workflow foundation: Python 3.12 / FastAPI (`GET /health`, `GET /ready`, research-job and evidence APIs), PostgreSQL/pgvector persistence, a background worker, a LangGraph research workflow, Milestone 8 model-provider adapters (default `fake`, optional OpenAI/Anthropic), Milestone 9 governed research tools with an optional FastMCP stdio boundary, and Milestone 10 evidence/provenance plus semantic retrieval (Slices 10A–10B). Work continues through small, tested vertical slices so every folder and file has a clear purpose.

## Read in this order

1. `PROJECT_STATE.md` — what is true now and what happens next
2. `docs/RESEARCH.md` — candidate, market, and technology context
3. `docs/LOCAL_BUILD_PLAN.md` — complete local roadmap, milestone status, rationale, and completion gates
4. `docs/PRD.md` — what Atlas must accomplish
5. `docs/TESTING.md` — how software and AI behavior will be verified
6. `docs/TECHNICAL_DESIGN.md` — architectural decisions validated during implementation, followed later by the complete local-to-AWS design
7. `AGENTS.md` — rules for AI assistants working in the repository

## Current milestone

Milestone 12 is **Complete** through Pull Request #17 (`e3412c3`) and calibration-closeout Pull Request #18 (`9d5abde`). Milestone 13 is **Current**: Slice 13A is **Complete** through Pull Request #19 (`dc19714`). Slice 13B (PostgreSQL transactional outbox + typed research-job domain events; global head-of-line `outbox_position` ordering) is **Complete** through Pull Request #20 (`48ce40a`). Slice 13C1 (real Kafka 4.3.1 broker, typed `confluent-kafka` producer, topic administration, and the executable `python -m atlas.outbox` relay) is **Complete** through Pull Request #21 (`cd5b25e`). Slice 13C2A (PostgreSQL-backed consumer inbox/deduplication, the research-job lifecycle projection business consumer, and the non-HTTP executable `python -m atlas.consumer`) is **Complete** through Pull Request #22 (`9f2b7af`). Retry/backoff, poison-event/DLQ handling, and replay remain Slice 13C2B. `evaluation.candidate.v1` remains provisional. The comprehensive Visio and AWS design remains deferred.

### Roadmap

The complete roadmap (`docs/LOCAL_BUILD_PLAN.md`) has 24 milestones: Milestones 1–19 build and fully validate Atlas locally (observability with mandatory LangSmith AI tracing, security/supply-chain CI, a Next.js frontend, full containerization, and local Kubernetes via `kind` before any cloud work); Milestones 20–24 then design, provision, deploy, and validate the same system on AWS (Terraform, EKS reusing the `kind`-validated Helm charts, cloud CI/CD, and final cloud validation). Every cloud capability requires a working local equivalent first wherever technically practical — see "Governing architecture rule" in `docs/LOCAL_BUILD_PLAN.md` and `AGENTS.md`.

### Run locally

Development-only Compose credentials are `atlas` / `atlas`. Postgres is published on `127.0.0.1:5433` only; Redis 8.8.1 on `127.0.0.1:6380` only; Kafka 4.3.1 (single-node KRaft, `auto.create.topics.enable=false`) on `127.0.0.1:9094` only, with a one-shot `kafka-topic-init` service creating/verifying the reserved topic `atlas.research-job-events.v1` (see `.env.example` and `docker-compose.yml`). Do not use these values outside local development.

```bash
# 1. Install locked dependencies
uv sync --frozen

# 2. Start PostgreSQL + Redis + Kafka (creates databases atlas and atlas_test,
#    and creates/verifies the reserved Kafka topic via kafka-topic-init)
docker compose up -d

# 3. Apply migrations to the local application database
export ATLAS_DATABASE_URL=postgresql+psycopg://atlas:atlas@127.0.0.1:5433/atlas
uv run alembic upgrade head

# 4. Start the API (terminal 1)
# Copy .env.example → .env for ATLAS_COORDINATION_PROVIDER=redis (Compose Redis).
uv run uvicorn atlas.main:app --reload

# 5. Start the worker (terminal 2)
# Default ATLAS_MODEL_PROVIDER=fake and ATLAS_TOOL_PROVIDER=fake (no live network).
# For real models: set ATLAS_MODEL_PROVIDER=openai|anthropic and the matching API key.
# For live search: set ATLAS_TOOL_PROVIDER=tavily and ATLAS_TAVILY_API_KEY.
# Live fetch is unavailable in Milestone 9 (ATLAS_TOOL_FETCH_ENABLED=true fails closed).
uv run python -m atlas.worker

# Optional: FastMCP stdio server for the same governed tools
# uv run python -m atlas.mcp

# Optional: Kafka outbox relay (terminal 3, Slice 13C1). Requires Compose
# Kafka to be healthy and the reserved topic already created (kafka-topic-init).
# uv run python -m atlas.outbox

# Optional: research-job lifecycle projection consumer (terminal 4, Slice 13C2A).
# Requires the same Compose Kafka/topic prerequisites as the relay above.
# uv run python -m atlas.consumer
```
