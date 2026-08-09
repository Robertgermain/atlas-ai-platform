"""Versioned API routers."""

from fastapi import APIRouter

from atlas.api.v1.evidence import router as evidence_router
from atlas.api.v1.research_jobs import router as research_jobs_router

api_v1_router = APIRouter(prefix="/v1")
api_v1_router.include_router(research_jobs_router)
api_v1_router.include_router(evidence_router)
