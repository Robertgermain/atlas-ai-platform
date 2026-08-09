"""HTTP routes for evidence documents and items."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Response, status

from atlas.api.deps import provide_evidence_ingest_service
from atlas.api.schemas.evidence import (
    CreateEvidenceDocumentRequest,
    EvidenceDocumentResponse,
    EvidenceItemResponse,
)
from atlas.api.schemas.research_jobs import ErrorResponse
from atlas.evidence.contracts import IngestDocumentRequest, MediaType
from atlas.evidence.service import EvidenceIngestService

router = APIRouter(prefix="/evidence", tags=["evidence"])


@router.post(
    "/documents",
    response_model=EvidenceDocumentResponse,
    responses={
        200: {"model": EvidenceDocumentResponse},
        201: {"model": EvidenceDocumentResponse},
        422: {"model": ErrorResponse},
        409: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def create_evidence_document(
    body: CreateEvidenceDocumentRequest,
    response: Response,
    service: Annotated[EvidenceIngestService, Depends(provide_evidence_ingest_service)],
) -> EvidenceDocumentResponse:
    """Ingest UTF-8 plain text or Markdown into durable evidence items."""
    result = service.ingest_document(
        IngestDocumentRequest(
            corpus_key=body.corpus_key,
            title=body.title,
            media_type=MediaType(body.media_type),
            text=body.text,
        )
    )
    response.status_code = (
        status.HTTP_201_CREATED if result.created else status.HTTP_200_OK
    )
    return EvidenceDocumentResponse.from_result(result)


@router.get(
    "/items/{evidence_item_id}",
    response_model=EvidenceItemResponse,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def get_evidence_item(
    evidence_item_id: str,
    service: Annotated[EvidenceIngestService, Depends(provide_evidence_ingest_service)],
) -> EvidenceItemResponse:
    """Return one evidence item with source and document provenance."""
    view = service.get_item(evidence_item_id)
    return EvidenceItemResponse.from_view(view)
