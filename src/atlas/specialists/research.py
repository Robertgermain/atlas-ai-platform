"""Research and retrieval specialist."""

from __future__ import annotations

from collections.abc import Sequence

from atlas.evidence.contracts import SourceKind
from atlas.evidence.retrieve import EvidenceRetriever
from atlas.evidence.service import EvidenceIngestService
from atlas.specialists.contracts import (
    MAX_RESEARCH_FINDINGS,
    ResearchSpecialistInput,
    ResearchSpecialistOutput,
)
from atlas.specialists.errors import (
    SpecialistConfigurationError,
    SpecialistValidationError,
)
from atlas.tools.runner import ResearchPlanExecutor, default_tool_call_context


def merge_evidence_ids_preserving_order(
    tool_evidence_ids: Sequence[str],
    retrieved_evidence_ids: Sequence[str],
) -> list[str]:
    """Merge tool then retrieval IDs with stable first-seen deduplication.

    Tool/search IDs keep their original order (first occurrence wins). Retrieved
    corpus IDs append in retrieval order when not already present. The result is
    a list, never an unordered set.
    """
    merged: list[str] = []
    seen: set[str] = set()
    for item_id in (*tool_evidence_ids, *retrieved_evidence_ids):
        if item_id in seen:
            continue
        seen.add(item_id)
        merged.append(item_id)
    return merged


class GovernedResearchRetrievalSpecialist:
    """Coordinates governed tools, retrieval, linking, and bounded projection.

    Owns the research specialist boundary: execute the plan through the
    governed tool executor, optionally retrieve operator-corpus evidence,
    link retrieved IDs to the job, merge/dedupe with search evidence, and
    return bounded findings without padding or fabricating missing items.
    """

    def __init__(
        self,
        *,
        research_executor: ResearchPlanExecutor,
        evidence_ingest: EvidenceIngestService | None = None,
        evidence_retriever: EvidenceRetriever | None = None,
    ) -> None:
        if evidence_retriever is not None and evidence_ingest is None:
            raise SpecialistConfigurationError(
                "research retriever requires evidence ingest for durable job linking"
            )
        self._research_executor = research_executor
        self._evidence_ingest = evidence_ingest
        self._evidence_retriever = evidence_retriever

    def run(self, request: ResearchSpecialistInput) -> ResearchSpecialistOutput:
        if len(request.plan) != 3 or any(not task.strip() for task in request.plan):
            raise SpecialistValidationError("research plan is invalid")
        context = default_tool_call_context(
            research_job_id=request.job_id,
            workflow_execution_id=request.workflow_execution_id,
            workflow_node_attempt=request.workflow_node_attempt,
        )
        outcome = self._research_executor.research(
            plan=list(request.plan),
            context=context,
        )
        findings = [item.strip() for item in outcome.findings if item.strip()]
        if len(findings) > MAX_RESEARCH_FINDINGS:
            raise SpecialistValidationError("research findings exceed bound")

        retrieved_ids: list[str] = []
        retriever = self._evidence_retriever
        ingest = self._evidence_ingest
        if retriever is not None:
            # Configuration already requires ingest when a retriever is present.
            assert ingest is not None
            query = " ".join([request.question, *request.plan])
            retrieved = retriever.retrieve(
                query=query,
                k=request.retrieval_k,
                source_kinds=[SourceKind.CORPUS_TEXT],
                research_job_id=None,
                include_operator_corpus=True,
            )
            retrieved_ids = [item.evidence.id for item in retrieved]
            if retrieved_ids:
                ingest.link_evidence_to_job(
                    research_job_id=request.job_id,
                    evidence_item_ids=retrieved_ids,
                    workflow_execution_id=request.workflow_execution_id,
                )

        evidence_ids = merge_evidence_ids_preserving_order(
            outcome.evidence_item_ids,
            retrieved_ids,
        )
        try:
            return ResearchSpecialistOutput(
                findings=findings,
                evidence_item_ids=evidence_ids,
            )
        except Exception as exc:
            raise SpecialistValidationError("research output is invalid") from exc
