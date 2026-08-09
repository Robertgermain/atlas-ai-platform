"""Deterministic fake planner and research tool (no live models or search)."""

from __future__ import annotations

from atlas.evidence.contracts import ClaimStructured


def build_research_plan(question: str) -> list[str]:
    """Return exactly three deterministic research tasks for the question."""
    return [
        f"Clarify scope: {question}",
        f"Gather background: {question}",
        f"Identify risks and open questions: {question}",
    ]


def run_fake_research(task: str) -> str:
    """Return a deterministic fake finding for one plan task."""
    return f"Fake finding for: {task}"


def build_draft(*, question: str, plan: list[str], findings: list[str]) -> str:
    """Synthesize a deterministic draft from plan and findings."""
    plan_lines = "\n".join(
        f"{index}. {task}" for index, task in enumerate(plan, start=1)
    )
    finding_lines = "\n".join(f"- {finding}" for finding in findings)
    return (
        f"Draft synthesis for: {question}\n"
        f"Covered plan items:\n{plan_lines}\n"
        f"Evidence:\n{finding_lines}"
    )


def format_research_report(
    *,
    question: str,
    plan: list[str],
    findings: list[str],
    draft: str,
    claims: list[ClaimStructured] | None = None,
) -> str:
    """Format the stable research report, optionally with a Citations section."""
    plan_block = "\n".join(
        f"{index}. {task}" for index, task in enumerate(plan, start=1)
    )
    findings_block = "\n".join(f"- {finding}" for finding in findings)
    report = (
        f"Question:\n{question}\n\n"
        f"Plan:\n{plan_block}\n\n"
        f"Findings:\n{findings_block}\n\n"
        f"Draft:\n{draft}"
    )
    if not claims:
        return report
    citation_lines: list[str] = []
    for index, claim in enumerate(claims, start=1):
        ids = ", ".join(claim.evidence_item_ids)
        citation_lines.append(f"{index}. {claim.text} [{ids}]")
    return report + "\n\nCitations:\n" + "\n".join(citation_lines)
