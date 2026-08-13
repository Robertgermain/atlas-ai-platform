"""Add durable W3C trace-context propagation columns (Slice 15A3).

Revision ID: 20260812_0014
Revises: 20260809_0013
Create Date: 2026-08-12 16:00:00.000000

Three nullable, persistence-only columns support OpenTelemetry trace
continuity across process/transaction boundaries. None of the three is
exposed through any public API or domain model -- they are read/written
only by ``atlas.persistence.repositories.research_job`` and
``atlas.persistence.repositories.outbox``.

``research_jobs.traceparent`` -- the W3C ``traceparent`` (version ``00``
only) active when the API created this row. Written once, at insert, and
never overwritten afterward (retry/review-continuation reuse the same
column value; a crash/lease reclaim never rewrites it either).

``research_jobs.initial_traceparent_consumed_at`` -- set atomically, in the
same transaction as the *first* successful claim of this row, exactly once,
the first time a claim both (a) has a non-null stored ``traceparent`` and
(b) finds this column still ``NULL``. This is what makes "may this claim
use the stored ``traceparent`` as a live parent" a durable fact rather than
a heuristic recomputed from ``continuation_mode``/``active_workflow_
execution_id`` (which a crash between claiming and creating the first
workflow execution would otherwise make ambiguous -- see
``atlas.persistence.repositories.research_job.claim_next`` for exactly
where this is read and set). Once non-null, it never reverts to ``NULL``
and never changes again; every later claim of the same row -- including an
immediate crash/lease reclaim before any workflow execution exists --
therefore always sees it already consumed and starts a new root trace
instead (with a Span Link to the original context, applied at the
application layer, not the database).

``outbox_events.traceparent`` -- the W3C ``traceparent`` active at the exact
transactional-outbox insert (``SqlAlchemyOutboxRepository.enqueue``). The
relay reads it once per row, starts an ``outbox.publish`` child span, and
injects *that* span's own resulting ``traceparent`` into the Kafka record
headers -- this column is a lineage source, never forwarded unchanged.

The CHECK constraints below enforce only the fixed *structural* shape
(exactly version ``00``, 55 characters, lowercase hex fields, non-zero
trace ID, non-zero parent span ID) as a defense-in-depth boundary. They
deliberately do not attempt to validate the trailing flags byte's
semantics or reject every malformed value a hostile input could produce --
that full strict parse (and safe-discard-if-malformed behavior) is the
application-level contract owned by
``atlas.observability.tracing.propagation``, which never trusts a stored
value's shape without re-parsing it. A row that fails this CHECK can never
be written by any Atlas code path (both write sites only ever store a
value that already passed the same application-level parser), so this is
a second, independent guarantee, not the only one.
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260812_0014"
down_revision: str | None = "20260809_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Shared structural shape for every `traceparent` column added by this
# migration: exactly the W3C version-`00` format, 55 ASCII characters,
# lowercase hex only, non-zero trace ID and parent span ID. `NULL` is
# always separately permitted (these columns are optional telemetry).
_TRACEPARENT_FORMAT_SQL = (
    "{column} ~ '^00-[0-9a-f]{{32}}-[0-9a-f]{{16}}-[0-9a-f]{{2}}$'"
)
_TRACEPARENT_NONZERO_TRACE_ID_SQL = (
    "{column} !~ '^00-0{{32}}-[0-9a-f]{{16}}-[0-9a-f]{{2}}$'"
)
_TRACEPARENT_NONZERO_SPAN_ID_SQL = (
    "{column} !~ '^00-[0-9a-f]{{32}}-0{{16}}-[0-9a-f]{{2}}$'"
)


def _traceparent_check_sql(column: str) -> str:
    return (
        f"{column} IS NULL OR ("
        f"{_TRACEPARENT_FORMAT_SQL.format(column=column)} AND "
        f"{_TRACEPARENT_NONZERO_TRACE_ID_SQL.format(column=column)} AND "
        f"{_TRACEPARENT_NONZERO_SPAN_ID_SQL.format(column=column)}"
        f")"
    )


def upgrade() -> None:
    op.add_column(
        "research_jobs",
        sa.Column("traceparent", sa.String(length=55), nullable=True),
    )
    op.add_column(
        "research_jobs",
        sa.Column(
            "initial_traceparent_consumed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.create_check_constraint(
        "ck_research_jobs_traceparent_format",
        "research_jobs",
        _traceparent_check_sql("traceparent"),
    )
    op.create_check_constraint(
        "ck_research_jobs_initial_traceparent_consumed_pair",
        "research_jobs",
        "initial_traceparent_consumed_at IS NULL OR traceparent IS NOT NULL",
    )

    op.add_column(
        "outbox_events",
        sa.Column("traceparent", sa.String(length=55), nullable=True),
    )
    op.create_check_constraint(
        "ck_outbox_events_traceparent_format",
        "outbox_events",
        _traceparent_check_sql("traceparent"),
    )


def downgrade() -> None:
    op.drop_constraint(
        "ck_outbox_events_traceparent_format", "outbox_events", type_="check"
    )
    op.drop_column("outbox_events", "traceparent")

    op.drop_constraint(
        "ck_research_jobs_initial_traceparent_consumed_pair",
        "research_jobs",
        type_="check",
    )
    op.drop_constraint(
        "ck_research_jobs_traceparent_format", "research_jobs", type_="check"
    )
    op.drop_column("research_jobs", "initial_traceparent_consumed_at")
    op.drop_column("research_jobs", "traceparent")
