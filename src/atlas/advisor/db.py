"""PostgreSQL READ ONLY transaction scope for advisory snapshot assembly.

Covers load and assembly only. The caller must exit this scope before
invoking the analyst. Success and failure always roll back and close.
This module never commits.

Process-local Prometheus observations of the CLI are not scrapeable.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker


@contextmanager
def advisory_read_only_scope(
    session_factory: sessionmaker[Session],
) -> Iterator[Session]:
    """Begin ``SET TRANSACTION READ ONLY``, yield, then rollback and close.

    ``session_scope`` is not used: that helper commits on success.
    """
    session = session_factory()
    session.autoflush = False
    trans = session.begin()
    try:
        session.execute(text("SET TRANSACTION READ ONLY"))
        yield session
    finally:
        try:
            trans.rollback()
        except Exception:
            try:
                session.rollback()
            except Exception:
                pass
        session.close()
