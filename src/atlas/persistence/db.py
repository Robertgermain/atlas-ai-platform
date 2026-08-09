"""SQLAlchemy engine and session helpers with lazy connection creation."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from atlas.config import get_settings


@lru_cache(maxsize=1)
def get_engine(database_url: str | None = None) -> Engine:
    """Create (or reuse) a SQLAlchemy engine. Does not connect until first use."""
    url = database_url if database_url is not None else get_settings().database_url
    return create_engine(url, pool_pre_ping=True)


def get_session_factory(engine: Engine | None = None) -> sessionmaker[Session]:
    """Return a session factory bound to the given or default engine."""
    return sessionmaker(
        bind=engine or get_engine(),
        autoflush=False,
        autocommit=False,
        expire_on_commit=False,
    )


@contextmanager
def session_scope(
    session_factory: sessionmaker[Session] | None = None,
) -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""
    factory = session_factory or get_session_factory()
    session = factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def reset_engine_cache() -> None:
    """Clear the cached default engine (for tests)."""
    get_engine.cache_clear()
