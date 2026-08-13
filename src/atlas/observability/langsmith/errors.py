"""Typed LangSmith configuration errors (Slice 15B)."""

from __future__ import annotations


class LangSmithConfigurationError(Exception):
    """Worker AI composition rejected live providers without a LangSmith key.

    The message is a fixed sanitized sentence: it never includes the
    configured URL, API key, provider credential, or any other secret.
    """
