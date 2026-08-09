"""Typed errors for specialist boundaries."""

from __future__ import annotations


class SpecialistError(Exception):
    """Base class for specialist-layer failures."""


class SpecialistValidationError(SpecialistError):
    """Raised when specialist input/output fails typed validation.

    Messages must stay sanitized: no raw provider text, evidence body,
    URLs, prompts, or exception payloads intended for persistence.
    """


class SpecialistConfigurationError(SpecialistError):
    """Raised when specialist dependencies are composed unsafely.

    Messages must stay sanitized: no provider, prompt, URL, evidence body,
    or raw exception text.
    """


class SpecialistCitationError(SpecialistError):
    """Raised when citation verification fails closed."""
