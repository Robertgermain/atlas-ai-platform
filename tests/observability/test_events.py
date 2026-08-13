"""``Event`` is a closed, fixed set -- proven directly, not assumed."""

from __future__ import annotations

from enum import StrEnum

from atlas.observability.events import Event


def test_event_is_a_str_enum() -> None:
    """Every member must be usable anywhere a plain ``str`` is expected

    (e.g. directly JSON-serializable) without an explicit ``.value`` call.
    """
    assert issubclass(Event, StrEnum)
    for member in Event:
        assert isinstance(member.value, str)
        assert member.value == member


def test_no_duplicate_values() -> None:
    values = [member.value for member in Event]
    assert len(values) == len(set(values))


def test_values_are_lowercase_snake_case() -> None:
    for member in Event:
        assert member.value == member.value.lower()
        assert " " not in member.value
