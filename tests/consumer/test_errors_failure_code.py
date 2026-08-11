"""Network-free unit tests for the poison-event failure_code allowlist mapping."""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

from atlas.consumer.errors import (
    ALLOWED_FAILURE_CODES,
    TIER_A_ELIGIBLE_FAILURE_CODES,
    AggregateTypeHeaderMismatchError,
    DuplicateHeaderKeyError,
    EventTypeHeaderMismatchError,
    EventVersionHeaderMismatchError,
    InvalidJsonError,
    LifecycleOrderViolationError,
    MissingHeadersError,
    MissingValueError,
    NullHeaderValueError,
    PoisonEventError,
    SchemaValidationFailedError,
    UndecodableHeaderValueError,
    UndecodableValueError,
    UnexpectedHeaderKeysError,
    UnexpectedHeaderKeyTypeError,
    UnexpectedHeadersShapeError,
    UnexpectedHeaderValueTypeError,
    UnmappedPoisonEventTypeError,
    ValueNotAnObjectError,
    ValueTooLargeError,
    failure_code_for,
)

_ALL_CONCRETE_POISON_TYPES = [
    MissingHeadersError,
    UnexpectedHeadersShapeError,
    UnexpectedHeaderKeyTypeError,
    DuplicateHeaderKeyError,
    NullHeaderValueError,
    UndecodableHeaderValueError,
    UnexpectedHeaderValueTypeError,
    UnexpectedHeaderKeysError,
    EventTypeHeaderMismatchError,
    EventVersionHeaderMismatchError,
    AggregateTypeHeaderMismatchError,
    MissingValueError,
    ValueTooLargeError,
    UndecodableValueError,
    InvalidJsonError,
    ValueNotAnObjectError,
    SchemaValidationFailedError,
    LifecycleOrderViolationError,
]


@pytest.mark.parametrize("exc_type", _ALL_CONCRETE_POISON_TYPES)
def test_every_concrete_poison_type_maps_to_its_own_fixed_failure_code(
    exc_type: type[PoisonEventError],
) -> None:
    code = failure_code_for(exc_type())
    assert code == exc_type.failure_code
    assert code in ALLOWED_FAILURE_CODES


def test_failure_codes_are_all_unique_across_the_allowlist() -> None:
    codes = [exc_type.failure_code for exc_type in _ALL_CONCRETE_POISON_TYPES]
    assert len(codes) == len(set(codes))


def test_allowed_failure_codes_exactly_matches_every_concrete_type() -> None:
    expected = {exc_type.failure_code for exc_type in _ALL_CONCRETE_POISON_TYPES}
    assert ALLOWED_FAILURE_CODES == expected


def test_only_lifecycle_order_violation_is_tier_a_eligible() -> None:
    assert TIER_A_ELIGIBLE_FAILURE_CODES == frozenset(
        {LifecycleOrderViolationError.failure_code}
    )


def test_an_unmapped_poison_event_subclass_fails_closed() -> None:
    """A hypothetical future subclass not added to the mapping must fail closed."""

    class _NotYetRegisteredError(PoisonEventError):
        failure_code = "not_yet_registered"

    with pytest.raises(UnmappedPoisonEventTypeError):
        failure_code_for(_NotYetRegisteredError())


def test_mapping_is_keyed_by_exact_type_not_isinstance() -> None:
    """A grouping base class (e.g. InvalidHeaderError) must never be looked up."""

    class _SubclassOfMissingHeaders(MissingHeadersError):
        pass

    with pytest.raises(UnmappedPoisonEventTypeError):
        failure_code_for(_SubclassOfMissingHeaders())


def test_failure_code_for_never_includes_raw_exception_args() -> None:
    exc = MissingHeadersError("some-argument-that-must-never-be-persisted")
    code = failure_code_for(exc)
    assert code == "missing_headers"
    assert "some-argument-that-must-never-be-persisted" not in code


def test_unmapped_error_message_only_contains_the_type_name() -> None:
    class _Rogue(PoisonEventError):
        failure_code = "rogue"

    with pytest.raises(UnmappedPoisonEventTypeError) as excinfo:
        failure_code_for(_Rogue())
    assert str(excinfo.value) == "_Rogue"


def test_migration_check_constraint_allowlist_matches_the_python_mapping() -> None:
    """Guards against the migration's SQL CHECK constraint list drifting.

    Loads ``_ALLOWED_FAILURE_CODES_SQL`` from the migration module by file
    path (Alembic revision filenames are not ordinary importable module
    names) and compares its parsed set of SQL string literals against
    ``ALLOWED_FAILURE_CODES``.
    """
    migration_path = (
        Path(__file__).resolve().parents[2]
        / "alembic"
        / "versions"
        / "20260809_0013_consumer_dead_letters_and_replay.py"
    )
    spec = importlib.util.spec_from_file_location(
        "atlas_test_migration_0013", migration_path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    sql_codes = set(re.findall(r"'([a-z_]+)'", module._ALLOWED_FAILURE_CODES_SQL))
    assert sql_codes == ALLOWED_FAILURE_CODES
