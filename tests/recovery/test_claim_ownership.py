"""Tests for claim ownership with lease validation."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from atlas.application.exceptions import ClaimOwnershipError
from atlas.application.job_processing import ContinuationMode


class TestClaimOwnershipError:
    """Verify ClaimOwnershipError contract."""

    def test_basic_message(self) -> None:
        err = ClaimOwnershipError()
        assert "Claim ownership lost" in str(err)

    def test_operation_detail(self) -> None:
        err = ClaimOwnershipError("schedule_retry")
        assert "schedule_retry" in str(err)
        assert "Claim ownership lost" in str(err)

    def test_is_application_error(self) -> None:
        from atlas.application.exceptions import ApplicationError

        err = ClaimOwnershipError()
        assert isinstance(err, ApplicationError)


class TestContinuationModeStrEnum:
    """Verify ContinuationMode is a proper StrEnum."""

    def test_is_str(self) -> None:
        assert isinstance(ContinuationMode.NONE, str)
        assert isinstance(ContinuationMode.JOB_RETRY, str)
        assert isinstance(ContinuationMode.REVIEW_COMPLETE, str)

    def test_values(self) -> None:
        assert ContinuationMode.NONE == "NONE"
        assert ContinuationMode.JOB_RETRY == "JOB_RETRY"
        assert ContinuationMode.REVIEW_COMPLETE == "REVIEW_COMPLETE"

    def test_construct_from_value(self) -> None:
        assert ContinuationMode("NONE") is ContinuationMode.NONE
        assert ContinuationMode("JOB_RETRY") is ContinuationMode.JOB_RETRY

    def test_invalid_value_raises(self) -> None:
        with pytest.raises(ValueError):
            ContinuationMode("INVALID")

    def test_membership(self) -> None:
        assert "NONE" in list(ContinuationMode)
        assert "JOB_RETRY" in list(ContinuationMode)
        assert "REVIEW_COMPLETE" in list(ContinuationMode)


class TestLeaseValidation:
    """Unit test _owns_running_claim lease checks without DB."""

    def test_expired_lease_rejects(self) -> None:
        from unittest.mock import MagicMock

        from atlas.persistence.repositories.research_job import (
            SqlAlchemyResearchJobRepository,
        )

        model = MagicMock()
        model.status = "RUNNING"
        model.claim_token = "tok123"
        model.lease_expires_at = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)

        repo = SqlAlchemyResearchJobRepository()
        at = datetime(2026, 8, 9, 12, 0, 0, tzinfo=UTC)
        assert repo._owns_running_claim(model, claim_token="tok123", at=at) is False

    def test_valid_lease_accepts(self) -> None:
        from unittest.mock import MagicMock

        from atlas.persistence.repositories.research_job import (
            SqlAlchemyResearchJobRepository,
        )

        model = MagicMock()
        model.status = "RUNNING"
        model.claim_token = "tok123"
        model.lease_expires_at = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)

        repo = SqlAlchemyResearchJobRepository()
        at = datetime(2026, 8, 9, 12, 0, 0, tzinfo=UTC)
        assert repo._owns_running_claim(model, claim_token="tok123", at=at) is True

    def test_null_lease_rejects(self) -> None:
        from unittest.mock import MagicMock

        from atlas.persistence.repositories.research_job import (
            SqlAlchemyResearchJobRepository,
        )

        model = MagicMock()
        model.status = "RUNNING"
        model.claim_token = "tok123"
        model.lease_expires_at = None

        repo = SqlAlchemyResearchJobRepository()
        at = datetime(2026, 8, 9, 12, 0, 0, tzinfo=UTC)
        assert repo._owns_running_claim(model, claim_token="tok123", at=at) is False

    def test_wrong_token_rejects(self) -> None:
        from unittest.mock import MagicMock

        from atlas.persistence.repositories.research_job import (
            SqlAlchemyResearchJobRepository,
        )

        model = MagicMock()
        model.status = "RUNNING"
        model.claim_token = "tok123"
        model.lease_expires_at = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)

        repo = SqlAlchemyResearchJobRepository()
        at = datetime(2026, 8, 9, 12, 0, 0, tzinfo=UTC)
        assert repo._owns_running_claim(model, claim_token="other", at=at) is False

    def test_not_running_rejects(self) -> None:
        from unittest.mock import MagicMock

        from atlas.persistence.repositories.research_job import (
            SqlAlchemyResearchJobRepository,
        )

        model = MagicMock()
        model.status = "COMPLETED"
        model.claim_token = "tok123"
        model.lease_expires_at = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)

        repo = SqlAlchemyResearchJobRepository()
        at = datetime(2026, 8, 9, 12, 0, 0, tzinfo=UTC)
        assert repo._owns_running_claim(model, claim_token="tok123", at=at) is False

    def test_none_model_rejects(self) -> None:
        from atlas.persistence.repositories.research_job import (
            SqlAlchemyResearchJobRepository,
        )

        repo = SqlAlchemyResearchJobRepository()
        at = datetime(2026, 8, 9, 12, 0, 0, tzinfo=UTC)
        assert repo._owns_running_claim(None, claim_token="tok123", at=at) is False
