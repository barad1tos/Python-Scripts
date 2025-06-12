"""Unit tests for the _filter_release_groups_by_artist helper.

These tests focus on the fallback-matching logic that allows partial and case-insensitive
artist name matching when filtering MusicBrainz release-groups.
"""

from __future__ import annotations

import logging

from typing import Any

import pytest

from services.cache_service import CacheService
from services.external_api_service import ExternalApiService
from services.pending_verification import PendingVerificationService

@pytest.fixture(name="api_service")
def _api_service() -> ExternalApiService:
    """Provide a minimally-configured *ExternalApiService* instance for testing."""
    # Minimal configuration and stub services - the tested method is synchronous
    # Create stub loggers with CRITICAL level to suppress output during tests
    stub_console_logger = logging.getLogger("stub_console")
    stub_console_logger.setLevel(logging.CRITICAL)
    stub_error_logger = logging.getLogger("stub_error")
    stub_error_logger.setLevel(logging.CRITICAL)

    config: dict[str, Any] = {}
    cache_stub = CacheService(
        config={},
        console_logger=stub_console_logger,
        error_logger=stub_error_logger,
    )
    pending_stub = PendingVerificationService(
        config={},
        console_logger=stub_console_logger,
        error_logger=stub_error_logger,
    )

    return ExternalApiService(
        config=config,
        console_logger=logging.getLogger("console_test"),
        error_logger=logging.getLogger("error_test"),
        cache_service=cache_stub,
        pending_verification_service=pending_stub,
    )


# ---------------------------------------------------------------------------
# Test data helpers
# ---------------------------------------------------------------------------

def _make_rg(artist_name: str, title: str = "Test RG") -> dict[str, Any]:
    """Create a minimal mock MusicBrainz release-group structure."""
    return {
        "title": title,
        "artist-credit": [
            {
                "artist": {
                    "name": artist_name,
                    "aliases": [
                        {"name": "TAFKAP"}
                    ],
                }
            }
        ],
    }


author_test_cases = [
    ("The Beatles", "beatles", True),  # simple substring
    ("Beatles", "the beatles", True),  # reverse containment
    ("Prince", "TAFKAP", True),  # alias match should pass after fix
    ("Unknown Artist", "beatles", False),  # negative case
]


@pytest.mark.parametrize("rg_artist, search_term, expected", author_test_cases)
def test_filter_release_groups_by_artist(
    api_service: ExternalApiService,
    rg_artist: str,
    search_term: str,
    expected: bool,
) -> None:
    """Validate that the filter returns expected results for given artist names."""
    release_groups = [_make_rg(rg_artist)]
    filtered = api_service._filter_release_groups_by_artist(release_groups, search_term)

    if expected:
        if not filtered:
            pytest.fail(f"Expected RG with artist '{rg_artist}' to match '{search_term}'")
    elif filtered:
        pytest.fail(f"Did *not* expect match for '{rg_artist}' vs '{search_term}'")
