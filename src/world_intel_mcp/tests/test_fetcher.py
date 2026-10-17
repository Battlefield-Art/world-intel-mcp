"""Tests for Fetcher — stale-cache fallback honesty (issue #4).

fetcher.py's module docstring promises that a stale-cache fallback is
marked with ``_stale=True`` so dashboards/callers never mistake old data
for fresh data. Prior to the fix, ``_stale_fallback`` returned the cached
value completely unmodified — the marker was documented but never set.
"""

from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher


@pytest.fixture
def cache(tmp_path: Path) -> Iterator[Cache]:
    cache = Cache(db_path=tmp_path / "test_cache.db")
    try:
        yield cache
    finally:
        cache.close()


@pytest.fixture
def breaker() -> CircuitBreaker:
    return CircuitBreaker()


@pytest.fixture
async def fetcher(cache: Cache, breaker: CircuitBreaker) -> AsyncIterator[Fetcher]:
    fetcher = Fetcher(cache=cache, breaker=breaker, default_timeout=5.0)
    try:
        yield fetcher
    finally:
        await fetcher.close()


def test_stale_fallback_marks_dict_responses(fetcher: Fetcher) -> None:
    key = "test:stale:dict"
    fetcher.cache.set(key, {"value": 42}, ttl_seconds=-10)  # already expired

    result = fetcher._stale_fallback(key, "test-source")

    assert result is not None
    assert result["value"] == 42
    assert result["_stale"] is True
    assert isinstance(result["_stale_age_seconds"], (int, float))
    assert result["_stale_age_seconds"] >= 0


def test_stale_fallback_counts_non_dict_responses(
    fetcher: Fetcher, breaker: CircuitBreaker
) -> None:
    """Non-dict payloads (raw text/XML, lists) can't carry an inline
    marker, so the failure must surface via the breaker's stale-serve
    counter instead of vanishing silently."""
    key = "test:stale:list"
    fetcher.cache.set(key, [1, 2, 3], ttl_seconds=-10)

    result = fetcher._stale_fallback(key, "test-source")

    assert result == [1, 2, 3]
    assert breaker.status()["test-source"]["total_stale_serves"] == 1


def test_stale_fallback_returns_none_when_nothing_cached(fetcher: Fetcher) -> None:
    assert fetcher._stale_fallback("test:missing", "test-source") is None
