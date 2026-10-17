"""Tests for sources/intelligence.py — downstream ACLED-failure honesty (issue #3)."""

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
async def fetcher(cache: Cache) -> AsyncIterator[Fetcher]:
    breaker = CircuitBreaker()
    fetcher = Fetcher(cache=cache, breaker=breaker, default_timeout=5.0)
    try:
        yield fetcher
    finally:
        await fetcher.close()


@pytest.mark.asyncio
async def test_fetch_risk_scores_distinguishes_fetch_failure_from_unconfigured(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #3 (downstream consumer audit): fetch_risk_scores previously
    reported "ACLED credentials not configured" for ANY acled_query()
    failure, even when credentials were fine and only the live fetch
    failed (rate limit, 500, malformed payload) — misdiagnosing a
    transient outage as a config problem an operator can't fix."""
    from world_intel_mcp.sources import conflict, intelligence

    async def _fake_token() -> str:
        return "fake-token"

    async def _fake_get_json(*args, **kwargs):
        return None

    monkeypatch.setattr(conflict, "_acled_get_token", _fake_token)
    monkeypatch.setattr(fetcher, "get_json", _fake_get_json)

    result = await intelligence.fetch_risk_scores(fetcher)

    assert result.get("error") is not None
    assert "not configured" not in result["error"]
    assert result.get("degraded") is True


@pytest.mark.asyncio
async def test_fetch_risk_scores_unconfigured_still_reports_unconfigured(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The original "not configured" message must still fire when
    credentials really are missing — only the misdiagnosis is fixed."""
    from world_intel_mcp.sources import conflict, intelligence

    async def _no_token() -> None:
        return None

    monkeypatch.setattr(conflict, "_acled_get_token", _no_token)
    monkeypatch.delenv("ACLED_EMAIL", raising=False)
    monkeypatch.delenv("ACLED_PASSWORD", raising=False)

    result = await intelligence.fetch_risk_scores(fetcher)

    assert result.get("error") is not None
    assert "not configured" in result["error"]
    assert result.get("degraded") is None


@pytest.mark.asyncio
async def test_fetch_hotspot_escalation_reports_unavailable_components(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue #6 at the tool level: intel_hotspot_escalation must disclose
    that news/convergence are not wired up rather than silently scoring
    every hotspot as if those signals were measured and quiet."""
    from world_intel_mcp.sources import conflict, intelligence, military

    async def _fake_token() -> str:
        return "fake-token"

    async def _fake_get_json(*args, **kwargs):
        return {"data": []}

    async def _fake_theater_posture(*args, **kwargs):
        return {"theaters": {}}

    monkeypatch.setattr(conflict, "_acled_get_token", _fake_token)
    monkeypatch.setattr(fetcher, "get_json", _fake_get_json)
    monkeypatch.setattr(military, "fetch_theater_posture", _fake_theater_posture)

    result = await intelligence.fetch_hotspot_escalation(fetcher)

    assert result["unavailable_components"] == ["news", "convergence"]
    assert result["data_gaps"] == []
    assert result["count"] > 0
    for hotspot in result["hotspots"]:
        assert hotspot["components"]["news"] is None
        assert hotspot["components"]["convergence"] is None
