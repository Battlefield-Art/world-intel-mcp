"""UCDP GED source: a missing or refused token is an error, never zero
events, and every page is cached under its own key (guide review F2).

The fake API below serves a flat list of events in pages of whatever
``pagesize`` the request asks for, the way the real one does.
"""

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources.conflict import fetch_ucdp_events

_TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")


class _Upstream:
    """Records every request; answers ``status`` or a page of ``events``."""

    def __init__(self, events: list[int] | None = None, status: int = 200):
        self.events = events or []
        self.status = status
        self.requests: list[httpx.Request] = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if self.status != 200:
            return httpx.Response(self.status, json={"detail": "API token required"})
        size = int(request.url.params["pagesize"])
        page = int(request.url.params["page"])
        ids = self.events[page * size : (page + 1) * size]
        return httpx.Response(
            200,
            json={
                "TotalPages": max(1, math.ceil(len(self.events) / size)),
                "Result": [
                    {"id": i, "date_start": _TODAY, "country": "Sudan", "type_of_violence": 1, "best": 1}
                    for i in ids
                ],
            },
        )

    def pages(self, start: int = 0) -> list[tuple[str, str]]:
        return sorted(
            (r.url.params["pagesize"], r.url.params["page"]) for r in self.requests[start:]
        )


@pytest.fixture
def cache(tmp_path: Path):
    cache = Cache(db_path=tmp_path / "ucdp.db")
    yield cache
    cache.close()


async def _fetch(cache: Cache, upstream: _Upstream, **kwargs) -> dict:
    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as client:
        fetcher = Fetcher(cache, CircuitBreaker(), max_retries=0, client=client)
        return await fetch_ucdp_events(fetcher, **kwargs)


@pytest.mark.asyncio
async def test_missing_token_is_an_error_not_zero_events(cache, monkeypatch) -> None:
    monkeypatch.delenv("UCDP_ACCESS_TOKEN", raising=False)
    upstream = _Upstream(status=401)
    result = await _fetch(cache, upstream, days=30)
    assert len(upstream.requests) == 1
    assert "x-ucdp-access-token" not in upstream.requests[0].headers
    assert (result["reason"], result["degraded"]) == ("ucdp_unconfigured", True)
    assert "UCDP_ACCESS_TOKEN is not set" in result["error"]
    assert (result["events"], result["count"], result["total_fatalities_best"]) == ([], 0, 0)


@pytest.mark.asyncio
async def test_refused_token_is_an_error_and_is_never_echoed(cache, monkeypatch) -> None:
    monkeypatch.setenv("UCDP_ACCESS_TOKEN", "refused-token-for-test")
    upstream = _Upstream(status=401)
    result = await _fetch(cache, upstream, days=30)
    assert upstream.requests[0].headers["x-ucdp-access-token"] == "refused-token-for-test"
    assert (result["reason"], result["degraded"]) == ("ucdp_fetch_failed", True)
    assert result["error"].startswith("UCDP fetch failed")
    assert (result["events"], result["count"]) == ([], 0)
    assert "refused-token-for-test" not in json.dumps(result)


@pytest.mark.asyncio
async def test_every_page_is_fetched_once_and_cached_under_its_own_key(cache, monkeypatch) -> None:
    """The first page used to share a cache key with the second, so the
    second page was answered with the first page's events."""
    monkeypatch.setenv("UCDP_ACCESS_TOKEN", "t")
    upstream = _Upstream(events=[0, 1, 100, 101, 200, 201])
    first = await _fetch(cache, upstream, days=30, limit=2)
    assert sorted(e["id"] for e in first["events"]) == [0, 1, 100, 101, 200, 201]
    assert upstream.pages() == [("2", "0"), ("2", "1"), ("2", "2")]
    second = await _fetch(cache, upstream, days=30, limit=2)
    assert sorted(e["id"] for e in second["events"]) == [0, 1, 100, 101, 200, 201]
    assert len(upstream.requests) == 3


@pytest.mark.asyncio
async def test_a_different_page_size_does_not_reuse_cached_pages(cache, monkeypatch) -> None:
    """Pages of different sizes hold different events, so a page cached
    for one limit must not answer a request made with another."""
    monkeypatch.setenv("UCDP_ACCESS_TOKEN", "t")
    upstream = _Upstream(events=list(range(7)))
    first = await _fetch(cache, upstream, days=30, limit=4)
    seen = len(upstream.requests)
    second = await _fetch(cache, upstream, days=30, limit=3)
    assert sorted(e["id"] for e in first["events"]) == list(range(7))
    assert sorted(e["id"] for e in second["events"]) == list(range(7))
    assert upstream.pages(seen) == [("3", "0"), ("3", "1"), ("3", "2")]
