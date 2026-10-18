"""An answer that sends no request must not wait out the source's spacing.

Per-source spacing protects the upstream; a fresh cache hit, a stale copy
served while the source's breaker is open, or an open-breaker refusal
sends nothing, so it returns at once. One real request stamps the source
just before, so any throttle on these paths would have to sleep.
"""

import httpx
import pytest

from world_intel_mcp import fetcher as fetcher_mod
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher

_SOURCE = "nominatim"  # 1.1 s spacing in _SOURCE_RATE_LIMITS


@pytest.fixture
def recorded_sleeps(monkeypatch) -> list[float]:
    sleeps: list[float] = []

    async def sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(fetcher_mod.asyncio, "sleep", sleep)
    return sleeps


def _upstream(requests: list[str]) -> httpx.MockTransport:
    def respond(request):
        requests.append(request.url.path)
        if request.url.path == "/seed":
            return httpx.Response(200, json={"v": 1})
        return httpx.Response(503)

    return httpx.MockTransport(respond)


@pytest.mark.parametrize("method", ["get_json", "get_text", "get_xml"])
@pytest.mark.parametrize("path", ["fresh cache hit", "stale copy, breaker open"])
async def test_cache_served_answer_skips_the_spacing(
    cache, recorded_sleeps, method: str, path: str,
) -> None:
    requests: list[str] = []
    breaker = CircuitBreaker(failure_threshold=1)
    async with httpx.AsyncClient(transport=_upstream(requests)) as client:
        fetch = getattr(Fetcher(cache, breaker, client=client, max_retries=0), method)
        ttl = 300 if path == "fresh cache hit" else -1
        seeded = await fetch("https://example.test/seed", _SOURCE, cache_key="k", cache_ttl=ttl)
        assert seeded is not None
        if path == "stale copy, breaker open":
            await fetch("https://example.test/down", _SOURCE, cache_key="down")
            assert not breaker.is_available(_SOURCE)
        sent, slept = len(requests), len(recorded_sleeps)

        result = await fetch("https://example.test/seed", _SOURCE, cache_key="k")

    assert len(requests) == sent, "the cache-served answer sent a request"
    assert recorded_sleeps[slept:] == [], "the cache-served answer waited for the source"
    if method == "get_json":
        assert result["v"] == 1
        assert result.get("_stale", False) is (path == "stale copy, breaker open")
    else:
        assert result == seeded


async def test_open_breaker_binary_refusal_skips_the_spacing(
    cache, recorded_sleeps,
) -> None:
    requests: list[str] = []
    breaker = CircuitBreaker(failure_threshold=1)
    async with httpx.AsyncClient(transport=_upstream(requests)) as client:
        fetcher = Fetcher(cache, breaker, client=client, max_retries=0)
        await fetcher.get_bytes("https://example.test/down", _SOURCE)
        assert not breaker.is_available(_SOURCE)
        sent, slept = len(requests), len(recorded_sleeps)

        result = await fetcher.get_bytes("https://example.test/down", _SOURCE)

    assert result.error == f"circuit open for {_SOURCE}"
    assert len(requests) == sent
    assert recorded_sleeps[slept:] == []
