"""Each retry must be spaced from the most recent attempt for its source."""

import httpx
import pytest

from world_intel_mcp import fetcher as fetcher_mod
from world_intel_mcp.fetcher import Fetcher


@pytest.mark.parametrize("method", ["get_json", "get_text", "get_xml", "get_bytes"])
@pytest.mark.parametrize("source", ["ripestat", "nominatim", "adsblol"])
async def test_retries_respect_spacing_after_another_request(
    cache, monkeypatch, method: str, source: str,
) -> None:
    now = 100.0
    attempts = []
    interleaved = False

    def respond(request):
        attempts.append(now)
        return httpx.Response(503)

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        primary = Fetcher(cache, fetcher_mod.CircuitBreaker(), client=client)
        other = Fetcher(cache, fetcher_mod.CircuitBreaker(), client=client, max_retries=0)

        async def sleep(delay):
            nonlocal now, interleaved
            now += delay
            if not interleaved:
                # Another caller reaches this source while the first backs off.
                interleaved = True
                await getattr(other, method)("https://example.test/other", source)

        monkeypatch.setattr(fetcher_mod.time, "time", lambda: now)
        monkeypatch.setattr(fetcher_mod.asyncio, "sleep", sleep)
        await getattr(primary, method)("https://example.test/primary", source)

    assert len(attempts) == 4  # three primary attempts and one other caller
    floor = fetcher_mod._SOURCE_RATE_LIMITS[source]
    assert all(
        later - earlier >= floor - 1e-9
        for earlier, later in zip(attempts, attempts[1:])
    ), attempts
