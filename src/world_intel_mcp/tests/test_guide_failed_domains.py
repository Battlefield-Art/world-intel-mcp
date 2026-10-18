"""The AOI and wildfire prompts describe failed fetches the way the code
reports them.

Since 44cbfba a total earthquake or wildfire outage is an ``error`` (the
AOI brief and changes list it in ``data_gaps`` and leave it out of the
diff), and a FIRMS region that did not answer is named in
``failed_regions`` with its own error. Round 3 of the guide was written on
a base where both came back empty, so its text said they fail silently;
these tests pin the merged behavior and the corrected text.

Gaps / not covered: a real USGS or FIRMS outage (every request here is a
mocked 503); an AOI whose wildfire scope partly fails is checked through
``fetch_wildfires`` only, not through a full ``fetch_aoi_changes`` run.
"""

import asyncio

import httpx

from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import seismology, wildfire
from world_intel_mcp.tests.test_guide import _text


def _render(name: str) -> str:
    return " ".join(_text(name).split())


async def test_failed_earthquake_and_wildfire_fetches_are_errors(monkeypatch, tmp_path):
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(503)))
    cache = Cache(db_path=tmp_path / "failed-domains.db")
    down = Fetcher(cache, CircuitBreaker(), max_retries=0, client=client)
    monkeypatch.setenv("NASA_FIRMS_API_KEY", "test-only")

    async def _no_wait(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _no_wait)
    try:
        quakes = await seismology.fetch_earthquakes(down)
        fires = await wildfire.fetch_wildfires(down)
    finally:
        await client.aclose()
        cache.close()
    assert quakes["earthquakes"] == [] and quakes["degraded"] is True
    assert "USGS earthquake fetch failed" in quakes["error"]
    assert "failed for every requested region" in fires["error"]
    assert fires["failed_regions"] == sorted(fires["fires_by_region"])
    assert all("error" in r for r in fires["fires_by_region"].values())


def test_aoi_prompts_say_a_failed_domain_is_a_data_gap():
    for name in ("aoi-watch-setup", "aoi-review"):
        text = _render(name)
        assert "is listed in data_gaps and left out of the" in text
        assert "listed as partial coverage" in text
        assert "failing_sources in intel://guide/source-health" in text
        assert "empty from a failed fetch" not in text


def test_wildfire_prompt_names_failed_regions():
    text = _render("wildfire-situation")
    assert "named in failed_regions" in text
    assert "every region failing is an error" in text
    assert "can still come back as empty regions" not in text
