"""Tests for analysis/situation.py: cited situation briefs (issue #15) and
the intel_situation_brief MCP tool (issue #18).

Uses respx to mock the Ollama endpoint, matching the network-edge-mocking
style used elsewhere in this suite (see test_sources.py, test_intelligence.py).
The tool-composition tests mock at the source-function boundary
(monkeypatch), matching the pattern test_daily_digest.py established for
this exact class of function: it composes existing sources/*.py and
analysis/*.py fetch functions rather than making HTTP calls itself.
"""

import re

import httpx
import pytest
import respx

from world_intel_mcp.analysis import alerts, posture
from world_intel_mcp.analysis.situation import (
    _extract_metrics,
    fetch_live_situation_brief,
    fetch_situation_brief,
)
from world_intel_mcp.sources import conflict as conflict_src
from world_intel_mcp.sources import (
    cyber,
    health,
    military,
    news,
    seismology,
    space_weather,
)
from world_intel_mcp.sources import wildfire

_OLLAMA_GENERATE = "http://localhost:11434/api/generate"


def _synthetic_overview() -> dict:
    """A synthetic overview shaped like the real per-domain fetch outputs
    (see the corresponding sources/*.py functions), covering every metric
    `_extract_metrics` reports on."""
    return {
        "earthquakes": {
            "earthquakes": [
                {
                    "id": "us1",
                    "magnitude": 4.1,
                    "place": "10km SW of Testville",
                    "time": "2026-08-16T01:00:00Z",
                    "url": "https://earthquake.usgs.gov/us1",
                },
                {
                    "id": "us2",
                    "magnitude": 5.7,
                    "place": "20km N of Sampleton",
                    "time": "2026-08-16T02:00:00Z",
                    "url": "https://earthquake.usgs.gov/us2",
                },
            ],
            "count": 2,
            "source": "usgs",
        },
        "military_flights": {
            "aircraft": [],
            "count": 3,
            "source": "adsb.lol",
            "timestamp": "2026-08-16T03:00:00Z",
        },
        "acled_events": {
            "events": [
                {
                    "event_type": "Battle",
                    "location": "Testcity",
                    "country": "Testland",
                    "event_date": "2026-08-15",
                },
            ],
            "count": 1,
            "source": "acled",
        },
        "wildfires": {
            "fires_by_region": {
                "california": {
                    "count": 5,
                    "top_clusters": [{"lat": 1, "lon": 2}, {"lat": 3, "lon": 4}],
                },
            },
            "total_fires": 5,
        },
        "cyber_threats": {
            "threats": [
                {
                    "type": "c2_ip",
                    "indicator": "1.2.3.4",
                    "threat": "TrickBot",
                    "severity": "critical",
                    "first_seen": "2026-08-15",
                },
            ],
            "count": 1,
        },
        "strategic_posture": {
            "composite_score": 42,
            "risk_level": "moderate",
            "timestamp": "2026-08-16T03:00:00Z",
        },
        "alert_digest": {
            "alerts": [
                {
                    "domain": "space",
                    "priority": "high",
                    "message": "Geomagnetic storm: Kp=6",
                },
            ],
            "alert_count": 1,
            "timestamp": "2026-08-16T03:00:00Z",
        },
        "space_weather": {"current_kp": 6.0, "timestamp": "2026-08-16T03:00:00Z"},
        "disease_outbreaks": {
            "items": [
                {
                    "title": "Outbreak of X in Y",
                    "link": "https://who.int/x",
                    "published": "2026-08-15",
                    "is_high_concern": True,
                },
            ],
            "high_concern_count": 1,
        },
        "news_feed": {
            "items": [
                {
                    "title": "Headline One",
                    "link": "https://news.example/1",
                    "published": "2026-08-16T00:00:00Z",
                },
                {
                    "title": "Headline Two",
                    "link": "https://news.example/2",
                    "published": "2026-08-16T00:30:00Z",
                },
            ],
        },
        "domestic_flights": {
            "total_aircraft": 1234,
            "timestamp": "2026-08-16T03:00:00Z",
        },
        "traffic_flow": {
            "global_avg_congestion": 27,
            "timestamp": "2026-08-16T03:00:00Z",
        },
    }


# ---------------------------------------------------------------------------
# _extract_metrics: unit level, precise citation-map assertions
# ---------------------------------------------------------------------------


def test_sources_populated_from_synthetic_overview() -> None:
    metrics, sources, citations = _extract_metrics(_synthetic_overview())

    assert len(sources) == 13  # 11 single-item domains + 2 news headlines
    # Numbered sequentially with no gaps or duplicates.
    assert [s["n"] for s in sources] == list(range(1, len(sources) + 1))

    domains = {s["domain"] for s in sources}
    assert domains == {
        "earthquakes",
        "military",
        "conflict",
        "wildfires",
        "cyber",
        "posture",
        "alerts",
        "space_weather",
        "health",
        "news",
        "aviation",
        "traffic",
    }

    eq_source = next(s for s in sources if s["domain"] == "earthquakes")
    assert (
        eq_source["url"] == "https://earthquake.usgs.gov/us2"
    )  # higher-magnitude quake
    assert "5.7" in eq_source["description"]
    assert eq_source["timestamp"] == "2026-08-16T02:00:00Z"

    news_sources = [s for s in sources if s["domain"] == "news"]
    assert len(news_sources) == 2
    assert news_sources[0]["url"] == "https://news.example/1"
    assert news_sources[0]["description"] == "Headline One"

    assert metrics["earthquakes"] == 2
    assert metrics["max_magnitude"] == 5.7
    assert citations["earthquakes"] == [eq_source["n"]]
    assert citations["headlines"] == [s["n"] for s in news_sources]


def test_earthquake_count_without_events_produces_no_citation() -> None:
    """Issue #15: never fabricate a citation. A count with no underlying
    event list is not a traceable item, so it must not be cited even
    though the count itself is still reported."""
    overview = _synthetic_overview()
    overview["earthquakes"] = {"earthquakes": [], "count": 3, "source": "usgs"}

    metrics, sources, citations = _extract_metrics(overview)

    assert metrics["earthquakes"] == 3
    assert "earthquakes" not in citations
    assert not any(s["domain"] == "earthquakes" for s in sources)


def test_errored_domain_produces_no_citation() -> None:
    """A domain that failed upstream (carries an `error` key) must not be
    cited even if some default count survives in the metrics dict."""
    overview = _synthetic_overview()
    overview["cyber_threats"] = {
        "error": "CISA KEV unreachable",
        "threats": [],
        "count": 0,
    }

    metrics, sources, citations = _extract_metrics(overview)

    assert "cyber_threats" not in citations
    assert not any(s["domain"] == "cyber" for s in sources)


def test_empty_overview_produces_no_sources() -> None:
    metrics, sources, citations = _extract_metrics({})

    assert sources == []
    assert citations == {}
    assert metrics["earthquakes"] == 0
    assert metrics["risk_level"] == "unknown"


# ---------------------------------------------------------------------------
# fetch_situation_brief: end to end, network edge mocked with respx
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fallback_brief_is_mechanically_cited() -> None:
    respx.post(_OLLAMA_GENERATE).mock(
        side_effect=httpx.ConnectError("no ollama in test env")
    )

    result = await fetch_situation_brief(_synthetic_overview())

    assert result["ai_generated"] is False
    assert result["model"] == "fallback"
    assert result["cited"] is True
    assert len(result["sources"]) == 13

    real_ns = {s["n"] for s in result["sources"]}
    found_ns = {int(n) for n in re.findall(r"\[(\d+)\]", result["brief"])}
    assert found_ns, "fallback brief carried no citations at all"
    assert found_ns <= real_ns, "fallback brief cited a number with no matching source"

    # Every line of the structured fallback is cited: it is assembled
    # per-metric and every metric here has a traceable source.
    for line in result["brief"].splitlines():
        assert re.search(r"\[\d+\]", line), f"fallback line not cited: {line!r}"


@respx.mock
@pytest.mark.asyncio
async def test_cited_false_when_llm_ignores_citation_instructions() -> None:
    """The load-bearing honesty property: an LLM response that never
    references the source list must be reported as uncited, not silently
    trusted because the model *could* have cited something real."""
    respx.post(_OLLAMA_GENERATE).mock(
        return_value=httpx.Response(
            200,
            json={"response": "Everything looks calm today, nothing much to report."},
        )
    )

    result = await fetch_situation_brief(_synthetic_overview())

    assert result["ai_generated"] is True
    assert result["sources"]  # a real source list was offered to the model
    assert result["cited"] is False  # but the model never used it


@respx.mock
@pytest.mark.asyncio
async def test_cited_true_when_llm_uses_a_real_source_number() -> None:
    respx.post(_OLLAMA_GENERATE).mock(
        return_value=httpx.Response(
            200,
            json={
                "response": "Seismic activity ticked up [1] while cyber indicators [5] held steady."
            },
        )
    )

    result = await fetch_situation_brief(_synthetic_overview())

    assert result["ai_generated"] is True
    assert result["cited"] is True


@respx.mock
@pytest.mark.asyncio
async def test_out_of_range_citation_does_not_count_as_cited() -> None:
    """Citation numbers never exceed the sources list: a bracketed number
    that looks like a citation but doesn't match a real source must not
    be treated as the model honoring the citation instruction."""
    respx.post(_OLLAMA_GENERATE).mock(
        return_value=httpx.Response(
            200,
            json={"response": "Everything is quiet [99], nothing to see [42]."},
        )
    )

    result = await fetch_situation_brief(_synthetic_overview())

    assert len(result["sources"]) == 13
    assert result["cited"] is False


@respx.mock
@pytest.mark.asyncio
async def test_empty_overview_falls_back_uncited() -> None:
    respx.post(_OLLAMA_GENERATE).mock(
        side_effect=httpx.ConnectError("no ollama in test env")
    )

    result = await fetch_situation_brief({})

    assert result["sources"] == []
    assert result["cited"] is False


# ---------------------------------------------------------------------------
# intel_situation_brief tool: fetch_live_situation_brief (issue #18)
# ---------------------------------------------------------------------------


async def _fake_earthquakes(fetcher):
    return {
        "earthquakes": [
            {
                "id": "us1",
                "magnitude": 6.4,
                "place": "5km E of Nowhere",
                "time": "2026-08-16T01:00:00Z",
                "url": "https://earthquake.usgs.gov/us1",
            },
        ],
        "count": 1,
        "source": "usgs",
    }


async def _fake_military(fetcher):
    return {
        "aircraft": [],
        "count": 4,
        "source": "adsb.lol",
        "timestamp": "2026-08-16T00:00:00Z",
    }


async def _fake_acled(fetcher, **kwargs):
    return {
        "events": [
            {
                "event_type": "Battle",
                "location": "Somewhere",
                "event_date": "2026-08-15",
            },
        ],
        "count": 1,
        "source": "acled",
    }


async def _fake_wildfires(fetcher):
    return {
        "fires_by_region": {"california": {"count": 3, "top_clusters": [{}, {}, {}]}},
        "total_fires": 3,
    }


async def _fake_cyber(fetcher):
    return {
        "threats": [
            {
                "type": "c2_ip",
                "indicator": "1.2.3.4",
                "threat": "Emotet",
                "severity": "critical",
                "first_seen": "2026-08-15",
            },
        ],
        "count": 1,
    }


async def _fake_health(fetcher):
    return {
        "items": [
            {
                "title": "Outbreak of Z",
                "link": "https://who.int/z",
                "published": "2026-08-15",
                "is_high_concern": True,
            },
        ],
        "high_concern_count": 1,
    }


async def _fake_news(fetcher):
    return {
        "items": [
            {
                "title": "Big headline",
                "link": "https://news.example/a",
                "published": "2026-08-16T00:00:00Z",
            },
        ],
    }


async def _fake_space_weather(fetcher):
    return {"current_kp": 5.3, "timestamp": "2026-08-16T00:00:00Z"}


async def _fake_strategic_posture(fetcher):
    return {
        "composite_score": 55,
        "risk_level": "elevated",
        "timestamp": "2026-08-16T00:00:00Z",
    }


async def _fake_alert_digest(fetcher):
    return {
        "alerts": [
            {
                "domain": "space",
                "priority": "high",
                "message": "Geomagnetic storm: Kp=6",
            },
        ],
        "alert_count": 1,
        "timestamp": "2026-08-16T00:00:00Z",
    }


def _patch_all_compact_domains(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(seismology, "fetch_earthquakes", _fake_earthquakes)
    monkeypatch.setattr(military, "fetch_military_flights", _fake_military)
    monkeypatch.setattr(conflict_src, "fetch_acled_events", _fake_acled)
    monkeypatch.setattr(wildfire, "fetch_wildfires", _fake_wildfires)
    monkeypatch.setattr(cyber, "fetch_cyber_threats", _fake_cyber)
    monkeypatch.setattr(health, "fetch_disease_outbreaks", _fake_health)
    monkeypatch.setattr(news, "fetch_news_feed", _fake_news)
    monkeypatch.setattr(space_weather, "fetch_space_weather", _fake_space_weather)
    monkeypatch.setattr(posture, "fetch_strategic_posture", _fake_strategic_posture)
    monkeypatch.setattr(alerts, "fetch_alert_digest", _fake_alert_digest)


@respx.mock
@pytest.mark.asyncio
async def test_situation_brief_tool_returns_sources_and_cited(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The compact server-side gather feeds fetch_situation_brief()
    unchanged: a real (mocked) Ollama response that cites a real source
    number must come back ai_generated + cited, with a populated sources
    list traceable to the mocked domains."""
    _patch_all_compact_domains(monkeypatch)
    respx.post(_OLLAMA_GENERATE).mock(
        return_value=httpx.Response(
            200,
            json={"response": "Seismic activity ticked up [1] while alerts held [2]."},
        )
    )

    result = await fetch_live_situation_brief(fetcher=None)

    assert result["ai_generated"] is True
    assert result["cited"] is True
    assert result["sources"]
    domains = {s["domain"] for s in result["sources"]}
    assert domains == {
        "earthquakes",
        "military",
        "conflict",
        "wildfires",
        "cyber",
        "health",
        "news",
        "space_weather",
        "posture",
        "alerts",
    }


@respx.mock
@pytest.mark.asyncio
async def test_situation_brief_tool_falls_back_when_ollama_unreachable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #18: the fallback must hold through the tool path, not just
    the already-tested fetch_situation_brief(overview) path. Blocks the
    Ollama URL with respx (ConnectError) exactly like
    test_fallback_brief_is_mechanically_cited does above."""
    _patch_all_compact_domains(monkeypatch)
    respx.post(_OLLAMA_GENERATE).mock(
        side_effect=httpx.ConnectError("no ollama in test env")
    )

    result = await fetch_live_situation_brief(fetcher=None)

    assert result["ai_generated"] is False
    assert result["model"] == "fallback"
    assert result["cited"] is True
    assert result["sources"]
    real_ns = {s["n"] for s in result["sources"]}
    found_ns = {int(n) for n in re.findall(r"\[(\d+)\]", result["brief"])}
    assert found_ns, "fallback brief carried no citations at all"
    assert found_ns <= real_ns


def _tools_registry():
    """Import world_intel_mcp.tools under the suite's temp cache path.

    Importing the tools package imports runtime, which opens a live
    ``Cache()``/``AOIStore()``; pointing WORLD_INTEL_CACHE_DB at
    test_server_registry's temp path (shared, so the override holds no
    matter which test file triggers the first import) keeps that off the
    developer's real cache database."""
    import importlib
    import os

    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        return importlib.import_module("world_intel_mcp.tools")
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


def test_intel_situation_brief_registered_and_dispatched() -> None:
    """Structural parity check: the TOOLS/handler 1:1 invariant this
    repo maintains (see ROADMAP.md 'MCP tool parity') must hold for the
    tool. Since the Phase 26 split it lives in tools/aoi.py; importing
    the aggregated registry verifies registration for real (membership
    in ALL_TOOLS/ALL_HANDLERS proves the module is wired into _MODULES)
    and that the handler still routes to fetch_live_situation_brief."""
    import inspect

    tools_pkg = _tools_registry()

    assert "intel_situation_brief" in {t.name for t in tools_pkg.ALL_TOOLS}

    handler = tools_pkg.ALL_HANDLERS["intel_situation_brief"]
    assert "fetch_live_situation_brief" in inspect.getsource(handler)


# A SWPC outage reaches the brief as the real source's failure shape,
# {"current_kp": None, ...} with no error key, not as a fake {"error": ...}.
_REAL_FETCH_SPACE_WEATHER = space_weather.fetch_space_weather


@respx.mock
@pytest.mark.asyncio
async def test_situation_brief_survives_a_swpc_outage(
    monkeypatch: pytest.MonkeyPatch, cache
) -> None:
    from world_intel_mcp.circuit_breaker import CircuitBreaker
    from world_intel_mcp.fetcher import Fetcher

    _patch_all_compact_domains(monkeypatch)
    monkeypatch.setattr(
        space_weather, "fetch_space_weather", _REAL_FETCH_SPACE_WEATHER
    )
    respx.get(url__startswith="https://services.swpc.noaa.gov/").mock(
        return_value=httpx.Response(503)
    )
    respx.post(_OLLAMA_GENERATE).mock(
        side_effect=httpx.ConnectError("no ollama in test env")
    )
    real_fetcher = Fetcher(cache=cache, breaker=CircuitBreaker(), max_retries=0)
    try:
        result = await fetch_live_situation_brief(real_fetcher)
    finally:
        await real_fetcher.close()

    assert result["metrics_snapshot"]["kp_index"] is None
    assert not any(s["domain"] == "space_weather" for s in result["sources"])
    assert "Kp unavailable" in result["brief"]
    assert "Kp None" not in result["brief"]
    assert "Kp 0" not in result["brief"]


def test_missing_kp_is_unavailable_in_the_prompt_not_zero() -> None:
    overview = _synthetic_overview()
    overview["space_weather"] = {"current_kp": None, "timestamp": None}
    metrics, sources, _ = _extract_metrics(overview)
    from world_intel_mcp.analysis.situation import _build_prompt

    prompt = _build_prompt(metrics, sources)
    assert "SPACE WEATHER: Kp unavailable (no reading)" in prompt
    assert metrics["kp_index"] is None
    # A real reading still renders as before.
    metrics_ok, sources_ok, _ = _extract_metrics(_synthetic_overview())
    assert "SPACE WEATHER: Kp 6.0" in _build_prompt(metrics_ok, sources_ok)
