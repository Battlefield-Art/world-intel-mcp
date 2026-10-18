"""Tests for analysis/daily_digest.py and the intel_daily_digest tool (issue #15).

Mocks at the source-function boundary (monkeypatch), matching the pattern
used in test_intelligence.py, since daily_digest composes existing
`sources/*.py` fetch functions rather than making HTTP calls itself.
"""

import re

import pytest

from world_intel_mcp.analysis.daily_digest import fetch_daily_digest
from world_intel_mcp.sources import (
    aviation,
    conflict,
    cyber,
    health,
    military,
    news,
    seismology,
    space_weather,
    traffic,
    wildfire,
)


class _FakeVectorStore:
    def __init__(self, trend_result: dict, timeline_result: dict) -> None:
        self._trend_result = trend_result
        self._timeline_result = timeline_result

    async def trend_detection(
        self, category=None, recent_hours=6.0, baseline_hours=48.0
    ) -> dict:
        return self._trend_result

    async def timeline(self, domain=None, category=None, hours=24.0, limit=50) -> dict:
        return self._timeline_result


async def _fake_earthquakes(fetcher):
    return {
        "earthquakes": [
            {
                "id": "us1",
                "magnitude": 6.1,
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
            }
        ],
        "count": 1,
        "source": "acled",
    }


async def _fake_ucdp(fetcher, **kwargs):
    return {"events": [], "count": 0, "source": "ucdp"}


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


async def _fake_domestic_flights(fetcher):
    return {"total_aircraft": 5000, "timestamp": "2026-08-16T00:00:00Z"}


async def _fake_traffic_flow(fetcher):
    return {"global_avg_congestion": 33, "timestamp": "2026-08-16T00:00:00Z"}


def _patch_all_domains(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(seismology, "fetch_earthquakes", _fake_earthquakes)
    monkeypatch.setattr(military, "fetch_military_flights", _fake_military)
    monkeypatch.setattr(conflict, "fetch_acled_events", _fake_acled)
    monkeypatch.setattr(conflict, "fetch_ucdp_events", _fake_ucdp)
    monkeypatch.setattr(wildfire, "fetch_wildfires", _fake_wildfires)
    monkeypatch.setattr(cyber, "fetch_cyber_threats", _fake_cyber)
    monkeypatch.setattr(health, "fetch_disease_outbreaks", _fake_health)
    monkeypatch.setattr(news, "fetch_news_feed", _fake_news)
    monkeypatch.setattr(space_weather, "fetch_space_weather", _fake_space_weather)
    monkeypatch.setattr(aviation, "fetch_domestic_flights", _fake_domestic_flights)
    monkeypatch.setattr(traffic, "fetch_traffic_flow", _fake_traffic_flow)


@pytest.mark.asyncio
async def test_digest_markdown_contains_citations_with_vector_store(
    fetcher, monkeypatch
) -> None:
    _patch_all_domains(monkeypatch)
    vs = _FakeVectorStore(
        trend_result={
            "trends": [
                {
                    "category": "Conflict & Security",
                    "recent_count": 10,
                    "baseline_count": 2,
                    "recent_rate_per_hr": 1.7,
                    "baseline_rate_per_hr": 0.2,
                    "change_pct": 750.0,
                    "trend": "SURGE",
                },
                {
                    "category": "Financial Markets",
                    "recent_count": 4,
                    "baseline_count": 4,
                    "recent_rate_per_hr": 0.5,
                    "baseline_rate_per_hr": 0.5,
                    "change_pct": 0.0,
                    "trend": "NORMAL",
                },
            ],
            "categories_analyzed": 2,
            "surges": 1,
            "drops": 0,
        },
        timeline_result={
            "entries": [
                {
                    "domain": "acled",
                    "category": "Conflict & Security",
                    "text": "Battle reported near Somewhere",
                    "datetime": "2026-08-16T00:10:00Z",
                    "timestamp": 0,
                },
            ],
            "count": 1,
            "hours": 24,
        },
    )

    result = await fetch_daily_digest(fetcher, vector_store=vs)

    assert result["vector_store_available"] is True
    assert result["cited"] is True
    assert not any("vector store unavailable" in gap for gap in result["data_gaps"])

    md = result["markdown"]
    assert "## Trends" in md
    assert "SURGE" in md
    assert "NORMAL" not in md  # only notable shifts are surfaced
    assert "## Timeline" in md
    assert "Battle reported near Somewhere" in md

    # Every [n] appearing in the markdown must be a real, existing source.
    found_ns = {int(n) for n in re.findall(r"\[(\d+)\]", md)}
    real_ns = {s["n"] for s in result["sources"]}
    assert found_ns, "expected at least one citation in the digest"
    assert found_ns <= real_ns, "digest cited a number with no matching source"


@pytest.mark.asyncio
async def test_digest_vector_degraded_path_returns_data_gaps(
    fetcher, monkeypatch
) -> None:
    """Primary tested path per the 0.2.0 conventions: no qdrant/fastembed
    installed means vector_store is None. The digest must say so honestly
    rather than rendering an empty Trends/Timeline section as a quiet day."""
    _patch_all_domains(monkeypatch)

    result = await fetch_daily_digest(fetcher, vector_store=None)

    assert result["vector_store_available"] is False
    assert (
        "vector store unavailable: trends and timeline omitted" in result["data_gaps"]
    )

    md = result["markdown"]
    assert (
        md.count("Omitted: vector store unavailable.") == 2
    )  # Trends + Timeline sections

    # Domain events still came through and are still cited; degradation
    # is scoped to the vector-store sections only.
    assert result["cited"] is True
    assert not any(s["domain"] in ("trends", "timeline") for s in result["sources"])


@pytest.mark.asyncio
async def test_digest_domain_failure_reports_gap_not_fabricated_event(
    fetcher, monkeypatch
) -> None:
    _patch_all_domains(monkeypatch)

    async def _broken_cyber(fetcher):
        return {"error": "CISA KEV unreachable"}

    monkeypatch.setattr(cyber, "fetch_cyber_threats", _broken_cyber)

    result = await fetch_daily_digest(fetcher, vector_store=None)

    assert any(
        "Cyber threats: CISA KEV unreachable" in gap for gap in result["data_gaps"]
    )
    assert not any(s["domain"] == "cyber" for s in result["sources"])


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


def test_intel_daily_digest_registered_and_dispatched() -> None:
    """Structural parity check: the TOOLS/handler 1:1 invariant this
    repo maintains (see ROADMAP.md 'MCP tool parity') must hold for the
    tool. Since the Phase 26 split it lives in tools/aoi.py; importing
    the aggregated registry verifies registration for real (membership
    in ALL_TOOLS/ALL_HANDLERS proves the module is wired into _MODULES)
    and that the handler still routes to fetch_daily_digest."""
    import inspect

    tools_pkg = _tools_registry()

    assert "intel_daily_digest" in {t.name for t in tools_pkg.ALL_TOOLS}

    handler = tools_pkg.ALL_HANDLERS["intel_daily_digest"]
    assert "fetch_daily_digest" in inspect.getsource(handler)


_REAL_FETCH_SPACE_WEATHER = space_weather.fetch_space_weather


@pytest.mark.asyncio
async def test_digest_survives_a_swpc_outage_and_names_the_gap(
    cache, monkeypatch
) -> None:
    """The real space weather source answers a SWPC outage with
    current_kp None and no error key; the digest must still render, cite
    the healthy domains, and list the missing reading as a gap."""
    import httpx
    import respx

    from world_intel_mcp.circuit_breaker import CircuitBreaker
    from world_intel_mcp.fetcher import Fetcher

    _patch_all_domains(monkeypatch)
    monkeypatch.setattr(
        space_weather, "fetch_space_weather", _REAL_FETCH_SPACE_WEATHER
    )
    real_fetcher = Fetcher(cache=cache, breaker=CircuitBreaker(), max_retries=0)
    try:
        with respx.mock(assert_all_called=False) as router:
            router.get(url__startswith="https://services.swpc.noaa.gov/").mock(
                return_value=httpx.Response(503)
            )
            result = await fetch_daily_digest(real_fetcher, vector_store=None)
    finally:
        await real_fetcher.close()

    assert "Space weather: no Kp reading" in result["data_gaps"]
    assert "- Space weather: no Kp reading" in result["markdown"]
    assert not any(s["domain"] == "space_weather" for s in result["sources"])
    assert any(s["domain"] == "earthquakes" for s in result["sources"])


@pytest.mark.asyncio
async def test_digest_with_a_kp_reading_lists_no_space_weather_gap(
    fetcher, monkeypatch
) -> None:
    _patch_all_domains(monkeypatch)
    result = await fetch_daily_digest(fetcher, vector_store=None)
    assert not any(g.startswith("Space weather") for g in result["data_gaps"])
