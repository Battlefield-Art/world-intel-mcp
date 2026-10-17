"""Tests for sources/intelligence.py success paths and degradation shapes.

Complements tests/test_intelligence.py (which covers the issue-#3 ACLED
failure-honesty paths for fetch_risk_scores and the hotspot-escalation
unavailable-components disclosure). This file covers the remaining ten
public functions.

Patching strategy: intelligence.py binds ``acled_query``/
``acled_failure_reason`` into its own namespace at import, so those are
patched on the intelligence module; sibling sources (news, military,
infrastructure, seismology, displacement, maritime, conflict) are imported
inside each function body and looked up as module attributes at call time,
so those are patched on their own modules.

Gaps / not covered: the real ACLED OAuth flow (covered by
test_intelligence.py via _acled_get_token); Ollama streaming; the
TemporalBaseline anomaly-firing path (needs 10+ historical observations —
only the recording path is exercised here); exact CII arithmetic
(instability tests assert raw_data pass-through and the UCDP floor, not
the weighted-sum internals, which belong to analysis/instability.py).
Vessel snapshot: the 300 km radius is a chosen value, tested at its
consequences (which waterways a warning reaches), not validated against
real chokepoint geometry; positions past the 500-character text cut are
not read (none in the 2026-10-17 capture changed a waterway's credits).
"""

import asyncio as asyncio_mod
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import (
    conflict,
    displacement,
    infrastructure,
    intelligence,
    maritime,
    military,
    news,
    seismology,
)
from world_intel_mcp.sources.intelligence import _haversine_km, _risk_level


def _async_return(value):
    async def _fake(*args, **kwargs):
        return value

    return _fake


def _fake_reason(reason: str):
    async def _fake(data):
        return reason

    return _fake


# ---------------------------------------------------------------------------
# _risk_level / _haversine_km helpers
# ---------------------------------------------------------------------------


def test_risk_level_boundaries() -> None:
    assert _risk_level(151) == "critical"
    assert _risk_level(150) == "elevated"
    assert _risk_level(101) == "elevated"
    assert _risk_level(100) == "moderate"
    assert _risk_level(51) == "moderate"
    assert _risk_level(50) == "low"
    assert _risk_level(0) == "low"


def test_haversine_km() -> None:
    assert _haversine_km(0.0, 0.0, 0.0, 0.0) == 0.0
    # One degree of latitude is ~111 km.
    d = _haversine_km(0.0, 0.0, 1.0, 0.0)
    assert 110.0 < d < 112.5


# ---------------------------------------------------------------------------
# Function 1: fetch_country_brief
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fetch_country_brief_with_ollama(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OLLAMA_API_URL", "http://ollama.test")

    respx.get("https://api.worldbank.org/v2/country/UA/indicator/NY.GDP.MKTP.CD").mock(
        return_value=httpx.Response(
            200,
            json=[
                {"page": 1},
                [
                    {"date": "2023", "value": 25000000000000},
                    {"date": "2022", "value": None},  # dropped
                ],
            ],
        )
    )
    respx.get("https://api.worldbank.org/v2/country/UA/indicator/FP.CPI.TOTL.ZG").mock(
        return_value=httpx.Response(
            200, json=[{"page": 1}, [{"date": "2023", "value": 3.2}]]
        )
    )
    respx.post("http://ollama.test/api/generate").mock(
        return_value=httpx.Response(200, json={"response": " Analytical brief. "})
    )

    # ACLED count query succeeds with a string count (exercises coercion).
    monkeypatch.setattr(intelligence, "acled_query", _async_return({"count": "7"}))

    result = await intelligence.fetch_country_brief(fetcher, country_code="UA")

    assert result["source"] == "country-intelligence"
    assert result["country_code"] == "UA"
    assert result["llm_available"] is True
    assert result["brief"] == "Analytical brief."  # stripped
    assert result["data"]["gdp"] == [{"year": "2023", "value": 25000000000000.0}]
    assert result["data"]["inflation"] == [{"year": "2023", "value": 3.2}]
    assert result["data"]["recent_events"] == 7
    assert result["data_gaps"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_country_brief_ollama_down_and_acled_gap(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("OLLAMA_API_URL", "http://ollama.test")

    respx.get(url__regex=r".*api\.worldbank\.org.*").mock(
        return_value=httpx.Response(200, json=[{"page": 1}, []])
    )
    respx.post("http://ollama.test/api/generate").mock(
        side_effect=httpx.ConnectError("connection refused")
    )

    monkeypatch.setattr(intelligence, "acled_query", _async_return(None))
    monkeypatch.setattr(
        intelligence, "acled_failure_reason", _fake_reason("acled_unconfigured")
    )

    result = await intelligence.fetch_country_brief(fetcher, country_code="UA")

    assert result["llm_available"] is False
    assert result["brief"].startswith("LLM brief unavailable")
    assert result["data"]["gdp"] == []
    assert result["data"]["recent_events"] == 0
    # The failed ACLED count is disclosed, not silently reported as 0 events.
    assert result["data_gaps"] == ["acled_unconfigured"]


# ---------------------------------------------------------------------------
# Function 2: fetch_risk_scores (success path; failure paths live in
# test_intelligence.py)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_risk_scores_success(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = [{"country": "Syria"}] * 100 + [{"country": "Atlantis"}] * 60
    monkeypatch.setattr(intelligence, "acled_query", _async_return({"data": events}))

    result = await intelligence.fetch_risk_scores(fetcher)

    assert result["source"] == "risk-analysis"
    assert result["count"] == 2
    # Atlantis has no curated baseline (defaults to 500/yr = 41.7/month):
    # 60 events -> 144.0 -> elevated. Syria baseline 5000/yr = 416.7/month:
    # 100 events -> 24.0 -> low. Sorted by risk descending.
    atlantis, syria = result["countries"]
    assert atlantis["country"] == "Atlantis"
    assert atlantis["monthly_baseline"] == 41.7
    assert atlantis["risk_score"] == 144.0
    assert atlantis["risk_level"] == "elevated"
    assert syria["country"] == "Syria"
    assert syria["events_30d"] == 100
    assert syria["monthly_baseline"] == 416.7
    assert syria["risk_score"] == 24.0
    assert syria["risk_level"] == "low"


@pytest.mark.asyncio
async def test_fetch_risk_scores_limit(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = [{"country": "Syria"}] * 10 + [{"country": "Atlantis"}] * 60
    monkeypatch.setattr(intelligence, "acled_query", _async_return({"data": events}))

    result = await intelligence.fetch_risk_scores(fetcher, limit=1)
    assert result["count"] == 1
    assert result["countries"][0]["country"] == "Atlantis"


# ---------------------------------------------------------------------------
# Function 3: fetch_instability_index
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_instability_index_single_country(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    acled_events = (
        [{"event_type": "Protests", "fatalities": "0"}] * 2
        + [{"event_type": "Riots", "fatalities": 0}]
        + [{"event_type": "Battles", "fatalities": 10}] * 3
    )
    monkeypatch.setattr(
        intelligence, "acled_query", _async_return({"data": acled_events})
    )
    monkeypatch.setattr(
        infrastructure,
        "fetch_internet_outages",
        _async_return({"outages": [{"countries": ["SYR"]}, {"countries": ["UKR"]}]}),
    )

    seen_bbox: list = []

    async def _fake_flights(f, bbox=None):
        seen_bbox.append(bbox)
        return {"count": 10}

    monkeypatch.setattr(military, "fetch_military_flights", _fake_flights)
    monkeypatch.setattr(news, "fetch_gdelt_search", _async_return({"count": 40}))

    result = await intelligence.fetch_instability_index(fetcher, country_code="SYR")

    assert result["source"] == "instability-index-v2"
    assert result["country_code"] == "SYR"
    assert result["country_name"] == "Syria"
    # Raw counts derived from the injected payloads.
    assert result["raw_data"] == {
        "acled_events": 6,
        "protests": 2,
        "riots": 1,
        "conflict_events": 3,
        "fatalities": 30,
        "military_aircraft": 10,
        "internet_outages": 1,  # only the SYR-matching outage
        "news_articles": 40,
    }
    # Syria bbox from the curated table is plumbed into the military query.
    assert seen_bbox == ["32,35,37,42"]
    # Syria (baseline_risk 80) gets the UCDP active-war floor and the
    # displacement boost; the index can never sit below the floor.
    assert result["ucdp_floor"] == 70.0
    assert result["displacement_boost"] == 3.0
    assert result["event_multiplier"] == 1.2
    assert result["instability_index"] >= 70.0
    assert result["risk_level"] in ("high", "critical")
    assert result["data_gaps"] == []


@pytest.mark.asyncio
async def test_fetch_instability_index_multi_country(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    events = [{"country": "Ukraine", "event_type": "Battles", "fatalities": 3}] * 5
    monkeypatch.setattr(intelligence, "acled_query", _async_return({"data": events}))

    result = await intelligence.fetch_instability_index(fetcher)

    assert result["source"] == "instability-index-v2"
    assert result["count"] == 10
    codes = {c["country_code"] for c in result["countries"]}
    assert codes == set(intelligence._FOCUS_COUNTRIES)

    ukraine = next(c for c in result["countries"] if c["country_code"] == "UKR")
    assert ukraine["events_30d"] == 5
    # UKR baseline_risk 85 -> 70.0 floor.
    assert ukraine["instability_index"] >= 70.0

    # Sorted by instability_index descending.
    indices = [c["instability_index"] for c in result["countries"]]
    assert indices == sorted(indices, reverse=True)


@pytest.mark.asyncio
async def test_fetch_instability_index_multi_degraded(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(intelligence, "acled_query", _async_return(None))
    monkeypatch.setattr(
        intelligence, "acled_failure_reason", _fake_reason("acled_fetch_failed")
    )

    result = await intelligence.fetch_instability_index(fetcher)
    assert result["degraded"] is True
    assert "fetch failed" in result["error"]


@pytest.mark.asyncio
async def test_fetch_instability_index_multi_unconfigured(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(intelligence, "acled_query", _async_return(None))
    monkeypatch.setattr(
        intelligence, "acled_failure_reason", _fake_reason("acled_unconfigured")
    )

    result = await intelligence.fetch_instability_index(fetcher)
    assert "not configured" in result["error"]
    assert result.get("degraded") is None


# ---------------------------------------------------------------------------
# Function 4: fetch_signal_convergence
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_fetch_signal_convergence_custom_point(fetcher: Fetcher) -> None:
    route = respx.get("https://earthquake.usgs.gov/fdsnws/event/1/query").mock(
        return_value=httpx.Response(200, json={"features": [{} for _ in range(20)]})
    )

    result = await intelligence.fetch_signal_convergence(fetcher, lat=10.0, lon=20.0)

    assert result["source"] == "signal-convergence"
    assert len(result["hotspots"]) == 1
    hotspot = result["hotspots"][0]
    assert hotspot["name"] == "custom"
    assert hotspot["signals"]["earthquakes"] == 20
    # 20 quakes saturate the 0-5 earthquake band; a custom point gets no
    # known-hotspot bonus, so the score is exactly 5.0.
    assert hotspot["convergence_score"] == 5.0

    # Bounding box derived from lat/lon +/- radius (default 5 degrees).
    params = route.calls.last.request.url.params
    assert float(params["minlatitude"]) == 5.0
    assert float(params["maxlatitude"]) == 15.0
    assert float(params["minlongitude"]) == 15.0
    assert float(params["maxlongitude"]) == 25.0


@respx.mock
@pytest.mark.asyncio
async def test_fetch_signal_convergence_default_hotspots(fetcher: Fetcher) -> None:
    respx.get("https://earthquake.usgs.gov/fdsnws/event/1/query").mock(
        return_value=httpx.Response(200, json={"features": []})
    )

    result = await intelligence.fetch_signal_convergence(fetcher)

    assert len(result["hotspots"]) == 5
    names = {h["name"] for h in result["hotspots"]}
    assert names == set(intelligence._HOTSPOTS)
    # No quakes: each known hotspot keeps only its 2.0 presence bonus.
    assert all(h["convergence_score"] == 2.0 for h in result["hotspots"])


@respx.mock
@pytest.mark.asyncio
async def test_fetch_signal_convergence_usgs_down(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    import asyncio as asyncio_mod

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)

    respx.get("https://earthquake.usgs.gov/fdsnws/event/1/query").mock(
        return_value=httpx.Response(500)
    )

    result = await intelligence.fetch_signal_convergence(fetcher, lat=10.0, lon=20.0)
    assert result["hotspots"][0]["signals"]["earthquakes"] == 0
    assert result["hotspots"][0]["convergence_score"] == 0.0


# ---------------------------------------------------------------------------
# Function 5: fetch_focal_points
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_focal_points_converging_entity(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    monkeypatch.setattr(
        news,
        "fetch_news_feed",
        _async_return(
            {"items": [{"title": "Ukraine strikes report", "published": now_iso}]}
        ),
    )
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _async_return(
            {"theaters": {"european": {"count": 12, "countries": ["Ukraine"]}}}
        ),
    )
    monkeypatch.setattr(
        infrastructure,
        "fetch_internet_outages",
        _async_return(
            {
                "outages": [
                    {"countries": ["Ukraine"], "start": now_iso, "is_ongoing": True}
                ]
            }
        ),
    )
    monkeypatch.setattr(
        intelligence,
        "acled_query",
        _async_return({"data": [{"country": "Ukraine", "event_date": today}]}),
    )

    result = await intelligence.fetch_focal_points(fetcher)

    assert result["source"] == "focal-point-analysis"
    assert result["total_events_analyzed"] == 4
    assert result["count"] == 1
    assert result["data_gaps"] == []

    fp = result["focal_points"][0]
    assert fp["entity"] == "Ukraine"
    assert fp["signal_count"] == 4
    assert fp["signal_types"] == ["infrastructure", "military", "news", "protest"]
    assert fp["urgency"] == "watch"  # < 5 signals
    assert fp["countries"] == ["Ukraine"]


# ---------------------------------------------------------------------------
# Function 6: fetch_signal_summary
# ---------------------------------------------------------------------------


def _patch_signal_summary_sources(
    monkeypatch: pytest.MonkeyPatch,
    conflict_result: dict,
    protest_query_result: dict | None,
) -> None:
    monkeypatch.setattr(conflict, "fetch_acled_events", _async_return(conflict_result))
    monkeypatch.setattr(
        seismology, "fetch_earthquakes", _async_return({"earthquakes": []})
    )
    monkeypatch.setattr(
        infrastructure, "fetch_internet_outages", _async_return({"outages": []})
    )
    monkeypatch.setattr(
        military, "fetch_theater_posture", _async_return({"theaters": {}})
    )
    monkeypatch.setattr(
        intelligence, "acled_query", _async_return(protest_query_result)
    )
    monkeypatch.setattr(
        displacement,
        "fetch_displacement_summary",
        _async_return(
            {"by_origin": [{"country": "Ukraine", "total_displaced": 250000}]}
        ),
    )


@pytest.mark.asyncio
async def test_fetch_signal_summary_aggregates_domains(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_signal_summary_sources(
        monkeypatch,
        conflict_result={
            "events": [{"country": "Ukraine", "fatalities": 5}],
            "count": 1,
        },
        protest_query_result={
            "data": [{"country": "Ukraine", "event_type": "Protests"}]
        },
    )

    result = await intelligence.fetch_signal_summary(fetcher)

    assert result["source"] == "signal-aggregation-v2"
    assert result["count"] == 1
    assert result["data_gaps"] == []

    ukraine = result["countries"][0]
    assert ukraine["country"] == "Ukraine"
    assert ukraine["conflict_events"] == 1
    assert ukraine["fatalities"] == 5
    assert ukraine["displaced_persons"] == 250000
    assert ukraine["protests"] == 1
    assert ukraine["active_domains"] == ["conflict", "displacement", "unrest"]
    # 3 domains x 20 + min(30, 5 x 2 signals) + 10 x 1 high-severity
    # (250k displaced) = 80.
    assert ukraine["convergence_score"] == 80


@pytest.mark.asyncio
async def test_fetch_signal_summary_country_filter(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_signal_summary_sources(
        monkeypatch,
        conflict_result={
            "events": [
                {"country": "Ukraine", "fatalities": 5},
                {"country": "Sudan", "fatalities": 2},
            ],
            "count": 2,
        },
        protest_query_result={"data": []},
    )

    result = await intelligence.fetch_signal_summary(fetcher, country="ukr")
    assert result["count"] == 1
    assert result["countries"][0]["country"] == "Ukraine"


@pytest.mark.asyncio
async def test_fetch_signal_summary_discloses_acled_gaps(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_signal_summary_sources(
        monkeypatch,
        conflict_result={
            "events": [],
            "count": 0,
            "reason": "acled_fetch_failed",
            "degraded": True,
        },
        protest_query_result=None,
    )
    monkeypatch.setattr(
        intelligence, "acled_failure_reason", _fake_reason("acled_fetch_failed")
    )

    result = await intelligence.fetch_signal_summary(fetcher)
    # Both gaps collapse into one deduplicated, sorted entry.
    assert result["data_gaps"] == ["acled_fetch_failed"]


# ---------------------------------------------------------------------------
# Function 7: fetch_temporal_anomalies
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_temporal_anomalies_records_observations(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path, temporal_baseline
) -> None:
    monkeypatch.setattr(
        intelligence, "_temporal", temporal_baseline
    )
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _async_return(
            {"theaters": {"european": {"count": 5}, "middle_east": {"count": 3}}}
        ),
    )
    monkeypatch.setattr(
        intelligence,
        "acled_query",
        _async_return(
            {
                "data": [
                    {"country": "Ukraine"},
                    {"country": "Ukraine"},
                    {"country": "Sudan"},
                ]
            }
        ),
    )

    result = await intelligence.fetch_temporal_anomalies(fetcher)

    assert result["source"] == "temporal-anomaly-detection"
    # 2 theaters + 2 distinct ACLED countries.
    assert result["observations_recorded"] == 4
    # A fresh baseline (n=1 < 10) can never flag anomalies.
    assert result["anomalies"] == []
    assert result["anomaly_count"] == 0
    assert result["data_gaps"] == []


@pytest.mark.asyncio
async def test_fetch_temporal_anomalies_acled_outage_disclosed(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path, temporal_baseline
) -> None:
    monkeypatch.setattr(
        intelligence, "_temporal", temporal_baseline
    )
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _async_return({"theaters": {"european": {"count": 5}}}),
    )
    monkeypatch.setattr(intelligence, "acled_query", _async_return(None))
    monkeypatch.setattr(
        intelligence, "acled_failure_reason", _fake_reason("acled_fetch_failed")
    )

    result = await intelligence.fetch_temporal_anomalies(fetcher)
    assert result["observations_recorded"] == 1  # theaters only
    assert result["data_gaps"] == ["acled_fetch_failed"]


# ---------------------------------------------------------------------------
# Function 8: fetch_unrest_events
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_unrest_events_parses_and_dedupes(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    raw = [
        {
            "event_date": "2026-08-30",
            "event_type": "Protests",
            "sub_event_type": "Peaceful protest",
            "country": "France",
            "admin1": "Ile-de-France",
            "location": "Paris",
            "latitude": "48.85",
            "longitude": "2.35",
            "fatalities": "0",
            "actor1": "Protesters",
            "notes": "march",
        },
        # Same day, ~7 km away: deduplicated into the first, which must
        # inherit the higher fatality count.
        {
            "event_date": "2026-08-30",
            "event_type": "Riots",
            "country": "France",
            "latitude": "48.90",
            "longitude": "2.40",
            "fatalities": 2,
        },
        # Same day but Marseille (~660 km away): kept.
        {
            "event_date": "2026-08-30",
            "event_type": "Protests",
            "country": "France",
            "latitude": 43.30,
            "longitude": 5.37,
            "fatalities": 0,
        },
        # Same place, different day: kept.
        {
            "event_date": "2026-08-29",
            "event_type": "Protests",
            "country": "France",
            "latitude": 48.85,
            "longitude": 2.35,
            "fatalities": 1,
        },
        # Unparseable coordinates: kept (cannot be deduplicated).
        {
            "event_date": "2026-08-30",
            "event_type": "Protests",
            "country": "France",
            "latitude": "garbage",
            "longitude": "2.0",
            "fatalities": "n/a",
        },
    ]
    monkeypatch.setattr(intelligence, "acled_query", _async_return({"data": raw}))

    result = await intelligence.fetch_unrest_events(fetcher, country="France", days=3)

    assert result["source"] == "acled-unrest"
    assert result["count"] == 4
    assert result["deduplicated"] == 1
    assert result["query"] == {"country": "France", "days": 3}

    first = result["events"][0]
    assert first["location"] == "Paris"
    assert first["latitude"] == 48.85  # string coerced to float
    assert first["fatalities"] == 2  # merged from the deduplicated riot
    assert first["sub_event_type"] == "Peaceful protest"

    unparseable = result["events"][-1]
    assert unparseable["latitude"] is None
    assert unparseable["fatalities"] == 0  # "n/a" coerced to 0


@pytest.mark.asyncio
async def test_fetch_unrest_events_degraded_and_unconfigured(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(intelligence, "acled_query", _async_return(None))

    monkeypatch.setattr(
        intelligence, "acled_failure_reason", _fake_reason("acled_fetch_failed")
    )
    result = await intelligence.fetch_unrest_events(fetcher)
    assert result["degraded"] is True
    assert "fetch failed" in result["error"]

    monkeypatch.setattr(
        intelligence, "acled_failure_reason", _fake_reason("acled_unconfigured")
    )
    result = await intelligence.fetch_unrest_events(fetcher)
    assert "not configured" in result["error"]
    assert result.get("degraded") is None


# ---------------------------------------------------------------------------
# Function 9: fetch_hotspot_escalation (event-attribution path; the empty
# path is covered by test_intelligence.py)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_hotspot_escalation_attributes_nearby_events(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    acled_events = [
        # Within 2 degrees of the gaza hotspot (31.42, 34.35).
        {
            "latitude": "31.50",
            "longitude": "34.30",
            "event_type": "Battles",
            "fatalities": 12,
        },
        {
            "latitude": "31.40",
            "longitude": "34.40",
            "event_type": "Protests",
            "fatalities": 0,
        },
    ]
    monkeypatch.setattr(
        intelligence, "acled_query", _async_return({"data": acled_events})
    )
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _async_return(
            {
                "theaters": {
                    "middle_east": {
                        "count": 10,
                        "countries": ["United States"],
                        "bbox": "10,25,45,65",
                    }
                }
            }
        ),
    )

    result = await intelligence.fetch_hotspot_escalation(fetcher)

    assert result["source"] == "hotspot-escalation"
    assert result["count"] == len(result["hotspots"]) == 22
    assert result["unavailable_components"] == ["news", "convergence"]
    assert result["data_gaps"] == []

    gaza = next(h for h in result["hotspots"] if h["name"] == "gaza")
    comp = gaza["components"]
    assert comp["baseline"] == 20.0  # baseline_escalation 5 x 4
    # 1 conflict event x 0.5 + 12 fatalities x 0.1 = 1.7.
    assert comp["conflict"] == 1.7
    assert comp["social_unrest"] == 0.4  # 1 protest x 0.4
    assert comp["news"] is None
    assert comp["convergence"] is None
    # Renormalized over measured components: 22.1 / 72 x 100 = 30.7.
    assert gaza["score"] == 30.7
    assert gaza["level"] == "watch"


# ---------------------------------------------------------------------------
# Function 10: fetch_military_surge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_military_surge_detects_surge(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path, temporal_baseline
) -> None:
    monkeypatch.setattr(
        intelligence, "_temporal", temporal_baseline
    )
    # 40 US aircraft in the european theater map onto baltic_sea, whose US
    # baseline is 2 -> ratio 20 -> critical. black_sea has no US baseline,
    # so nothing fires there.
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _async_return(
            {"theaters": {"european": {"count": 40, "countries": ["United States"]}}}
        ),
    )

    result = await intelligence.fetch_military_surge(fetcher)

    assert result["source"] == "military-surge-detection"
    assert result["regions_checked"] == 8
    assert result["surge_count"] == 1
    surge = result["surges"][0]
    assert surge["region"] == "baltic_sea"
    assert surge["country"] == "United States"
    assert surge["current"] == 40
    assert surge["baseline"] == 2
    assert surge["surge_ratio"] == 20.0
    assert surge["severity"] == "critical"


@pytest.mark.asyncio
async def test_fetch_military_surge_quiet_posture(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path, temporal_baseline
) -> None:
    monkeypatch.setattr(
        intelligence, "_temporal", temporal_baseline
    )
    monkeypatch.setattr(
        military, "fetch_theater_posture", _async_return({"theaters": {}})
    )

    result = await intelligence.fetch_military_surge(fetcher)
    assert result["surges"] == []
    assert result["surge_count"] == 0


# ---------------------------------------------------------------------------
# Function 11: fetch_vessel_snapshot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_vessel_snapshot_naval_keywords(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Suspected bug (documented, not fixed): the loop matches naval
    keywords against ALL warnings with no proximity/navarea filter, so
    every one of the 9 waterways receives the identical naval count and
    status — 3 naval warnings anywhere on Earth mark Panama and Hormuz
    'critical' alike."""
    warnings = [
        {"id": "2026-1", "text": "NAVAL EXERCISES IN AREA", "navarea": "IV"},
        {"id": "2026-2", "text": "SUBMARINE OPERATIONS", "navarea": "XI"},
        {"id": "2026-3", "text": "LIVE FIRING IN PROGRESS", "navarea": "IX"},
        {"id": "2026-4", "text": "BUOY UNLIT", "navarea": "IV"},
    ]
    monkeypatch.setattr(
        maritime, "fetch_nav_warnings", _async_return({"warnings": warnings})
    )

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert result["source"] == "nga-msi-vessel-snapshot"
    assert result["count"] == 9
    assert result["total_nav_warnings"] == 4

    names = {w["name"] for w in result["waterways"]}
    assert "Strait of Hormuz" in names
    for ww in result["waterways"]:
        assert ww["naval_warnings"] == 3  # keyword matches; BUOY UNLIT excluded
        assert ww["status"] == "critical"  # >= 3 naval warnings
        assert len(ww["warning_details"]) == 3


@pytest.mark.asyncio
async def test_fetch_vessel_snapshot_clear(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(maritime, "fetch_nav_warnings", _async_return({"warnings": []}))

    result = await intelligence.fetch_vessel_snapshot(fetcher)
    assert all(ww["status"] == "clear" for ww in result["waterways"])
    assert all(ww["naval_warnings"] == 0 for ww in result["waterways"])


# Verbatim live records from GET
# https://msi.nga.mil/api/publications/broadcast-warn?output=json, captured
# 2026-10-17T16:43:08Z. Distances are to the STRATEGIC_WATERWAYS reference
# points, cross-checked with the spherical law of cosines.
def _nga_record(year, number, area, subregion, text, issued, authority) -> dict:
    return {
        "msgYear": year, "msgNumber": number, "navArea": area,
        "subregion": subregion, "text": text, "status": "A",
        "issueDate": issued, "authority": authority, "cancelDate": None,
        "cancelNavArea": None, "cancelMsgYear": None, "cancelMsgNumber": None,
        "number": number, "year": year, "area": area,
    }


_NGA_LIVE = [
    # 68.5 km from the Strait of Hormuz.
    _nga_record(
        2022, 3371, "P", "62",
        "PERSIAN GULF.\nU.A.E.\nDNC 10.\nODAS BUOYS, Q Y, ESTABLISHED IN:\n"
        "A. 25-59.00N 056-02.40E.\nB. 25-52.22N 055-51.20E.\n",
        "251705Z NOV 2022", "NAVAREA IX 483/22 251524Z NOV 22.",
    ),
    # 40.8 km from the Panama Canal.
    _nga_record(
        2024, 15, "4", "26",
        "WESTERN CARIBBEAN SEA.\nPANAMA.\nCHART 26060.\n"
        "BUOY NO1 09-25.09N 079-49.50W DESTROYED.\n",
        "041210Z JAN 2024", "PANAMA MARITIME AUTHORITY 0/24 040926Z JAN 24.",
    ),
    # 53.7 km from the Panama Canal.
    _nga_record(
        2022, 978, "12", "21",
        "GULF OF PANAMA.\nPANAMA.\nCHART 26060.\n"
        "DANGEROUS WRECK REPORTED IN 08-50.68N 079-15.18W.\n",
        "231758Z DEC 2022", "PANAMA MARITIME AUTHORITY 0/22 231731Z DEC 22.",
    ),
    # Nearest of its five positions: 274.7 km from the Taiwan Strait.
    _nga_record(
        2024, 1561, "P", "94",
        "PHILIPPINE SEA.\nTAIWAN.\nDNC 23.\n"
        "CABLE OPERATIONS 100900Z MAY UNTIL FURTHER NOTICE\n"
        "BY CABLESHIP PETER FABER IN AREA BOUND BY\n"
        "24-51.27N 122-05.38E, 24-50.57N 122-03.14E,\n"
        "24-48.00N 122-03.97E, 24-48.13N 122-06.33E,\n"
        "24-48.52N 122-06.30E.\nWIDE BERTH REQUESTED.\n",
        "100559Z MAY 2024", "NAVAREA XI 223/24 100544Z MAY 24.",
    ),
    # Naval keyword (ORDNANCE); 988.6 km from Gibraltar, the nearest waterway.
    _nga_record(
        2023, 1435, "A", "37",
        "BAY OF BISCAY.\nFRANCE.\nDNC 08.\nUNEXPLODED ORDNANCE IN 44-23.00N 001-46.00W.\n",
        "261350Z JUN 2023", "AVURNAV BREST 206/23 261335Z JUN 23.",
    ),
    # Naval keyword (EXERCISE); states no position, and HYDROLANT spans
    # several NAVAREAs.
    _nga_record(
        2022, 789, "A", "55",
        "BLACK SEA.\nDNC 10.\nMINES ADRIFT REPORTED IN NORTHWESTERN, WESTERN\n"
        "AND SOUTHWESTERN BLACK SEA. VESSELS IN VICINITY\n"
        "ARE ADVISED TO EXERCISE EXTREME CAUTION.\n",
        "181218Z MAR 2022", "NAVAREA III 118/22 181124Z MAR 22.",
    ),
    # States no position; NAVAREA XII contains the canal's Pacific end.
    _nga_record(
        2023, 249, "12", "83",
        "EASTERN NORTH PACIFIC.\nCANCEL NAVAREA XII 243/23 AND THIS MSG,\n"
        "SEARCH SUSPENDED.\n",
        "280353Z APR 2023", "JRCC HONOLULU 0/23 280342Z APR 23.",
    ),
    # Its only position, 154-84.0W, is malformed (84 minutes): no NAVAREA
    # credit either, since it does state a place (south of Hawaii).
    _nga_record(
        2023, 243, "12", "83",
        "EASTERN NORTH PACIFIC.\nMAN OVERBOARD IN VICINITY 10-52.0N 154-84.0W\n"
        "500 NAUTICAL MILES SOUTH OF BIG ISLAND HAWAII\n"
        "AT 260903Z APR, PERSON DRESSED IN GREEN SHORTS\n"
        "AND WHITE HOODIE. VESSELS IN VICINITY REQUESTED\n"
        "TO KEEP A SHARP LOOKOUT, ASSIST IF POSSIBLE.\n"
        "REPORTS TO JRCC HONOLULU,\nTELEX: 392401,\nPHONE: 808 535 3333,\n"
        "FAX: 808 535 3338,\nE-MAIL: JRCCHONOLULU@USCG.MIL.\n",
        "261142Z APR 2023", "JRCC HONOLULU 0/23 261128Z APR 23.",
    ),
    # Position in degrees-minutes-seconds, 1325.2 km from the Panama Canal.
    _nga_record(
        2022, 219, "12", "21",
        "EASTERN NORTH PACIFIC.\nGUATEMALA.\nBUOY 13-55-18N 090-47-53W TEMPORARILY REMOVED.\n",
        "062141Z APR 2022", "GUATEMALA 1/22 062127Z APR 22.",
    ),
]

_WATERWAY_NAMES = [
    "Strait of Hormuz", "Strait of Malacca", "Suez Canal", "Panama Canal",
    "Bab-el-Mandeb", "Taiwan Strait", "Strait of Gibraltar", "GIUK Gap",
    "Bosphorus",
]


async def _no_sleep(*args, **kwargs) -> None:
    return None


@respx.mock
@pytest.mark.asyncio
async def test_fetch_vessel_snapshot_credits_only_the_waterways_a_live_warning_concerns(
    fetcher: Fetcher,
) -> None:
    """Regression: every warning used to count at all nine waterways, and
    total_nearby was computed but never emitted. On these live records the
    old code marked every waterway 'elevated' (two naval-keyword warnings,
    one in the Bay of Biscay, one in the Black Sea)."""
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json={"broadcast-warn": _NGA_LIVE})
    )

    result = await intelligence.fetch_vessel_snapshot(fetcher)
    by_name = {ww["name"]: ww for ww in result["waterways"]}

    assert list(by_name) == _WATERWAY_NAMES
    assert result["total_nav_warnings"] == 9

    # Neither naval-keyword warning is near a waterway.
    for ww in result["waterways"]:
        assert ww["naval_warnings"] == 0
        assert ww["status"] == "clear"
        assert ww["warning_details"] == []
        assert ww["naval_by_match"] == {"proximity": 0, "navarea": 0}
    assert result["total_naval_warnings"] == 2

    assert {name: ww["total_nearby"] for name, ww in by_name.items()} == {
        "Strait of Hormuz": 1, "Strait of Malacca": 0, "Suez Canal": 0,
        "Panama Canal": 3, "Bab-el-Mandeb": 0, "Taiwan Strait": 1,
        "Strait of Gibraltar": 0, "GIUK Gap": 0, "Bosphorus": 0,
    }
    # Two by stated position, one (the positionless cancellation) by NAVAREA.
    assert by_name["Panama Canal"]["nearby_by_match"] == {"proximity": 2, "navarea": 1}
    assert by_name["Strait of Hormuz"]["nearby_by_match"] == {"proximity": 1, "navarea": 0}

    assert result["unplaced_naval_warnings"] == [
        {
            "id": "2022-789",
            "navarea": "A",
            "series": "HYDROLANT",
            "issue_date": "181218Z MAR 2022",
            "reason": "multi_navarea_series",
            "text_snippet": _NGA_LIVE[5]["text"][:200],
        }
    ]

    rule = result["matching_rule"]
    assert rule["proximity_km"] == 300.0
    assert rule["waterway_navareas"] == {"Panama Canal": ["IV", "XII"]}
    assert rule["area_codes"] == maritime.area_codes_view()
    assert "proximity_km" in rule["rule"]

    # The newest of these was issued 10 May 2024: flagged, not passed off
    # as current.
    assert result["newest_issue_date"] == "2024-05-10T05:59:00Z"
    assert result["degraded"] is True
    assert "error" not in result
    assert result["data_gaps"][0].startswith(
        "NGA feed: newest active warning was issued 2024-05-10T05:59:00Z, "
    )
    assert result["data_gaps"][1:] == [
        "1 naval warning(s) state no readable position and no single-NAVAREA "
        "area code, so no waterway is credited with them "
        "(unplaced_naval_warnings): 2022-789 (multi_navarea_series)"
    ]


@respx.mock
@pytest.mark.asyncio
async def test_fetch_vessel_snapshot_naval_status_counts_only_nearby_warnings(
    fetcher: Fetcher,
) -> None:
    """Constructed in the live record shape (texts are not real warnings,
    issue dates are two hours old): three naval warnings near Hormuz, one
    near Bab-el-Mandeb, one positionless NAVAREA IV warning, one whose only
    position is malformed."""
    issued = datetime.now(timezone.utc) - timedelta(hours=2)
    month = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()[issued.month - 1]
    stamp = f"{issued:%d%H%M}Z {month} {issued.year}"
    records = [
        # 11.4 km from Hormuz (nearer of two positions).
        _nga_record(
            2026, 901, "P", "62",
            "PERSIAN GULF.\nIRAN.\nDNC 10.\nNAVAL EXERCISES IN AREA BOUND BY\n"
            "26-30.00N 056-10.00E, 26-40.00N 056-30.00E.\n",
            stamp, "NAVAREA IX 1/26",
        ),
        # 140.6 km from Hormuz.
        _nga_record(
            2026, 902, "P", "63",
            "GULF OF OMAN.\nIRAN.\nDNC 10.\nLIVE FIRING 25-30.00N 057-00.00E.\n",
            stamp, "NAVAREA IX 2/26",
        ),
        # 98.0 km from Hormuz.
        _nga_record(
            2026, 903, "P", "62",
            "PERSIAN GULF.\nU.A.E.\nDNC 10.\nSUBMARINE OPERATIONS IN VICINITY "
            "26-00.00N 055-30.00E.\n",
            stamp, "NAVAREA IX 3/26",
        ),
        # 9.6 km from Bab-el-Mandeb.
        _nga_record(
            2026, 904, "P", "62",
            "RED SEA.\nYEMEN.\nDNC 10.\nWARSHIP OPERATIONS 12-40.00N 043-20.00E.\n",
            stamp, "NAVAREA IX 4/26",
        ),
        # No position, single-NAVAREA code 4: credited to Panama by NAVAREA.
        _nga_record(
            2026, 905, "4", "24",
            "WESTERN NORTH ATLANTIC.\nNAVAL OPERATIONS IN PROGRESS.\n",
            stamp, "NAVSAFETY 5/26",
        ),
        # Malformed position (84 minutes): credited nowhere.
        _nga_record(
            2026, 906, "12", "83",
            "EASTERN NORTH PACIFIC.\nGUNNERY FIRING EXERCISE IN 10-52.0N 154-84.0W.\n",
            stamp, "NAVSAFETY 6/26",
        ),
    ]
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json={"broadcast-warn": records})
    )

    result = await intelligence.fetch_vessel_snapshot(fetcher)
    by_name = {ww["name"]: ww for ww in result["waterways"]}

    hormuz = by_name["Strait of Hormuz"]
    assert hormuz["naval_warnings"] == 3
    assert hormuz["status"] == "critical"
    assert hormuz["naval_by_match"] == {"proximity": 3, "navarea": 0}
    assert [(d["id"], d["match"], d["distance_km"]) for d in hormuz["warning_details"]] == [
        ("2026-901", "proximity", 11.4),
        ("2026-903", "proximity", 98.0),
        ("2026-902", "proximity", 140.6),
    ]
    assert by_name["Bab-el-Mandeb"]["status"] == "advisory"
    assert by_name["Bab-el-Mandeb"]["warning_details"][0]["distance_km"] == 9.6

    panama = by_name["Panama Canal"]
    assert panama["status"] == "advisory"
    assert panama["naval_by_match"] == {"proximity": 0, "navarea": 1}
    assert panama["warning_details"][0]["id"] == "2026-905"
    assert panama["warning_details"][0]["match"] == "navarea"
    assert panama["warning_details"][0]["distance_km"] is None

    others = set(_WATERWAY_NAMES) - {"Strait of Hormuz", "Bab-el-Mandeb", "Panama Canal"}
    assert {by_name[name]["status"] for name in others} == {"clear"}

    assert [(w["id"], w["reason"], w["series"]) for w in result["unplaced_naval_warnings"]] == [
        ("2026-906", "position_unreadable", "NAVAREA XII")
    ]
    assert result["total_naval_warnings"] == 6
    assert result["newest_issue_age_days"] == pytest.approx(2 / 24, abs=0.1)
    assert "degraded" not in result


@respx.mock
@pytest.mark.asyncio
async def test_fetch_vessel_snapshot_nga_outage_reads_unknown_not_clear(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (silent-empty class): with NGA down every waterway read
    'clear', a calm-seas verdict drawn from no data."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(return_value=httpx.Response(500))

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert [ww["status"] for ww in result["waterways"]] == ["unknown"] * 9
    assert all(ww["naval_warnings"] == 0 for ww in result["waterways"])
    assert all(ww["total_nearby"] == 0 for ww in result["waterways"])
    assert result["error"] == (
        "NGA navigational warnings unavailable: "
        "NGA MSI API unavailable (no live or cached data)"
    )
    assert result["degraded"] is True
    assert result["reason"] == "nga_msi_fetch_failed"
    assert result["data_gaps"] == [
        "NGA feed: NGA MSI broadcast warnings unavailable (no live or cached "
        "data): zero warnings here means unknown, not a quiet sea"
    ]


@pytest.mark.asyncio
async def test_fetch_vessel_snapshot_roman_numeral_navarea_is_not_an_nga_code(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The live API never sends Roman numerals in navArea, so a warning
    that carries one and states no position is placed nowhere, and said
    so, instead of counting everywhere."""
    warnings = [
        {"id": "2026-1", "text": "NAVAL EXERCISES IN AREA", "navarea": "IV"},
        {"id": "2026-2", "text": "SUBMARINE OPERATIONS", "navarea": "XI"},
        {"id": "2026-3", "text": "LIVE FIRING IN PROGRESS", "navarea": "IX"},
        {"id": "2026-4", "text": "BUOY UNLIT", "navarea": "IV"},
        {"id": "2026-5", "text": "X" * 500 + "...", "navarea": "A"},
    ]
    monkeypatch.setattr(
        maritime, "fetch_nav_warnings", _async_return({"warnings": warnings})
    )

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert all(ww["naval_warnings"] == 0 for ww in result["waterways"])
    assert all(ww["status"] == "clear" for ww in result["waterways"])
    assert all(ww["total_nearby"] == 0 for ww in result["waterways"])
    assert result["total_naval_warnings"] == 3
    assert [
        (w["id"], w["reason"], w["series"]) for w in result["unplaced_naval_warnings"]
    ] == [
        ("2026-1", "unknown_area_code", None),
        ("2026-2", "unknown_area_code", None),
        ("2026-3", "unknown_area_code", None),
    ]
    assert result["data_gaps"] == [
        "3 naval warning(s) state no readable position and no single-NAVAREA "
        "area code, so no waterway is credited with them "
        "(unplaced_naval_warnings): 2026-1 (unknown_area_code), "
        "2026-2 (unknown_area_code), 2026-3 (unknown_area_code)",
        "1 warning text(s) were cut at 500 characters; positions after the "
        "cut are not read",
    ]
    assert "degraded" not in result


# ---------------------------------------------------------------------------
# Function 12: fetch_cascade_analysis
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_cascade_analysis_specific_corridor(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        infrastructure,
        "fetch_cable_health",
        _async_return(
            {"corridors": {"red_sea": {"status_score": 3, "status_label": "disrupted"}}}
        ),
    )

    result = await intelligence.fetch_cascade_analysis(fetcher, corridor="red_sea")

    assert result["source"] == "cascade-analysis"
    assert result["scenario_count"] == 1
    scenario = result["scenarios"][0]
    assert scenario["scenario"] == "Disruption of red_sea"
    assert scenario["corridors"] == ["red_sea"]
    assert scenario["disrupted"] == ["red_sea"]
    # Djibouti carries the highest red_sea dependency (0.8): with full
    # disruption severity it must top the impact list at score 80.
    top_impact = scenario["country_impacts"][0]
    assert top_impact["country"] == "Djibouti"
    assert top_impact["impact_score"] == 80
    assert top_impact["risk_level"] == "critical"

    assert result["current_health"]["red_sea"]["status_score"] == 3
    assert result["current_health"]["red_sea"]["status_label"] == "disrupted"


@pytest.mark.asyncio
async def test_fetch_cascade_analysis_no_at_risk_corridors(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        infrastructure,
        "fetch_cable_health",
        _async_return(
            {
                "corridors": {
                    "red_sea": {"status_score": 0, "status_label": "clear"},
                    "asia_europe": {"status_score": 1, "status_label": "advisory"},
                }
            }
        ),
    )

    result = await intelligence.fetch_cascade_analysis(fetcher)
    assert result["scenario_count"] == 1
    assert (
        result["scenarios"][0]["scenario"]
        == "Hypothetical: Red Sea corridor disruption"
    )


@pytest.mark.asyncio
async def test_fetch_cascade_analysis_multiple_at_risk(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(
        infrastructure,
        "fetch_cable_health",
        _async_return(
            {
                "corridors": {
                    "red_sea": {"status_score": 2, "status_label": "at_risk"},
                    "asia_europe": {"status_score": 3, "status_label": "disrupted"},
                }
            }
        ),
    )

    result = await intelligence.fetch_cascade_analysis(fetcher)
    # Two individual scenarios plus the combined worst case.
    assert result["scenario_count"] == 3
    labels = [s["scenario"] for s in result["scenarios"]]
    assert "Disruption of red_sea" in labels
    assert "Disruption of asia_europe" in labels
    assert "Combined disruption (worst case)" in labels
    combined = next(
        s
        for s in result["scenarios"]
        if s["scenario"] == "Combined disruption (worst case)"
    )
    assert sorted(combined["corridors"]) == ["asia_europe", "red_sea"]
