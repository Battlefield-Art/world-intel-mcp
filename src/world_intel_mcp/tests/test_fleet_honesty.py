"""Tests for the second maritime-honesty review (2026-10-17), fleet side:
an outage never reads as LOW_ACTIVITY (M1), a frozen NGA feed is not
scored as current (M2), military flights, theater posture and surge
detection say when no flight source answered, and the two-components-down
mutant (F2) that
survived the first round.

Components are monkeypatched at the sibling-module boundary, except in
the all-upstream-outage test, which runs the real posture, surge and
snapshot code against respx-mocked NGA, adsb.lol and OpenSky 500s.

Gaps / not covered: no live upstream call is made here; a partial
OpenSky outage is driven through a faked fetch_military_flights, not
through rate limiting or a tripped circuit breaker; consumers of posture
other than surge and fleet (strategic posture, temporal anomalies, the
dashboard) are not exercised.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx

from world_intel_mcp.analysis.temporal import TemporalBaseline
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import intelligence, military
from world_intel_mcp.sources.fleet import fetch_fleet_report

_UNKNOWN_LINE = (
    "readiness_level is UNKNOWN and readiness_score is null: no component "
    "was available to score"
)
_NO_FLIGHTS = (
    "No military flight source available for any theater "
    "(neither adsb.lol nor OpenSky returned military flights)"
)


def _returning(value):
    async def _fake(*args, **kwargs):
        return value

    return _fake


def _raising(exc: Exception):
    async def _fake(*args, **kwargs):
        raise exc

    return _fake


async def _no_sleep(*args, **kwargs) -> None:
    return None


def _patch_components(monkeypatch, posture, vessels, surge) -> None:
    monkeypatch.setattr(military, "fetch_theater_posture", posture)
    monkeypatch.setattr(intelligence, "fetch_vessel_snapshot", vessels)
    monkeypatch.setattr(intelligence, "fetch_military_surge", surge)


_HORMUZ_CRITICAL = {
    "waterways": [{"name": "Strait of Hormuz", "status": "critical", "naval_warnings": 3}]
}
_PERSIAN_GULF_SURGE = {
    "surges": [{"region": "persian_gulf", "aircraft_count": 9, "baseline": 3, "ratio": 3.0}]
}


# ---------------------------------------------------------------------------
# M1: fleet readiness with components down
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_all_components_down_reads_unknown_not_low_activity(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (M1): with every component down the report carried an
    error but still read LOW_ACTIVITY with score 0, the reading of a quiet
    fleet."""
    boom = _raising(RuntimeError("component down"))
    _patch_components(monkeypatch, boom, boom, boom)

    result = await fetch_fleet_report(fetcher)

    assert result["readiness_level"] == "UNKNOWN"
    assert result["readiness_score"] is None
    assert result["readiness_lower_bound"] is None
    assert result["error"] == (
        "All fleet report components unavailable: "
        "military_surge, theater_posture, vessel_snapshot"
    )
    assert result["reason"] == "fleet_components_unavailable"
    assert result["data_gaps"] == [
        f"{name}: unavailable (RuntimeError: component down)"
        for name in ("military_surge", "theater_posture", "vessel_snapshot")
    ] + [_UNKNOWN_LINE]


@pytest.mark.asyncio
async def test_one_component_down_makes_the_level_a_lower_bound(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (M1): with posture down the score lost its aircraft term
    (at most 60 of 90, so HIGH_ACTIVITY was out of reach) and nothing said
    the level could only be higher."""
    _patch_components(
        monkeypatch,
        _raising(TimeoutError("opensky timed out")),
        _returning(_HORMUZ_CRITICAL),
        _returning(_PERSIAN_GULF_SURGE),
    )

    result = await fetch_fleet_report(fetcher)

    assert result["readiness_score"] == 45
    assert result["readiness_level"] == "ELEVATED_ACTIVITY"
    assert result["readiness_lower_bound"] is True
    assert result["unavailable_components"] == ["theater_posture"]
    assert result["data_gaps"] == [
        "theater_posture: unavailable (TimeoutError: opensky timed out)",
        "readiness_level and readiness_score are lower bounds: they leave out "
        "theater_posture, so the score can reach at most 60 of 90",
    ]
    assert "error" not in result


@pytest.mark.asyncio
async def test_a_complete_report_is_not_a_lower_bound(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_components(
        monkeypatch,
        _returning({"theaters": {"european": {"count": 4}}}),
        _returning(_HORMUZ_CRITICAL),
        _returning(_PERSIAN_GULF_SURGE),
    )

    result = await fetch_fleet_report(fetcher)

    assert result["readiness_score"] == 47
    assert result["readiness_lower_bound"] is False
    assert result["data_gaps"] == []
    assert "degraded" not in result


@pytest.mark.asyncio
async def test_two_components_down_is_degraded_not_an_error(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutant F2 (error set at 2 of 3 components down) survived: no test
    took exactly two components down."""
    boom = _raising(RuntimeError("component down"))
    _patch_components(monkeypatch, boom, _returning(_HORMUZ_CRITICAL), boom)

    result = await fetch_fleet_report(fetcher)

    assert "error" not in result
    assert "reason" not in result
    assert result["degraded"] is True
    assert result["unavailable_components"] == ["military_surge", "theater_posture"]
    assert result["waterway_summary"] == [
        {"name": "Strait of Hormuz", "status": "critical", "warning_count": 3}
    ]


@pytest.mark.asyncio
async def test_theaters_without_a_flight_source_are_left_out_not_zero(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    posture = {
        "theaters": {
            "european": {"count": 20, "source": "opensky"},
            "arctic": {"count": 0, "source": "none"},
        },
        "unavailable_theaters": ["arctic"],
        "degraded": True,
        "data_gaps": ["no flights for arctic"],
    }
    _patch_components(
        monkeypatch,
        _returning(posture),
        _returning({"waterways": []}),
        _returning({"surges": [], "unchecked_regions": ["arctic"], "degraded": True,
                    "data_gaps": ["arctic unchecked"]}),
    )

    result = await fetch_fleet_report(fetcher)

    assert [t["name"] for t in result["theater_summary"]] == ["european"]
    assert result["total_tracked_aircraft"] == 20
    assert result["unavailable_theaters"] == ["arctic"]
    assert result["readiness_score"] == 10
    assert result["readiness_lower_bound"] is True
    assert result["degraded"] is True
    assert result["data_gaps"] == [
        "military_surge: arctic unchecked",
        "theater_posture: no flights for arctic",
        "readiness_level and readiness_score are lower bounds: they leave out "
        "the aircraft of arctic; surges in arctic",
    ]
    assert "unavailable_components" not in result


# ---------------------------------------------------------------------------
# M2: a frozen NGA feed is scored like an unavailable one
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_stale_vessel_snapshot_is_not_scored(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (M2): three 2024 warnings near Hormuz scored as a
    critical waterway in today's readiness."""
    stale_snapshot = {
        **_HORMUZ_CRITICAL,
        "feed_stale": True,
        "newest_issue_date": "2024-05-10T15:15:00Z",
        "stale_after_days": 14,
        "degraded": True,
        "data_gaps": ["NGA feed: stale"],
    }
    _patch_components(
        monkeypatch,
        _returning({"theaters": {"european": {"count": 20}}}),
        _returning(stale_snapshot),
        _returning({"surges": []}),
    )

    result = await fetch_fleet_report(fetcher)

    assert result["readiness_score"] == 10
    assert result["readiness_level"] == "LOW_ACTIVITY"
    assert result["readiness_lower_bound"] is True
    assert result["unavailable_components"] == ["vessel_snapshot"]
    assert result["data_gaps"] == [
        "vessel_snapshot: stale (its newest NGA warning was issued "
        "2024-05-10T15:15:00Z, past the 14-day threshold)",
        "readiness_level and readiness_score are lower bounds: they leave out "
        "vessel_snapshot, so the score can reach at most 60 of 90",
    ]
    # The waterway still shows what the snapshot said.
    assert result["waterway_summary"][0]["warning_count"] == 3
    assert "error" not in result


# ---------------------------------------------------------------------------
# Military flights, theater posture and surge: no flight source is unknown,
# not zero
# ---------------------------------------------------------------------------

_ADSBLOL = r".*api\.adsb\.lol.*"
_OPENSKY = r".*opensky-network\.org.*"


@respx.mock
@pytest.mark.asyncio
async def test_military_flights_with_both_sources_down_is_an_error(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: with adsb.lol and OpenSky both down the result was an
    empty sky, count 0, told apart only by source "none"."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(500))
    respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))

    result = await military.fetch_military_flights(fetcher)

    assert result["error"] == (
        "No military flight source available (adsb.lol and OpenSky both failed)"
    )
    assert result["degraded"] is True
    assert result["reason"] == "military_flights_unavailable"
    # The keys consumers already read are kept as they were.
    assert result["source"] == "none"
    assert result["aircraft"] == []
    assert result["count"] == 0
    assert result["military_filter"] == "icao_prefix+callsign"


@respx.mock
@pytest.mark.asyncio
async def test_military_flights_names_an_empty_adsblol_answer(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(200, json={"ac": []}))
    respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))

    result = await military.fetch_military_flights(fetcher)

    assert result["error"] == (
        "No military flight source available "
        "(adsb.lol returned no positioned aircraft and OpenSky failed)"
    )
    assert result["reason"] == "military_flights_unavailable"
    assert result["source"] == "none"


@respx.mock
@pytest.mark.asyncio
async def test_military_flights_zero_in_a_box_from_a_live_source_is_not_an_error(
    fetcher: Fetcher,
) -> None:
    """A live answer with no aircraft inside the box is a real zero."""
    respx.get(url__regex=_ADSBLOL).mock(
        return_value=httpx.Response(
            200, json={"ac": [{"hex": "ae1234", "flight": "RCH1", "lat": 50.0, "lon": 10.0}]}
        )
    )
    opensky = respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))

    result = await military.fetch_military_flights(fetcher, bbox="-10,95,55,155")

    assert result["count"] == 0
    assert result["source"] == "adsb.lol"
    assert not {"error", "degraded", "reason"} & set(result)
    assert not opensky.called


def _flights(no_source: set[str]):
    async def _fake(fetcher, bbox=None):
        name = next(n for n, info in military.THEATERS.items() if info["bbox"] == bbox)
        if name in no_source:
            return {
                "aircraft": [],
                "count": 0,
                "source": "none",
                "error": "No military flight source available (adsb.lol and OpenSky both failed)",
                "degraded": True,
                "reason": "military_flights_unavailable",
            }
        aircraft = [{"callsign": "RCH1", "origin_country": "United States"}]
        return {"aircraft": aircraft, "count": 1, "source": "adsb.lol"}

    return _fake


@pytest.mark.asyncio
async def test_posture_names_theaters_no_flight_source_answered_for(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (M1): a theater no flight source answered for came back
    as zero aircraft, the reading of an empty sky."""
    monkeypatch.setattr(military, "fetch_military_flights", _flights({"arctic", "korean_peninsula"}))

    result = await military.fetch_theater_posture(fetcher)

    assert result["unavailable_theaters"] == ["arctic", "korean_peninsula"]
    assert result["degraded"] is True
    assert "error" not in result
    assert result["total_military_aircraft"] == 3
    assert result["theaters"]["arctic"]["source"] == "none"
    assert result["theaters"]["european"]["source"] == "adsb.lol"
    assert result["data_gaps"] == [
        "neither adsb.lol nor OpenSky returned military flights for arctic, "
        "korean_peninsula: their counts are unknown, not zero, and "
        "total_military_aircraft counts only the other theaters"
    ]
    # A partial posture is not cached, so it is not replayed after recovery.
    assert fetcher.cache.get("military:posture") is None


@pytest.mark.asyncio
async def test_posture_with_no_flight_source_anywhere_is_an_error(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(military, "fetch_military_flights", _flights(set(military.THEATERS)))

    result = await military.fetch_theater_posture(fetcher)

    assert result["error"] == _NO_FLIGHTS
    assert result["reason"] == "military_flights_unavailable"
    assert result["unavailable_theaters"] == list(military.THEATERS)
    assert fetcher.cache.get("military:posture") is None


@pytest.mark.asyncio
async def test_a_complete_posture_is_cached_with_no_gaps(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(military, "fetch_military_flights", _flights(set()))

    result = await military.fetch_theater_posture(fetcher)

    assert result["data_gaps"] == []
    assert "degraded" not in result
    assert "unavailable_theaters" not in result
    assert fetcher.cache.get("military:posture") == result


def _baseline_keys(baseline: TemporalBaseline) -> list[str]:
    return [row[0] for row in baseline._conn.execute("SELECT key FROM baselines")]


@pytest.mark.asyncio
async def test_surge_with_posture_down_is_an_error_and_records_nothing(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Regression (M1): a posture outage recorded a zero in every region's
    surge baseline and reported no surges, the reading of an empty sky."""
    baseline = TemporalBaseline(db_path=str(tmp_path / "t.db"))
    monkeypatch.setattr(intelligence, "_temporal", baseline)
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _returning({"theaters": {}, "error": _NO_FLIGHTS, "reason": "military_flights_unavailable"}),
    )

    result = await intelligence.fetch_military_surge(fetcher)

    assert result["error"] == f"Theater posture unavailable: {_NO_FLIGHTS}"
    assert result["reason"] == "military_flights_unavailable"
    assert result["degraded"] is True
    assert result["regions_checked"] == 0
    assert result["surges"] == []
    assert _baseline_keys(baseline) == []


@pytest.mark.asyncio
async def test_surge_skips_regions_whose_theater_had_no_source(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    baseline = TemporalBaseline(db_path=str(tmp_path / "t.db"))
    monkeypatch.setattr(intelligence, "_temporal", baseline)
    monkeypatch.setattr(
        military,
        "fetch_theater_posture",
        _returning(
            {
                "theaters": {
                    "european": {"count": 40, "countries": ["United States"]},
                    "arctic": {"count": 0, "countries": [], "source": "none"},
                },
                "unavailable_theaters": ["arctic"],
            }
        ),
    )

    result = await intelligence.fetch_military_surge(fetcher)

    assert result["unchecked_regions"] == ["arctic"]
    assert result["regions_checked"] == 7
    assert result["degraded"] is True
    assert result["data_gaps"] == [
        "no military flight source answered for arctic, so arctic went "
        "unchecked and nothing was recorded in their baselines"
    ]
    assert [s["region"] for s in result["surges"]] == ["baltic_sea"]
    keys = _baseline_keys(baseline)
    assert len(keys) == 7
    assert not any(":arctic:" in key for key in keys)
    assert "error" not in result


@respx.mock
@pytest.mark.asyncio
async def test_every_upstream_down_reads_unknown_through_the_real_components(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    """Regression (M1, measured by the reviewer with a real Fetcher): NGA,
    adsb.lol and OpenSky all failing gave LOW_ACTIVITY, score 0, zero
    aircraft and no error, because posture and surge turned the outage into
    zero aircraft and only the snapshot reported it."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    monkeypatch.setattr(
        intelligence, "_temporal", TemporalBaseline(db_path=str(tmp_path / "t.db"))
    )
    for host in (r"msi\.nga\.mil", r"api\.adsb\.lol", r"opensky-network\.org"):
        respx.get(url__regex=rf".*{host}.*").mock(return_value=httpx.Response(500))

    result = await fetch_fleet_report(fetcher)

    assert result["readiness_level"] == "UNKNOWN"
    assert result["readiness_score"] is None
    assert result["error"] == (
        "All fleet report components unavailable: "
        "military_surge, theater_posture, vessel_snapshot"
    )
    assert result["data_gaps"] == [
        f"military_surge: unavailable (Theater posture unavailable: {_NO_FLIGHTS})",
        f"theater_posture: unavailable ({_NO_FLIGHTS})",
        "vessel_snapshot: unavailable (NGA navigational warnings unavailable: "
        "NGA MSI API unavailable (no live or cached data))",
        _UNKNOWN_LINE,
    ]
    assert result["total_tracked_aircraft"] == 0
    assert result["theater_summary"] == []
    assert {ww["status"] for ww in result["waterway_summary"]} == {"unknown"}
    assert fetcher.cache.get("military:posture") is None
