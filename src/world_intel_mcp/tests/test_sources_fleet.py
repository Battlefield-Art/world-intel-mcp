"""Tests for sources/fleet.py — component aggregation is monkeypatched at
the sibling-module boundary (fleet imports them inside the function, so
patching the module attributes intercepts the real calls).

Gaps / not covered: the real intelligence/military component functions are
not exercised here (they have their own tests), except the vessel snapshot
in the NGA-outage test, which runs the real snapshot against a mocked NGA
500; naval-base static data is asserted only as non-empty, not for content;
theater posture and military surge never report an error themselves (an
OpenSky outage reads as zero aircraft there), so only a raise marks them
unavailable.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import intelligence, military
from world_intel_mcp.sources.fleet import _fleet_readiness, _safe, fetch_fleet_report


def test_fleet_readiness_low_activity() -> None:
    level, score = _fleet_readiness({}, [], [])
    assert level == "LOW_ACTIVITY"
    assert score == 0


def test_fleet_readiness_high_activity() -> None:
    # aircraft 60 -> capped 30; one critical waterway -> 30; one surge -> 15.
    theaters = {"european": {"count": 60}}
    waterways = [{"status": "critical"}]
    surges = [{"region": "baltic_sea"}]
    level, score = _fleet_readiness(theaters, waterways, surges)
    assert score == 75
    assert level == "HIGH_ACTIVITY"


def test_fleet_readiness_normal_operations() -> None:
    # aircraft 20 -> 10; one advisory waterway -> 10; no surges. Total 20.
    theaters = {"a": {"count": 20}}
    waterways = [{"status": "advisory"}]
    level, score = _fleet_readiness(theaters, waterways, [])
    assert score == 20
    assert level == "NORMAL_OPERATIONS"


def test_fleet_readiness_ignores_non_dict_theaters() -> None:
    level, score = _fleet_readiness({"bad": "not-a-dict"}, [], [])
    assert score == 0
    assert level == "LOW_ACTIVITY"


@pytest.mark.asyncio
async def test_fetch_fleet_report(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _posture(f) -> dict:
        return {
            "theaters": {
                "european": {
                    "count": 4,
                    "top_types": ["C-17", "KC-135", "F-16", "P-8"],
                },
            }
        }

    async def _vessels(f) -> dict:
        return {
            "waterways": [
                {"name": "Suez Canal", "status": "elevated", "warning_count": 2},
            ]
        }

    async def _surges(f) -> dict:
        return {
            "surges": [
                {
                    "region": "black_sea",
                    "aircraft_count": 12,
                    "baseline": 5,
                    "ratio": 2.4,
                },
            ]
        }

    monkeypatch.setattr(military, "fetch_theater_posture", _posture)
    monkeypatch.setattr(intelligence, "fetch_vessel_snapshot", _vessels)
    monkeypatch.setattr(intelligence, "fetch_military_surge", _surges)

    result = await fetch_fleet_report(fetcher)

    assert result["source"] == "fleet-activity-report"
    assert result["theater_count"] == 1
    assert result["theater_summary"] == [
        {
            "name": "european",
            "aircraft_count": 4,
            "top_types": ["C-17", "KC-135", "F-16"],  # truncated to 3
        }
    ]
    assert result["waterway_summary"] == [
        {"name": "Suez Canal", "status": "elevated", "warning_count": 2}
    ]
    assert result["active_surges"] == [
        {"region": "black_sea", "aircraft_count": 12, "baseline": 5, "ratio": 2.4}
    ]
    assert result["surge_count"] == 1
    assert result["total_tracked_aircraft"] == 4
    # Static dataset ships naval bases; the report must see a non-zero count.
    assert result["naval_base_count"] > 0

    # Readiness: 4 aircraft -> 2; elevated waterway -> 20; 1 surge -> 15 = 37.
    assert result["readiness_score"] == 37
    assert result["readiness_level"] == "NORMAL_OPERATIONS"


@pytest.mark.asyncio
async def test_fetch_fleet_report_survives_component_failures(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every dynamic component raising still yields a well-formed report.
    Observation (flagged in review, not fixed): the report carries no
    degraded marker, so three dead components read as a quiet fleet."""

    async def _boom(f) -> dict:
        raise RuntimeError("component down")

    monkeypatch.setattr(military, "fetch_theater_posture", _boom)
    monkeypatch.setattr(intelligence, "fetch_vessel_snapshot", _boom)
    monkeypatch.setattr(intelligence, "fetch_military_surge", _boom)

    result = await fetch_fleet_report(fetcher)

    assert result["readiness_level"] == "LOW_ACTIVITY"
    assert result["readiness_score"] == 0
    assert result["theater_summary"] == []
    assert result["waterway_summary"] == []
    assert result["active_surges"] == []
    assert result["total_tracked_aircraft"] == 0
    assert "error" not in result  # current (dishonest-quiet) behavior


@pytest.mark.asyncio
async def test_waterway_warning_count_from_real_vessel_snapshot_shape(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: fetch_vessel_snapshot emits the count as
    "naval_warnings", and the fleet report read only "warning_count",
    so per-waterway warning counts were 0 in every real report (the
    silent-zero class). The report must propagate the count from the
    shape the snapshot actually emits."""
    from world_intel_mcp.sources import intelligence, military

    async def _snapshot_real_shape(f, **kwargs):
        return {
            "waterways": [
                {"name": "Strait of Hormuz", "status": "critical", "naval_warnings": 3}
            ]
        }

    async def _empty(f, **kwargs):
        return {}

    monkeypatch.setattr(intelligence, "fetch_vessel_snapshot", _snapshot_real_shape)
    monkeypatch.setattr(military, "fetch_theater_posture", _empty)
    monkeypatch.setattr(intelligence, "fetch_military_surge", _empty)

    from world_intel_mcp.sources.fleet import fetch_fleet_report

    result = await fetch_fleet_report(fetcher)
    ww = result["waterway_summary"][0]
    assert ww["name"] == "Strait of Hormuz"
    assert ww["warning_count"] == 3


_COMPONENTS = ("military_surge", "theater_posture", "vessel_snapshot")


async def _no_sleep(*args, **kwargs) -> None:
    return None


@pytest.mark.asyncio
async def test_fetch_fleet_report_all_components_down_is_an_error(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (silent-empty class): with every component raising,
    _safe returned {} for each and the report read LOW_ACTIVITY with no
    marker. The zeroed structure stays; error / reason /
    unavailable_components / data_gaps say the report has no inputs."""

    async def _boom(f) -> dict:
        raise RuntimeError("component down")

    monkeypatch.setattr(military, "fetch_theater_posture", _boom)
    monkeypatch.setattr(intelligence, "fetch_vessel_snapshot", _boom)
    monkeypatch.setattr(intelligence, "fetch_military_surge", _boom)

    result = await fetch_fleet_report(fetcher)

    assert result["error"] == (
        "All fleet report components unavailable: "
        "military_surge, theater_posture, vessel_snapshot"
    )
    assert result["reason"] == "fleet_components_unavailable"
    assert result["degraded"] is True
    assert result["unavailable_components"] == list(_COMPONENTS)
    assert result["data_gaps"] == [
        f"{name}: unavailable (RuntimeError: component down); "
        "readiness_score is computed without it"
        for name in _COMPONENTS
    ]
    assert result["readiness_level"] == "LOW_ACTIVITY"
    assert result["readiness_score"] == 0


@pytest.mark.asyncio
async def test_fetch_fleet_report_partial_failure_is_named_not_fatal(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _boom(f) -> dict:
        raise TimeoutError("opensky timed out")

    async def _vessels(f) -> dict:
        return {
            "waterways": [
                {"name": "Strait of Hormuz", "status": "critical", "naval_warnings": 3}
            ]
        }

    async def _surges(f) -> dict:
        return {
            "surges": [
                {"region": "persian_gulf", "aircraft_count": 9, "baseline": 3, "ratio": 3.0}
            ]
        }

    monkeypatch.setattr(military, "fetch_theater_posture", _boom)
    monkeypatch.setattr(intelligence, "fetch_vessel_snapshot", _vessels)
    monkeypatch.setattr(intelligence, "fetch_military_surge", _surges)

    result = await fetch_fleet_report(fetcher)

    assert "error" not in result
    assert result["degraded"] is True
    assert result["unavailable_components"] == ["theater_posture"]
    assert result["data_gaps"] == [
        "theater_posture: unavailable (TimeoutError: opensky timed out); "
        "readiness_score is computed without it"
    ]
    # Critical waterway 30 + one surge 15; no aircraft term without posture.
    assert result["readiness_score"] == 45
    assert result["readiness_level"] == "ELEVATED_ACTIVITY"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_fleet_report_names_an_nga_outage_from_the_real_snapshot(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Through the real vessel snapshot: with NGA down the snapshot is an
    error, so the report lists it unavailable instead of scoring nine
    'clear' waterways as calm."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(return_value=httpx.Response(500))

    async def _posture(f) -> dict:
        return {"theaters": {"european": {"count": 4, "top_types": []}}}

    async def _surges(f) -> dict:
        return {"surges": []}

    monkeypatch.setattr(military, "fetch_theater_posture", _posture)
    monkeypatch.setattr(intelligence, "fetch_military_surge", _surges)

    result = await fetch_fleet_report(fetcher)

    assert "error" not in result
    assert result["degraded"] is True
    assert result["unavailable_components"] == ["vessel_snapshot"]
    assert result["data_gaps"] == [
        "vessel_snapshot: unavailable (NGA navigational warnings unavailable: "
        "NGA MSI API unavailable (no live or cached data)); readiness_score is "
        "computed without it"
    ]
    assert result["waterway_count"] == 9
    assert {ww["status"] for ww in result["waterway_summary"]} == {"unknown"}
    # 4 aircraft -> 2; 'unknown' waterways add nothing.
    assert result["readiness_score"] == 2


@pytest.mark.asyncio
async def test_fetch_fleet_report_carries_component_data_gaps(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    stale = (
        "NGA feed: newest active warning was issued 2024-05-10T15:15:00Z, 890 "
        "days ago, past the 14-day staleness threshold: these are historical "
        "warnings, not current conditions"
    )

    async def _vessels(f) -> dict:
        return {
            "waterways": [{"name": "Suez Canal", "status": "clear", "naval_warnings": 0}],
            "degraded": True,
            "data_gaps": [stale],
        }

    async def _posture(f) -> dict:
        return {"theaters": {}}

    async def _surges(f) -> dict:
        return {"surges": [], "degraded": True}

    monkeypatch.setattr(military, "fetch_theater_posture", _posture)
    monkeypatch.setattr(intelligence, "fetch_vessel_snapshot", _vessels)
    monkeypatch.setattr(intelligence, "fetch_military_surge", _surges)

    result = await fetch_fleet_report(fetcher)

    assert "error" not in result
    assert "unavailable_components" not in result
    assert result["degraded"] is True
    assert result["data_gaps"] == ["military_surge: degraded", f"vessel_snapshot: {stale}"]


@pytest.mark.asyncio
async def test_safe_turns_raises_and_bad_shapes_into_error_dicts() -> None:
    async def _raises() -> dict:
        raise ValueError("bad payload")

    async def _returns_list() -> list:
        return ["not", "a", "dict"]

    assert await _safe(_raises(), "vessel_snapshot") == {
        "error": "ValueError: bad payload",
        "degraded": True,
        "reason": "component_raised",
    }
    assert await _safe(_returns_list(), "vessel_snapshot") == {
        "error": "returned list, not a dict",
        "degraded": True,
        "reason": "component_bad_shape",
    }
