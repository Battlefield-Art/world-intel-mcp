"""Tests for the third maritime-honesty review (2026-10-17): expired
cached flight copies (behavior F1, tests F1), the CLI posture table
(behavior F2, tests F8), an undated NGA feed (behavior F3, tests F7), an
empty NGA answer (behavior F4, tests F6), malformed bounding boxes
(behavior F7), and the parser guards no test pinned (behavior F8, tests
F2-F5).

The flight tests run the real Fetcher and Cache against respx-mocked
adsb.lol and OpenSky 500s, with an expired row seeded into the cache the
way a long-running server holds one; NGA records are built in the live
API's shape.

Gaps / not covered: no live upstream call is made here; a timeout and the
circuit-open branch reach the same stale fallback but only HTTP 500 is
driven; consumers of military flights other than posture, surge, fleet
and the CLI (AOI briefs, situation, daily digest, the dashboard and the
collector) are not exercised; the MCP schema pattern for bbox is checked
for what it accepts, not enforced by a running server.
"""

import asyncio as asyncio_mod
import re
import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx
from click.testing import CliRunner

from world_intel_mcp import cli
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.reports import _render_maritime
from world_intel_mcp.sources import intelligence, maritime, military
from world_intel_mcp.sources.fleet import fetch_fleet_report
from world_intel_mcp.tools import conflict

_ADSBLOL = r".*api\.adsb\.lol.*"
_OPENSKY = r".*opensky-network\.org.*"
_NGA = r".*msi\.nga\.mil.*"
_MONTHS = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()
_THREE_DAYS = 3 * 86400

# RCH1 over Germany (inside the european box), RCH2 over the Gulf.
_ADSBLOL_BODY = {
    "ac": [
        {"hex": "ae1234", "flight": "RCH1 ", "lat": 50.0, "lon": 10.0},
        {"hex": "ae5678", "flight": "RCH2", "lat": 26.5, "lon": 56.2},
    ]
}


def _opensky_state(icao24: str, callsign: str, lat: float, lon: float) -> list:
    return [icao24, callsign, "United States", None, None, lon, lat, 9000.0,
            False, 200.0, 90.0, None, None, None, "1234", False, 0]


async def _no_sleep(*args, **kwargs) -> None:
    return None


def _returning(value):
    async def _fake(*args, **kwargs):
        return value

    return _fake


def _seed_expired(fetcher: Fetcher, key: str, value, age_seconds: float) -> None:
    """A cache row written age_seconds ago with a 300 s TTL, long expired."""
    fetcher.cache.set(key, value, 300)
    created = time.time() - age_seconds
    conn = fetcher.cache._get_conn()
    conn.execute(
        "UPDATE cache SET created_at = ?, expires_at = ? WHERE key = ?",
        (created, created + 300, key),
    )
    conn.commit()


def _stamp(when: datetime) -> str:
    return f"{when:%d%H%M}Z {_MONTHS[when.month - 1]} {when.year}"


def _record(number: int, area: str, text: str, issued: str, cancel=None) -> dict:
    return {
        "msgYear": 2026, "msgNumber": number, "navArea": area,
        "subregion": "00", "text": text, "status": "A",
        "issueDate": issued, "authority": "NAVSAFETY", "cancelDate": cancel,
    }


def _mock_nga(records: list[dict]):
    return respx.get(url__regex=_NGA).mock(
        return_value=httpx.Response(200, json={"broadcast-warn": records})
    )


def _hormuz_naval(issued: str) -> list[dict]:
    # 26-34.00N 056-15.00E is about 10 km from the Hormuz reference point.
    return [
        _record(n, "P", "PERSIAN GULF.\nNAVAL EXERCISE IN AREA 26-34.00N 056-15.00E.\n", issued)
        for n in (1, 2, 3)
    ]


def _by_name(result: dict) -> dict[str, dict]:
    return {ww["name"]: ww for ww in result["waterways"]}


# ---------------------------------------------------------------------------
# F1: an expired cached flight copy is never current flights
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_expired_adsblol_copy_is_an_outage_not_current_flights(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: with adsb.lol and OpenSky both down, a 3-day-old
    adsb.lol copy came back as source adsb.lol, stamped now, with no error
    or degraded flag, and OpenSky was never tried."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    _seed_expired(fetcher, "military:adsblol:mil", _ADSBLOL_BODY, _THREE_DAYS)
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(500))
    opensky = respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))

    result = await military.fetch_military_flights(fetcher)

    assert result["source"] == "none"
    assert result["aircraft"] == []
    assert result["count"] == 0
    assert result["degraded"] is True
    assert result["reason"] == "military_flights_unavailable"
    assert opensky.called
    copy = result["stale_copy"]
    assert copy["source"] == "adsb.lol"
    assert [a["callsign"] for a in copy["aircraft"]] == ["RCH1", "RCH2"]
    assert copy["count"] == 2
    assert copy["age_seconds"] == pytest.approx(_THREE_DAYS, abs=60)
    assert result["stale_age_seconds"] == copy["age_seconds"]
    as_of = datetime.fromisoformat(copy["as_of"])
    expected = datetime.now(timezone.utc) - timedelta(seconds=_THREE_DAYS)
    assert abs((as_of - expected).total_seconds()) < 60
    assert result["error"] == (
        "No live military flight source (adsb.lol and OpenSky both failed); only "
        f"an expired cached copy from adsb.lol, {copy['age_seconds']:.0f} s old, "
        "remains (stale_copy)"
    )
    assert result["data_gaps"] == [
        "no live military flight source answered; the 2 aircraft in stale_copy "
        f"are adsb.lol's cached copy as of {copy['as_of']} "
        f"({copy['age_seconds']:.0f} s old), not current positions, and are not counted"
    ]


@respx.mock
@pytest.mark.asyncio
async def test_expired_adsblol_copy_is_filtered_to_the_box(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    _seed_expired(fetcher, "military:adsblol:mil", _ADSBLOL_BODY, _THREE_DAYS)
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(500))
    respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))

    result = await military.fetch_military_flights(fetcher, bbox="35,-25,72,45")

    assert result["count"] == 0
    assert [a["callsign"] for a in result["stale_copy"]["aircraft"]] == ["RCH1"]


@respx.mock
@pytest.mark.asyncio
async def test_live_opensky_is_tried_before_an_expired_adsblol_copy(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    _seed_expired(fetcher, "military:adsblol:mil", _ADSBLOL_BODY, _THREE_DAYS)
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(500))
    respx.get(url__regex=_OPENSKY).mock(
        return_value=httpx.Response(
            200, json={"states": [_opensky_state("ae0009", "RCH9", 50.0, 10.0)]}
        )
    )

    result = await military.fetch_military_flights(fetcher)

    assert result["source"] == "opensky"
    assert [a["callsign"] for a in result["aircraft"]] == ["RCH9"]
    assert not {"error", "degraded", "reason", "stale_copy"} & set(result)


@respx.mock
@pytest.mark.asyncio
async def test_expired_opensky_copy_is_an_outage_not_current_flights(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    _seed_expired(
        fetcher,
        "military:flights:global",
        {"states": [_opensky_state("ae0009", "RCH9", 50.0, 10.0)]},
        3000,
    )
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(500))
    respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))

    result = await military.fetch_military_flights(fetcher)

    assert result["source"] == "none"
    assert result["aircraft"] == []
    assert result["stale_copy"]["source"] == "opensky"
    assert result["stale_copy"]["count"] == 1
    assert result["stale_copy"]["age_seconds"] == pytest.approx(3000, abs=60)
    assert "only an expired cached copy from opensky" in result["error"]


@respx.mock
@pytest.mark.asyncio
async def test_warm_cache_outage_reaches_posture_surge_and_fleet_as_an_outage(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch, temporal_baseline
) -> None:
    """Regression (tests F1, HIGH): the 3-day-old copy was cached as a
    complete posture, recorded into the surge baselines as today's
    traffic, and scored as a firm fleet readiness with no data gap."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    monkeypatch.setattr(intelligence, "_temporal", temporal_baseline)
    _seed_expired(fetcher, "military:adsblol:mil", _ADSBLOL_BODY, _THREE_DAYS)
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(500))
    respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))
    _mock_nga(_hormuz_naval(_stamp(datetime.now(timezone.utc) - timedelta(hours=2))))

    posture = await military.fetch_theater_posture(fetcher)

    assert posture["unavailable_theaters"] == list(military.THEATERS)
    assert posture["stale_theaters"] == list(military.THEATERS)
    assert posture["total_military_aircraft"] == 0
    assert posture["error"].startswith("No military flight source available for any theater")
    assert any(
        gap.startswith(f"for {', '.join(military.THEATERS)} only an expired cached copy answered")
        for gap in posture["data_gaps"]
    )
    assert all(
        t["stale_copy_age_seconds"] == pytest.approx(_THREE_DAYS, abs=60)
        for t in posture["theaters"].values()
    )
    assert fetcher.cache.get("military:posture") is None

    surge = await intelligence.fetch_military_surge(fetcher)

    assert surge["error"].startswith("Theater posture unavailable")
    assert surge["regions_checked"] == 0
    rows = temporal_baseline._get_conn().execute("SELECT key FROM baselines").fetchall()
    assert rows == []

    fleet = await fetch_fleet_report(fetcher)

    assert fleet["unavailable_components"] == ["military_surge", "theater_posture"]
    assert fleet["readiness_lower_bound"] is True
    assert fleet["total_tracked_aircraft"] == 0
    assert fleet["degraded"] is True
    assert temporal_baseline._get_conn().execute("SELECT key FROM baselines").fetchall() == []


@pytest.mark.asyncio
async def test_posture_names_a_theater_answered_only_by_an_expired_copy(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _flights(fetcher, bbox=None):
        name = next(n for n, info in military.THEATERS.items() if info["bbox"] == bbox)
        if name == "arctic":
            return {
                "aircraft": [], "count": 0, "source": "none",
                "error": "No live military flight source", "degraded": True,
                "stale_copy": {"source": "adsb.lol", "aircraft": [], "count": 0,
                               "as_of": "2026-10-17T00:00:00+00:00", "age_seconds": 3000.0},
            }
        aircraft = [{"callsign": "RCH1", "origin_country": "United States"}] * (
            2 if name == "european" else 0
        )
        return {"aircraft": aircraft, "count": len(aircraft), "source": "opensky"}

    monkeypatch.setattr(military, "fetch_military_flights", _flights)

    result = await military.fetch_theater_posture(fetcher)

    assert result["unavailable_theaters"] == ["arctic"]
    assert result["stale_theaters"] == ["arctic"]
    assert result["theaters"]["arctic"]["stale_copy_age_seconds"] == 3000.0
    assert "stale_copy_age_seconds" not in result["theaters"]["european"]
    assert result["total_military_aircraft"] == 2
    assert result["data_gaps"] == [
        "neither adsb.lol nor OpenSky returned military flights for arctic: their "
        "counts are unknown, not zero, and total_military_aircraft counts only the "
        "other theaters",
        "for arctic only an expired cached copy answered (up to 3000 s old); it is "
        "not counted as current flights",
    ]
    assert "error" not in result
    assert fetcher.cache.get("military:posture") is None


# ---------------------------------------------------------------------------
# Behavior F7: a malformed bbox is an error, never the whole world
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bbox",
    [
        "35,-25,72",          # three parts: the filter used to be skipped
        "35;-25;72;45",       # semicolons
        "35,-25,72,45,9",     # five parts
        "a,b,c,d",            # used to raise a bare ValueError
        "nan,-25,72,45",
        "35,-25,inf,45",
        "-91,-25,72,45",      # latitude out of range
        "35,-25,72,181",      # longitude out of range
        "72,-25,35,45",       # lamin above lamax
        "35,45,72,-25",       # lomin above lomax (crosses the antimeridian)
        "35,-25,35,45",       # empty latitude span
    ],
)
@respx.mock
@pytest.mark.asyncio
async def test_malformed_bbox_is_an_error_and_fetches_nothing(
    fetcher: Fetcher, bbox: str
) -> None:
    adsblol = respx.get(url__regex=_ADSBLOL).mock(
        return_value=httpx.Response(200, json=_ADSBLOL_BODY)
    )
    opensky = respx.get(url__regex=_OPENSKY).mock(return_value=httpx.Response(500))

    result = await military.fetch_military_flights(fetcher, bbox=bbox)

    assert result["reason"] == "invalid_bbox"
    assert result["error"].startswith(f"invalid bbox {bbox!r}: expected lamin,lomin,lamax,lomax")
    assert result["aircraft"] == []
    assert result["source"] == "none"
    assert not adsblol.called
    assert not opensky.called


@respx.mock
@pytest.mark.asyncio
async def test_bbox_with_spaces_and_decimals_is_read(fetcher: Fetcher, monkeypatch) -> None:
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=_ADSBLOL).mock(return_value=httpx.Response(500))
    opensky = respx.get(url__regex=_OPENSKY).mock(
        return_value=httpx.Response(200, json={"states": []})
    )

    result = await military.fetch_military_flights(fetcher, bbox=" 35.25, -25, 72, 45.5 ")

    assert result["source"] == "opensky"
    params = opensky.calls.last.request.url.params
    assert (params["lamin"], params["lomin"], params["lamax"], params["lomax"]) == (
        "35.25", "-25", "72", "45.5"
    )


def test_bbox_schema_pattern_matches_the_validator() -> None:
    tool = next(t for t in conflict.TOOLS if t.name == "intel_military_flights")
    pattern = re.compile(tool.inputSchema["properties"]["bbox"]["pattern"])
    assert pattern.match("35,-25,72,45")
    assert pattern.match("-10.5, 95, 55, 155")
    for bad in ("35,-25,72", "35;-25;72;45", "a,b,c,d", "35,-25,72,45,9"):
        assert not pattern.match(bad)


# ---------------------------------------------------------------------------
# Behavior F2 / tests F8: the CLI posture table never prints 0 for unknown
# ---------------------------------------------------------------------------


def test_cli_posture_prints_unknown_theaters_and_gaps(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)

    async def _flights(fetcher, bbox=None):
        name = next(n for n, info in military.THEATERS.items() if info["bbox"] == bbox)
        if name in {"arctic", "korean_peninsula"}:
            return {"aircraft": [], "count": 0, "source": "none",
                    "error": "No military flight source available", "degraded": True}
        aircraft = [{"callsign": "RCH1", "origin_country": "United States"}] * (
            3 if name == "european" else 0
        )
        return {"aircraft": aircraft, "count": len(aircraft), "source": "opensky"}

    monkeypatch.setattr(military, "fetch_military_flights", _flights)

    result = CliRunner().invoke(cli.main, ["posture"], catch_exceptions=False)

    assert result.exit_code == 0
    out = result.output
    assert "3 military aircraft in the theaters that answered (3 of 5; the rest are unknown)" in out
    assert "total military aircraft" not in out
    assert re.search(r"Arctic\s+unknown", out)
    assert re.search(r"Korean Peninsula\s+unknown", out)
    assert not re.search(r"Arctic\s+0\b", out)
    assert re.search(r"Middle East\s+0\b", out)
    assert re.search(r"European\s+3\b", out)
    assert "Gap: neither adsb.lol nor OpenSky returned military flights for arctic, " \
        "korean_peninsula" in " ".join(out.split())


def test_cli_posture_complete_prints_a_plain_total(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)
    monkeypatch.setattr(
        military,
        "fetch_military_flights",
        _returning({"aircraft": [], "count": 0, "source": "opensky"}),
    )

    result = CliRunner().invoke(cli.main, ["posture"], catch_exceptions=False)

    assert "0 total military aircraft" in result.output
    assert "unknown" not in result.output
    assert "Gap:" not in result.output


# ---------------------------------------------------------------------------
# Behavior F3 / tests F7: an undated feed is not scored as current
# ---------------------------------------------------------------------------


_UNKNOWN_CURRENCY_GAP = (
    "every waterway reads unknown, not a threat level: how current the NGA "
    "feed is cannot be checked (feed_stale null), so naval_warnings is not "
    "scored as today's activity"
)
_HEALTHY_POSTURE = {"theaters": {"european": {"count": 4, "source": "opensky"}}}


@respx.mock
@pytest.mark.asyncio
async def test_undated_feed_reads_unknown_not_critical(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: three warnings near Hormuz with no parseable issue date
    read "critical" with status_as_of null, and the fleet scored them as
    current with readiness_lower_bound False."""
    _mock_nga(_hormuz_naval("SOMETIME IN OCTOBER"))

    snapshot = await intelligence.fetch_vessel_snapshot(fetcher)

    hormuz = _by_name(snapshot)["Strait of Hormuz"]
    assert hormuz["status"] == "unknown"
    assert hormuz["naval_warnings"] == 3
    assert hormuz["status_as_of"] is None
    assert {ww["status"] for ww in snapshot["waterways"]} == {"unknown"}
    assert snapshot["feed_stale"] is None
    assert snapshot["degraded"] is True
    assert _UNKNOWN_CURRENCY_GAP in snapshot["data_gaps"]
    assert "error" not in snapshot

    html = _render_maritime(snapshot)
    assert "UNDATED" in html
    assert "STALE" not in html

    monkeypatch.setattr(military, "fetch_theater_posture", _returning(_HEALTHY_POSTURE))
    monkeypatch.setattr(intelligence, "fetch_military_surge", _returning({"surges": []}))
    fleet = await fetch_fleet_report(fetcher)

    assert fleet["unavailable_components"] == ["vessel_snapshot"]
    assert fleet["readiness_lower_bound"] is True
    assert fleet["data_gaps"][0] == (
        "vessel_snapshot: how current its NGA feed is cannot be checked "
        "(feed_stale null), so its waterway statuses are not scored"
    )
    assert f"vessel_snapshot: {_UNKNOWN_CURRENCY_GAP}" in fleet["data_gaps"]


@respx.mock
@pytest.mark.asyncio
async def test_current_feed_is_scored_by_the_fleet(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Control: a feed dated today keeps its threat levels and its score."""
    _mock_nga(_hormuz_naval(_stamp(datetime.now(timezone.utc) - timedelta(hours=2))))

    snapshot = await intelligence.fetch_vessel_snapshot(fetcher)

    assert _by_name(snapshot)["Strait of Hormuz"]["status"] == "critical"
    assert snapshot["feed_stale"] is False
    assert "UNDATED" not in _render_maritime(snapshot)

    monkeypatch.setattr(military, "fetch_theater_posture", _returning(_HEALTHY_POSTURE))
    monkeypatch.setattr(intelligence, "fetch_military_surge", _returning({"surges": []}))
    fleet = await fetch_fleet_report(fetcher)

    assert "unavailable_components" not in fleet
    assert fleet["readiness_lower_bound"] is False


# ---------------------------------------------------------------------------
# Behavior F4 / tests F6: an empty NGA answer is not a quiet sea
# ---------------------------------------------------------------------------


_EMPTY_GAP = (
    "NGA returned no active warnings (the feed normally carries hundreds), so a "
    "quiet sea cannot be told from a feed fault and how current the list is "
    "cannot be checked"
)


@pytest.mark.parametrize(
    "records",
    [[], [_record(9, "P", "BUOY UNLIT.\n", "011600Z MAY 2024", cancel="031600Z MAY 2024")]],
    ids=["empty", "only_cancelled"],
)
@respx.mock
@pytest.mark.asyncio
async def test_empty_nga_answer_is_degraded_and_reads_unknown(
    fetcher: Fetcher, records: list[dict]
) -> None:
    _mock_nga(records)

    nav = await maritime.fetch_nav_warnings(fetcher)

    assert nav["count"] == 0
    assert nav["degraded"] is True
    assert nav["feed_stale"] is None
    assert nav["data_gaps"] == [_EMPTY_GAP]
    assert "error" not in nav

    snapshot = await intelligence.fetch_vessel_snapshot(fetcher)

    assert {ww["status"] for ww in snapshot["waterways"]} == {"unknown"}
    assert snapshot["degraded"] is True
    assert snapshot["data_gaps"] == [f"NGA feed: {_EMPTY_GAP}", _UNKNOWN_CURRENCY_GAP]


@respx.mock
@pytest.mark.asyncio
async def test_empty_nga_answer_is_reused_only_briefly(fetcher: Fetcher) -> None:
    route = _mock_nga([])
    key = "maritime:warnings:all"

    await maritime.fetch_nav_warnings(fetcher)
    assert route.call_count == 1
    assert fetcher.cache.get(key) == {"broadcast-warn": []}

    # Within the reuse window the cached answer is served again.
    await maritime.fetch_nav_warnings(fetcher)
    assert route.call_count == 1
    assert fetcher.cache.get(key) == {"broadcast-warn": []}

    # Past it (the row still has most of its hour), the row is dropped and
    # the next call fetches again.
    conn = fetcher.cache._get_conn()
    conn.execute(
        "UPDATE cache SET created_at = ? WHERE key = ?",
        (time.time() - maritime._EMPTY_ANSWER_REUSE_SECONDS - 1, key),
    )
    conn.commit()
    await maritime.fetch_nav_warnings(fetcher)
    assert route.call_count == 1
    assert fetcher.cache.get_stale_meta(key) is None
    await maritime.fetch_nav_warnings(fetcher)
    assert route.call_count == 2


@respx.mock
@pytest.mark.asyncio
async def test_a_full_nga_answer_keeps_its_hour(fetcher: Fetcher) -> None:
    route = _mock_nga(_hormuz_naval(_stamp(datetime.now(timezone.utc) - timedelta(hours=2))))
    key = "maritime:warnings:all"
    await maritime.fetch_nav_warnings(fetcher)
    conn = fetcher.cache._get_conn()
    conn.execute(
        "UPDATE cache SET created_at = ? WHERE key = ?",
        (time.time() - maritime._EMPTY_ANSWER_REUSE_SECONDS - 1, key),
    )
    conn.commit()

    result = await maritime.fetch_nav_warnings(fetcher)

    assert result["count"] == 3
    assert fetcher.cache.get(key) is not None
    assert route.call_count == 1


# ---------------------------------------------------------------------------
# Behavior F8 / tests F2-F5: parser guards
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "BUOY 20-03-44-10N 072-58.00W.",   # four-part latitude (lookbehind)
        "BUOY 20-03-44-5N 072-58.00W.",    # four-part latitude (lookbehind)
        "BUOY 2O-03-44N 072-58.00W.",      # letter O for the degrees' zero
        "BUOY 20-O3-44N 072-58.00W.",      # letter O inside the minutes
        "CHART 26060-03.5N 072-58.00W.",   # starts inside a chart number
        "BUOY 20-034-44N 072-58.00W.",     # three-digit minutes
        "BUOY 20-03.5-44N 072-58.00W.",    # decimal minutes followed by seconds
        "BUOY 20-03.5-10N 072-58.00W.",    # decimal minutes followed by seconds
        "X 10-15.25-30S 120-05-07E.",      # decimal minutes followed by seconds
    ],
)
def test_malformed_coordinates_read_as_no_position(text: str) -> None:
    """A malformed coordinate used to be read as a confident position about
    1,800 km off (20-03-44-10N as 3.74N), or made the whole feed raise
    (decimal minutes followed by seconds)."""
    assert maritime.extract_positions(text) == []


def test_well_formed_mixed_pair_still_reads() -> None:
    assert maritime.extract_positions("BUOY 20-03-44N 072-58.00W.") == [(20.0622, -72.9667)]


@pytest.mark.parametrize(
    "text",
    ["SECTOR 1234-56N.", "CHART 26060-03N.", "LIGHT 1234-56E REPORTED."],
)
def test_a_number_inside_a_longer_number_is_not_a_coordinate(text: str) -> None:
    assert not maritime.mentions_position(text)


@pytest.mark.parametrize(
    "text",
    [
        "NAVAL OPERATIONS WEST OF 080-00W.",
        # Live 4:2024-273's letter-O typo leaves only the longitude readable.
        "CABLE OPERATIONS IN AREA BOUND BY 22-20.OON 069-39.00W.",
        # Mistyped: no point is read, but a position is stated.
        "BUOY 20-O3-44N.",
    ],
)
def test_a_lone_or_mistyped_coordinate_counts_as_a_stated_position(text: str) -> None:
    assert maritime.mentions_position(text)


@respx.mock
@pytest.mark.asyncio
async def test_meridian_only_naval_warning_is_unplaced_as_unreadable(fetcher: Fetcher) -> None:
    _mock_nga([
        _record(
            7, "4", "CARIBBEAN SEA.\nNAVAL OPERATIONS WEST OF 080-00W.\n",
            _stamp(datetime.now(timezone.utc) - timedelta(hours=2)),
        )
    ])

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert [
        (w["navarea"], w["id"], w["reason"]) for w in result["unplaced_naval_warnings"]
    ] == [("4", "2026-7", "position_unreadable")]
