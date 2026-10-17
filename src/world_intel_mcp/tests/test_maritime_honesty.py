"""Tests for the second maritime-honesty review (2026-10-17): NAVAREA
credits (H1), the frozen feed (M2), the navarea filter (M3), positions and
dates (L1-L4), the report and CLI surfaces, and the mutants that survived
the first round (I2, M5, M6, M9).

NGA records are built in the live API's shape (msgYear, msgNumber,
navArea, text, issueDate, cancelDate); texts marked "live" are verbatim
from the 2026-10-17 capture, the others are constructed. The real
fetch_nav_warnings and fetch_vessel_snapshot run against a respx-mocked
NGA endpoint.

Gaps / not covered: no live NGA call is made here; the 300 km radius is a
chosen value tested at its boundary, not validated against chokepoint
geometry; positions written with a space between degrees and minutes
('25 30.00N') are not read, and no test claims they are; the dashboard and
collector, which also read these outputs, are not exercised.
"""

import asyncio as asyncio_mod
import math
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx
from click.testing import CliRunner

from world_intel_mcp import cli
from world_intel_mcp.config.countries import STRATEGIC_WATERWAYS
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.reports import _render_maritime
from world_intel_mcp.sources import intelligence, maritime
from world_intel_mcp.sources.intelligence import _haversine_km

_MONTHS = "JAN FEB MAR APR MAY JUN JUL AUG SEP OCT NOV DEC".split()
_NGA_URL = r".*msi\.nga\.mil.*"


def _stamp(when: datetime) -> str:
    """An NGA issue date ('101515Z MAY 2024')."""
    return f"{when:%d%H%M}Z {_MONTHS[when.month - 1]} {when.year}"


def _ago(**delta: float) -> str:
    return _stamp(datetime.now(timezone.utc) - timedelta(**delta))


def _iso(stamp: str) -> str:
    return maritime._parse_issue_date(stamp).strftime("%Y-%m-%dT%H:%M:%SZ")


def _record(year: int, number: int, area: str, text: str, issued: str) -> dict:
    return {
        "msgYear": year, "msgNumber": number, "navArea": area,
        "subregion": "00", "text": text, "status": "A",
        "issueDate": issued, "authority": "NAVSAFETY", "cancelDate": None,
    }


def _mock_nga(records: list[dict]) -> None:
    respx.get(url__regex=_NGA_URL).mock(
        return_value=httpx.Response(200, json={"broadcast-warn": records})
    )


def _by_name(result: dict) -> dict[str, dict]:
    return {ww["name"]: ww for ww in result["waterways"]}


# ---------------------------------------------------------------------------
# H1: a warning that states no position is credited to no waterway
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_positionless_navarea_iv_and_xii_warnings_credit_no_waterway(
    fetcher: Fetcher,
) -> None:
    """Regression (H1): NAVAREA IV and XII both contain the Panama Canal, so
    a warning that stated no position in either was credited to Panama.
    All five such credits in the live capture were wrong (Hawaii, California
    and "warnings in force" lists), and a naval one raised Panama's status.
    Requiring the name would not help: 4:2024-468 says "PANAMA CITY", in
    Florida."""
    issued = _ago(hours=2)
    _mock_nga([
        _record(2026, 1, "4", "WESTERN NORTH ATLANTIC.\nNAVAL OPERATIONS IN PROGRESS.\n", issued),
        _record(2026, 2, "12", "EASTERN NORTH PACIFIC.\nMISSILE FIRING IN PROGRESS.\n", issued),
        _record(
            2026, 3, "4",
            "GULF OF MEXICO.\nFLORIDA.\nROCKET LAUNCHING OPERATIONS\n"
            "VISIBLE FROM:\nC. PENSACOLA.\nD. PANAMA CITY.\n",
            issued,
        ),
    ])

    result = await intelligence.fetch_vessel_snapshot(fetcher)
    panama = _by_name(result)["Panama Canal"]

    assert panama["status"] == "clear"
    assert panama["naval_warnings"] == 0
    assert panama["total_nearby"] == 0
    assert panama["nearby_by_match"] == {"proximity": 0}
    assert {ww["total_nearby"] for ww in result["waterways"]} == {0}
    assert [
        (w["navarea"], w["id"], w["reason"]) for w in result["unplaced_naval_warnings"]
    ] == [
        ("4", "2026-1", "multi_navarea_series"),
        ("12", "2026-2", "navarea_too_broad"),
    ]
    assert result["total_naval_warnings"] == 2
    assert "waterway_navareas" not in result["matching_rule"]
    assert result["data_gaps"] == [
        "2 naval warning(s) state no readable position, so no waterway is "
        "credited with them (unplaced_naval_warnings): "
        "4:2026-1 (multi_navarea_series), 12:2026-2 (navarea_too_broad)"
    ]


@respx.mock
@pytest.mark.asyncio
async def test_a_stated_position_still_credits_panama(fetcher: Fetcher) -> None:
    """The positions rule is unchanged: live 4:2024-15 (BUOY NO1, 40.8 km
    from the canal) still counts there, by proximity only."""
    _mock_nga([
        _record(
            2024, 15, "4",
            "WESTERN CARIBBEAN SEA.\nPANAMA.\nCHART 26060.\n"
            "BUOY NO1 09-25.09N 079-49.50W DESTROYED.\n",
            _ago(hours=2),
        )
    ])

    panama = _by_name(await intelligence.fetch_vessel_snapshot(fetcher))["Panama Canal"]

    assert panama["total_nearby"] == 1
    assert panama["nearby_by_match"] == {"proximity": 1}
    assert panama["naval_by_match"] == {"proximity": 0}


def test_area_code_4_carries_more_than_navarea_iv() -> None:
    """Regression (H1): code 4 was marked single-NAVAREA, but live
    4:2023-808 "HUDSON BAY. CANADA." names NAVAREA XVIII as its authority."""
    entry = maritime.NGA_AREA_CODES["4"]
    assert entry["single_navarea"] is False
    assert entry["navareas"] == ("IV",)
    assert entry["unlisted_navareas"] == ("XVIII",)
    assert maritime.area_codes_view()["4"]["single_navarea"] is False


# ---------------------------------------------------------------------------
# L4: unplaced warnings are named by area code and id
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_unplaced_gap_names_each_warning_by_area_code_and_id(
    fetcher: Fetcher,
) -> None:
    """Regression (L4): NGA numbers each series separately, so live id
    2024-12 is both a NAVAREA IV and a HYDROPAC warning; the gap text named
    them by id alone. The id field itself keeps its format."""
    issued = _ago(hours=2)
    _mock_nga([
        _record(2024, 12, "4", "WESTERN NORTH ATLANTIC.\nNAVAL OPERATIONS.\n", issued),
        _record(2024, 12, "P", "NORTH PACIFIC.\nSUBMARINE OPERATIONS.\n", issued),
    ])

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert [w["id"] for w in result["unplaced_naval_warnings"]] == ["2024-12", "2024-12"]
    assert result["data_gaps"][-1].endswith(
        "4:2024-12 (multi_navarea_series), P:2024-12 (multi_navarea_series)"
    )


# ---------------------------------------------------------------------------
# M2: a frozen feed reads stale, per waterway
# ---------------------------------------------------------------------------


def _hormuz_naval_records(issued: str) -> list[dict]:
    # 11.4, 140.6 and 98.0 km from the Strait of Hormuz.
    return [
        _record(2026, 1, "P", "PERSIAN GULF.\nNAVAL EXERCISE IN 26-30.00N 056-10.00E.\n", issued),
        _record(2026, 2, "P", "GULF OF OMAN.\nLIVE FIRING 25-30.00N 057-00.00E.\n", issued),
        _record(2026, 3, "P", "PERSIAN GULF.\nSUBMARINE OPERATIONS 26-00.00N 055-30.00E.\n", issued),
    ]


@respx.mock
@pytest.mark.asyncio
async def test_frozen_feed_marks_every_waterway_stale_with_its_date(
    fetcher: Fetcher,
) -> None:
    """Regression (M2): with the feed past its staleness threshold, three
    naval warnings near Hormuz still read "critical", as if current; only
    the top-level degraded flag said otherwise. The live feed's newest
    warning dates from 10 May 2024."""
    issued = _ago(days=30)
    _mock_nga(_hormuz_naval_records(issued))

    result = await intelligence.fetch_vessel_snapshot(fetcher)
    hormuz = _by_name(result)["Strait of Hormuz"]

    assert {ww["status"] for ww in result["waterways"]} == {"stale"}
    assert {ww["status_as_of"] for ww in result["waterways"]} == {_iso(issued)}
    assert hormuz["naval_warnings"] == 3
    assert result["feed_stale"] is True
    assert result["degraded"] is True
    assert "error" not in result
    assert result["data_gaps"][-1] == (
        "every waterway reads stale, not a threat level: naval_warnings counts "
        f"warnings from a feed last updated {_iso(issued)} (status_as_of)"
    )


@respx.mock
@pytest.mark.asyncio
async def test_fresh_feed_scores_waterways_and_says_it_is_not_stale(
    fetcher: Fetcher,
) -> None:
    issued = _ago(hours=2)
    _mock_nga(_hormuz_naval_records(issued))

    result = await intelligence.fetch_vessel_snapshot(fetcher)
    hormuz = _by_name(result)["Strait of Hormuz"]

    assert hormuz["status"] == "critical"
    assert hormuz["status_as_of"] == _iso(issued)
    assert result["feed_stale"] is False
    assert "degraded" not in result
    assert result["data_gaps"] == []


@respx.mock
@pytest.mark.asyncio
async def test_feed_stale_is_unknown_when_no_issue_date_parses(fetcher: Fetcher) -> None:
    """feed_stale is the structured form of the staleness gap, and None (not
    False) when no usable issue date says how current the list is."""
    _mock_nga([_record(2026, 1, "A", "BUOY UNLIT.\n", "NOT A DATE")])

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert result["feed_stale"] is None
    assert result["degraded"] is True
    assert {ww["status_as_of"] for ww in result["waterways"]} == {None}


@respx.mock
@pytest.mark.asyncio
async def test_feed_stale_is_unknown_during_an_outage(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=_NGA_URL).mock(return_value=httpx.Response(500))

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert result["feed_stale"] is None
    assert {ww["status"] for ww in result["waterways"]} == {"unknown"}
    assert {ww["status_as_of"] for ww in result["waterways"]} == {None}


# ---------------------------------------------------------------------------
# M3: the navarea filter takes the values it documents
# ---------------------------------------------------------------------------


def _one_per_area(issued: str) -> list[dict]:
    return [
        _record(2026, number, area, f"WARNING IN AREA {area}.\n", issued)
        for number, area in enumerate(("4", "12", "A", "P", "C"), start=1)
    ]


@pytest.mark.parametrize(
    ("value", "code"),
    [
        ("IV", "4"),
        ("4", "4"),
        ("NAVAREA IV", "4"),
        ("xii", "12"),
        ("12", "12"),
        ("HYDROPAC", "P"),
        ("a", "A"),
        ("hydroarc", "C"),
    ],
)
@respx.mock
@pytest.mark.asyncio
async def test_navarea_filter_accepts_codes_numerals_and_series_names(
    fetcher: Fetcher, value: str, code: str
) -> None:
    """Regression (M3): the API sends only the codes 4, 12, A, P and C, so
    the documented numerals matched nothing: navarea="IV" returned count 0
    with no marker on a feed holding 71 code-4 warnings."""
    _mock_nga(_one_per_area(_ago(hours=2)))

    result = await maritime.fetch_nav_warnings(fetcher, navarea=value)

    assert [w["navarea"] for w in result["warnings"]] == [code]
    assert result["count"] == 1
    assert result["navarea_filter"] == {"requested": value, "area_code": code}
    assert "error" not in result


@respx.mock
@pytest.mark.asyncio
async def test_navarea_filter_value_no_area_carries_is_an_error_not_zero(
    fetcher: Fetcher,
) -> None:
    _mock_nga(_one_per_area(_ago(hours=2)))

    ix = await maritime.fetch_nav_warnings(fetcher, navarea="IX")
    assert ix["error"] == (
        "navarea 'IX' matches no area the NGA feed carries; filter by an area "
        "code (4, 12, A, P, C) or by IV or XII"
    )
    assert ix["reason"] == "navarea_not_carried"
    assert ix["data_gaps"] == [
        "NAVAREA IX has no area code of its own in the NGA feed: its warnings "
        "arrive inside series that also carry other NAVAREAs (P (HYDROPAC); "
        "see area_codes)"
    ]
    assert ix["count"] == 0
    assert ix["navarea_filter"] == {"requested": "IX", "area_code": None}

    bogus = await maritime.fetch_nav_warnings(fetcher, navarea="Z")
    assert bogus["reason"] == "navarea_not_carried"
    assert bogus["data_gaps"] == [
        "no NGA area code, series or NAVAREA is called 'Z' (see area_codes)"
    ]


@respx.mock
@pytest.mark.asyncio
async def test_a_carried_area_with_no_active_warnings_is_an_honest_zero(
    fetcher: Fetcher,
) -> None:
    _mock_nga([_record(2026, 1, "A", "WARNING.\n", _ago(hours=2))])

    result = await maritime.fetch_nav_warnings(fetcher, navarea="C")

    assert result["count"] == 0
    assert result["navarea_filter"] == {"requested": "C", "area_code": "C"}
    assert "error" not in result
    assert result["data_gaps"] == []
    assert result["feed_stale"] is False


@respx.mock
@pytest.mark.asyncio
async def test_a_filtered_area_on_a_frozen_feed_is_marked_stale(
    fetcher: Fetcher,
) -> None:
    """Regression (M2, M3): freshness was read from the filtered slice, so
    on a frozen feed an area with nothing in force came back as a count of
    0 with feed_stale null and no gap, and an area's own older warnings
    dated the feed."""
    _mock_nga(
        [
            _record(2024, 1, "A", "WARNING.\n", "101515Z MAY 2024"),
            _record(2024, 2, "P", "WARNING.\n", "011200Z MAR 2024"),
        ]
    )

    arctic = await maritime.fetch_nav_warnings(fetcher, navarea="C")

    assert arctic["count"] == 0
    assert arctic["feed_stale"] is True
    assert arctic["degraded"] is True
    assert arctic["newest_issue_date"] == "2024-05-10T15:15:00Z"
    assert arctic["data_gaps"][0].startswith(
        "newest active warning was issued 2024-05-10T15:15:00Z, "
    )

    pacific = await maritime.fetch_nav_warnings(fetcher, navarea="P")

    assert [w["id"] for w in pacific["warnings"]] == ["2024-2"]
    assert pacific["newest_issue_date"] == "2024-05-10T15:15:00Z"
    assert pacific["feed_stale"] is True


@respx.mock
@pytest.mark.asyncio
async def test_a_filtered_area_on_an_undated_feed_says_its_age_is_unknown(
    fetcher: Fetcher,
) -> None:
    _mock_nga([_record(2024, 1, "A", "WARNING.\n", None)])

    result = await maritime.fetch_nav_warnings(fetcher, navarea="C")

    assert result["count"] == 0
    assert result["feed_stale"] is None
    assert result["degraded"] is True
    assert result["data_gaps"] == [
        "no warning carries a parseable issue date, so how current this "
        "list is cannot be checked"
    ]


def test_nav_warnings_tool_documents_the_area_codes() -> None:
    from world_intel_mcp.tools import infrastructure as infra_tools
    from world_intel_mcp.tools import intelligence as intel_tools

    nav = next(t for t in infra_tools.TOOLS if t.name == "intel_nav_warnings")
    navarea = nav.inputSchema["properties"]["navarea"]["description"]
    assert "4 (NAVAREA IV)" in navarea
    assert "IV and XII are accepted for 4 and 12" in navarea
    assert "I-XVI" not in nav.description
    assert "feed_stale" in nav.description

    snapshot = next(t for t in intel_tools.TOOLS if t.name == "intel_vessel_snapshot")
    for status in ("clear", "advisory", "elevated", "critical", "stale", "unknown"):
        assert status in snapshot.description
    for tool in (nav, snapshot):
        assert "—" not in tool.description


# ---------------------------------------------------------------------------
# L1, L2: positions read in full and in every notation the feed uses
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_positions_after_the_500_character_cut_are_read(fetcher: Fetcher) -> None:
    """Regression (L1): positions were read from the text after its cut at
    500 characters, so a naval warning whose position came later was
    unplaced (or, under NAVAREA IV, credited to Panama)."""
    text = (
        "PERSIAN GULF.\nNAVAL EXERCISE.\n"
        + "VESSELS ARE ADVISED TO KEEP CLEAR OF THE AREA. " * 13
        + "\nIN AREA 26-30.00N 056-10.00E.\n"
    )
    assert text.index("26-30.00N") > maritime._MAX_TEXT_LENGTH
    _mock_nga([_record(2026, 1, "4", text, _ago(hours=2))])

    nav = await maritime.fetch_nav_warnings(fetcher)
    warning = nav["warnings"][0]
    assert warning["text"].endswith("...")
    assert warning["positions"] == [[26.5, 56.1667]]
    assert (warning["lat"], warning["lon"]) == (26.5, 56.1667)
    assert warning["mentions_position"] is True

    result = await intelligence.fetch_vessel_snapshot(fetcher)
    hormuz = _by_name(result)["Strait of Hormuz"]
    assert hormuz["status"] == "advisory"
    assert hormuz["warning_details"][0]["distance_km"] == 11.4
    assert result["unplaced_naval_warnings"] == []
    assert result["data_gaps"] == []


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        # Live HYDROLANT 2024-473, 479 and 488 put a comma between them.
        ("ICE 60-17.40S, 050-16.80W.", [(-60.29, -50.28)]),
        ("AREA 25-30.00N/056-10.00E.", [(25.5, 56.1667)]),
        # Mixed notation used to read the '03-44N' as latitude 3.73.
        ("BUOY 20-03-44N 072-58.00W.", [(20.0622, -72.9667)]),
        ("BUOY 20-03.50N 072-58-30W.", [(20.0583, -72.975)]),
    ],
)
def test_positions_with_comma_slash_or_mixed_notation(text: str, expected: list) -> None:
    """Regression (L2)."""
    assert maritime.extract_positions(text) == expected


def test_a_lone_latitude_or_longitude_counts_as_a_stated_position() -> None:
    """Regression (L2): latitude bands and meridians do state a place, so
    such a warning is unreadable, not positionless."""
    assert maritime.mentions_position("BETWEEN 24-30N AND 25-00N, WEST OF 080-00W.")
    assert maritime.mentions_position("SOUTH OF 65-07.80 S.")
    assert not maritime.mentions_position("NAVAL OPERATIONS IN PROGRESS. CHART 26060.")
    assert maritime.extract_positions("BETWEEN 24-30N AND 25-00N, WEST OF 080-00W.") == []


@respx.mock
@pytest.mark.asyncio
async def test_a_warning_given_as_latitude_bands_is_unplaced_as_unreadable(
    fetcher: Fetcher,
) -> None:
    _mock_nga([
        _record(
            2026, 1, "4",
            "STRAITS OF FLORIDA.\nNAVAL OPERATIONS BETWEEN 24-30N AND 25-00N,\n"
            "WEST OF 080-00W.\n",
            _ago(hours=2),
        )
    ])

    result = await intelligence.fetch_vessel_snapshot(fetcher)

    assert [(w["id"], w["reason"]) for w in result["unplaced_naval_warnings"]] == [
        ("2026-1", "position_unreadable")
    ]
    assert _by_name(result)["Panama Canal"]["status"] == "clear"


# ---------------------------------------------------------------------------
# L3: a future-dated issue date cannot make a frozen feed look fresh
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_future_dated_warning_is_left_out_of_the_freshness_check(
    fetcher: Fetcher,
) -> None:
    """Regression (L3): one "MAY 2042" among old warnings made the feed read
    5,684 days fresh and cleared the staleness flag."""
    old = _ago(days=30)
    _mock_nga([
        _record(2024, 1, "A", "BUOY UNLIT.\n", old),
        _record(2042, 7, "P", "BUOY UNLIT.\n", "101515Z MAY 2042"),
    ])

    result = await maritime.fetch_nav_warnings(fetcher)

    assert result["newest_issue_date"] == _iso(old)
    assert result["feed_stale"] is True
    assert result["degraded"] is True
    assert result["data_gaps"][0] == (
        "1 warning(s) carry an issue date more than 1 day ahead, left out of "
        "the freshness check: P:2042-7 (101515Z MAY 2042)"
    )
    assert "staleness threshold" in result["data_gaps"][1]


@respx.mock
@pytest.mark.asyncio
async def test_only_future_dated_warnings_leave_freshness_unknown(
    fetcher: Fetcher,
) -> None:
    _mock_nga([_record(2042, 7, "P", "BUOY UNLIT.\n", "101515Z MAY 2042")])

    result = await maritime.fetch_nav_warnings(fetcher)

    assert result["newest_issue_date"] is None
    assert result["feed_stale"] is None
    assert result["degraded"] is True
    assert result["data_gaps"][1] == (
        "no warning carries a usable issue date, so how current this list is "
        "cannot be checked"
    )


@respx.mock
@pytest.mark.asyncio
async def test_an_issue_date_within_a_day_ahead_still_counts(fetcher: Fetcher) -> None:
    """Clock skew of hours is tolerated: such a date is not called a typo."""
    _mock_nga([_record(2026, 1, "A", "BUOY UNLIT.\n", _stamp(datetime.now(timezone.utc) + timedelta(hours=3)))])

    result = await maritime.fetch_nav_warnings(fetcher)

    assert result["newest_issue_date"] is not None
    assert result["feed_stale"] is False
    assert result["data_gaps"] == []


# ---------------------------------------------------------------------------
# Report and CLI surfaces
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_report_renders_the_real_vessel_snapshot(fetcher: Fetcher) -> None:
    """Regression: _render_maritime read vessels / snapshot, keys the
    snapshot never emits, so every report said "No maritime data"."""
    issued = _ago(hours=2)
    _mock_nga(_hormuz_naval_records(issued))

    html = _render_maritime(await intelligence.fetch_vessel_snapshot(fetcher))

    assert "No maritime data" not in html
    assert "Strait of Hormuz" in html
    assert '<span class="metric red">critical</span>' in html
    assert "Bosphorus" in html
    assert "STALE" not in html


def test_report_marks_a_stale_snapshot_and_names_an_outage() -> None:
    stale = {
        "waterways": [{"name": "Strait of Hormuz", "status": "stale", "naval_warnings": 3}],
        "feed_stale": True,
        "newest_issue_date": "2024-05-10T15:15:00Z",
        "data_gaps": ["NGA feed: <old>"],
    }
    html = _render_maritime(stale)
    assert "STALE" in html
    assert "2024-05-10T15:15:00Z" in html
    assert "<li>NGA feed: &lt;old&gt;</li>" in html

    down = _render_maritime({"error": "NGA navigational warnings unavailable: x", "waterways": []})
    assert "Maritime data unavailable: NGA navigational warnings unavailable: x" in down


@pytest.fixture
def _cli_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)


def _invoke(*args: str):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def test_cli_warnings_help_names_the_area_codes(_cli_env) -> None:
    result = _invoke("warnings", "--help")
    assert "4, 12, A, P or C" in result.output
    assert "NAVAREA number" not in result.output


@respx.mock
def test_cli_warnings_prints_staleness_and_gaps(_cli_env) -> None:
    issued = _ago(days=30)
    _mock_nga([_record(2026, 1, "4", "BUOY UNLIT.\n", issued)])

    result = _invoke("warnings", "-n", "IV")

    assert result.exit_code == 0
    assert f"STALE: newest warning issued {_iso(issued)}" in result.output
    assert "1 active warnings" in result.output
    assert "Gap: newest active warning was issued" in result.output


@respx.mock
def test_cli_warnings_names_an_area_the_feed_does_not_carry(_cli_env) -> None:
    _mock_nga([_record(2026, 1, "4", "BUOY UNLIT.\n", _ago(hours=2))])

    result = _invoke("warnings", "-n", "IX")

    assert result.exit_code == 0
    assert "Error: navarea 'IX' matches no area the NGA feed carries" in result.output
    assert "Gap: NAVAREA IX has no area code of its own" in result.output
    assert "0 active warnings" not in result.output


# ---------------------------------------------------------------------------
# Mutants that survived the first review round
# ---------------------------------------------------------------------------


@respx.mock
@pytest.mark.asyncio
async def test_a_position_exactly_at_the_radius_is_credited(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Mutant I2 (<= to <) survived: no position sat on the boundary."""
    text = "GULF OF OMAN.\nLIVE FIRING 25-30.00N 057-00.00E.\n"
    hormuz = next(ww for ww in STRATEGIC_WATERWAYS if ww["name"] == "Strait of Hormuz")
    ((lat, lon),) = maritime.extract_positions(text)
    boundary = _haversine_km(hormuz["lat"], hormuz["lon"], lat, lon)
    _mock_nga([_record(2026, 1, "P", text, _ago(hours=2))])

    monkeypatch.setattr(intelligence, "_WATERWAY_PROXIMITY_KM", boundary)
    at = await intelligence.fetch_vessel_snapshot(fetcher)
    assert _by_name(at)["Strait of Hormuz"]["naval_warnings"] == 1

    monkeypatch.setattr(intelligence, "_WATERWAY_PROXIMITY_KM", math.nextafter(boundary, 0.0))
    inside = await intelligence.fetch_vessel_snapshot(fetcher)
    assert _by_name(inside)["Strait of Hormuz"]["naval_warnings"] == 0


def test_dms_fields_of_60_or_more_are_malformed() -> None:
    """Mutant M5 (the DMS >= 60 guard off) survived: no test fed one."""
    assert maritime.extract_positions("BUOY 20-03-75N 072-58-02W MISSING.") == []
    assert maritime.extract_positions("BUOY 20-60-10N 072-58-02W MISSING.") == []
    assert maritime.extract_positions("BUOY 20-03-44N 072-58-60W MISSING.") == []
    assert maritime.extract_positions("BUOY 20-03-44N 072-58-02W MISSING.") == [
        (20.0622, -72.9672)
    ]


def test_positions_come_back_in_text_order_across_notations() -> None:
    """Mutant M6 (text-order sort removed) survived: no input put a DMS
    position before a degrees-minutes one."""
    text = "A. 13-55-18N 090-47-53W.\nB. 05-30.0S 120-15.0E.\n"
    assert maritime.extract_positions(text) == [(13.9217, -90.7981), (-5.5, 120.25)]


@respx.mock
@pytest.mark.asyncio
async def test_a_feed_exactly_at_the_staleness_threshold_is_not_stale(
    fetcher: Fetcher,
) -> None:
    """Mutant M9 (> to >=) survived: no newest warning sat on the threshold."""
    issued = datetime.now(timezone.utc) - timedelta(days=maritime.STALE_AFTER_DAYS)
    _mock_nga([_record(2026, 1, "A", "BUOY UNLIT.\n", _stamp(issued))])

    result = await maritime.fetch_nav_warnings(fetcher)

    assert result["newest_issue_age_days"] == maritime.STALE_AFTER_DAYS
    assert "degraded" not in result
    assert result["data_gaps"] == []
