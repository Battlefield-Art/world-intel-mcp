"""Tests for sources/maritime.py — respx-mocked NGA MSI broadcast warnings.

The _LIVE_* records further down are verbatim from the live API
(GET https://msi.nga.mil/api/publications/broadcast-warn?output=json,
captured 2026-10-17T16:43:08Z: 386 active warnings, the newest issued
10 May 2024). The live API sends navArea as an area code ("4", "12", "A",
"P", "C"), never a Roman numeral, and issueDate as "DDHHMMZ MON YYYY".

Gaps / not covered: live NGA payload variants beyond the three shapes the
module handles (list, dict-with-key, single dict); seconds notation in the
per-warning lat/lon fields (_extract_coords still reads degrees-minutes
only; extract_positions reads both); the 14-day staleness threshold is
derived from 2024 issue gaps only (the live API holds nothing newer); a
stale cache serve of a list payload, which cannot carry the fetcher's
_stale marker.
"""

import asyncio as asyncio_mod
import re
from datetime import datetime, timedelta, timezone

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import maritime as maritime_mod
from world_intel_mcp.sources.maritime import (
    NAVAREAS,
    _extract_coords,
    _is_active,
    _parse_warning,
    fetch_nav_warnings,
)

_W_GULF = {
    "msgYear": 2026,
    "msgNumber": 100,
    "navArea": "IV",
    "subregion": "24",
    "status": "in force",
    "issueDate": "2026-08-30",
    "text": (
        "GULF OF MEXICO. HAZARDOUS OPERATIONS 22-16.65N 097-44.48W. "
        "WIDE BERTH REQUESTED."
    ),
    "authority": "NGA",
}
_W_CANCELLED = {
    "msgYear": 2026,
    "msgNumber": 90,
    "navArea": "IV",
    "issueDate": "2026-07-01",
    "cancelDate": "2026-08-01",
    "text": "CANCELLED WARNING.",
}
_W_PACIFIC = {
    "msgYear": 2026,
    "msgNumber": 200,
    "navArea": "XII",
    "issueDate": "2026-08-31",
    "text": "ROCKET LAUNCHING HAZARD AREA.",  # no coordinates
}
_W_LONG = {
    "msgYear": 2026,
    "msgNumber": 300,
    "navArea": "IV",
    "issueDate": "2026-08-29",
    "text": "A" * 600,
}


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_list_payload(fetcher: Fetcher) -> None:
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(
            200, json=[_W_GULF, _W_CANCELLED, _W_PACIFIC, _W_LONG]
        )
    )

    result = await fetch_nav_warnings(fetcher)

    assert result["source"] == "nga-msi"
    # Cancelled warning excluded.
    assert result["count"] == 3
    ids = [w["id"] for w in result["warnings"]]
    assert "2026-90" not in ids
    # Sorted by issue_date descending: Aug 31, Aug 30, Aug 29.
    assert ids == ["2026-200", "2026-100", "2026-300"]

    gulf = next(w for w in result["warnings"] if w["id"] == "2026-100")
    assert gulf["navarea"] == "IV"
    assert gulf["subregion"] == "24"
    assert gulf["status"] == "active"
    assert gulf["cancel_date"] is None
    # 22 + 16.65/60 = 22.2775 N; 97 + 44.48/60 = 97.7413 W (negative).
    assert gulf["lat"] == 22.2775
    assert gulf["lon"] == -97.7413

    pacific = next(w for w in result["warnings"] if w["id"] == "2026-200")
    assert pacific["lat"] is None
    assert pacific["lon"] is None

    long_w = next(w for w in result["warnings"] if w["id"] == "2026-300")
    assert len(long_w["text"]) == 503  # 500 chars + "..."
    assert long_w["text"].endswith("...")

    assert result["by_navarea"] == {"XII": 1, "IV": 2}
    assert result["navareas"] == NAVAREAS


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_navarea_filter_case_insensitive(
    fetcher: Fetcher,
) -> None:
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json=[_W_GULF, _W_PACIFIC])
    )

    result = await fetch_nav_warnings(fetcher, navarea="xii")
    assert result["count"] == 1
    assert result["warnings"][0]["id"] == "2026-200"
    assert result["by_navarea"] == {"XII": 1}


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_dict_payload_single_warning(
    fetcher: Fetcher,
) -> None:
    # The API sometimes wraps a single warning dict under "broadcast-warn".
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json={"broadcast-warn": _W_GULF})
    )

    result = await fetch_nav_warnings(fetcher)
    assert result["count"] == 1
    assert result["warnings"][0]["id"] == "2026-100"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_api_down(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression: an NGA outage returned warnings=[] with no error key,
    shape-identical to calm seas. The empty lists stay; error says the
    feed is down."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)

    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(return_value=httpx.Response(500))

    result = await fetch_nav_warnings(fetcher)
    assert result["warnings"] == []
    assert result["count"] == 0
    assert result["by_navarea"] == {}
    assert result["error"] == "NGA MSI API unavailable (no live or cached data)"


def test_extract_coords() -> None:
    assert _extract_coords("22-16.65N 097-44.48W") == (22.2775, -97.7413)
    # Southern/eastern hemisphere signs.
    assert _extract_coords("05-30.0S 120-15.0E") == (-5.5, 120.25)
    assert _extract_coords("no coordinates here") == (None, None)
    assert _extract_coords("") == (None, None)


def test_is_active_and_parse_warning() -> None:
    assert _is_active({"cancelDate": None}) is True
    assert _is_active({"cancelDate": ""}) is True
    assert _is_active({"cancelDate": "2026-08-01"}) is False

    parsed = _parse_warning(_W_CANCELLED)
    assert parsed["status"] == "cancelled"
    assert parsed["cancel_date"] == "2026-08-01"

    # Missing msgYear/msgNumber leaves id as None.
    assert _parse_warning({"text": "x"})["id"] is None


# ---------------------------------------------------------------------------
# Data honesty: outage markers, frozen-feed staleness, NGA area codes
# ---------------------------------------------------------------------------

# Verbatim live records (see the module docstring for the capture).
_LIVE_P_PERSIAN_GULF = {
    "msgYear": 2022, "msgNumber": 3371, "navArea": "P", "subregion": "62",
    "text": (
        "PERSIAN GULF.\nU.A.E.\nDNC 10.\nODAS BUOYS, Q Y, ESTABLISHED IN:\n"
        "A. 25-59.00N 056-02.40E.\nB. 25-52.22N 055-51.20E.\n"
    ),
    "status": "A", "issueDate": "251705Z NOV 2022",
    "authority": "NAVAREA IX 483/22 251524Z NOV 22.", "cancelDate": None,
    "cancelNavArea": None, "cancelMsgYear": None, "cancelMsgNumber": None,
    "number": 3371, "year": 2022, "area": "P",
}
_LIVE_A_BLACK_SEA_MINES = {
    "msgYear": 2022, "msgNumber": 789, "navArea": "A", "subregion": "55",
    "text": (
        "BLACK SEA.\nDNC 10.\nMINES ADRIFT REPORTED IN NORTHWESTERN, WESTERN\n"
        "AND SOUTHWESTERN BLACK SEA. VESSELS IN VICINITY\n"
        "ARE ADVISED TO EXERCISE EXTREME CAUTION.\n"
    ),
    "status": "A", "issueDate": "181218Z MAR 2022",
    "authority": "NAVAREA III 118/22 181124Z MAR 22.", "cancelDate": None,
    "cancelNavArea": None, "cancelMsgYear": None, "cancelMsgNumber": None,
    "number": 789, "year": 2022, "area": "A",
}
_LIVE_P_TAIWAN_CABLE = {
    "msgYear": 2024, "msgNumber": 1561, "navArea": "P", "subregion": "94",
    "text": (
        "PHILIPPINE SEA.\nTAIWAN.\nDNC 23.\n"
        "CABLE OPERATIONS 100900Z MAY UNTIL FURTHER NOTICE\n"
        "BY CABLESHIP PETER FABER IN AREA BOUND BY\n"
        "24-51.27N 122-05.38E, 24-50.57N 122-03.14E,\n"
        "24-48.00N 122-03.97E, 24-48.13N 122-06.33E,\n"
        "24-48.52N 122-06.30E.\nWIDE BERTH REQUESTED.\n"
    ),
    "status": "A", "issueDate": "100559Z MAY 2024",
    "authority": "NAVAREA XI 223/24 100544Z MAY 24.", "cancelDate": None,
    "cancelNavArea": None, "cancelMsgYear": None, "cancelMsgNumber": None,
    "number": 1561, "year": 2024, "area": "P",
}
# The newest active warning in the capture.
_LIVE_P_BASS_STRAIT_NEWEST = {
    "msgYear": 2024, "msgNumber": 1571, "navArea": "P", "subregion": "75",
    "text": (
        "BASS STRAIT.\nAUSTRALIA.\nDNC 05.\n1. UNDERWATER OPERATIONS IN PROGRESS\n"
        "   UNTIL FURTHER NOTICE BY M/V INVESTIGATOR\n   IN AREA BOUND BY\n"
        "   37-57.00S 148-13.00E, 38-54.00S 148-13.00E,\n"
        "   38-54.00S 149-03.00E, 37-57.00S 149-03.00E.\n"
        "   ONE MILE BERTH REQUESTED.\n2. CANCEL HYDROLANT 1564/24.\n"
    ),
    "status": "A", "issueDate": "101515Z MAY 2024",
    "authority": "AUSCOAST 154/24 101452Z MAY 24.", "cancelDate": None,
    "cancelNavArea": None, "cancelMsgYear": None, "cancelMsgNumber": None,
    "number": 1571, "year": 2024, "area": "P",
}
_LIVE_4_HAITI_DMS = {
    "msgYear": 2021, "msgNumber": 1157, "navArea": "4", "subregion": "26",
    "text": (
        "WESTERN NORTH ATLANTIC.\nHAITI.\nCHART 26141.\n"
        "ILE DE LA TORTUE W. POINT LIGHT\n20-03-44N 072-58-02W UNLIT.\n"
    ),
    "status": "A", "issueDate": "281021Z DEC 2021",
    "authority": "HAITI 5/21 280205Z DEC 21.", "cancelDate": None,
    "cancelNavArea": None, "cancelMsgYear": None, "cancelMsgNumber": None,
    "number": 1157, "year": 2021, "area": "4",
}
# GET https://msi.nga.mil/api/publications/broadcast-warn/navareas, same day.
_LIVE_NAVAREAS_ENDPOINT = {
    "NAVAREA IV": "4", "HYDROLANT": "A", "NAVAREA XII": "12",
    "HYDROPAC": "P", "HYDROARC": "C",
}

_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
           "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def _nga_issue_date(dt: datetime) -> str:
    """Format a datetime the way the live API writes issueDate."""
    return f"{dt:%d%H%M}Z {_MONTHS[dt.month - 1]} {dt.year}"


def _days_since(dt: datetime) -> float:
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400


async def _no_sleep(*args, **kwargs) -> None:
    return None


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_outage_is_marked_not_quiet(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Regression (silent-empty class): an NGA outage returned warnings=[]
    with no marker, shape-identical to a sea with no active warnings. The
    zeroed fields stay; error / degraded / reason / data_gaps say why."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(return_value=httpx.Response(500))

    result = await fetch_nav_warnings(fetcher)

    assert result["error"] == "NGA MSI API unavailable (no live or cached data)"
    assert result["degraded"] is True
    assert result["reason"] == "nga_msi_fetch_failed"
    assert result["data_gaps"] == [
        "NGA MSI broadcast warnings unavailable (no live or cached data): "
        "zero warnings here means unknown, not a quiet sea"
    ]
    assert result["warnings"] == []
    assert result["count"] == 0
    assert result["newest_issue_date"] is None


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_unrecognized_payload_is_an_error(
    fetcher: Fetcher,
) -> None:
    """A 200 whose JSON holds no warnings list used to read as zero
    warnings; an empty list is a real (if unlikely) answer and stays one."""
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json={"message": "Service Unavailable"})
    )

    result = await fetch_nav_warnings(fetcher)

    assert result["error"] == "NGA MSI returned an unrecognized payload (keys: ['message'])"
    assert result["degraded"] is True
    assert result["reason"] == "nga_msi_unexpected_payload"
    assert result["count"] == 0
    assert len(result["data_gaps"]) == 1


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_empty_list_is_not_an_outage(fetcher: Fetcher) -> None:
    """An empty answer is not an outage (no error), but it is degraded: the
    feed carried 386 active warnings on 2026-10-17, so an empty one cannot
    be told from a feed fault, and how current it is cannot be checked
    (feed_stale None)."""
    # The live API answers msgYear=2026 with exactly this.
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json={"broadcast-warn": []})
    )

    result = await fetch_nav_warnings(fetcher)

    assert "error" not in result
    assert result["degraded"] is True
    assert result["count"] == 0
    assert result["data_gaps"] == [
        "NGA returned no active warnings (the feed normally carries hundreds), "
        "so a quiet sea cannot be told from a feed fault and how current the "
        "list is cannot be checked"
    ]
    assert result["feed_stale"] is None
    assert result["newest_issue_date"] is None
    assert result["newest_issue_age_days"] is None


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_stale_cache_serve_is_degraded(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A failed fetch answered from the expired cache is named as such."""
    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    fresh = datetime.now(timezone.utc) - timedelta(hours=2)
    record = dict(_LIVE_P_PERSIAN_GULF, issueDate=_nga_issue_date(fresh))
    fetcher.cache.set(
        "maritime:warnings:all", {"broadcast-warn": [record]}, ttl_seconds=-10
    )
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(return_value=httpx.Response(500))

    result = await fetch_nav_warnings(fetcher)

    assert "error" not in result
    assert result["degraded"] is True
    assert result["count"] == 1
    assert result["warnings"][0]["id"] == "2022-3371"
    assert len(result["data_gaps"]) == 1
    assert re.fullmatch(
        r"NGA MSI fetch failed; serving a cached copy \d+(\.\d+)? s old",
        result["data_gaps"][0],
    )


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_frozen_feed_names_its_age(fetcher: Fetcher) -> None:
    """Measured 2026-10-17: the live API's newest active warning was issued
    10 May 2024, yet the response carried nothing to say so. Live-shaped
    records: the age of the newest issue date is reported and, past
    STALE_AFTER_DAYS, flagged in data_gaps."""
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(
            200,
            json={
                "broadcast-warn": [
                    _LIVE_P_PERSIAN_GULF,
                    _LIVE_A_BLACK_SEA_MINES,
                    _LIVE_P_BASS_STRAIT_NEWEST,
                    _LIVE_P_TAIWAN_CABLE,
                ]
            },
        )
    )

    result = await fetch_nav_warnings(fetcher)

    expected_age = _days_since(datetime(2024, 5, 10, 15, 15, tzinfo=timezone.utc))
    assert result["newest_issue_date"] == "2024-05-10T15:15:00Z"
    assert result["newest_issue_age_days"] == pytest.approx(expected_age, abs=0.1)
    assert result["stale_after_days"] == 14
    assert result["degraded"] is True
    assert "error" not in result
    age_text = f"{result['newest_issue_age_days']:.0f}"
    assert result["data_gaps"] == [
        f"newest active warning was issued 2024-05-10T15:15:00Z, {age_text} days "
        "ago, past the 14-day staleness threshold: these are historical "
        "warnings, not current conditions"
    ]
    # The live area codes pass through as the API sends them.
    assert result["by_navarea"] == {"P": 3, "A": 1}
    assert result["area_codes"] == maritime_mod.area_codes_view()


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_fresh_feed_is_not_flagged(fetcher: Fetcher) -> None:
    issued = datetime.now(timezone.utc).replace(second=0, microsecond=0) - timedelta(hours=2)
    record = dict(_LIVE_P_PERSIAN_GULF, issueDate=_nga_issue_date(issued))
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json={"broadcast-warn": [record]})
    )

    result = await fetch_nav_warnings(fetcher)

    assert result["newest_issue_date"] == issued.strftime("%Y-%m-%dT%H:%M:%SZ")
    assert result["newest_issue_age_days"] == pytest.approx(_days_since(issued), abs=0.1)
    assert result["data_gaps"] == []
    assert "degraded" not in result


@respx.mock
@pytest.mark.asyncio
async def test_fetch_nav_warnings_unparseable_dates_are_flagged(fetcher: Fetcher) -> None:
    records = [
        dict(_LIVE_P_PERSIAN_GULF, issueDate=None),
        dict(_LIVE_P_TAIWAN_CABLE, issueDate="SOMETIME IN MAY"),
    ]
    respx.get(url__regex=r".*msi\.nga\.mil.*").mock(
        return_value=httpx.Response(200, json={"broadcast-warn": records})
    )

    result = await fetch_nav_warnings(fetcher)

    assert result["count"] == 2
    assert result["newest_issue_date"] is None
    assert result["degraded"] is True
    assert result["data_gaps"] == [
        "no warning carries a parseable issue date, so how current this list "
        "is cannot be checked"
    ]


def test_parse_issue_date() -> None:
    parse = maritime_mod._parse_issue_date
    assert parse("101515Z MAY 2024") == datetime(2024, 5, 10, 15, 15, tzinfo=timezone.utc)
    assert parse("281021z dec 2021") == datetime(2021, 12, 28, 10, 21, tzinfo=timezone.utc)
    # The older fixtures above use ISO dates; those still parse, as UTC.
    assert parse("2026-08-30") == datetime(2026, 8, 30, tzinfo=timezone.utc)
    assert parse("2026-08-30T12:00:00+02:00") == datetime(
        2026, 8, 30, 10, 0, tzinfo=timezone.utc
    )
    assert parse("321515Z MAY 2024") is None  # day 32
    assert parse("101515Z MAI 2024") is None  # not an English month
    assert parse("SOMETIME IN MAY") is None
    assert parse("") is None
    assert parse(None) is None


def test_extract_positions_reads_dm_and_dms_in_text_order() -> None:
    assert maritime_mod.extract_positions(_LIVE_P_TAIWAN_CABLE["text"]) == [
        (24.8545, 122.0897),
        (24.8428, 122.0523),
        (24.8, 122.0662),
        (24.8022, 122.1055),
        (24.8087, 122.105),
    ]
    # 20 + 3/60 + 44/3600 = 20.0622 N; 72 + 58/60 + 2/3600 = 72.9672 W.
    assert maritime_mod.extract_positions(_LIVE_4_HAITI_DMS["text"]) == [
        (20.0622, -72.9672)
    ]
    assert _extract_coords(_LIVE_4_HAITI_DMS["text"]) == (None, None)
    assert maritime_mod.extract_positions(
        "A. 05-30.0S 120-15.0E. B. 13-55-18N 090-47-53W."
    ) == [(-5.5, 120.25), (13.9217, -90.7981)]
    # Live HYDROPAC 2023-243 gives '154-84.0W': 84 minutes is malformed.
    assert maritime_mod.extract_positions("IN VICINITY 10-52.0N 154-84.0W") == []
    assert maritime_mod.extract_positions("95-00.0N 010-00.0E") == []
    assert maritime_mod.extract_positions(_LIVE_A_BLACK_SEA_MINES["text"]) == []


def test_mentions_position_sees_malformed_pairs() -> None:
    assert maritime_mod.mentions_position("IN VICINITY 10-52.0N 154-84.0W") is True
    assert maritime_mod.mentions_position(_LIVE_4_HAITI_DMS["text"]) is True
    assert maritime_mod.mentions_position(_LIVE_P_TAIWAN_CABLE["text"]) is True
    assert maritime_mod.mentions_position(_LIVE_A_BLACK_SEA_MINES["text"]) is False


def test_nga_area_codes_cover_the_live_code_table() -> None:
    codes = maritime_mod.NGA_AREA_CODES
    # Exactly the codes the live navareas endpoint lists, under its names.
    assert set(codes) == set(_LIVE_NAVAREAS_ENDPOINT.values())
    for series, code in _LIVE_NAVAREAS_ENDPOINT.items():
        assert codes[code]["series"] == series

    assert codes["4"]["navareas"] == ("IV",)
    assert codes["12"]["navareas"] == ("XII",)
    assert [c for c in codes if codes[c]["single_navarea"]] == ["12"]
    assert codes["C"]["unlisted_navareas"] == ("XVII", "XVIII", "XIX", "XX", "XXI")

    listed: set[str] = set()
    for entry in codes.values():
        assert set(entry["navareas"]) <= set(NAVAREAS)
        assert not set(entry["unlisted_navareas"]) & set(NAVAREAS)
        listed |= set(entry["navareas"])
    assert listed == set(NAVAREAS)

    # The coordinator a live warning names in its authority field belongs
    # to an area its code maps to.
    for record in (_LIVE_P_PERSIAN_GULF, _LIVE_A_BLACK_SEA_MINES, _LIVE_P_TAIWAN_CABLE):
        area = re.match(r"NAVAREA ([IVX]+) ", record["authority"]).group(1)
        entry = codes[record["navArea"]]
        assert area in entry["navareas"] + entry["unlisted_navareas"]

    view = maritime_mod.area_codes_view()
    assert view["A"] == {
        "series": "HYDROLANT",
        "navareas": ["I", "II", "III", "IV", "V", "VI", "VII", "XV"],
        "unlisted_navareas": [],
        "single_navarea": False,
    }
