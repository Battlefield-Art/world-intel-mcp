"""Tests for sources/migration.py (UNHCR corridors, surges, history; World Bank net migration).

respx serves fixtures shaped like the live APIs as probed on 2026-10-11: UNHCR rows
carry both its own codes (coo/coa, where "AUS" is AUSTRIA) and ISO-3 (coo_iso/coa_iso);
World Bank responses are [meta, rows] with aggregates flagged by region id "NA".

Gaps / not covered: live API calls (run through the MCP stdio path by hand, see the
CHANGELOG entry); UNHCR responses spanning more than one page; WB pagination.
"""

from __future__ import annotations

import asyncio as asyncio_mod

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import migration
from world_intel_mcp.sources.displacement import fetch_displacement_summary

_COUNTRIES = {"maxPages": 1, "items": [
    {"iso": "SYR", "iso2": "SY", "name": "Syrian Arab Rep.", "nameShort": "Syrian Arab Rep.",
     "region": "Western Asia", "majorArea": "Asia"},
    {"iso": "TUR", "iso2": "TR", "name": "Türkiye", "region": "Western Asia", "majorArea": "Asia"},
    {"iso": "DEU", "iso2": "DE", "name": "Germany", "region": "Western Europe", "majorArea": "Europe"},
    {"iso": "AUT", "iso2": "AT", "name": "Austria", "region": "Western Europe", "majorArea": "Europe"},
    {"iso": "SDN", "iso2": "SD", "name": "Sudan", "region": "Northern Africa", "majorArea": "Africa"},
    {"iso": "TCD", "iso2": "TD", "name": "Chad", "region": "Middle Africa", "majorArea": "Africa"},
    {"iso": "COG", "iso2": "CG", "name": "Congo", "region": "Middle Africa", "majorArea": "Africa"},
    {"iso": "COD", "iso2": "CD", "name": "Dem. Rep. of the Congo", "region": "Middle Africa",
     "majorArea": "Africa"},
    # Live UNHCR lists these two non-country buckets as if they were countries.
    {"code": "UKN", "iso": "UNK", "name": "Unknown ", "region": None, "majorArea": None},
    {"code": "STA", "iso": "XXA", "name": "Stateless", "region": "Stateless", "majorArea": "Stateless"},
]}


def _row(o, d, refugees=0, asylum=0, oip=0, idps=0, coo=None):
    return {"coo": coo or o, "coo_iso": o, "coo_name": o, "coa_iso": d, "coa_name": d,
            "refugees": refugees, "asylum_seekers": asylum, "oip": oip if oip else "-", "idps": idps}


_PAIRS = {
    2025: [
        _row("SYR", "TUR", refugees=2_000_000),
        _row("SYR", "DEU", refugees=600_000, asylum=60_000),
        _row("SYR", "SYR", idps=5_000_000),
        _row("SDN", "TCD", refugees=1_200_000),
        _row("SDN", "SDN", idps=9_000_000),
        # UNHCR's own code AUS means Austria; only coa_iso/coo_iso are trustworthy.
        _row("AUT", "DEU", asylum=10, coo="AUS"),
        _row("UNK", "DEU", refugees=900_000),
    ],
    2024: [
        _row("SYR", "TUR", refugees=2_900_000),
        _row("SYR", "DEU", refugees=650_000, asylum=60_000),
        _row("SYR", "SYR", idps=7_000_000),
        _row("SDN", "TCD", refugees=400_000),
        _row("SDN", "SDN", idps=11_000_000),
        _row("UNK", "DEU", refugees=100_000),
    ],
}

_HISTORY_SYR = [  # deliberately unordered, as the live API returns it
    {"year": 2013, "refugees": 2_400_000, "asylum_seekers": 0, "idps": 0},
    {"year": 2011, "refugees": 20_000, "asylum_seekers": 0, "idps": 0},
    {"year": 2012, "refugees": 700_000, "asylum_seekers": 29_000, "idps": 0},
    {"year": 2014, "refugees": 3_800_000, "asylum_seekers": 87_000, "idps": 0},
]


def _unhcr(request: httpx.Request) -> httpx.Response:
    path, q = request.url.path, request.url.params
    if path.endswith("/countries/"):
        return httpx.Response(200, json=_COUNTRIES)
    assert q.get("cf_type") == "ISO", "every UNHCR population call must ask for ISO codes"
    if q.get("coo") == "SYR":
        return httpx.Response(200, json={"maxPages": 1, "items": _HISTORY_SYR})
    year = int(q["yearFrom"])
    return httpx.Response(200, json={"maxPages": 1, "items": _PAIRS.get(year, [])})


_WB_COUNTRIES = [{"page": 1, "pages": 1}, [
    {"id": "SYR", "iso2Code": "SY", "name": "Syrian Arab Republic", "region": {"id": "MEA", "value": "Middle East"},
     "incomeLevel": {"value": "Low income"}},
    {"id": "DEU", "iso2Code": "DE", "name": "Germany", "region": {"id": "ECS", "value": "Europe & Central Asia"},
     "incomeLevel": {"value": "High income"}},
    {"id": "SDN", "iso2Code": "SD", "name": "Sudan", "region": {"id": "SSF", "value": "Sub-Saharan Africa"},
     "incomeLevel": {"value": "Low income"}},
    {"id": "TCD", "iso2Code": "TD", "name": "Chad", "region": {"id": "SSF", "value": "Sub-Saharan Africa"},
     "incomeLevel": {"value": "Low income"}},
    {"id": "WLD", "iso2Code": "1W", "name": "World", "region": {"id": "NA", "value": "Aggregates"}},
    {"id": "SSF", "iso2Code": "ZG", "name": "Sub-Saharan Africa", "region": {"id": "NA", "value": "Aggregates"}},
]]

_WB_LATEST = {
    "SM.POP.NETM": {"SYR": 400_000, "DEU": -300_000, "SDN": 290_000, "TCD": -120_000, "WLD": 0, "SSF": -200_000},
    "SP.POP.TOTL": {"SYR": 25_000_000, "DEU": 83_000_000, "SDN": 50_000_000, "TCD": 20_000_000,
                    "WLD": 8_000_000_000, "SSF": 1_200_000_000},
    "SM.POP.TOTL": {"SYR": 900_000, "DEU": 16_000_000, "SDN": 2_400_000, "TCD": 1_200_000},
}


def _wb(request: httpx.Request) -> httpx.Response:
    path, q = request.url.path, request.url.params
    if path.rstrip("/").endswith("/country"):
        return httpx.Response(200, json=_WB_COUNTRIES)
    _, _, country, _, indicator = path.rstrip("/").split("/")[-5:]
    if country == "all":
        assert q.get("mrnev") == "1"
        rows = [{"countryiso3code": iso, "date": "2025", "value": v}
                for iso, v in _WB_LATEST[indicator].items()]
        rows.append({"countryiso3code": "XKX", "date": "2025", "value": None})
        return httpx.Response(200, json=[{"page": 1, "pages": 1}, rows])
    series = {"SM.POP.NETM": {"2014": 500_000, "2015": 1_175_000, "2016": 600_000, "1998": -750_000},
              "SP.POP.TOTL": {"2014": 81_000_000, "2015": 82_000_000, "2016": 82_500_000, "1998": 82_000_000},
              "SM.POP.TOTL": {"2015": 12_000_000, "2020": 15_000_000}}[indicator]
    rows = [{"countryiso3code": country, "date": y, "value": v} for y, v in series.items()]
    rows.append({"countryiso3code": country, "date": "2026", "value": None})
    return httpx.Response(200, json=[{"page": 1, "pages": 1}, rows])


@pytest.fixture
def apis():
    with respx.mock(assert_all_called=False) as mock:
        mock.get(url__regex=r".*api\.unhcr\.org.*").mock(side_effect=_unhcr)
        mock.get(url__regex=r".*api\.worldbank\.org.*").mock(side_effect=_wb)
        yield mock


# ---------------------------------------------------------------------------
# Place resolution
# ---------------------------------------------------------------------------

def _countries():
    return {
        "SYR": {"name": "Syria", "iso2": "SY", "region": "Western Asia", "major_area": "Asia",
                "names": ["Syrian Arab Rep.", "syria", "syrian"]},
        "COG": {"name": "Congo", "iso2": "CG", "region": "Middle Africa", "major_area": "Africa",
                "names": ["Congo"]},
        "COD": {"name": "Dem. Rep. of the Congo", "iso2": "CD", "region": "Middle Africa",
                "major_area": "Africa", "names": ["Dem. Rep. of the Congo"]},
        "DEU": {"name": "Germany", "iso2": "DE", "region": "Western Europe", "major_area": "Europe",
                "names": ["Germany"]},
    }


def test_resolve_place_by_code_name_and_region() -> None:
    c = _countries()
    assert migration.resolve_place("syr", c) == ("country", "Syria", {"SYR"})
    assert migration.resolve_place("DE", c) == ("country", "Germany", {"DEU"})
    assert migration.resolve_place("Syrian Arab Rep.", c)[2] == {"SYR"}
    assert migration.resolve_place("syrian", c)[2] == {"SYR"}
    assert migration.resolve_place("Africa", c) == ("region", "Africa", {"COG", "COD"})
    assert migration.resolve_place("western europe", c) == ("region", "Western Europe", {"DEU"})
    # An exact name wins over the substring that would make it ambiguous.
    assert migration.resolve_place("Congo", c)[2] == {"COG"}


def test_resolve_place_rejects_ambiguous_and_unknown() -> None:
    c = _countries()
    with pytest.raises(ValueError, match="ambiguous"):
        migration.resolve_place("Cong", c)
    with pytest.raises(ValueError, match="unknown"):
        migration.resolve_place("Atlantis", c)


# ---------------------------------------------------------------------------
# Corridors
# ---------------------------------------------------------------------------

def test_corridor_table_uses_iso_fields_and_drops_internal_rows() -> None:
    table = migration._corridor_table(_PAIRS[2025])
    assert ("SYR", "SYR") not in table
    assert table[("AUT", "DEU")]["people"] == 10  # coo "AUS" ignored, coo_iso AUT used
    assert table[("SYR", "DEU")]["people"] == 660_000
    assert migration._idps_by_origin(_PAIRS[2025]) == {"SYR": 5_000_000, "SDN": 9_000_000}


async def test_flows_filtered_by_origin_with_prior_year_change(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_migration_flows(fetcher, year=2025, origin="Syria")
    assert result["year"] == 2025
    assert result["origin_filter"] == {"kind": "country", "name": "Syria"}
    assert [c["destination_iso3"] for c in result["corridors"]] == ["TUR", "DEU"]
    tur = result["corridors"][0]
    assert tur["people"] == 2_000_000
    assert tur["prior_year_people"] == 2_900_000
    assert tur["change"] == -900_000
    assert result["total_people"] == 2_660_000
    assert result["top_destinations"][0]["iso3"] == "TUR"
    assert "year-end stock" in result["measure"].lower()


async def test_flows_destination_region_and_min_people(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_migration_flows(fetcher, year=2025, destination="Europe", min_people=100)
    assert result["destination_filter"] == {"kind": "region", "name": "Europe"}
    assert {(c["origin_iso3"], c["destination_iso3"]) for c in result["corridors"]} == {
        ("UNK", "DEU"), ("SYR", "DEU")}


async def test_flows_default_year_falls_back_when_latest_is_empty(fetcher: Fetcher, apis,
                                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    class _Clock:
        @staticmethod
        def now(tz=None):
            from datetime import datetime, timezone
            return datetime(2027, 3, 1, tzinfo=timezone.utc)

    monkeypatch.setattr(migration, "datetime", _Clock)
    result = await migration.fetch_migration_flows(fetcher)
    assert result["year"] == 2025  # 2026 has no rows yet, so the newest year with data is used


async def test_flows_unknown_place_is_an_error_not_an_empty_result(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_migration_flows(fetcher, origin="Atlantis")
    assert "unknown" in result["error"]
    assert "corridors" not in result


async def test_flows_api_down_is_degraded(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    with respx.mock:
        respx.get(url__regex=r".*api\.unhcr\.org.*").mock(return_value=httpx.Response(500))
        result = await migration.fetch_migration_flows(fetcher, year=2025)
    assert result["degraded"] is True
    assert result["reason"] == "unhcr_fetch_failed"


# ---------------------------------------------------------------------------
# Surges
# ---------------------------------------------------------------------------

async def test_surges_rank_origins_levels_and_population_share(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_migration_surges(fetcher, year=2025, min_change=50_000)
    assert result["compared_with"] == 2024

    up = result["origin_outflow_increases"]
    assert [o["origin_iso3"] for o in up] == ["SDN"]
    sdn = up[0]
    assert sdn["change_abroad"] == 800_000
    assert sdn["population"] == 50_000_000
    assert sdn["change_abroad_pct_of_population"] == 1.6
    assert sdn["level"] == "mass"
    assert sdn["change_idps"] == -2_000_000

    down = result["origin_outflow_decreases"]
    assert down[0]["origin_iso3"] == "SYR"
    assert down[0]["change_abroad"] == -950_000
    assert down[0]["level"] is None

    assert result["corridor_increases"][0]["origin_iso3"] == "SDN"
    assert result["corridor_decreases"][0]["destination_iso3"] == "TUR"
    # "Unknown" origin is not a country: reported separately, never ranked.
    assert [o["origin_iso3"] for o in result["unattributed_origin_changes"]] == ["UNK"]
    assert all(c["origin_iso3"] != "UNK" for c in result["corridor_increases"])
    assert "heuristic" in result["levels"]["basis"]


def test_surge_levels() -> None:
    assert migration._surge_level(600_000, None) == "mass"
    assert migration._surge_level(60_000, 5_000_000) == "mass"  # 1.2% of population
    assert migration._surge_level(150_000, 100_000_000) == "major"
    assert migration._surge_level(60_000, 100_000_000) == "notable"


async def test_surges_without_a_comparison_year_is_degraded(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_migration_surges(fetcher, year=2024)  # no 2023 rows
    assert result["degraded"] is True
    assert result["reason"] == "no_comparison_year"


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

async def test_history_sorts_years_and_finds_waves(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_migration_history(fetcher, "SYR", year_from=2011, year_to=2014)
    assert result["iso3"] == "SYR" and result["role"] == "origin"
    assert [e["year"] for e in result["series"]] == [2011, 2012, 2013, 2014]
    assert result["peak"] == {"year": 2014, "people": 3_887_000}
    assert result["largest_waves"][0] == {"year": 2013, "increase": 1_671_000, "people": 2_400_000,
                                          "pct_change": 229.2}
    request = apis.calls.last.request
    assert request.url.params["coo"] == "SYR" and request.url.params["cf_type"] == "ISO"


async def test_history_rejects_regions_and_bad_role(fetcher: Fetcher, apis) -> None:
    assert "region" in (await migration.fetch_migration_history(fetcher, "Europe"))["error"]
    assert "role" in (await migration.fetch_migration_history(fetcher, "SYR", role="both"))["error"]


# ---------------------------------------------------------------------------
# World Bank net migration
# ---------------------------------------------------------------------------

async def test_net_migration_ranking_excludes_aggregates(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_net_migration(fetcher, limit=5)
    assert result["countries"] == 4
    assert result["net_total"] == 270_000
    assert [r["iso3"] for r in result["top_net_immigration"]] == ["SYR", "SDN"]
    assert [r["iso3"] for r in result["top_net_emigration"]] == ["DEU", "TCD"]
    syr = result["top_net_immigration"][0]
    assert syr["per_1000"] == 16.0
    assert syr["migrant_stock_pct"] == 3.6
    assert result["highest_rate_emigration"][0]["iso3"] == "TCD"  # -6.0 per 1,000 beats DEU -3.61
    assert [a["iso3"] for a in result["world_regions"]] == ["WLD", "SSF"]
    assert "projection" in result["measure"]


async def test_net_migration_region_filter(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_net_migration(fetcher, region="SSF")
    assert result["region_filter"] == "Sub-Saharan Africa"
    assert {r["iso3"] for r in result["top_net_immigration"] + result["top_net_emigration"]} == {"SDN", "TCD"}
    assert result["world_regions"] == []
    assert "country" in (await migration.fetch_net_migration(fetcher, region="Germany"))["error"]


async def test_net_migration_country_series(fetcher: Fetcher, apis) -> None:
    result = await migration.fetch_net_migration(fetcher, country="Germany")
    assert result["iso3"] == "DEU"
    assert [e["year"] for e in result["series"]] == [1998, 2014, 2015, 2016]  # null 2026 dropped
    assert result["latest"]["year"] == 2016
    assert result["largest_net_inflow_years"][0]["year"] == 2015
    assert result["largest_net_inflow_years"][0]["per_1000"] == 14.33
    assert result["largest_net_outflow_years"][0]["year"] == 1998
    assert [s["year"] for s in result["migrant_stock"]] == [2015, 2020]


# ---------------------------------------------------------------------------
# intel_displacement_summary regression (2026-10-11)
# ---------------------------------------------------------------------------

async def test_displacement_summary_requests_per_origin_rows(fetcher: Fetcher) -> None:
    """Without coo_all=true UNHCR answers with one global row named "-", and the
    tool reported that single row as its whole "by_origin" list."""
    items = [{"coo_iso": "SYR", "coo_name": "Syrian Arab Rep.", "refugees": 10},
             {"coo_iso": "SDN", "coo_name": "Sudan", "refugees": 20}]
    with respx.mock:
        route = respx.get(url__regex=r".*api\.unhcr\.org.*").mock(
            return_value=httpx.Response(200, json={"items": items}))
        result = await fetch_displacement_summary(fetcher, year=2025)
    assert route.calls.last.request.url.params["coo_all"] == "true"
    assert [(e["country"], e["iso3"]) for e in result["by_origin"]] == [("Sudan", "SDN"), ("Syria", "SYR")]
