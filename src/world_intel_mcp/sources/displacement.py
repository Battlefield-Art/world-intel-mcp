"""UNHCR displacement data source for world-intel-mcp.

Provides refugee and displacement population statistics from the UNHCR
Population Statistics API. No API key required.
"""

import logging
from datetime import datetime, timezone

from ..config.countries import TIER1_COUNTRIES
from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.displacement")

_UNHCR_POPULATION_URL = "https://api.unhcr.org/population/v1/population/"


async def fetch_displacement_summary(
    fetcher: Fetcher,
    year: int | None = None,
) -> dict:
    """Fetch refugee/displacement population statistics from the UNHCR API.

    Aggregates displacement data by country of origin and computes global
    totals across refugees, asylum seekers, IDPs, stateless persons, and
    others of concern.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        year: Reporting year to query. Defaults to the previous calendar
              year since UNHCR data typically lags by one year.

    Returns:
        Dict with top-30 countries by displacement (sorted descending),
        global totals, year, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    # Default to previous year since UNHCR data lags
    if year is None:
        year = now.year - 1
    else:
        # Ensure year is an int even if passed as string
        year = int(year)

    # coo_all=true asks for one row per origin country. Without it UNHCR
    # returns a single global row (coo_name "-"), which is what this tool
    # served until 2026-10-11: "by_origin" held one entry named "-".
    params = {
        "limit": 1000,
        "yearFrom": year,
        "yearTo": year,
        "coo_all": "true",
        "cf_type": "ISO",
    }

    data = await fetcher.get_json(
        url=_UNHCR_POPULATION_URL,
        source="unhcr",
        cache_key=f"displacement:unhcr:by_origin:{year}",
        cache_ttl=43200,
        params=params,
    )

    if data is None:
        logger.warning("UNHCR API returned no data")
        # Honest outage shape: without these markers this return was
        # byte-shape identical to a world with zero displaced persons
        # (the fail-reads-as-success class). The zeroed structure stays
        # so field-reading consumers (cli.py reads global_totals
        # directly) keep working; error/degraded/reason distinguish
        # "UNHCR is down" from "zero refugees worldwide".
        return {
            "error": "UNHCR API unavailable (no live or cached data)",
            "degraded": True,
            "reason": "unhcr_fetch_failed",
            "by_origin": [],
            "global_totals": {
                "total_refugees": 0,
                "total_asylum_seekers": 0,
                "total_idps": 0,
                "total_stateless": 0,
                "total_ooc": 0,
                "grand_total": 0,
            },
            "year": year,
            "count": 0,
            "source": "unhcr",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    items = data.get("items", [])

    # Aggregate by country of origin. Rows carry coo_iso (ISO-3) under
    # cf_type=ISO; the display name prefers the TIER1 short name ("Syria",
    # not UNHCR's "Syrian Arab Rep.") so signals.py can join it with the
    # conflict sources, which use short names.
    by_country: dict[str, dict[str, int]] = {}
    names: dict[str, tuple[str, str | None]] = {}

    for item in items:
        iso3 = (item.get("coo_iso") or "").upper()
        iso3 = iso3 if len(iso3) == 3 else None
        tier1 = TIER1_COUNTRIES.get(iso3) if iso3 else None
        coo_name = tier1["name"] if tier1 else (item.get("coo_name") or "Unknown").strip()
        key = iso3 or coo_name
        names[key] = (coo_name, iso3)

        refugees = _safe_int(item.get("refugees"))
        asylum_seekers = _safe_int(item.get("asylum_seekers"))
        idps = _safe_int(item.get("idps"))
        stateless = _safe_int(item.get("stateless"))
        ooc = _safe_int(item.get("ooc"))

        if key not in by_country:
            by_country[key] = {
                "refugees": 0,
                "asylum_seekers": 0,
                "idps": 0,
                "stateless": 0,
                "ooc": 0,
            }

        entry = by_country[key]
        entry["refugees"] += refugees
        entry["asylum_seekers"] += asylum_seekers
        entry["idps"] += idps
        entry["stateless"] += stateless
        entry["ooc"] += ooc

    # Build sorted list with total_displaced
    by_origin = []
    for key, totals in by_country.items():
        country, iso3 = names[key]
        total_displaced = (
            totals["refugees"]
            + totals["asylum_seekers"]
            + totals["idps"]
            + totals["stateless"]
            + totals["ooc"]
        )
        by_origin.append(
            {
                "country": country,
                "iso3": iso3,
                "refugees": totals["refugees"],
                "asylum_seekers": totals["asylum_seekers"],
                "internally_displaced": totals["idps"],
                "stateless": totals["stateless"],
                "others_of_concern": totals["ooc"],
                "total_displaced": total_displaced,
            }
        )

    # Sort by total_displaced descending, take top 30
    by_origin.sort(key=lambda e: e["total_displaced"], reverse=True)
    by_origin = by_origin[:30]

    # Compute global totals across all countries (not just top 30)
    global_refugees = sum(t["refugees"] for t in by_country.values())
    global_asylum_seekers = sum(t["asylum_seekers"] for t in by_country.values())
    global_idps = sum(t["idps"] for t in by_country.values())
    global_stateless = sum(t["stateless"] for t in by_country.values())
    global_ooc = sum(t["ooc"] for t in by_country.values())

    return {
        "by_origin": by_origin,
        "global_totals": {
            "total_refugees": global_refugees,
            "total_asylum_seekers": global_asylum_seekers,
            "total_idps": global_idps,
            "total_stateless": global_stateless,
            "total_ooc": global_ooc,
            "grand_total": (
                global_refugees
                + global_asylum_seekers
                + global_idps
                + global_stateless
                + global_ooc
            ),
        },
        "year": year,
        "count": len(by_origin),
        "source": "unhcr",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def _safe_int(value: int | float | str | None) -> int:
    """Convert a value to int, returning 0 for None or unparseable values."""
    if value is None:
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0
