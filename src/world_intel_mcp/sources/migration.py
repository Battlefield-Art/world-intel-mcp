"""Cross-border migration: UNHCR origin->destination corridors and World Bank net migration.

Two public, keyless APIs:

* UNHCR Population Statistics (https://api.unhcr.org/population/v1/): refugees,
  asylum seekers and other people in need of international protection for every
  origin/asylum country pair, 1951 to the latest reporting year. Figures are
  YEAR-END STOCKS (people present at the end of the year), not border crossings,
  so a year-over-year change is net arrivals minus returns, resettlement onward,
  naturalisation and deaths.
* World Bank WDI (https://api.worldbank.org/v2/): net migration (SM.POP.NETM,
  immigrants minus emigrants of all kinds, not only forced displacement),
  international migrant stock (SM.POP.TOTL) and population (SP.POP.TOTL).

UNHCR's own country codes differ from ISO-3 (its "AUS" is Austria), so every
UNHCR request sends ``cf_type=ISO`` and every row is read through its
``coo_iso`` / ``coa_iso`` fields.

Gaps / not covered: no near-real-time source (IOM DTM and HDX HAPI need keys);
UNHCR publishes once a year, so the latest year lags by months; economic
migration appears only as World Bank net totals, never as country-to-country
flows; the surge levels are heuristics (stated in each response), not a UN
classification.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..config.countries import TIER1_COUNTRIES
from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.migration")

_UNHCR = "https://api.unhcr.org/population/v1/"
_WB = "https://api.worldbank.org/v2/"

_UNHCR_TTL = 43200  # 12 h; the data changes once a year
_META_TTL = 604800  # 7 d for country lists
_MAX_PAGES = 10

UNHCR_MEASURE = (
    "UNHCR year-end stock: people from the origin present in the destination at "
    "the end of the year (refugees + asylum seekers + others in need of "
    "international protection). Year-over-year change = net arrivals minus "
    "returns, onward movement, naturalisation and deaths; not a crossing count."
)

# UNHCR's country list carries two buckets that are not countries.
NON_COUNTRY_ISO = {"UNK": "Unknown origin", "XXA": "Stateless"}

# Surge levels (heuristic, reported with every result).
MASS_SHARE = 0.01  # >= 1% of the origin population in one year
MASS_ABS = 500_000
MAJOR_ABS = 100_000


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _int(value: Any) -> int:
    """UNHCR mixes ints, numeric strings and "-"; anything unparseable is 0."""
    if value is None:
        return 0
    try:
        return int(value)
    except (ValueError, TypeError):
        return 0


def _degraded(error: str, **extra: Any) -> dict:
    return {"error": error, "degraded": True, "source": extra.pop("source", "unhcr"),
            "timestamp": _now(), **extra}


# ---------------------------------------------------------------------------
# Raw fetches
# ---------------------------------------------------------------------------

async def _unhcr_items(fetcher: Fetcher, endpoint: str, params: dict, cache_key: str,
                       ttl: int = _UNHCR_TTL) -> list[dict] | None:
    """All pages of a UNHCR list endpoint, or None if the first page failed."""
    items: list[dict] = []
    page = 1
    while page <= _MAX_PAGES:
        data = await fetcher.get_json(
            url=_UNHCR + endpoint, source="unhcr",
            cache_key=f"{cache_key}:p{page}", cache_ttl=ttl,
            params={**params, "page": page},
        )
        if not isinstance(data, dict):
            return None if page == 1 else items
        items.extend(data.get("items") or [])
        if page >= _int(data.get("maxPages")):
            return items
        page += 1
    logger.warning("UNHCR %s exceeded %d pages; result truncated", endpoint, _MAX_PAGES)
    return items


async def unhcr_countries(fetcher: Fetcher) -> dict[str, dict] | None:
    """ISO-3 -> {name, region, major_area, names} from UNHCR's country list."""
    items = await _unhcr_items(fetcher, "countries/", {"limit": 1000},
                               "migration:unhcr:countries", ttl=_META_TTL)
    if items is None:
        return None
    out: dict[str, dict] = {}
    for c in items:
        iso = (c.get("iso") or "").upper()
        if len(iso) != 3:
            continue
        names = {c.get(k) for k in ("name", "nameShort", "nameLong", "nameOrigin", "nameFormal")}
        names.discard(None)
        tier1 = TIER1_COUNTRIES.get(iso)
        if tier1:
            names.add(tier1["name"])
            names.update(tier1.get("keywords", []))
        out[iso] = {
            "name": NON_COUNTRY_ISO.get(iso) or (tier1["name"] if tier1 else (c.get("name") or iso).strip()),
            "is_country": iso not in NON_COUNTRY_ISO,
            "iso2": c.get("iso2"),
            "region": c.get("region"),
            "major_area": c.get("majorArea"),
            "names": sorted(n for n in names if n),
        }
    return out


async def unhcr_pairs(fetcher: Fetcher, year: int) -> list[dict] | None:
    """Every origin/asylum pair row for one year (about 6,300 rows for 2025)."""
    return await _unhcr_items(
        fetcher, "population/",
        {"yearFrom": year, "yearTo": year, "coo_all": "true", "coa_all": "true",
         "cf_type": "ISO", "limit": 10000},
        f"migration:unhcr:pairs:{year}",
    )


async def _latest_pairs(fetcher: Fetcher, year: int | None) -> tuple[int, list[dict]] | None:
    """Rows for ``year``, or for the newest year that has data when None."""
    if year is not None:
        rows = await unhcr_pairs(fetcher, int(year))
        return None if rows is None else (int(year), rows)
    this_year = datetime.now(timezone.utc).year
    for candidate in (this_year - 1, this_year - 2):
        rows = await unhcr_pairs(fetcher, candidate)
        if rows is None:
            return None
        if rows:
            return candidate, rows
    return this_year - 2, []


async def _wb_indicator(fetcher: Fetcher, indicator: str, country: str = "all",
                        date: str | None = None) -> list[dict] | None:
    """World Bank indicator rows (all pages). Latest non-empty value when no date."""
    params: dict[str, Any] = {"format": "json", "per_page": 1000}
    if date:
        params["date"] = date
    else:
        params["mrnev"] = 1
    rows: list[dict] = []
    page = 1
    while page <= _MAX_PAGES:
        data = await fetcher.get_json(
            url=f"{_WB}country/{country}/indicator/{indicator}", source="worldbank",
            cache_key=f"migration:wb:{indicator}:{country}:{date or 'latest'}:p{page}",
            cache_ttl=_UNHCR_TTL, params={**params, "page": page},
        )
        if not isinstance(data, list) or len(data) < 2 or not isinstance(data[0], dict):
            return None if page == 1 else rows
        rows.extend(data[1] or [])
        if page >= _int(data[0].get("pages")):
            return rows
        page += 1
    return rows


async def wb_countries(fetcher: Fetcher) -> dict[str, dict] | None:
    """ISO-3 -> {name, region, income, aggregate} from the World Bank country list."""
    data = await fetcher.get_json(
        url=_WB + "country", source="worldbank", cache_key="migration:wb:countries",
        cache_ttl=_META_TTL, params={"format": "json", "per_page": 1000},
    )
    if not isinstance(data, list) or len(data) < 2:
        return None
    out = {}
    for c in data[1] or []:
        region = (c.get("region") or {})
        out[c["id"]] = {
            "name": c.get("name", "").strip(),
            "iso2": c.get("iso2Code"),
            "region": (region.get("value") or "").strip(),
            "region_id": region.get("id"),
            "income": ((c.get("incomeLevel") or {}).get("value") or "").strip(),
            "aggregate": region.get("id") == "NA",
        }
    return out


# ---------------------------------------------------------------------------
# Place resolution
# ---------------------------------------------------------------------------

def resolve_place(query: str, countries: dict[str, dict]) -> tuple[str, str, set[str]]:
    """Resolve a country (ISO-3, ISO-2 or name) or a region to ISO-3 codes.

    Returns (kind, label, codes) with kind "country" or "region". Regions are
    UNHCR's major areas ("Europe") and UN sub-regions ("Eastern Africa").
    Raises ValueError when nothing, or more than one country, matches.
    """
    q = query.strip()
    if not q:
        raise ValueError("empty place")
    up, low = q.upper(), q.lower()
    if len(up) == 3 and up in countries:
        return "country", countries[up]["name"], {up}
    if len(up) == 2:
        hits = [iso for iso, c in countries.items() if (c.get("iso2") or "").upper() == up]
        if len(hits) == 1:
            return "country", countries[hits[0]]["name"], set(hits)
    for field in ("major_area", "region"):
        members = {iso for iso, c in countries.items() if (c.get(field) or "").lower() == low}
        if members:
            return "region", next(c[field] for c in countries.values() if (c.get(field) or "").lower() == low), members
    exact = [iso for iso, c in countries.items()
             if low == c["name"].lower() or any(low == n.lower() for n in c.get("names", []))]
    if len(exact) == 1:
        return "country", countries[exact[0]]["name"], set(exact)
    partial = exact or [iso for iso, c in countries.items()
                        if len(low) >= 4 and any(low in n.lower() for n in [c["name"], *c.get("names", [])])]
    if len(partial) == 1:
        return "country", countries[partial[0]]["name"], set(partial)
    if partial:
        names = sorted(f"{countries[i]['name']} ({i})" for i in partial)[:8]
        raise ValueError(f"'{query}' is ambiguous: {', '.join(names)}")
    raise ValueError(f"unknown country or region: '{query}'")


def _name(iso: str, row_name: str | None, countries: dict[str, dict]) -> str:
    c = countries.get(iso)
    return c["name"] if c else (row_name or iso or "Unknown").strip()


# ---------------------------------------------------------------------------
# Corridors
# ---------------------------------------------------------------------------

def _corridor_table(rows: list[dict]) -> dict[tuple[str, str], dict]:
    """(origin_iso3, destination_iso3) -> summed counts; same-country rows dropped."""
    table: dict[tuple[str, str], dict] = {}
    for r in rows:
        o = (r.get("coo_iso") or "").upper()
        d = (r.get("coa_iso") or "").upper()
        if not o or not d or o == "-" or d == "-" or o == d:
            continue
        e = table.setdefault((o, d), {"refugees": 0, "asylum_seekers": 0, "oip": 0,
                                      "origin_name": r.get("coo_name"), "destination_name": r.get("coa_name")})
        e["refugees"] += _int(r.get("refugees"))
        e["asylum_seekers"] += _int(r.get("asylum_seekers"))
        e["oip"] += _int(r.get("oip"))
    for e in table.values():
        e["people"] = e["refugees"] + e["asylum_seekers"] + e["oip"]
    return table


def _idps_by_origin(rows: list[dict]) -> dict[str, int]:
    out: dict[str, int] = {}
    for r in rows:
        o = (r.get("coo_iso") or "").upper()
        if o and o == (r.get("coa_iso") or "").upper():
            out[o] = out.get(o, 0) + _int(r.get("idps"))
    return out


async def fetch_migration_flows(
    fetcher: Fetcher,
    year: int | None = None,
    origin: str | None = None,
    destination: str | None = None,
    limit: int = 25,
    min_people: int = 0,
) -> dict:
    """Largest origin->destination corridors, optionally filtered by country or region."""
    countries = await unhcr_countries(fetcher)
    if countries is None:
        return _degraded("UNHCR country list unavailable", reason="unhcr_fetch_failed")
    try:
        o_sel = resolve_place(origin, countries) if origin else None
        d_sel = resolve_place(destination, countries) if destination else None
    except ValueError as exc:
        return {"error": str(exc), "source": "unhcr", "timestamp": _now()}

    got = await _latest_pairs(fetcher, year)
    if got is None:
        return _degraded("UNHCR population API unavailable", reason="unhcr_fetch_failed")
    year, rows = got
    prior_rows = await unhcr_pairs(fetcher, year - 1)
    cur = _corridor_table(rows)
    prev = _corridor_table(prior_rows) if prior_rows else None

    selected = []
    for (o, d), e in cur.items():
        if o_sel and o not in o_sel[2]:
            continue
        if d_sel and d not in d_sel[2]:
            continue
        if e["people"] <= 0 or e["people"] < min_people:
            continue
        p = prev.get((o, d), {}).get("people", 0) if prev is not None else None
        selected.append({
            "origin": _name(o, e["origin_name"], countries), "origin_iso3": o,
            "destination": _name(d, e["destination_name"], countries), "destination_iso3": d,
            "people": e["people"], "refugees": e["refugees"],
            "asylum_seekers": e["asylum_seekers"], "other_in_need_of_protection": e["oip"],
            "prior_year_people": p, "change": None if p is None else e["people"] - p,
        })
    selected.sort(key=lambda x: x["people"], reverse=True)

    def top(key: str, iso_key: str) -> list[dict]:
        agg: dict[str, dict] = {}
        for c in selected:
            a = agg.setdefault(c[iso_key], {"name": c[key], "iso3": c[iso_key], "people": 0, "corridors": 0})
            a["people"] += c["people"]
            a["corridors"] += 1
        return sorted(agg.values(), key=lambda x: x["people"], reverse=True)[:10]

    return {
        "year": year,
        "origin_filter": None if not o_sel else {"kind": o_sel[0], "name": o_sel[1]},
        "destination_filter": None if not d_sel else {"kind": d_sel[0], "name": d_sel[1]},
        "total_people": sum(c["people"] for c in selected),
        "corridor_count": len(selected),
        "corridors": selected[: max(1, int(limit))],
        "top_origins": top("origin", "origin_iso3"),
        "top_destinations": top("destination", "destination_iso3"),
        "prior_year_available": prev is not None,
        "measure": UNHCR_MEASURE,
        "source": "unhcr",
        "timestamp": _now(),
    }


def _surge_level(increase: int, population: int | None) -> str:
    if increase >= MASS_ABS or (population and increase >= MASS_SHARE * population):
        return "mass"
    if increase >= MAJOR_ABS:
        return "major"
    return "notable"


async def fetch_migration_surges(
    fetcher: Fetcher,
    year: int | None = None,
    min_change: int = 50_000,
    limit: int = 20,
) -> dict:
    """Largest year-over-year increases and decreases, by origin and by corridor."""
    countries = await unhcr_countries(fetcher)
    if countries is None:
        return _degraded("UNHCR country list unavailable", reason="unhcr_fetch_failed")
    got = await _latest_pairs(fetcher, year)
    if got is None:
        return _degraded("UNHCR population API unavailable", reason="unhcr_fetch_failed")
    year, rows = got
    prior_rows = await unhcr_pairs(fetcher, year - 1)
    if not rows or not prior_rows:
        return _degraded(f"UNHCR has no data for {year} and {year - 1} to compare",
                         reason="no_comparison_year", year=year)

    cur, prev = _corridor_table(rows), _corridor_table(prior_rows)
    idp_cur, idp_prev = _idps_by_origin(rows), _idps_by_origin(prior_rows)

    pop_rows = await _wb_indicator(fetcher, "SP.POP.TOTL")
    population = {r["countryiso3code"]: _int(r.get("value")) for r in pop_rows or []
                  if r.get("countryiso3code") and r.get("value") is not None}

    corridors = []
    for key in set(cur) | set(prev):
        now_p = cur.get(key, {}).get("people", 0)
        was_p = prev.get(key, {}).get("people", 0)
        delta = now_p - was_p
        if abs(delta) < min_change:
            continue
        e = cur.get(key) or prev[key]
        corridors.append({
            "origin": _name(key[0], e["origin_name"], countries), "origin_iso3": key[0],
            "destination": _name(key[1], e["destination_name"], countries), "destination_iso3": key[1],
            "people": now_p, "prior_year_people": was_p, "change": delta,
            "pct_change": round(100.0 * delta / was_p, 1) if was_p else None,
        })

    origins: dict[str, dict] = {}
    for table, sign in ((cur, 1), (prev, -1)):
        for (o, _d), e in table.items():
            a = origins.setdefault(o, {"origin_name": e["origin_name"], "now": 0, "was": 0})
            a["now" if sign == 1 else "was"] += e["people"]
    # A country with only internal displacement has no corridor rows; it still belongs here.
    for o in set(idp_cur) | set(idp_prev):
        origins.setdefault(o, {"origin_name": None, "now": 0, "was": 0})
    origin_changes, unattributed = [], []
    for o, a in origins.items():
        delta = a["now"] - a["was"]
        idp_delta = idp_cur.get(o, 0) - idp_prev.get(o, 0)
        if abs(delta) < min_change and abs(idp_delta) < min_change:
            continue
        pop = population.get(o)
        target = origin_changes if countries.get(o, {}).get("is_country") else unattributed
        target.append({
            "origin": _name(o, a["origin_name"], countries), "origin_iso3": o,
            "people_abroad": a["now"], "prior_year_people_abroad": a["was"], "change_abroad": delta,
            "idps": idp_cur.get(o, 0), "change_idps": idp_delta,
            # No IDP figure the year before: the jump may be a first report, not new displacement.
            "idps_first_reported": idp_cur.get(o, 0) > 0 and idp_prev.get(o, 0) == 0,
            "population": pop,
            "change_abroad_pct_of_population": round(100.0 * delta / pop, 2) if pop else None,
            "level": _surge_level(delta, pop) if delta >= min_change else None,
        })

    attributed = [c for c in corridors if countries.get(c["origin_iso3"], {}).get("is_country")]

    def ranked(items: list[dict], key: str, rising: bool) -> list[dict]:
        picked = [i for i in items if (i[key] > 0) == rising and i[key] != 0]
        return sorted(picked, key=lambda i: i[key], reverse=rising)[: max(1, int(limit))]

    return {
        "year": year,
        "compared_with": year - 1,
        "min_change": min_change,
        "origin_outflow_increases": ranked(origin_changes, "change_abroad", True),
        "origin_outflow_decreases": ranked(origin_changes, "change_abroad", False),
        "internal_displacement_increases": sorted(
            [o for o in origin_changes if o["change_idps"] >= min_change],
            key=lambda o: o["change_idps"], reverse=True)[: max(1, int(limit))],
        "corridor_increases": ranked(attributed, "change", True),
        "corridor_decreases": ranked(attributed, "change", False),
        # UNHCR's "Unknown" origin and "Stateless" are not countries; a jump in
        # them is usually a host's reporting change, so they stay out of the rankings.
        "unattributed_origin_changes": sorted(unattributed, key=lambda o: abs(o["change_abroad"]), reverse=True),
        "levels": {
            "mass": f"increase >= {MASS_ABS:,} people or >= {MASS_SHARE:.0%} of the origin population",
            "major": f"increase >= {MAJOR_ABS:,} people",
            "notable": f"increase >= min_change ({min_change:,})",
            "basis": "heuristic thresholds of this tool, not a UNHCR classification",
        },
        "population_source": "worldbank SP.POP.TOTL" if population else None,
        "measure": UNHCR_MEASURE,
        "source": "unhcr",
        "timestamp": _now(),
    }


async def fetch_migration_history(
    fetcher: Fetcher,
    country: str,
    role: str = "origin",
    year_from: int = 1951,
    year_to: int | None = None,
) -> dict:
    """Yearly series for one country as an origin or a host, with its largest waves."""
    if role not in ("origin", "host"):
        return {"error": "role must be 'origin' or 'host'", "source": "unhcr", "timestamp": _now()}
    countries = await unhcr_countries(fetcher)
    if countries is None:
        return _degraded("UNHCR country list unavailable", reason="unhcr_fetch_failed")
    try:
        kind, label, codes = resolve_place(country, countries)
    except ValueError as exc:
        return {"error": str(exc), "source": "unhcr", "timestamp": _now()}
    if kind != "country":
        return {"error": f"'{country}' is a region; history needs one country", "source": "unhcr",
                "timestamp": _now()}
    iso = next(iter(codes))
    year_to = int(year_to or datetime.now(timezone.utc).year)
    field = "coo" if role == "origin" else "coa"
    items = await _unhcr_items(
        fetcher, "population/",
        {field: iso, "cf_type": "ISO", "yearFrom": int(year_from), "yearTo": year_to, "limit": 1000},
        f"migration:unhcr:history:{role}:{iso}:{year_from}:{year_to}",
    )
    if items is None:
        return _degraded("UNHCR population API unavailable", reason="unhcr_fetch_failed")

    by_year: dict[int, dict] = {}
    for r in items:
        y = _int(r.get("year"))
        if not y:
            continue
        e = by_year.setdefault(y, {"year": y, "refugees": 0, "asylum_seekers": 0,
                                   "other_in_need_of_protection": 0, "idps": 0, "returned_refugees": 0})
        e["refugees"] += _int(r.get("refugees"))
        e["asylum_seekers"] += _int(r.get("asylum_seekers"))
        e["other_in_need_of_protection"] += _int(r.get("oip"))
        e["idps"] += _int(r.get("idps"))
        e["returned_refugees"] += _int(r.get("returned_refugees"))
    series = [by_year[y] for y in sorted(by_year)]
    for e in series:
        e["people"] = e["refugees"] + e["asylum_seekers"] + e["other_in_need_of_protection"]

    waves = []
    for prev, cur in zip(series, series[1:]):
        if cur["year"] != prev["year"] + 1:
            continue
        delta = cur["people"] - prev["people"]
        if delta > 0:
            waves.append({"year": cur["year"], "increase": delta, "people": cur["people"],
                          "pct_change": round(100.0 * delta / prev["people"], 1) if prev["people"] else None})
    waves.sort(key=lambda w: w["increase"], reverse=True)
    peak = max(series, key=lambda e: e["people"]) if series else None

    return {
        "country": label, "iso3": iso, "role": role,
        "meaning": ("people FROM this country living abroad (and IDPs inside it)" if role == "origin"
                    else "people from other countries hosted IN this country"),
        "years": len(series),
        "first_year": series[0]["year"] if series else None,
        "latest": series[-1] if series else None,
        "peak": None if not peak else {"year": peak["year"], "people": peak["people"]},
        "largest_waves": waves[:5],
        "series": series,
        "measure": UNHCR_MEASURE,
        "source": "unhcr",
        "timestamp": _now(),
    }


# ---------------------------------------------------------------------------
# World Bank net migration (all migration, not only forced displacement)
# ---------------------------------------------------------------------------

WB_MEASURE = (
    "World Bank SM.POP.NETM: immigrants minus emigrants over the year, citizens and "
    "non-citizens, all reasons (work, family, study, forced). Positive = net "
    "inflow. Source is the UN World Population Prospects: modelled values, and the "
    "most recent years can be WPP projections rather than observed counts."
)


def _wb_place(query: str, wb: dict[str, dict]) -> tuple[str, str, set[str]]:
    """Resolve a World Bank country (code or name) or region (code or name)."""
    countries = {iso: {"name": c["name"], "iso2": c["iso2"], "region": c["region"],
                       "names": [c["name"], *(TIER1_COUNTRIES.get(iso, {}).get("keywords", []))]}
                 for iso, c in wb.items() if not c["aggregate"]}
    for iso, c in TIER1_COUNTRIES.items():
        if iso in countries:
            countries[iso]["names"].append(c["name"])
    up = query.strip().upper()
    region_codes = {c["region_id"]: c["region"] for c in wb.values() if not c["aggregate"]}
    if up in region_codes:
        label = region_codes[up]
        return "region", label, {i for i, c in countries.items() if c["region"] == label}
    return resolve_place(query, countries)


async def fetch_net_migration(
    fetcher: Fetcher,
    country: str | None = None,
    region: str | None = None,
    limit: int = 20,
) -> dict:
    """Net migration ranking (latest year) or one country's series since 1960."""
    wb = await wb_countries(fetcher)
    if wb is None:
        return _degraded("World Bank country list unavailable", reason="worldbank_fetch_failed",
                         source="worldbank")

    if country:
        try:
            kind, label, codes = _wb_place(country, wb)
        except ValueError as exc:
            return {"error": str(exc), "source": "worldbank", "timestamp": _now()}
        if kind != "country":
            return {"error": f"'{country}' is a region; pass it as region=", "source": "worldbank",
                    "timestamp": _now()}
        iso = next(iter(codes))
        span = f"1960:{datetime.now(timezone.utc).year}"
        net = await _wb_indicator(fetcher, "SM.POP.NETM", iso, span)
        if net is None:
            return _degraded("World Bank API unavailable", reason="worldbank_fetch_failed", source="worldbank")
        stock = await _wb_indicator(fetcher, "SM.POP.TOTL", iso, span) or []
        pop = await _wb_indicator(fetcher, "SP.POP.TOTL", iso, span) or []
        pop_by = {_int(r["date"]): _int(r["value"]) for r in pop if r.get("value") is not None}
        series: list[dict[str, Any]] = []
        for r in net:
            if r.get("value") is None:
                continue
            y, v = _int(r["date"]), _int(r["value"])
            series.append({"year": y, "net_migration": v,
                           "per_1000": round(1000.0 * v / pop_by[y], 2) if pop_by.get(y) else None})
        series.sort(key=lambda e: e["year"])
        stock_series = sorted(({"year": _int(r["date"]), "migrant_stock": _int(r["value"])}
                               for r in stock if r.get("value") is not None), key=lambda e: e["year"])
        return {
            "country": label, "iso3": iso, "region": wb[iso]["region"],
            "latest": series[-1] if series else None,
            "largest_net_inflow_years": sorted((e for e in series if e["net_migration"] > 0),
                                               key=lambda e: e["net_migration"], reverse=True)[:5],
            "largest_net_outflow_years": sorted((e for e in series if e["net_migration"] < 0),
                                                key=lambda e: e["net_migration"])[:5],
            "migrant_stock": stock_series,
            "series": series,
            "measure": WB_MEASURE,
            "source": "worldbank",
            "timestamp": _now(),
        }

    selected: set[str] | None = None
    region_label = None
    if region:
        try:
            kind, region_label, selected = _wb_place(region, wb)
        except ValueError as exc:
            return {"error": str(exc), "source": "worldbank", "timestamp": _now()}
        if kind != "region":
            return {"error": f"'{region}' is a country; pass it as country=", "source": "worldbank",
                    "timestamp": _now()}

    net = await _wb_indicator(fetcher, "SM.POP.NETM")
    if net is None:
        return _degraded("World Bank API unavailable", reason="worldbank_fetch_failed", source="worldbank")
    pop = {r["countryiso3code"]: _int(r["value"]) for r in await _wb_indicator(fetcher, "SP.POP.TOTL") or []
           if r.get("value") is not None}
    stock = {r["countryiso3code"]: _int(r["value"]) for r in await _wb_indicator(fetcher, "SM.POP.TOTL") or []
             if r.get("value") is not None}

    rows, aggregates = [], []
    for r in net:
        iso = r.get("countryiso3code") or ""
        meta = wb.get(iso)
        if meta is None or r.get("value") is None:
            continue
        value = _int(r["value"])
        entry = {"name": meta["name"], "iso3": iso, "year": _int(r.get("date")), "net_migration": value,
                 "per_1000": round(1000.0 * value / pop[iso], 2) if pop.get(iso) else None,
                 "migrant_stock": stock.get(iso),
                 "migrant_stock_pct": round(100.0 * stock[iso] / pop[iso], 1)
                 if stock.get(iso) and pop.get(iso) else None}
        if meta["aggregate"]:
            aggregates.append(entry)
        elif selected is None or iso in selected:
            entry["region"] = meta["region"]
            rows.append(entry)

    n = max(1, int(limit))
    geo = {"WLD", "EAS", "ECS", "LCN", "MEA", "NAC", "SAS", "SSF"}
    return {
        "region_filter": region_label,
        "countries": len(rows),
        "net_total": sum(r["net_migration"] for r in rows),
        "top_net_immigration": sorted((r for r in rows if r["net_migration"] > 0),
                                      key=lambda r: r["net_migration"], reverse=True)[:n],
        "top_net_emigration": sorted((r for r in rows if r["net_migration"] < 0),
                                     key=lambda r: r["net_migration"])[:n],
        "highest_rate_immigration": sorted((r for r in rows if r["per_1000"] is not None and r["per_1000"] > 0),
                                           key=lambda r: r["per_1000"], reverse=True)[:n],
        "highest_rate_emigration": sorted((r for r in rows if r["per_1000"] is not None and r["per_1000"] < 0),
                                          key=lambda r: r["per_1000"])[:n],
        "world_regions": sorted((a for a in aggregates if a["iso3"] in geo),
                                key=lambda a: a["net_migration"], reverse=True) if region is None else [],
        "measure": WB_MEASURE,
        "source": "worldbank",
        "timestamp": _now(),
    }
