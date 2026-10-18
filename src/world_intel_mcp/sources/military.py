"""Military aviation tracking source for world-intel-mcp.

Provides real-time military aircraft tracking via OpenSky Network and
aircraft detail lookups via hexdb.io (free, no API key).
"""

import asyncio
import base64
import logging
import os
from datetime import datetime, timezone

from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.military")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_OPENSKY_STATES_URL = "https://opensky-network.org/api/states/all"
_ADSBLOL_MIL_URL = "https://api.adsb.lol/v2/mil"
_HEXDB_BASE_URL = "https://hexdb.io/api/v1/aircraft"

# ICAO hex prefix ranges known to be allocated to military operators.
MILITARY_ICAO_PREFIXES = [
    "AE",                                  # US Military
    "A8", "A9", "AA", "AB", "AC", "AD",   # US Military extended
    "43C", "43D", "43E", "43F",            # UK Military
    "3F",                                  # Germany Military
    "3A8", "3A9", "3AA", "3AB",            # France Military
    "500", "501", "502",                   # Israel Military
    "70",                                  # Pakistan Military
    "C0",                                  # Canada Military
]

# Callsign prefixes commonly used by military flights.
MILITARY_CALLSIGN_PREFIXES = [
    "RCH", "DUKE", "REACH", "EVAC", "JAKE", "TOPCAT", "BOXER",
    "IRON", "CASA", "NATO", "LAGR", "TEAL", "SAM", "EXEC",
    "SPAR", "VALOR", "BLADE",
]

# ICAO hex prefix → country mapping for military aircraft.
_ICAO_COUNTRY: list[tuple[str, str]] = [
    ("AE", "United States"), ("A8", "United States"), ("A9", "United States"),
    ("AA", "United States"), ("AB", "United States"), ("AC", "United States"),
    ("AD", "United States"),
    ("43C", "United Kingdom"), ("43D", "United Kingdom"),
    ("43E", "United Kingdom"), ("43F", "United Kingdom"),
    ("3F", "Germany"), ("3A8", "France"), ("3A9", "France"),
    ("3AA", "France"), ("3AB", "France"),
    ("500", "Israel"), ("501", "Israel"), ("502", "Israel"),
    ("70", "Pakistan"), ("C0", "Canada"),
    ("34", "Italy"), ("3C", "Germany"),
    ("E4", "Brazil"), ("71", "Turkey"),
    ("50", "Israel"), ("48", "Netherlands"),
    ("44", "Austria"), ("45", "Belgium"),
    ("46", "Bulgaria"), ("49", "Denmark"),
    ("4A", "Finland"), ("39", "France"),
    ("4B", "Greece"), ("4D", "Hungary"),
    ("4C", "Ireland"), ("30", "Italy"),
    ("73", "Japan"), ("78", "China"),
    ("7C", "Australia"), ("C8", "Australia"),
]


def _icao_to_country(icao24: str) -> str:
    """Derive country from ICAO24 hex address prefix."""
    upper = icao24.upper()
    # Try longest prefixes first for specificity
    for prefix, country in sorted(_ICAO_COUNTRY, key=lambda x: -len(x[0])):
        if upper.startswith(prefix):
            return country
    return ""

# Theater bounding boxes for global military posture assessment.
THEATERS = {
    "european": {"bbox": "35,-25,72,45", "desc": "NATO/Russia theater"},
    "indo_pacific": {"bbox": "-10,95,55,155", "desc": "China/Taiwan/SCS"},
    "middle_east": {"bbox": "10,25,45,65", "desc": "Persian Gulf/Red Sea"},
    "arctic": {"bbox": "65,-180,90,180", "desc": "Arctic region"},
    "korean_peninsula": {"bbox": "33,124,43,132", "desc": "Korean DMZ"},
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _is_military_icao(icao24: str) -> bool:
    """Check whether an ICAO24 hex address falls within known military ranges."""
    upper = icao24.upper()
    for prefix in MILITARY_ICAO_PREFIXES:
        if upper.startswith(prefix):
            return True
    return False


def _is_military_callsign(callsign: str | None) -> bool:
    """Check whether a callsign matches known military callsign patterns."""
    if not callsign:
        return False
    cs = callsign.strip().upper()
    for prefix in MILITARY_CALLSIGN_PREFIXES:
        if cs.startswith(prefix):
            return True
    return False


def _build_opensky_auth_headers() -> dict[str, str] | None:
    """Build HTTP Basic auth header from OpenSky env vars, or None if unset."""
    client_id = os.environ.get("OPENSKY_CLIENT_ID")
    client_secret = os.environ.get("OPENSKY_CLIENT_SECRET")
    if client_id and client_secret:
        credentials = base64.b64encode(
            f"{client_id}:{client_secret}".encode()
        ).decode()
        return {"Authorization": f"Basic {credentials}"}
    return None


def _extract_aircraft(state: list) -> dict:
    """Extract a structured aircraft dict from an OpenSky state vector."""
    return {
        "icao24": state[0],
        "callsign": state[1].strip() if state[1] else None,
        "origin_country": state[2],
        "latitude": state[6],
        "longitude": state[5],
        "altitude_m": state[7],
        "velocity_ms": state[9],
        "heading": state[10],
        "on_ground": state[8],
        "squawk": state[14],
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

class _FlightRead:
    """One source's military aircraft, with how old the copy is.

    stale_age_seconds is None for a live answer. When the live fetch fails
    the fetcher may hand back its last cached copy, marked _stale with
    _stale_age_seconds; those markers used to be dropped here, so a copy
    days old read as current flights.
    """

    __slots__ = ("aircraft", "stale_age_seconds")

    def __init__(self, aircraft: list[dict], stale_age_seconds: float | None) -> None:
        self.aircraft = aircraft
        self.stale_age_seconds = stale_age_seconds


def _stale_age(data: object) -> float | None:
    if isinstance(data, dict) and data.get("_stale"):
        age = data.get("_stale_age_seconds")
        return float(age) if isinstance(age, (int, float)) else 0.0
    return None


def _parse_bbox(bbox: str) -> tuple[float, float, float, float] | None:
    """lamin, lomin, lamax, lomax from "lamin,lomin,lamax,lomax", or None
    when the box is malformed: not exactly four comma-separated finite
    numbers, a latitude outside -90..90, a longitude outside -180..180, or
    a minimum not below its maximum. A 3-part or semicolon-split box used
    to skip the filter and return every aircraft in the world as the box's.
    """
    parts = bbox.split(",")
    if len(parts) != 4:
        return None
    try:
        la_min, lo_min, la_max, lo_max = (float(p) for p in parts)
    except ValueError:
        return None
    # The chained comparisons are False for nan and inf too.
    if not (-90 <= la_min < la_max <= 90 and -180 <= lo_min < lo_max <= 180):
        return None
    return la_min, lo_min, la_max, lo_max


def _in_bbox(aircraft: list[dict], box: tuple[float, float, float, float]) -> list[dict]:
    la_min, lo_min, la_max, lo_max = box
    return [
        a for a in aircraft
        if a["latitude"] is not None and a["longitude"] is not None
        and la_min <= a["latitude"] <= la_max
        and lo_min <= a["longitude"] <= lo_max
    ]


async def _fetch_adsblol_military(fetcher: Fetcher) -> _FlightRead | None:
    """Fetch military aircraft from adsb.lol free API (pre-filtered)."""
    data = await fetcher.get_json(
        url=_ADSBLOL_MIL_URL,
        source="adsblol",
        cache_key="military:adsblol:mil",
        cache_ttl=300,
    )
    if data is None:
        return None

    aircraft: list[dict] = []
    for ac in data.get("ac") or []:
        lat = ac.get("lat")
        lon = ac.get("lon")
        if lat is None or lon is None:
            continue
        icao24 = ac.get("hex", "")
        aircraft.append({
            "icao24": icao24,
            "callsign": (ac.get("flight") or "").strip() or None,
            "origin_country": _icao_to_country(icao24),
            "latitude": lat,
            "longitude": lon,
            "altitude_m": ac.get("alt_baro"),
            "velocity_ms": ac.get("gs"),
            "heading": ac.get("track"),
            "on_ground": ac.get("alt_baro") == "ground",
            "squawk": ac.get("squawk"),
            "aircraft_type": ac.get("t"),
            "registration": ac.get("r"),
        })
    return _FlightRead(aircraft, _stale_age(data))


async def _fetch_opensky_military(
    fetcher: Fetcher, bbox: tuple[float, float, float, float] | None = None,
) -> _FlightRead | None:
    """Fetch military aircraft from OpenSky (requires filtering).

    bbox is an already validated (lamin, lomin, lamax, lomax).
    """
    params: dict[str, str] = {}
    if bbox is not None:
        for key, value in zip(("lamin", "lomin", "lamax", "lomax"), bbox):
            params[key] = str(int(value)) if value.is_integer() else repr(value)

    headers = _build_opensky_auth_headers()
    cache_label = ",".join(params.values()) if params else "global"

    data = await fetcher.get_json(
        url=_OPENSKY_STATES_URL,
        source="opensky",
        cache_key=f"military:flights:{cache_label}",
        cache_ttl=300,
        headers=headers,
        params=params if params else None,
    )

    if data is None:
        return None

    states = data.get("states") or []
    aircraft: list[dict] = []
    for state in states:
        if not state or len(state) < 17:
            continue
        icao24 = state[0] or ""
        callsign = state[1] or ""
        if _is_military_icao(icao24) or _is_military_callsign(callsign):
            aircraft.append(_extract_aircraft(state))
    return _FlightRead(aircraft, _stale_age(data))


async def fetch_military_flights(
    fetcher: Fetcher,
    bbox: str | None = None,
) -> dict:
    """Fetch current military aircraft positions.

    Tries adsb.lol (free, pre-filtered military endpoint) first,
    falls back to OpenSky Network if unavailable.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        bbox: Optional bounding box as "lamin,lomin,lamax,lomax": four
            comma-separated numbers, latitudes -90..90 and longitudes
            -180..180, each minimum below its maximum (a box crossing the
            antimeridian is two boxes). Anything else returns error /
            reason "invalid_bbox" without fetching.

    Returns:
        Dict with aircraft list, count, filter description, source, and timestamp.
        When neither source answers live, source is "none" and the dict also
        carries error, degraded and reason: the count is unknown, not zero.
        An expired cached copy is never returned as current flights: when
        it is all that is left it goes under stale_copy, with its source,
        as_of and age, and aircraft stays empty.
    """
    box: tuple[float, float, float, float] | None = None
    if bbox is not None:
        box = _parse_bbox(bbox)
        if box is None:
            return {
                "aircraft": [],
                "count": 0,
                "bbox": bbox,
                "source": "none",
                "timestamp": _utc_now_iso(),
                "error": (
                    f"invalid bbox {bbox!r}: expected lamin,lomin,lamax,lomax as four "
                    "comma-separated numbers, latitudes -90..90, longitudes "
                    "-180..180, each minimum below its maximum (split a box that "
                    "crosses the antimeridian in two)"
                ),
                "reason": "invalid_bbox",
            }

    # Primary: adsb.lol (free, dedicated military endpoint, no filtering needed)
    adsblol = await _fetch_adsblol_military(fetcher)
    adsblol_live = adsblol is not None and adsblol.stale_age_seconds is None
    # A live answer with aircraft elsewhere but none in the box is a real zero.
    adsblol_any = adsblol is not None and bool(adsblol.aircraft)
    if adsblol is not None and box is not None:
        # Apply bbox filter client-side if requested
        adsblol.aircraft = _in_bbox(adsblol.aircraft, box)
    if adsblol_live and adsblol_any:
        return {
            "aircraft": adsblol.aircraft,
            "count": len(adsblol.aircraft),
            "military_filter": "adsblol_mil_endpoint",
            "source": "adsb.lol",
            "timestamp": _utc_now_iso(),
        }

    # Fallback: OpenSky Network, tried live before any cached copy is
    # considered.
    logger.debug("adsb.lol unavailable, trying OpenSky")
    opensky = await _fetch_opensky_military(fetcher, box)
    if opensky is not None and opensky.stale_age_seconds is None:
        return {
            "aircraft": opensky.aircraft,
            "count": len(opensky.aircraft),
            "military_filter": "icao_prefix+callsign",
            "source": "opensky",
            "timestamp": _utc_now_iso(),
        }

    # Neither source answered live. This used to read as an empty sky (count
    # 0, told apart only by source "none"), and an expired cached copy used
    # to come back as current flights stamped now; the keys stay for the
    # consumers that read them, and error / degraded / reason mark the outage.
    if adsblol_live:
        detail = "adsb.lol returned no positioned aircraft and OpenSky failed"
    else:
        detail = "adsb.lol and OpenSky both failed"
    response = {
        "aircraft": [],
        "count": 0,
        "military_filter": "icao_prefix+callsign",
        "source": "none",
        "timestamp": _utc_now_iso(),
        "error": f"No military flight source available ({detail})",
        "degraded": True,
        "reason": "military_flights_unavailable",
        "data_gaps": [],
    }
    copies = [
        (name, read) for name, read in (("adsb.lol", adsblol), ("opensky", opensky))
        if read is not None and read.stale_age_seconds is not None
    ]
    if copies:
        # Prefer a copy that holds aircraft; adsb.lol first.
        name, read = next(((n, r) for n, r in copies if r.aircraft), copies[0])
        age = read.stale_age_seconds
        as_of = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() - age, tz=timezone.utc
        ).isoformat()
        response["stale_copy"] = {
            "source": name,
            "aircraft": read.aircraft,
            "count": len(read.aircraft),
            "as_of": as_of,
            "age_seconds": age,
        }
        response["stale_age_seconds"] = age
        response["error"] = (
            f"No live military flight source ({detail}); only an expired cached "
            f"copy from {name}, {age:.0f} s old, remains (stale_copy)"
        )
        response["data_gaps"].append(
            f"no live military flight source answered; the {len(read.aircraft)} "
            f"aircraft in stale_copy are {name}'s cached copy as of {as_of} "
            f"({age:.0f} s old), not current positions, and are not counted"
        )
    else:
        response["data_gaps"].append(
            "no military flight source answered: the aircraft count is unknown, not zero"
        )
    return response


async def fetch_theater_posture(fetcher: Fetcher) -> dict:
    """Assess global military air posture across 5 theater regions.

    Calls fetch_military_flights for each theater in parallel and aggregates
    the results into a per-theater summary.

    A theater that no flight source answered for (fetch_military_flights
    source "none") has an unknown count, not zero: it is named in
    unavailable_theaters and data_gaps and the response is degraded; with
    every theater unavailable it carries error / reason. Its count stays 0
    for consumers that read the field directly. A theater for which only an
    expired cached copy answered is unavailable too, and is also named in
    stale_theaters (its stale_copy_age_seconds gives the copy's age). Only a
    complete, live posture is cached, so an outage is not replayed after the
    sources recover.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with per-theater summaries (each with the source that answered),
        total count, data_gaps, source, and timestamp.
    """
    # Check cache first to avoid redundant parallel fetches.
    cached = fetcher.cache.get("military:posture")
    if cached is not None:
        return cached

    async def _fetch_theater(name: str, info: dict) -> tuple[str, dict]:
        result = await fetch_military_flights(fetcher, bbox=info["bbox"])
        aircraft_list = result.get("aircraft", [])

        countries: set[str] = set()
        callsigns: list[str] = []
        for ac in aircraft_list:
            if ac.get("origin_country"):
                countries.add(ac["origin_country"])
            if ac.get("callsign"):
                callsigns.append(ac["callsign"])

        summary = {
            "count": len(aircraft_list),
            "countries": sorted(countries),
            "sample_callsigns": callsigns[:5],
            "bbox": info["bbox"],
            "description": info["desc"],
            "source": result.get("source"),
        }
        if result.get("stale_copy"):
            summary["stale_copy_age_seconds"] = result["stale_copy"]["age_seconds"]
        return (name, summary)

    tasks = [
        _fetch_theater(name, info) for name, info in THEATERS.items()
    ]
    results = await asyncio.gather(*tasks)

    theaters: dict[str, dict] = {}
    total = 0
    for name, summary in results:
        theaters[name] = summary
        total += summary["count"]

    response = {
        "theaters": theaters,
        "total_military_aircraft": total,
        "data_gaps": [],
        "source": "opensky",
        "timestamp": _utc_now_iso(),
    }

    # An outage used to come back as zero aircraft in every theater, the
    # reading of an empty sky, and fleet readiness scored it LOW_ACTIVITY.
    unavailable = [name for name, summary in theaters.items() if summary["source"] == "none"]
    if unavailable:
        response["unavailable_theaters"] = unavailable
        response["degraded"] = True
        response["data_gaps"].append(
            "neither adsb.lol nor OpenSky returned military flights for "
            f"{', '.join(unavailable)}: their counts are unknown, not zero, and "
            "total_military_aircraft counts only the other theaters"
        )
        stale = [name for name in unavailable if "stale_copy_age_seconds" in theaters[name]]
        if stale:
            # An expired cached copy used to read as current flights, and a
            # posture built from it was cached as complete.
            oldest = max(theaters[name]["stale_copy_age_seconds"] for name in stale)
            response["stale_theaters"] = stale
            response["data_gaps"].append(
                f"for {', '.join(stale)} only an expired cached copy answered "
                f"(up to {oldest:.0f} s old); it is not counted as current flights"
            )
        if len(unavailable) == len(theaters):
            response["error"] = (
                "No military flight source available for any theater "
                "(neither adsb.lol nor OpenSky returned military flights)"
            )
            response["reason"] = "military_flights_unavailable"
        return response

    fetcher.cache.set("military:posture", response, 300)
    return response


async def fetch_aircraft_details_batch(fetcher: Fetcher, icao24_list: list[str]) -> dict:
    """Look up multiple aircraft by ICAO24 hex codes in parallel.

    Args:
        fetcher: Shared HTTP fetcher.
        icao24_list: List of ICAO24 hex addresses (max 20).

    Returns:
        Dict with aircraft[] details, count, source, timestamp.
    """
    icao24_list = icao24_list[:20]  # cap at 20

    async def _lookup(icao: str) -> dict | None:
        result = await fetch_aircraft_details(fetcher, icao)
        ac = result.get("aircraft", {})
        if not ac:
            return None
        ac["icao24"] = icao
        return ac

    tasks = [_lookup(icao) for icao in icao24_list]
    results = await asyncio.gather(*tasks)

    aircraft = [a for a in results if a is not None]
    return {
        "aircraft": aircraft,
        "count": len(aircraft),
        "requested": len(icao24_list),
        "source": "hexdb",
        "timestamp": _utc_now_iso(),
    }


async def fetch_aircraft_details(fetcher: Fetcher, icao24: str) -> dict:
    """Look up detailed aircraft information from hexdb.io (free, no API key).

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        icao24: The ICAO 24-bit hex address of the aircraft.

    Returns:
        Dict with aircraft detail payload, source, and timestamp.
    """
    url = f"{_HEXDB_BASE_URL}/{icao24}"

    data = await fetcher.get_json(
        url=url,
        source="hexdb",
        cache_key=f"military:aircraft:{icao24}",
        cache_ttl=3600,
    )

    if data is None:
        logger.warning("hexdb.io returned no data for icao24=%s", icao24)
        return {
            "aircraft": {},
            "source": "hexdb",
            "timestamp": _utc_now_iso(),
        }

    return {
        "aircraft": data,
        "source": "hexdb",
        "timestamp": _utc_now_iso(),
    }
