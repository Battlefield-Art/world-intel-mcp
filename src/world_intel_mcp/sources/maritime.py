"""NGA Maritime Safety Information source for world-intel-mcp.

Provides navigational warnings from the National Geospatial-Intelligence Agency
(NGA) Maritime Safety Information (MSI) broadcast warnings API.
No API key required.
"""

import logging
import re
from datetime import datetime, timezone

from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.maritime")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_NGA_WARNINGS_URL = "https://msi.nga.mil/api/publications/broadcast-warn?output=json"

NAVAREAS = {
    "I": "UK (NE Atlantic, North Sea)",
    "II": "France (Bay of Biscay, W Africa)",
    "III": "Spain (Mediterranean W)",
    "IV": "United States (W Atlantic, Caribbean, Gulf of Mexico)",
    "V": "Brazil (SW Atlantic)",
    "VI": "Argentina (SE Atlantic)",
    "VII": "South Africa (Indian Ocean W)",
    "VIII": "India (Indian Ocean N)",
    "IX": "Pakistan (Arabian Sea, Persian Gulf)",
    "X": "Australia (Indian Ocean S)",
    "XI": "Japan (NW Pacific)",
    "XII": "United States (NE Pacific)",
    "XIII": "Russia (Arctic)",
    "XIV": "New Zealand (SW Pacific)",
    "XV": "Chile (SE Pacific)",
    "XVI": "Peru (E Pacific)",
}

# Area codes the NGA MSI API puts in each warning's navArea field, mapped to
# the NAVAREAS keys above. The API never sends the Roman numerals: each of
# the 386 active warnings in a 2026-10-17 capture carried one of these five
# codes, the set the API's own GET /api/publications/broadcast-warn/navareas
# lists ({"NAVAREA IV": "4", "HYDROLANT": "A", "NAVAREA XII": "12",
# "HYDROPAC": "P", "HYDROARC": "C"}). Where each code's areas come from:
#   4, 12    that endpoint names them NAVAREA IV and NAVAREA XII: one area
#            each, so single_navarea is True.
#   A, P, C  NGA's HYDROLANT, HYDROPAC and HYDROARC series, which rebroadcast
#            other coordinators' warnings (Federal Register 2018-10215: the
#            Atlantic edition carries HYDROLANT, the Pacific edition
#            HYDROPAC, and HYDROARC covers the Arctic the other two leave
#            out). A series lists a NAVAREA when at least one of its warnings
#            names that NAVAREA's coordinator in its authority field, in the
#            2026-10-17 capture or in the 3,480 warnings of msgYear=2024
#            status=all (e.g. HYDROPAC "NAVAREA IX 483/22" on a PERSIAN GULF
#            warning, HYDROLANT "NAVAREA III 118/22" on a BLACK SEA one).
#            IV and XII are NGA's own areas, whose warnings name no
#            coordinator; they sit under A and P because those series carry
#            GULF OF MEXICO and CARIBBEAN, and EASTERN NORTH PACIFIC,
#            warnings. Most HYDROARC coordinators are the Arctic NAVAREAs
#            XVII-XXI, which NAVAREAS does not define (unlisted_navareas).
#            NAVAREAS labels XIII "Russia (Arctic)", but the feed carries
#            NAVAREA XIII warnings for the Sea of Okhotsk and the Sea of
#            Japan under HYDROPAC, so XIII sits under P.
NGA_AREA_CODES: dict[str, dict] = {
    "4": {
        "series": "NAVAREA IV",
        "navareas": ("IV",),
        "unlisted_navareas": (),
        "single_navarea": True,
    },
    "12": {
        "series": "NAVAREA XII",
        "navareas": ("XII",),
        "unlisted_navareas": (),
        "single_navarea": True,
    },
    "A": {
        "series": "HYDROLANT",
        "navareas": ("I", "II", "III", "IV", "V", "VI", "VII", "XV"),
        "unlisted_navareas": (),
        "single_navarea": False,
    },
    "P": {
        "series": "HYDROPAC",
        "navareas": ("VII", "VIII", "IX", "X", "XI", "XII", "XIII", "XIV", "XV", "XVI"),
        "unlisted_navareas": (),
        "single_navarea": False,
    },
    "C": {
        "series": "HYDROARC",
        "navareas": ("I",),
        "unlisted_navareas": ("XVII", "XVIII", "XIX", "XX", "XXI"),
        "single_navarea": False,
    },
}

# A response whose newest active warning is older than this is flagged as
# stale. Measured on the live API (msgYear=2024, status=all: 3,480 warnings
# issued 1 Jan to 10 May 2024): consecutive issues were never more than 0.42
# days apart across all series, or more than 7.0 days apart within one series
# (HYDROARC, the slowest), so 14 days is twice the slowest series' worst gap.
STALE_AFTER_DAYS = 14

_MAX_TEXT_LENGTH = 500


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_active(warning: dict) -> bool:
    """Return True if the warning has no cancelDate (i.e. still active)."""
    cancel = warning.get("cancelDate")
    return cancel is None or cancel == ""


_COORD_RE = re.compile(
    r"(\d{1,3})-(\d{1,2}(?:\.\d+)?)\s*([NS])\s+"
    r"(\d{1,3})-(\d{1,2}(?:\.\d+)?)\s*([EW])",
)


def _extract_coords(text: str) -> tuple[float | None, float | None]:
    """Extract first lat/lon pair from NGA warning text.

    Parses formats like '22-16.65N 097-44.48W' into decimal degrees.
    """
    m = _COORD_RE.search(text)
    if not m:
        return None, None
    lat = int(m.group(1)) + float(m.group(2)) / 60
    if m.group(3) == "S":
        lat = -lat
    lon = int(m.group(4)) + float(m.group(5)) / 60
    if m.group(6) == "W":
        lon = -lon
    return round(lat, 4), round(lon, 4)


# Degrees-minutes-seconds positions ('20-03-44N 072-58-02W'), which NGA uses
# in some light and buoy warnings. _COORD_RE never matches them: it needs a
# hemisphere letter straight after the minutes.
_COORD_DMS_RE = re.compile(
    r"(\d{1,3})-(\d{1,2})-(\d{1,2}(?:\.\d+)?)\s*([NS])\s+"
    r"(\d{1,3})-(\d{1,2})-(\d{1,2}(?:\.\d+)?)\s*([EW])",
)


def extract_positions(text: str) -> list[tuple[float, float]]:
    """Every lat/lon position stated in NGA warning text, in text order.

    Reads degrees-minutes ('22-16.65N 097-44.48W') and
    degrees-minutes-seconds ('20-03-44N 072-58-02W'). A pair with minutes
    or seconds of 60 or more, a latitude past 90 or a longitude past 180 is
    skipped as malformed rather than guessed at.
    """
    found: list[tuple[int, float, float]] = []
    for m in _COORD_RE.finditer(text):
        lat_min, lon_min = float(m.group(2)), float(m.group(5))
        if lat_min >= 60 or lon_min >= 60:
            continue
        lat = int(m.group(1)) + lat_min / 60
        lon = int(m.group(4)) + lon_min / 60
        found.append(
            (m.start(), -lat if m.group(3) == "S" else lat, -lon if m.group(6) == "W" else lon)
        )
    for m in _COORD_DMS_RE.finditer(text):
        lat_m, lat_s = int(m.group(2)), float(m.group(3))
        lon_m, lon_s = int(m.group(6)), float(m.group(7))
        if max(lat_m, lat_s, lon_m, lon_s) >= 60:
            continue
        lat = int(m.group(1)) + lat_m / 60 + lat_s / 3600
        lon = int(m.group(5)) + lon_m / 60 + lon_s / 3600
        found.append(
            (m.start(), -lat if m.group(4) == "S" else lat, -lon if m.group(8) == "W" else lon)
        )
    found.sort()
    return [
        (round(lat, 4), round(lon, 4))
        for _, lat, lon in found
        if abs(lat) <= 90 and abs(lon) <= 180
    ]


def mentions_position(text: str) -> bool:
    """True when the text holds a lat/lon pair in either notation, readable
    or not, so a warning whose only stated position is malformed (a live
    2023 HYDROPAC text gives '10-52.0N 154-84.0W') is not mistaken for one
    that states no position at all."""
    return bool(_COORD_RE.search(text) or _COORD_DMS_RE.search(text))


_ISSUE_DATE_RE = re.compile(r"^(\d{2})(\d{2})(\d{2})Z\s+([A-Z]{3})\s+(\d{4})$")
_MONTHS = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _parse_issue_date(value: object) -> datetime | None:
    """Parse an NGA issue date ('101515Z MAY 2024': day, hour, minute,
    Zulu, month, year) or an ISO 8601 string. None when unparseable.

    The month table is fixed English, not strptime's locale-dependent %b.
    """
    if not isinstance(value, str) or not value.strip():
        return None
    m = _ISSUE_DATE_RE.match(value.strip().upper())
    if m:
        day, hour, minute, month_name, year = m.groups()
        month = _MONTHS.get(month_name)
        if month is None:
            return None
        try:
            return datetime(
                int(year), month, int(day), int(hour), int(minute), tzinfo=timezone.utc
            )
        except ValueError:
            return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _freshness(warnings: list[dict], now: datetime) -> tuple[str | None, float | None]:
    """(newest issue date as ISO 8601, its age in days) over the warnings
    whose issue date parses; (None, None) when none does."""
    dates = [
        d for d in (_parse_issue_date(w.get("issue_date")) for w in warnings) if d
    ]
    if not dates:
        return None, None
    newest = max(dates)
    age_days = (now - newest).total_seconds() / 86400
    return newest.strftime("%Y-%m-%dT%H:%M:%SZ"), round(age_days, 1)


def area_codes_view() -> dict[str, dict]:
    """NGA_AREA_CODES as plain JSON (lists, not tuples) for responses."""
    return {
        code: {
            "series": entry["series"],
            "navareas": list(entry["navareas"]),
            "unlisted_navareas": list(entry["unlisted_navareas"]),
            "single_navarea": entry["single_navarea"],
        }
        for code, entry in NGA_AREA_CODES.items()
    }


def _parse_warning(warning: dict) -> dict:
    """Extract structured fields from a single NGA broadcast warning."""
    msg_year = warning.get("msgYear", "")
    msg_number = warning.get("msgNumber", "")
    warning_id = f"{msg_year}-{msg_number}" if msg_year and msg_number else None

    cancel_date = warning.get("cancelDate")
    status = "cancelled" if cancel_date else "active"

    text = warning.get("text", "") or ""
    if len(text) > _MAX_TEXT_LENGTH:
        text = text[:_MAX_TEXT_LENGTH] + "..."

    lat, lon = _extract_coords(text)

    return {
        "id": warning_id,
        "navarea": warning.get("navArea", ""),
        "subregion": warning.get("subregion", ""),
        "status": status,
        "issue_date": warning.get("issueDate"),
        "cancel_date": cancel_date if cancel_date else None,
        "text": text,
        "authority": warning.get("authority", "NGA"),
        "lat": lat,
        "lon": lon,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def fetch_nav_warnings(
    fetcher: Fetcher,
    navarea: str | None = None,
) -> dict:
    """Fetch active navigational warnings from NGA Maritime Safety Information.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        navarea: Optional NAVAREA identifier (e.g. "IV", "XII") to filter by.

    Returns:
        Dict with warnings list, count, per-navarea breakdown, NAVAREA
        definitions, the NGA area-code table (area_codes), the newest
        warning's issue date and age in days, data_gaps, source, and
        timestamp. A failed fetch or an unrecognized payload carries
        error / degraded / reason, so an outage never reads as a quiet sea;
        a response whose newest warning is older than STALE_AFTER_DAYS is
        degraded, with the age named in data_gaps.
    """
    cache_label = navarea.upper() if navarea else "all"
    cache_key = f"maritime:warnings:{cache_label}"

    data = await fetcher.get_json(
        url=_NGA_WARNINGS_URL,
        source="nga-msi",
        cache_key=cache_key,
        cache_ttl=3600,
    )

    now = datetime.now(timezone.utc)
    response: dict = {
        "warnings": [],
        "count": 0,
        "by_navarea": {},
        "navareas": NAVAREAS,
        "area_codes": area_codes_view(),
        "newest_issue_date": None,
        "newest_issue_age_days": None,
        "stale_after_days": STALE_AFTER_DAYS,
        "data_gaps": [],
        "source": "nga-msi",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if data is None:
        logger.warning("NGA MSI API returned no data")
        # Honest outage shape. Without these markers this return was
        # shape-identical to a sea with no active warnings (the
        # silent-degradation class). The zeroed fields stay for consumers
        # that read them directly; error / degraded / reason say NGA is
        # down, and cli.py's bail path, the dashboard and the collector all
        # key on "error".
        response.update(
            error="NGA MSI API unavailable (no live or cached data)",
            degraded=True,
            reason="nga_msi_fetch_failed",
        )
        response["data_gaps"].append(
            "NGA MSI broadcast warnings unavailable (no live or cached data): "
            "zero warnings here means unknown, not a quiet sea"
        )
        return response

    # The API returns either a list directly or a dict with a data key
    raw_warnings: object = None
    if isinstance(data, list):
        raw_warnings = data
    elif isinstance(data, dict):
        raw_warnings = data.get("broadcast-warn", data.get("data"))
        if isinstance(raw_warnings, dict):
            raw_warnings = [raw_warnings]
    if not isinstance(raw_warnings, list):
        # A payload with no warnings list used to read as zero warnings.
        if isinstance(data, dict):
            shape = f"keys: {sorted(str(k) for k in data)[:5]}"
        else:
            shape = f"type: {type(data).__name__}"
        logger.warning("NGA MSI payload had no warnings list (%s)", shape)
        response.update(
            error=f"NGA MSI returned an unrecognized payload ({shape})",
            degraded=True,
            reason="nga_msi_unexpected_payload",
        )
        response["data_gaps"].append(
            "NGA MSI payload carried no 'broadcast-warn' or 'data' list, so the "
            "active warnings are unknown"
        )
        return response

    if isinstance(data, dict) and data.get("_stale"):
        # The fetcher fell back to its last cached copy after a failed
        # fetch; say so instead of passing old data off as a fresh read.
        response["degraded"] = True
        response["data_gaps"].append(
            "NGA MSI fetch failed; serving a cached copy "
            f"{data.get('_stale_age_seconds')} s old"
        )

    # Filter to active warnings only
    active_raw = [w for w in raw_warnings if _is_active(w)]

    # Filter by NAVAREA if specified
    if navarea is not None:
        navarea_upper = navarea.upper()
        active_raw = [
            w for w in active_raw
            if str(w.get("navArea", "")).upper() == navarea_upper
        ]

    # Parse each warning into structured format
    warnings = [_parse_warning(w) for w in active_raw]

    # Sort by issue date descending (most recent first)
    def _issue_sort_key(w: dict) -> str:
        return w.get("issue_date") or ""

    warnings.sort(key=_issue_sort_key, reverse=True)

    # Summary stats: count per navarea
    by_navarea: dict[str, int] = {}
    for w in warnings:
        na = w.get("navarea", "UNKNOWN")
        by_navarea[na] = by_navarea.get(na, 0) + 1

    newest, age_days = _freshness(warnings, now)
    response.update(
        warnings=warnings,
        count=len(warnings),
        by_navarea=by_navarea,
        newest_issue_date=newest,
        newest_issue_age_days=age_days,
    )
    if warnings and newest is None:
        response["degraded"] = True
        response["data_gaps"].append(
            "no warning carries a parseable issue date, so how current this "
            "list is cannot be checked"
        )
    elif age_days is not None and age_days > STALE_AFTER_DAYS:
        # Measured 2026-10-17: the live API's newest warning was issued
        # 10 May 2024 and it holds none from 2025 or 2026 (status=all),
        # yet this response looked current. Name the age instead.
        response["degraded"] = True
        response["data_gaps"].append(
            f"newest active warning was issued {newest}, {age_days:.0f} days "
            f"ago, past the {STALE_AFTER_DAYS}-day staleness threshold: these "
            "are historical warnings, not current conditions"
        )
    return response
