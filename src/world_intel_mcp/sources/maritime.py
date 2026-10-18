"""NGA Maritime Safety Information source for world-intel-mcp.

Provides navigational warnings from the National Geospatial-Intelligence Agency
(NGA) Maritime Safety Information (MSI) broadcast warnings API.
No API key required.
"""

import logging
import re
import time
from datetime import datetime, timedelta, timezone

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
#   4, 12    that endpoint names them NAVAREA IV and NAVAREA XII. 12 carries
#            only XII (single_navarea True). 4 also carries Canadian Arctic
#            warnings: live 4:2023-808 "HUDSON BAY. CANADA." names "NAVAREA
#            XVIII 56/23" in its authority field (2026-10-17 capture), so
#            XVIII is listed for it and single_navarea is False.
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
        "unlisted_navareas": ("XVIII",),
        "single_navarea": False,
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

# An issue date further ahead than this is a typo (one "MAY 2042" among
# 2024 warnings used to make a frozen feed read 5,684 days fresh), so it is
# left out of the freshness check and named in data_gaps.
_FUTURE_TOLERANCE = timedelta(days=1)

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
    Degrees-minutes only and unvalidated; _parse_warning takes its lat/lon
    from extract_positions instead.
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


# One coordinate: degrees-minutes ('22-16.65') or degrees-minutes-seconds
# ('20-03-44'). Latitude and longitude are read separately, so a mixed pair
# ('20-03-44N 072-58.00W') parses as written: one pattern for the whole
# lat/lon pair fixed that, where two separate patterns read the pair's
# '03-44N' as latitude 3.73, about 1,800 km off. The lookbehinds below do
# something else: they keep a match from starting inside a longer number
# or word, so a malformed coordinate ('20-03-44-10N', '2O-03-44N' with a
# letter O, '20-O3-44N', chart number '26060-03.5N') reads as no position
# rather than a confident wrong one. _POSITION_RE's lookbehind also
# excludes letters ('20-O3-44N' read as 3.73N); widening it changed no
# position in 3,643 real warnings (2024 and live). _COORD_TOKEN_RE keeps
# the digit-only one, so a mistyped coordinate still counts as a stated
# position (unplaced as position_unreadable, not by its area code).
_COORD_PART = r"(\d{1,3})-(\d{1,2}(?:\.\d+)?)(?:-(\d{1,2}(?:\.\d+)?))?"

# A lat/lon pair. Latitude and longitude may be separated by whitespace, a
# comma or a slash: live HYDROLANT 2024-473, 479 and 488 write
# '60-17.40S, 050-16.80W', which a whitespace-only separator never read.
_POSITION_RE = re.compile(
    rf"(?<![\w.-]){_COORD_PART}\s*([NS])(?:\s*[,/]\s*|\s+){_COORD_PART}\s*([EW])"
)

# Any single latitude or longitude ('24-30N', '080-00W', '65-07.80 S'),
# paired or not: a warning that gives its area as latitude bands or
# meridians does state a position, just not one point.
_COORD_TOKEN_RE = re.compile(
    r"(?<![\d.-])\d{1,3}-\d{1,2}(?:\.\d+)?(?:-\d{1,2}(?:\.\d+)?)?\s*[NSEW]\b"
)


def _decimal_degrees(degrees: str, minutes: str, seconds: str | None) -> float | None:
    """Decimal degrees from NGA's degree, minute and optional second fields,
    or None when the fields are malformed: minutes or seconds of 60 or
    more, or decimal minutes followed by seconds."""
    if seconds is None:
        whole_minutes = float(minutes)
        if whole_minutes >= 60:
            return None
        return int(degrees) + whole_minutes / 60
    if "." in minutes or int(minutes) >= 60 or float(seconds) >= 60:
        return None
    return int(degrees) + int(minutes) / 60 + float(seconds) / 3600


def extract_positions(text: str) -> list[tuple[float, float]]:
    """Every lat/lon position stated in NGA warning text, in text order.

    Reads degrees-minutes ('22-16.65N 097-44.48W'),
    degrees-minutes-seconds ('20-03-44N 072-58-02W') and the two mixed,
    with whitespace, a comma or a slash between latitude and longitude. A
    pair with minutes or seconds of 60 or more, a latitude past 90 or a
    longitude past 180 is skipped as malformed rather than guessed at.
    """
    positions: list[tuple[float, float]] = []
    for m in _POSITION_RE.finditer(text):
        lat = _decimal_degrees(m.group(1), m.group(2), m.group(3))
        lon = _decimal_degrees(m.group(5), m.group(6), m.group(7))
        if lat is None or lon is None or lat > 90 or lon > 180:
            continue
        positions.append(
            (
                round(-lat if m.group(4) == "S" else lat, 4),
                round(-lon if m.group(8) == "W" else lon, 4),
            )
        )
    return positions


def mentions_position(text: str) -> bool:
    """True when the text holds any latitude or longitude, readable or not,
    so a warning whose only stated position is malformed (a live 2023
    HYDROPAC text gives '10-52.0N 154-84.0W') or given as bands ('BETWEEN
    24-30N AND 25-00N, WEST OF 080-00W') is not mistaken for one that
    states no position at all."""
    return bool(_COORD_TOKEN_RE.search(text))


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


def _freshness(
    warnings: list[dict], now: datetime
) -> tuple[str | None, float | None, list[dict]]:
    """(newest issue date as ISO 8601, its age in days, future-dated
    warnings) over the warnings whose issue date parses. A date more than
    _FUTURE_TOLERANCE ahead of now is left out of the newest date and
    returned in the third item; (None, None, ...) when no date is usable."""
    dates: list[datetime] = []
    future: list[dict] = []
    for w in warnings:
        issued = _parse_issue_date(w.get("issue_date"))
        if issued is None:
            continue
        if issued - now > _FUTURE_TOLERANCE:
            future.append(w)
        else:
            dates.append(issued)
    if not dates:
        return None, None, future
    newest = max(dates)
    age_days = (now - newest).total_seconds() / 86400
    return newest.strftime("%Y-%m-%dT%H:%M:%SZ"), round(age_days, 1), future


def _warning_label(warning: dict) -> str:
    """'<area code>:<id>' for data_gaps text. The id alone is ambiguous:
    NGA numbers each series separately, so one YEAR-NUMBER can name two
    active warnings (live 2024-12 is both a NAVAREA IV and a HYDROPAC one)."""
    return f"{warning.get('navarea') or '?'}:{warning.get('id')}"


# navarea filter values, mapped to the NGA area code each selects: the code
# itself, its series name from the navareas endpoint, and the Roman numeral
# of the two NAVAREAs that have a code of their own. The API sends only the
# codes, so the numerals the tool documented used to match nothing.
_NAVAREA_FILTER_CODES: dict[str, str] = {
    **{code: code for code in NGA_AREA_CODES},
    **{entry["series"]: code for code, entry in NGA_AREA_CODES.items()},
    "IV": "4",
    "XII": "12",
}


def _navarea_not_carried(requested: str) -> str:
    """data_gaps text for a navarea filter value that selects no area code."""
    numeral = requested.removeprefix("NAVAREA ")
    carriers = [
        f"{code} ({entry['series']})"
        for code, entry in NGA_AREA_CODES.items()
        if numeral in entry["navareas"] + entry["unlisted_navareas"]
    ]
    if carriers:
        return (
            f"NAVAREA {numeral} has no area code of its own in the NGA feed: its "
            "warnings arrive inside series that also carry other NAVAREAs "
            f"({', '.join(carriers)}; see area_codes)"
        )
    return f"no NGA area code, series or NAVAREA is called {requested!r} (see area_codes)"


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

    full_text = warning.get("text", "") or ""
    # Positions come from the full text: reading them after the cut lost
    # any stated past character 500.
    positions = extract_positions(full_text)
    text = full_text
    if len(text) > _MAX_TEXT_LENGTH:
        text = text[:_MAX_TEXT_LENGTH] + "..."

    lat, lon = positions[0] if positions else (None, None)

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
        "positions": [[p_lat, p_lon] for p_lat, p_lon in positions],
        "mentions_position": mentions_position(full_text),
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# An empty answer is kept in the cache this long, not the hour a full feed
# is, so a feed fault is not replayed for an hour after NGA recovers.
_EMPTY_ANSWER_REUSE_SECONDS = 300


def _expire_empty_answer(fetcher: Fetcher, cache_key: str) -> None:
    """Drop a cached empty NGA answer once it is _EMPTY_ANSWER_REUSE_SECONDS
    old, so the next call fetches again. The fetcher caches every answer for
    the TTL it was given; this shortens that for an empty one without
    restamping when it was fetched."""
    cache = getattr(fetcher, "cache", None)
    get_meta = getattr(cache, "get_stale_meta", None)
    if get_meta is None:
        return
    meta = get_meta(cache_key)
    if not isinstance(meta, tuple):
        return
    if time.time() - meta[1] >= _EMPTY_ANSWER_REUSE_SECONDS:
        cache.delete(cache_key)


async def fetch_nav_warnings(
    fetcher: Fetcher,
    navarea: str | None = None,
) -> dict:
    """Fetch active navigational warnings from NGA Maritime Safety Information.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        navarea: Optional NGA area code to filter by ("4", "12", "A", "P",
            "C"), its series name ("HYDROPAC"), or "IV" / "XII" for codes 4
            and 12. A value that selects no area the feed carries returns
            error / reason "navarea_not_carried" and a data_gaps line,
            never a bare count of 0.

    Returns:
        Dict with warnings list (each with every position its full text
        states), count, per-navarea breakdown, NAVAREA definitions, the NGA
        area-code table (area_codes), the newest active warning's issue date
        and age in days, feed_stale, data_gaps, source, and timestamp. A
        failed fetch or an unrecognized payload carries error / degraded /
        reason, so an outage never reads as a quiet sea; a feed whose newest
        active warning is older than STALE_AFTER_DAYS has feed_stale True and
        the response is degraded, with the age named in data_gaps. The
        freshness fields describe the whole active feed, before any navarea
        filter. feed_stale is None when no usable issue date says how current
        the feed is, or when the active feed is empty; either way the
        response is degraded, with the reason in data_gaps, and an empty
        answer is reused from the cache for at most
        _EMPTY_ANSWER_REUSE_SECONDS.
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
        "feed_stale": None,
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

    # Parse the active warnings into structured format
    feed = [_parse_warning(w) for w in raw_warnings if _is_active(w)]
    warnings = feed

    # Filter by NAVAREA if specified
    if navarea is not None:
        requested = " ".join(navarea.upper().split())
        code = _NAVAREA_FILTER_CODES.get(requested)
        accepted = (
            {value for value, c in _NAVAREA_FILTER_CODES.items() if c == code}
            if code
            else {requested}
        )
        warnings = [
            w for w in feed
            if " ".join(str(w.get("navarea", "")).upper().split()) in accepted
        ]
        response["navarea_filter"] = {"requested": navarea, "area_code": code}
        if code is None and not warnings:
            # "IV" used to return count 0 with no marker, which read as a
            # NAVAREA with no active warnings.
            response.update(
                error=(
                    f"navarea {navarea!r} matches no area the NGA feed carries; "
                    "filter by an area code (4, 12, A, P, C) or by IV or XII"
                ),
                reason="navarea_not_carried",
            )
            response["data_gaps"].append(_navarea_not_carried(requested))
            return response

    # Sort by issue date descending (most recent first)
    def _issue_sort_key(w: dict) -> str:
        return w.get("issue_date") or ""

    warnings.sort(key=_issue_sort_key, reverse=True)

    # Summary stats: count per navarea
    by_navarea: dict[str, int] = {}
    for w in warnings:
        na = w.get("navarea", "UNKNOWN")
        by_navarea[na] = by_navarea.get(na, 0) + 1

    # Freshness is the feed's, not the filtered slice's: a frozen feed is
    # frozen for every area, and an area with nothing in force used to come
    # back as a count of 0 dated by nothing.
    newest, age_days, future = _freshness(feed, now)
    response.update(
        warnings=warnings,
        count=len(warnings),
        by_navarea=by_navarea,
        newest_issue_date=newest,
        newest_issue_age_days=age_days,
    )
    if future:
        response["data_gaps"].append(
            f"{len(future)} warning(s) carry an issue date more than "
            f"{_FUTURE_TOLERANCE.days} day ahead, left out of the freshness check: "
            + ", ".join(f"{_warning_label(w)} ({w.get('issue_date')})" for w in future)
        )
    if not feed:
        # NGA answering 200 with no active warnings (or only cancelled ones)
        # used to read as a quiet sea: count 0, no marker, every waterway
        # clear. The feed carried 386 active warnings on 2026-10-17, so an
        # empty one is more likely a fault than calm; feed_stale stays None.
        response["degraded"] = True
        response["data_gaps"].append(
            "NGA returned no active warnings (the feed normally carries "
            "hundreds), so a quiet sea cannot be told from a feed fault and how "
            "current the list is cannot be checked"
        )
        _expire_empty_answer(fetcher, cache_key)
    elif newest is None:
        response["degraded"] = True
        if future:
            response["data_gaps"].append(
                "no warning carries a usable issue date, so how current this "
                "list is cannot be checked"
            )
        else:
            response["data_gaps"].append(
                "no warning carries a parseable issue date, so how current this "
                "list is cannot be checked"
            )
    elif age_days is not None:
        response["feed_stale"] = age_days > STALE_AFTER_DAYS
        if response["feed_stale"]:
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
