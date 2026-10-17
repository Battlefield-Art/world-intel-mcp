"""Country intelligence, risk scoring, signal convergence, and analysis sources.

Provides higher-level analytical functions that combine data from multiple
APIs (ACLED, World Bank, USGS, Ollama, Cloudflare, OpenSky, NASA) into
country briefs, risk scores, instability indices, geographic signal
convergence, focal point detection, signal summaries, temporal anomalies,
hotspot escalation, military surge, vessel tracking, and cascade analysis.
"""

import asyncio
import logging
import math
import os
import re
from datetime import datetime, timezone, timedelta

import httpx

from ..fetcher import Fetcher
from .conflict import acled_query, acled_failure_reason
from ..analysis.focal_points import detect_focal_points
from ..analysis.signals import aggregate_country_signals
from ..analysis.temporal import TemporalBaseline
from ..analysis.instability import (
    compute_cii,
    score_unrest,
    score_conflict_v2,
    score_security,
    score_information,
)
from ..analysis.escalation import score_all_hotspots
from ..analysis.surge import detect_surges, SENSITIVE_REGIONS
from ..analysis.cascade import simulate_cascade
from ..config.countries import (
    TIER1_COUNTRIES,
    INTEL_HOTSPOTS,
    STRATEGIC_WATERWAYS,
    get_event_multiplier,
    match_country_by_name,
)

logger = logging.getLogger("world-intel-mcp.sources.intelligence")

# Shared temporal baseline instance
_temporal = TemporalBaseline()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_WB_BASE = "https://api.worldbank.org/v2/country"
_USGS_ENDPOINT = "https://earthquake.usgs.gov/fdsnws/event/1/query"

_BASELINES = {
    "Syria": 5000,
    "Yemen": 3000,
    "Ukraine": 8000,
    "Myanmar": 4000,
    "Somalia": 2500,
    "Nigeria": 3500,
    "DR Congo": 3000,
    "Afghanistan": 2000,
    "Iraq": 1500,
    "Mali": 2000,
    "Burkina Faso": 2500,
    "Ethiopia": 2000,
    "Sudan": 3000,
    "South Sudan": 1500,
    "Cameroon": 1000,
    "Mozambique": 800,
    "Pakistan": 1200,
    "India": 1000,
    "Colombia": 1500,
    "Mexico": 4000,
}

_FOCUS_COUNTRIES = [
    "SYR",
    "UKR",
    "YEM",
    "MMR",
    "SDN",
    "ETH",
    "NGA",
    "COD",
    "AFG",
    "IRQ",
]

_HOTSPOTS = {
    "middle_east": (33.0, 44.0),
    "east_africa": (5.0, 38.0),
    "south_asia": (30.0, 70.0),
    "eastern_europe": (48.0, 35.0),
    "sahel": (15.0, 2.0),
}

# ISO-3166 alpha-3 to country name for ACLED queries and display.
_ISO3_TO_NAME = {
    "SYR": "Syria",
    "UKR": "Ukraine",
    "YEM": "Yemen",
    "MMR": "Myanmar",
    "SDN": "Sudan",
    "ETH": "Ethiopia",
    "NGA": "Nigeria",
    "COD": "DR Congo",
    "AFG": "Afghanistan",
    "IRQ": "Iraq",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _risk_level(score: float) -> str:
    if score > 150:
        return "critical"
    if score > 100:
        return "elevated"
    if score > 50:
        return "moderate"
    return "low"


# ---------------------------------------------------------------------------
# Function 1: Country Intelligence Brief
# ---------------------------------------------------------------------------


async def fetch_country_brief(
    fetcher: Fetcher,
    country_code: str = "US",
) -> dict:
    """Generate a country intelligence brief using local LLM and public data.

    Gathers economic indicators from World Bank and conflict data from ACLED
    in parallel, then optionally enriches with an Ollama-generated analytical
    brief.  Falls back to a data-only summary when Ollama is unavailable.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        country_code: ISO 3166-1 alpha-2 country code (e.g. ``US``, ``UA``).

    Returns:
        Dict with brief text, supporting data, LLM availability flag,
        source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    # --- Gather background data in parallel --------------------------------
    async def _fetch_gdp() -> list:
        url = f"{_WB_BASE}/{country_code}/indicator/NY.GDP.MKTP.CD"
        params = {
            "format": "json",
            "per_page": 5,
            "date": "2020:2025",
        }
        data = await fetcher.get_json(
            url,
            source="world-bank",
            cache_key=f"intel:wb:gdp:{country_code}",
            cache_ttl=86400,
            params=params,
        )
        if data is None:
            return []

        values = []
        try:
            if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
                for rec in data[1]:
                    year = rec.get("date")
                    value = rec.get("value")
                    if year is not None and value is not None:
                        try:
                            values.append({"year": year, "value": float(value)})
                        except (ValueError, TypeError):
                            pass
        except (KeyError, TypeError, IndexError) as exc:
            logger.warning(
                "Failed to parse World Bank GDP for %s: %s", country_code, exc
            )
        return values

    async def _fetch_inflation() -> list:
        url = f"{_WB_BASE}/{country_code}/indicator/FP.CPI.TOTL.ZG"
        params = {
            "format": "json",
            "per_page": 5,
            "date": "2020:2025",
        }
        data = await fetcher.get_json(
            url,
            source="world-bank",
            cache_key=f"intel:wb:inflation:{country_code}",
            cache_ttl=86400,
            params=params,
        )
        if data is None:
            return []

        values = []
        try:
            if isinstance(data, list) and len(data) >= 2 and isinstance(data[1], list):
                for rec in data[1]:
                    year = rec.get("date")
                    value = rec.get("value")
                    if year is not None and value is not None:
                        try:
                            values.append({"year": year, "value": float(value)})
                        except (ValueError, TypeError):
                            pass
        except (KeyError, TypeError, IndexError) as exc:
            logger.warning(
                "Failed to parse World Bank inflation for %s: %s", country_code, exc
            )
        return values

    async def _fetch_acled_count() -> tuple[int, str | None]:
        start_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")

        data = await acled_query(
            fetcher,
            params={
                "limit": 0,
                "event_date": f"{start_date}|{end_date}",
                "event_date_where": "BETWEEN",
                "country": country_code,
            },
            cache_key=f"intel:acled:count:{country_code}",
            cache_ttl=900,
        )
        if data is None:
            # 0 here is ambiguous between "queried, genuinely zero events"
            # and "the fetch failed" — surface the reason via data_gaps
            # below instead of letting the caller assume peace.
            return 0, await acled_failure_reason(data)

        # ACLED returns a count field when limit=0
        try:
            return int(data.get("count", len(data.get("data", [])))), None
        except (ValueError, TypeError):
            return len(data.get("data", [])), None

    gdp_values, inflation_values, (event_count, acled_gap) = await asyncio.gather(
        _fetch_gdp(),
        _fetch_inflation(),
        _fetch_acled_count(),
    )

    # --- Attempt Ollama-generated brief ------------------------------------
    llm_available = False
    brief_text = "LLM brief unavailable. Data summary below."

    prompt = (
        f"Provide a concise 3-paragraph intelligence brief for {country_code}. "
        "Cover: (1) current political stability and governance, "
        "(2) economic outlook and risks, "
        "(3) security concerns and regional dynamics. "
        "Be factual and analytical."
    )

    ollama_url = os.environ.get("OLLAMA_API_URL", "http://localhost:11434")
    model = os.environ.get("OLLAMA_MODEL", "llama3.2:latest")

    try:
        async with httpx.AsyncClient(timeout=30.0, proxy=None) as client:
            resp = await client.post(
                f"{ollama_url}/api/generate",
                json={
                    "model": model,
                    "prompt": prompt,
                    "stream": False,
                },
            )
            resp.raise_for_status()
            resp_data = resp.json()
            generated = resp_data.get("response", "")
            if generated and generated.strip():
                brief_text = generated.strip()
                llm_available = True
    except (httpx.ConnectError, httpx.TimeoutException, httpx.HTTPStatusError) as exc:
        logger.info("Ollama unavailable for country brief (%s): %s", country_code, exc)
    except Exception as exc:
        logger.warning("Unexpected error calling Ollama: %s", exc)

    return {
        "country_code": country_code,
        "brief": brief_text,
        "data": {
            "gdp": gdp_values,
            "inflation": inflation_values,
            "recent_events": event_count,
        },
        "llm_available": llm_available,
        "data_gaps": [acled_gap] if acled_gap else [],
        "source": "country-intelligence",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 2: Country Risk Scores
# ---------------------------------------------------------------------------


async def fetch_risk_scores(
    fetcher: Fetcher,
    limit: int = 20,
) -> dict:
    """Compute country risk scores from ACLED conflict data and baselines.

    Fetches recent global conflict events, counts per country, and computes
    a risk score as ``(events_30d / monthly_baseline) * 100``.  Higher
    scores indicate conflict above historical norms.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        limit: Maximum number of countries to return (sorted by risk).

    Returns:
        Dict with ranked country list, count, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    start_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    data = await acled_query(
        fetcher,
        params={
            "limit": 500,
            "event_date": f"{start_date}|{end_date}",
            "event_date_where": "BETWEEN",
        },
        cache_key="intel:risk:global:30d",
        cache_ttl=1800,
    )

    if data is None:
        reason = await acled_failure_reason(data)
        if reason == "acled_unconfigured":
            return {
                "error": "ACLED credentials not configured (ACLED_EMAIL + ACLED_PASSWORD)",
                "note": "Free academic access at acleddata.com",
                "source": "risk-analysis",
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        # Credentials are fine; the live fetch failed. Previously this
        # branch was unreachable in practice for a configured deployment
        # under outage — it fell through to the same "not configured"
        # message, misdiagnosing a transient failure as a config error.
        return {
            "error": "ACLED fetch failed (rate limited, API error, or malformed response)",
            "degraded": True,
            "source": "risk-analysis",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # Count events per country
    country_counts: dict[str, int] = {}
    for event in data.get("data", []):
        country_name = event.get("country")
        if country_name:
            country_counts[country_name] = country_counts.get(country_name, 0) + 1

    # Compute risk scores
    countries: list[dict] = []
    for country_name, events_30d in country_counts.items():
        baseline_annual = _BASELINES.get(country_name, 500)
        monthly_baseline = baseline_annual / 12.0
        risk_score = (
            (events_30d / monthly_baseline) * 100 if monthly_baseline > 0 else 0.0
        )

        countries.append(
            {
                "country": country_name,
                "events_30d": events_30d,
                "monthly_baseline": round(monthly_baseline, 1),
                "risk_score": round(risk_score, 1),
                "risk_level": _risk_level(risk_score),
            }
        )

    # Sort by risk_score descending, take top N
    countries.sort(key=lambda c: c["risk_score"], reverse=True)
    countries = countries[:limit]

    return {
        "countries": countries,
        "count": len(countries),
        "source": "risk-analysis",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 3: Country Instability Index
# ---------------------------------------------------------------------------


async def fetch_instability_index(
    fetcher: Fetcher,
    country_code: str | None = None,
) -> dict:
    """Compute a Country Instability Index (CII) from multiple signals.

    Combines conflict intensity, economic stress, humanitarian crisis data,
    internet disruption indicators, and military activity into a 0-100
    composite score.  Higher values indicate greater instability.

    When *country_code* is ``None``, returns a simplified index for 10
    focus countries using ACLED data as the primary signal.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        country_code: Optional ISO 3166-1 alpha-3 code (e.g. ``UKR``).

    Returns:
        Dict with instability index, component scores, risk level, source,
        and timestamp.
    """
    now = datetime.now(timezone.utc)

    if country_code is not None:
        return await _instability_single(fetcher, country_code, now)

    return await _instability_multi(fetcher, now)


async def _instability_single(
    fetcher: Fetcher,
    country_code: str,
    now: datetime,
) -> dict:
    """Compute CII v2 instability index for a single country.

    Uses 4 weighted domains: unrest, conflict, security, information.
    Applies country-specific event multiplier and UCDP/displacement boosts.
    """
    country_name = _ISO3_TO_NAME.get(country_code, country_code)
    event_multiplier = get_event_multiplier(country_code)
    start_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    # --- Parallel data gathering -------------------------------------------

    async def _fetch_acled() -> tuple[list[dict], str | None]:
        """Fetch ACLED events for this country."""
        data = await acled_query(
            fetcher,
            params={
                "limit": 500,
                "event_date": f"{start_date}|{end_date}",
                "event_date_where": "BETWEEN",
                "country": country_name,
            },
            cache_key=f"intel:cii2:acled:{country_code}",
            cache_ttl=1800,
        )
        if data is None:
            return [], await acled_failure_reason(data)
        events = data.get("data", []) if isinstance(data, dict) else []
        return events, None

    async def _fetch_outages() -> int:
        """Count internet outages mentioning this country."""
        from . import infrastructure

        result = await infrastructure.fetch_internet_outages(fetcher)
        count = 0
        for outage in result.get("outages", []):
            countries_list = outage.get("countries", [])
            if isinstance(countries_list, list):
                for c in countries_list:
                    if isinstance(c, str) and country_code.lower() in c.lower():
                        count += 1
        return count

    async def _fetch_military() -> int:
        """Count military aircraft near this country."""
        _COUNTRY_BBOX = {
            "SYR": "32,35,37,42",
            "UKR": "44,22,52,40",
            "YEM": "12,42,19,55",
            "MMR": "10,92,28,101",
            "SDN": "8,21,23,39",
            "ETH": "3,33,15,48",
            "NGA": "4,3,14,15",
            "COD": "-13,12,5,31",
            "AFG": "29,60,38,75",
            "IRQ": "29,39,37,49",
            "IRN": "25,44,40,63",
            "ISR": "29,34,33,36",
            "PSE": "31,34,32,35",
            "LBN": "33,35,34,37",
            "TWN": "21,119,26,122",
            "PRK": "37,124,43,131",
        }
        bbox = _COUNTRY_BBOX.get(country_code)
        if bbox is None:
            return 0

        from . import military as mil_mod

        result = await mil_mod.fetch_military_flights(fetcher, bbox=bbox)
        return result.get("count", 0)

    async def _fetch_news_velocity() -> int:
        """Estimate news velocity from GDELT."""
        from . import news

        result = await news.fetch_gdelt_search(
            fetcher,
            query=country_name,
            mode="artlist",
            limit=100,
        )
        return result.get("count", 0)

    (acled_events, acled_gap), outage_count, mil_count, news_vel = await asyncio.gather(
        _fetch_acled(),
        _fetch_outages(),
        _fetch_military(),
        _fetch_news_velocity(),
    )

    # Classify ACLED events into protests/riots vs armed conflict
    protest_count = 0
    riot_count = 0
    conflict_count = 0
    total_fatalities = 0
    for ev in acled_events:
        event_type = (ev.get("event_type") or "").lower()
        fat = 0
        try:
            fat = int(ev.get("fatalities", 0))
        except (ValueError, TypeError):
            pass
        total_fatalities += fat

        if "protest" in event_type:
            protest_count += 1
        elif "riot" in event_type:
            riot_count += 1
        else:
            conflict_count += 1

    # Score each domain (0-25)
    unrest_val = score_unrest(protest_count, riot_count)
    conflict_val = score_conflict_v2(conflict_count, total_fatalities)
    security_val = score_security(mil_count, outage_count)
    info_val = score_information(news_vel)

    # UCDP floor: active wars get a minimum score
    ucdp_floor = None
    country_cfg = TIER1_COUNTRIES.get(country_code)
    if country_cfg and country_cfg.get("baseline_risk", 0) >= 80:
        ucdp_floor = 70.0
    elif country_cfg and country_cfg.get("baseline_risk", 0) >= 60:
        ucdp_floor = 50.0

    # Displacement boost
    displacement_boost = 0.0
    # (Would require UNHCR fetch; simplified: use baseline_risk as proxy)
    if country_cfg and country_cfg.get("baseline_risk", 0) >= 70:
        displacement_boost = 3.0

    cii = compute_cii(
        unrest=unrest_val,
        conflict=conflict_val,
        security=security_val,
        information=info_val,
        event_multiplier=event_multiplier,
        ucdp_floor=ucdp_floor,
        displacement_boost=displacement_boost,
    )

    return {
        "country_code": country_code,
        "country_name": country_name,
        **cii,
        "raw_data": {
            "acled_events": len(acled_events),
            "protests": protest_count,
            "riots": riot_count,
            "conflict_events": conflict_count,
            "fatalities": total_fatalities,
            "military_aircraft": mil_count,
            "internet_outages": outage_count,
            "news_articles": news_vel,
        },
        "data_gaps": [acled_gap] if acled_gap else [],
        "source": "instability-index-v2",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


async def _instability_multi(fetcher: Fetcher, now: datetime) -> dict:
    """Compute CII v2 instability index for focus countries using ACLED."""
    start_date = (now - timedelta(days=30)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    # Fetch global events and bucket by country
    data = await acled_query(
        fetcher,
        params={
            "limit": 500,
            "event_date": f"{start_date}|{end_date}",
            "event_date_where": "BETWEEN",
        },
        cache_key="intel:cii2:multi:global:30d",
        cache_ttl=1800,
    )

    if data is None:
        reason = await acled_failure_reason(data)
        if reason == "acled_unconfigured":
            return {
                "error": "ACLED credentials not configured (ACLED_EMAIL + ACLED_PASSWORD)",
                "source": "instability-index-v2",
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        return {
            "error": "ACLED fetch failed (rate limited, API error, or malformed response)",
            "degraded": True,
            "source": "instability-index-v2",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    # Classify events by country and type
    country_data: dict[str, dict] = {}
    if data is not None:
        events_list = data.get("data", []) if isinstance(data, dict) else []
        for event in events_list:
            country_name = event.get("country")
            if not country_name:
                continue
            if country_name not in country_data:
                country_data[country_name] = {
                    "protests": 0,
                    "riots": 0,
                    "conflict": 0,
                    "fatalities": 0,
                    "total": 0,
                }
            cd = country_data[country_name]
            cd["total"] += 1
            event_type = (event.get("event_type") or "").lower()
            fat = 0
            try:
                fat = int(event.get("fatalities", 0))
            except (ValueError, TypeError):
                pass
            cd["fatalities"] += fat
            if "protest" in event_type:
                cd["protests"] += 1
            elif "riot" in event_type:
                cd["riots"] += 1
            else:
                cd["conflict"] += 1

    # Compute CII v2 for each focus country
    results: list[dict] = []
    for code in _FOCUS_COUNTRIES:
        name = _ISO3_TO_NAME.get(code, code)
        cd = country_data.get(
            name,
            {
                "protests": 0,
                "riots": 0,
                "conflict": 0,
                "fatalities": 0,
                "total": 0,
            },
        )
        multiplier = get_event_multiplier(code)

        unrest_val = score_unrest(cd["protests"], cd["riots"])
        conflict_val = score_conflict_v2(cd["conflict"], cd["fatalities"])
        # Security and information not available in multi-country mode
        security_val = 0.0
        info_val = 0.0

        # UCDP floor from countries config
        country_cfg = TIER1_COUNTRIES.get(code)
        ucdp_floor = None
        if country_cfg and country_cfg.get("baseline_risk", 0) >= 80:
            ucdp_floor = 70.0
        elif country_cfg and country_cfg.get("baseline_risk", 0) >= 60:
            ucdp_floor = 50.0

        cii = compute_cii(
            unrest=unrest_val,
            conflict=conflict_val,
            security=security_val,
            information=info_val,
            event_multiplier=multiplier,
            ucdp_floor=ucdp_floor,
        )

        results.append(
            {
                "country_code": code,
                "country_name": name,
                **cii,
                "events_30d": cd["total"],
            }
        )

    results.sort(key=lambda r: r["instability_index"], reverse=True)

    return {
        "countries": results,
        "count": len(results),
        "note": "Multi-country CII v2 using ACLED unrest + conflict. "
        "Use country_code for full 4-domain analysis.",
        "source": "instability-index-v2",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 4: Signal Convergence
# ---------------------------------------------------------------------------


async def fetch_signal_convergence(
    fetcher: Fetcher,
    lat: float | None = None,
    lon: float | None = None,
    radius_deg: float = 5.0,
) -> dict:
    """Detect geographic convergence of signals in hotspot regions.

    Checks for overlapping seismic activity and other observable signals
    within a radius of known or specified hotspot coordinates.  Higher
    convergence scores indicate multiple signal types in close proximity,
    which may warrant deeper investigation.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        lat: Latitude of center point.  If ``None``, scans 5 global
             hotspot regions.
        lon: Longitude of center point.
        radius_deg: Radius in degrees for bounding box queries.

    Returns:
        Dict with hotspot list, convergence scores, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    if lat is not None and lon is not None:
        regions = {"custom": (lat, lon)}
    else:
        regions = dict(_HOTSPOTS)

    async def _assess_hotspot(name: str, center: tuple[float, float]) -> dict:
        center_lat, center_lon = center

        # Earthquake count within bounding box
        min_lat = center_lat - radius_deg
        max_lat = center_lat + radius_deg
        min_lon = center_lon - radius_deg
        max_lon = center_lon + radius_deg

        starttime = (now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")

        quake_data = await fetcher.get_json(
            _USGS_ENDPOINT,
            source="usgs",
            cache_key=f"intel:convergence:usgs:{name}:{radius_deg}",
            cache_ttl=600,
            params={
                "format": "geojson",
                "minmagnitude": 2.5,
                "starttime": starttime,
                "minlatitude": min_lat,
                "maxlatitude": max_lat,
                "minlongitude": min_lon,
                "maxlongitude": max_lon,
                "limit": 100,
            },
        )

        earthquake_count = 0
        if quake_data is not None:
            earthquake_count = len(quake_data.get("features", []))

        # Convergence score heuristic (0-10)
        # Each signal type present adds to the score.
        score = 0.0

        # Earthquakes: 0-5 points based on count
        if earthquake_count > 0:
            score += min(5.0, (earthquake_count / 20.0) * 5.0)

        # Hotspot presence bonus (known conflict zones get a baseline)
        if name in _HOTSPOTS:
            score += 2.0

        score = min(10.0, round(score, 1))

        return {
            "name": name,
            "lat": center_lat,
            "lon": center_lon,
            "signals": {
                "earthquakes": earthquake_count,
            },
            "convergence_score": score,
        }

    tasks = [_assess_hotspot(name, center) for name, center in regions.items()]
    results = await asyncio.gather(*tasks)

    # Sort by convergence score descending
    hotspots = sorted(results, key=lambda h: h["convergence_score"], reverse=True)

    return {
        "hotspots": hotspots,
        "source": "signal-convergence",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 5: Focal Point Detection
# ---------------------------------------------------------------------------


async def fetch_focal_points(fetcher: Fetcher) -> dict:
    """Gather multi-source events and detect focal points.

    Fetches news headlines, military flights, internet outages, and ACLED
    protests in parallel, normalizes them into events, and runs focal point
    detection to find entities where multiple signals converge.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with focal_points list, count, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    # Import source modules for parallel data gathering
    from . import news, military, infrastructure

    async def _fetch_news_events() -> list[dict]:
        result = await news.fetch_news_feed(fetcher, limit=100)
        events = []
        for item in result.get("items", []):
            title = item.get("title", "")
            # Extract entity: try to match country names from title
            matched_iso = match_country_by_name(title)
            if matched_iso:
                country_cfg = TIER1_COUNTRIES.get(matched_iso)
                entity = country_cfg["name"] if country_cfg else matched_iso
                events.append(
                    {
                        "entity": entity,
                        "type": "news",
                        "timestamp": item.get("published")
                        or now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "country": entity,
                        "weight": 1.0,
                    }
                )
        return events

    async def _fetch_military_events() -> list[dict]:
        result = await military.fetch_theater_posture(fetcher)
        events = []
        for theater_name, theater_data in result.get("theaters", {}).items():
            count = theater_data.get("count", 0)
            if count > 0:
                for country in theater_data.get("countries", []):
                    events.append(
                        {
                            "entity": country,
                            "type": "military",
                            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "country": country,
                            "weight": min(3.0, count / 10.0),
                        }
                    )
        return events

    async def _fetch_outage_events() -> list[dict]:
        result = await infrastructure.fetch_internet_outages(fetcher)
        events = []
        for outage in result.get("outages", []):
            countries_list = outage.get("countries", [])
            if isinstance(countries_list, list):
                for c in countries_list:
                    if c:
                        events.append(
                            {
                                "entity": c,
                                "type": "infrastructure",
                                "timestamp": outage.get("start")
                                or now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "country": c,
                                "weight": 2.0 if outage.get("is_ongoing") else 1.0,
                            }
                        )
        return events

    async def _fetch_protest_events() -> tuple[list[dict], str | None]:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
        data = await acled_query(
            fetcher,
            params={
                "limit": 200,
                "event_date": f"{start_date}|{end_date}",
                "event_date_where": "BETWEEN",
                "event_type": "Protests",
            },
            cache_key="intel:focal:acled:protests:7d",
            cache_ttl=1800,
        )
        events = []
        if data is not None:
            acled_list = data.get("data", []) if isinstance(data, dict) else []
            for ev in acled_list:
                country = ev.get("country")
                if country:
                    events.append(
                        {
                            "entity": country,
                            "type": "protest",
                            "timestamp": ev.get("event_date")
                            or now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                            "country": country,
                            "weight": 1.0,
                        }
                    )
            return events, None
        return events, await acled_failure_reason(data)

    (
        news_events,
        mil_events,
        outage_events,
        (protest_events, acled_gap),
    ) = await asyncio.gather(
        _fetch_news_events(),
        _fetch_military_events(),
        _fetch_outage_events(),
        _fetch_protest_events(),
    )

    all_events = news_events + mil_events + outage_events + protest_events
    focal_points = detect_focal_points(all_events)

    return {
        "focal_points": focal_points,
        "count": len(focal_points),
        "total_events_analyzed": len(all_events),
        "data_gaps": [acled_gap] if acled_gap else [],
        "source": "focal-point-analysis",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 6: Signal Summary
# ---------------------------------------------------------------------------


async def fetch_signal_summary(
    fetcher: Fetcher,
    country: str | None = None,
) -> dict:
    """Run signal aggregator v2 across all domains.

    Fetches ACLED conflict, USGS earthquakes, NASA FIRMS wildfires,
    Cloudflare outages, military flights, and UNHCR displacement in parallel,
    then aggregates signals by country with convergence scoring.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        country: Optional country name to filter results.

    Returns:
        Dict with countries list, count, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    from . import conflict, infrastructure, military, displacement

    async def _fetch_conflict() -> tuple[list[dict], str | None]:
        result = await conflict.fetch_acled_events(fetcher, days=7, limit=200)
        return result.get("events", []), result.get("reason")

    async def _fetch_earthquakes() -> list[dict]:
        from . import seismology

        result = await seismology.fetch_earthquakes(
            fetcher, min_magnitude=4.5, hours=168, limit=100
        )
        return result.get("earthquakes", [])

    async def _fetch_outages() -> list[dict]:
        result = await infrastructure.fetch_internet_outages(fetcher)
        return result.get("outages", [])

    async def _fetch_military() -> list[dict]:
        result = await military.fetch_theater_posture(fetcher)
        aircraft = []
        for theater_data in result.get("theaters", {}).values():
            # Theater posture returns summary, not individual aircraft
            for c in theater_data.get("countries", []):
                aircraft.append(
                    {
                        "origin_country": c,
                        "count": theater_data.get("count", 0),
                    }
                )
        return aircraft

    async def _fetch_protests() -> tuple[list[dict], str | None]:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
        data = await acled_query(
            fetcher,
            params={
                "limit": 200,
                "event_date": f"{start_date}|{end_date}",
                "event_date_where": "BETWEEN",
                "event_type": "Protests",
            },
            cache_key="intel:signals:acled:protests:7d",
            cache_ttl=1800,
        )
        if data is None:
            return [], await acled_failure_reason(data)
        acled_list = data.get("data", []) if isinstance(data, dict) else []
        return [
            {"country": ev.get("country"), "event_type": ev.get("event_type")}
            for ev in acled_list
            if ev.get("country")
        ], None

    async def _fetch_displacement() -> list[dict]:
        result = await displacement.fetch_displacement_summary(fetcher)
        return result.get("by_origin", [])

    (
        (conflict_events, conflict_gap),
        earthquake_data,
        outage_data,
        military_data,
        (protest_data, protest_gap),
        displacement_data,
    ) = await asyncio.gather(
        _fetch_conflict(),
        _fetch_earthquakes(),
        _fetch_outages(),
        _fetch_military(),
        _fetch_protests(),
        _fetch_displacement(),
    )

    aggregated = aggregate_country_signals(
        conflict_events=conflict_events,
        displacement_data=displacement_data,
        earthquake_data=earthquake_data,
        outage_data=outage_data,
        military_data=military_data,
        protest_data=protest_data,
    )

    # Filter to specific country if requested
    if country:
        filtered = {}
        lower_country = country.lower()
        for c_name, c_data in aggregated.items():
            if lower_country in c_name.lower():
                filtered[c_name] = c_data
        aggregated = filtered

    # Convert to list format
    countries_list = [{"country": name, **data} for name, data in aggregated.items()]

    data_gaps = sorted({g for g in (conflict_gap, protest_gap) if g})

    return {
        "countries": countries_list[:50],
        "count": len(countries_list),
        "data_gaps": data_gaps,
        "source": "signal-aggregation-v2",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 7: Temporal Anomaly Detection
# ---------------------------------------------------------------------------


async def fetch_temporal_anomalies(fetcher: Fetcher) -> dict:
    """Record observations and check for temporal anomalies.

    Fetches current counts of military flights (by theater), ACLED events
    (by country), and fires (by region), records each as a temporal
    observation, and reports any that deviate significantly from baselines.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with anomalies list, observations_recorded count, source,
        and timestamp.
    """
    now = datetime.now(timezone.utc)

    from . import military

    anomalies: list[dict] = []
    observations_recorded = 0

    # Military flights by theater
    posture = await military.fetch_theater_posture(fetcher)
    for theater_name, theater_data in posture.get("theaters", {}).items():
        count = theater_data.get("count", 0)
        result = _temporal.record_and_check("military_flights", theater_name, count)
        observations_recorded += 1
        if result is not None:
            anomalies.append(result)

    # ACLED events by country (top focus countries)
    start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")
    data = await acled_query(
        fetcher,
        params={
            "limit": 500,
            "event_date": f"{start_date}|{end_date}",
            "event_date_where": "BETWEEN",
        },
        cache_key="intel:temporal:acled:global:7d",
        cache_ttl=1800,
    )
    acled_gap = None
    if data is not None:
        country_counts: dict[str, int] = {}
        events_list = data.get("data", []) if isinstance(data, dict) else []
        for event in events_list:
            c = event.get("country")
            if c:
                country_counts[c] = country_counts.get(c, 0) + 1

        for c_name, c_count in country_counts.items():
            result = _temporal.record_and_check("acled_events", c_name, c_count)
            observations_recorded += 1
            if result is not None:
                anomalies.append(result)
    else:
        # No ACLED observations were recorded this cycle — silently
        # skipping (the prior behavior) makes a live outage indistinguishable
        # from "nothing happened," which understates the anomaly count.
        acled_gap = await acled_failure_reason(data)

    # Sort anomalies by z_score descending
    anomalies.sort(key=lambda a: a.get("z_score", 0), reverse=True)

    return {
        "anomalies": anomalies,
        "anomaly_count": len(anomalies),
        "observations_recorded": observations_recorded,
        "data_gaps": [acled_gap] if acled_gap else [],
        "source": "temporal-anomaly-detection",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 8: Social Unrest Events (Protests + Riots)
# ---------------------------------------------------------------------------


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


async def fetch_unrest_events(
    fetcher: Fetcher,
    country: str | None = None,
    days: int = 7,
    limit: int = 100,
) -> dict:
    """Fetch social unrest events (protests + riots) from ACLED.

    Filters by event_type in (Protests, Riots).
    Applies Haversine deduplication: merges events within 50 km on the
    same day to remove redundant reports.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        country: Optional country name filter.
        days: Lookback period in days.
        limit: Maximum results from ACLED.

    Returns:
        Dict with events list, count, dedup stats, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    start_date = (now - timedelta(days=days)).strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    params: dict = {
        "limit": limit,
        "event_date": f"{start_date}|{end_date}",
        "event_date_where": "BETWEEN",
        "event_type": "Protests:Riots",
        "event_type_where": "IN",
    }
    if country:
        params["country"] = country

    cache_label = country or "global"
    data = await acled_query(
        fetcher,
        params=params,
        cache_key=f"intel:unrest:{cache_label}:{days}",
        cache_ttl=900,
    )

    if data is None:
        reason = await acled_failure_reason(data)
        if reason == "acled_unconfigured":
            return {
                "error": "ACLED credentials not configured (ACLED_EMAIL + ACLED_PASSWORD)",
                "source": "acled-unrest",
                "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        return {
            "error": "ACLED fetch failed (rate limited, API error, or malformed response)",
            "degraded": True,
            "source": "acled-unrest",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        }

    raw_events = data.get("data", []) if isinstance(data, dict) else []

    # Parse events
    parsed: list[dict] = []
    for ev in raw_events:
        lat_raw = ev.get("latitude")
        lon_raw = ev.get("longitude")
        lat = None
        lon = None
        try:
            lat = float(lat_raw) if lat_raw is not None else None
            lon = float(lon_raw) if lon_raw is not None else None
        except (ValueError, TypeError):
            pass

        fat = 0
        try:
            fat = int(ev.get("fatalities", 0))
        except (ValueError, TypeError):
            pass

        parsed.append(
            {
                "event_date": ev.get("event_date"),
                "event_type": ev.get("event_type"),
                "sub_event_type": ev.get("sub_event_type"),
                "country": ev.get("country"),
                "admin1": ev.get("admin1"),
                "location": ev.get("location"),
                "latitude": lat,
                "longitude": lon,
                "fatalities": fat,
                "actor1": ev.get("actor1"),
                "notes": ev.get("notes"),
            }
        )

    # Haversine deduplication: merge events within 50km on same day
    DEDUP_RADIUS_KM = 50.0
    deduped: list[dict] = []
    original_count = len(parsed)

    for event in parsed:
        lat = event.get("latitude")
        lon = event.get("longitude")
        edate = event.get("event_date")

        is_dup = False
        if lat is not None and lon is not None:
            for existing in deduped:
                if existing.get("event_date") != edate:
                    continue
                ex_lat = existing.get("latitude")
                ex_lon = existing.get("longitude")
                if ex_lat is None or ex_lon is None:
                    continue
                dist = _haversine_km(lat, lon, ex_lat, ex_lon)
                if dist < DEDUP_RADIUS_KM:
                    # Merge: keep higher fatality count
                    if event["fatalities"] > existing["fatalities"]:
                        existing["fatalities"] = event["fatalities"]
                    is_dup = True
                    break

        if not is_dup:
            deduped.append(event)

    return {
        "events": deduped,
        "count": len(deduped),
        "deduplicated": original_count - len(deduped),
        "query": {"country": country, "days": days},
        "source": "acled-unrest",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 9: Hotspot Escalation Scoring
# ---------------------------------------------------------------------------


async def fetch_hotspot_escalation(fetcher: Fetcher) -> dict:
    """Score all 22 intel hotspots using multi-source signals.

    For each hotspot:
    - Fetch GDELT mentions (news velocity near lat/lon)
    - Count military aircraft near hotspot (+/- 2 deg)
    - Count ACLED events near hotspot (+/- 2 deg, last 7 days)

    Runs analysis.escalation.score_all_hotspots().

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with scored hotspots, count, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    from . import military as mil_mod

    # Fetch global data once, then distribute to hotspots
    async def _fetch_global_acled() -> tuple[list[dict], str | None]:
        start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
        end_date = now.strftime("%Y-%m-%d")
        data = await acled_query(
            fetcher,
            params={
                "limit": 500,
                "event_date": f"{start_date}|{end_date}",
                "event_date_where": "BETWEEN",
            },
            cache_key="intel:escalation:acled:global:7d",
            cache_ttl=1800,
        )
        if data is None:
            return [], await acled_failure_reason(data)
        events = data.get("data", []) if isinstance(data, dict) else []
        return events, None

    async def _fetch_global_military() -> list[dict]:
        # Use theater posture for global coverage
        result = await mil_mod.fetch_theater_posture(fetcher)
        aircraft = []
        for theater_data in result.get("theaters", {}).values():
            for ac in theater_data.get("countries", []):
                # Create pseudo-aircraft entries at theater bbox center
                bbox = theater_data.get("bbox", "")
                parts = bbox.split(",")
                if len(parts) == 4:
                    try:
                        lat = (float(parts[0]) + float(parts[2])) / 2
                        lon = (float(parts[1]) + float(parts[3])) / 2
                        for _ in range(
                            theater_data.get("count", 0)
                            // max(1, len(theater_data.get("countries", [1])))
                        ):
                            aircraft.append(
                                {"lat": lat, "lon": lon, "origin_country": ac}
                            )
                    except (ValueError, TypeError):
                        pass
        return aircraft

    (acled_events, acled_gap), military_data = await asyncio.gather(
        _fetch_global_acled(),
        _fetch_global_military(),
    )

    # Build signal dict for each hotspot
    RADIUS_DEG = 2.0
    hotspot_signals: dict[str, dict] = {}

    for hs_name, hs_config in INTEL_HOTSPOTS.items():
        hs_lat = hs_config["lat"]
        hs_lon = hs_config["lon"]

        # Count ACLED events near hotspot
        conflict_count = 0
        protest_count = 0
        fatality_count = 0
        for ev in acled_events:
            try:
                ev_lat = float(ev.get("latitude", 0))
                ev_lon = float(ev.get("longitude", 0))
            except (ValueError, TypeError):
                continue
            if (
                abs(ev_lat - hs_lat) <= RADIUS_DEG
                and abs(ev_lon - hs_lon) <= RADIUS_DEG
            ):
                event_type = (ev.get("event_type") or "").lower()
                if "protest" in event_type:
                    protest_count += 1
                else:
                    conflict_count += 1
                try:
                    fatality_count += int(ev.get("fatalities", 0))
                except (ValueError, TypeError):
                    pass

        # Count military aircraft near hotspot
        mil_count = 0
        for ac in military_data:
            ac_lat = ac.get("lat", 0)
            ac_lon = ac.get("lon", 0)
            if (
                abs(ac_lat - hs_lat) <= RADIUS_DEG
                and abs(ac_lon - hs_lon) <= RADIUS_DEG
            ):
                mil_count += 1

        hotspot_signals[hs_name] = {
            # news_mentions/convergence_score are left unset (None via
            # score_hotspot's default) rather than 0 — no per-hotspot
            # GDELT query or geo-convergence signal is wired up yet, and a
            # hardcoded 0 is indistinguishable from "measured, genuinely
            # quiet." score_all_hotspots renormalizes around the gap.
            "military_count": mil_count,
            "conflict_events": conflict_count,
            "fatalities": fatality_count,
            "protests": protest_count,
        }

    scored = score_all_hotspots(INTEL_HOTSPOTS, hotspot_signals)

    return {
        "hotspots": scored,
        "count": len(scored),
        "unavailable_components": ["news", "convergence"],
        "data_gaps": [acled_gap] if acled_gap else [],
        "source": "hotspot-escalation",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 10: Military Surge Detection
# ---------------------------------------------------------------------------


async def fetch_military_surge(fetcher: Fetcher) -> dict:
    """Detect military surge anomalies across sensitive regions.

    1. Fetch theater posture (existing)
    2. Build temporal baselines for each region
    3. Run analysis.surge.detect_surges()

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with surges list, regions checked, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    from . import military as mil_mod

    posture = await mil_mod.fetch_theater_posture(fetcher)
    theater_data = posture.get("theaters", {})

    # Build temporal baselines for each region
    temporal_baselines: dict[str, dict] = {}
    for region_name in SENSITIVE_REGIONS:
        # Record total aircraft count in the region's matching theaters
        total = 0
        from ..analysis.surge import _THEATER_REGION_MAP

        for theater_name, mapped_regions in _THEATER_REGION_MAP.items():
            if region_name in mapped_regions:
                total += theater_data.get(theater_name, {}).get("count", 0)

        result = _temporal.record_and_check("surge_aircraft", region_name, total)
        if result is not None:
            temporal_baselines[region_name] = {
                "z_score": result["z_score"],
                "multiplier": result.get("multiplier"),
            }

    surges = detect_surges(theater_data, temporal_baselines)

    return {
        "surges": surges,
        "surge_count": len(surges),
        "regions_checked": len(SENSITIVE_REGIONS),
        "source": "military-surge-detection",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ---------------------------------------------------------------------------
# Function 11: Vessel Snapshot at Strategic Waterways
# ---------------------------------------------------------------------------

_NAVAL_KEYWORDS = re.compile(
    r"\b(naval|warship|destroyer|frigate|carrier|submarine|fleet|military\s+vessel|"
    r"exercise|mine|ordnance|firing|weapons)\b",
    re.IGNORECASE,
)

# A warning concerns a waterway when a position stated in its text lies
# within this distance of the waterway's reference point in
# STRATEGIC_WATERWAYS. 300 km is a chosen radius, not a measured boundary:
# it spans each canal end to end and a strait's approaches, but one point
# models the long GIUK Gap and Strait of Malacca only near that point.
_WATERWAY_PROXIMITY_KM = 300.0

# NAVAREAs a waterway lies in, used only for warnings that state no
# position. Only areas the NGA feed emits as single-NAVAREA codes can place
# such a warning (code 4 is NAVAREA IV, code 12 is NAVAREA XII; see
# maritime.NGA_AREA_CODES), and of the nine waterways only the Panama Canal
# lies in either. NGA's own limits (IHO WWNWS13/3/2/IV-XII, 27 July 2021)
# run the IV/XII boundary across the isthmus between 09-08-41N 078-44-03W
# and 08-21-53N 081-39-33W, so the canal's Caribbean end is in IV and its
# Pacific end in XII. IV lies west of 035W in the Atlantic and XII between
# the Americas and the date line, so the other eight waterways are in
# neither.
_WATERWAY_NAVAREAS: dict[str, tuple[str, ...]] = {
    "Panama Canal": ("IV", "XII"),
}

_VESSEL_MATCHING_RULE = (
    "A warning is credited to a waterway when any position stated in its "
    "text lies within proximity_km of the waterway's reference point. A "
    "warning that states no position is credited by NAVAREA only when its "
    "area code names a single NAVAREA (4 is NAVAREA IV, 12 is NAVAREA XII) "
    "that contains the waterway (waterway_navareas). HYDROLANT (A), HYDROPAC "
    "(P) and HYDROARC (C) each span several NAVAREAs (area_codes), so a "
    "positionless warning in those series is credited to no waterway, nor "
    "is one whose only stated positions are malformed; naval ones are "
    "listed in unplaced_naval_warnings. status counts the naval-keyword "
    "warnings credited to the waterway; total_nearby counts credited "
    "warnings of any topic, split by match in nearby_by_match."
)


async def fetch_vessel_snapshot(fetcher: Fetcher) -> dict:
    """Naval activity snapshot at strategic waterways using NGA warnings.

    Uses NGA MSI (maritime.fetch_nav_warnings) filtered for naval/vessel
    keywords. Each warning is credited only to the STRATEGIC_WATERWAYS it
    concerns: a stated position within _WATERWAY_PROXIMITY_KM, or, for a
    warning that states no position, a single-NAVAREA area code that
    contains the waterway. The response states the rule (matching_rule).
    Before 2026-10-17 every warning counted at all nine waterways, so three
    naval warnings anywhere on Earth marked Panama and Hormuz critical alike.
    Scores each waterway: clear/advisory/elevated/critical, or unknown when
    the warning feed itself is unavailable.

    Note: Real-time AIS requires paid API.  This uses NGA MSI as a
    free proxy for naval activity indicators.

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.

    Returns:
        Dict with waterways list (each with total_nearby, the warnings of
        any topic credited to it, and how each was matched), the matching
        rule, naval warnings no rule could place, the feed's newest issue
        date, data_gaps, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    from . import maritime

    nav_data = await maritime.fetch_nav_warnings(fetcher)
    all_warnings = nav_data.get("warnings", []) or []
    feed_error = nav_data.get("error")

    credited: dict[str, dict] = {
        ww["name"]: {"naval": [], "nearby": {"proximity": 0, "navarea": 0}}
        for ww in STRATEGIC_WATERWAYS
    }
    unplaced_naval: list[dict] = []
    naval_total = 0
    truncated = 0

    for warning in all_warnings:
        text = warning.get("text") or ""
        is_naval = bool(_NAVAL_KEYWORDS.search(text))
        naval_total += is_naval
        if len(text) > maritime._MAX_TEXT_LENGTH and text.endswith("..."):
            truncated += 1
        positions = maritime.extract_positions(text)

        matches: list[tuple[str, str, float | None]] = []
        if positions:
            for ww in STRATEGIC_WATERWAYS:
                distance = min(
                    _haversine_km(ww["lat"], ww["lon"], lat, lon)
                    for lat, lon in positions
                )
                if distance <= _WATERWAY_PROXIMITY_KM:
                    matches.append((ww["name"], "proximity", round(distance, 1)))
        else:
            area = maritime.NGA_AREA_CODES.get(str(warning.get("navarea") or "").upper())
            unplaced_reason = None
            if maritime.mentions_position(text):
                # Its only stated positions are malformed; crediting it by
                # NAVAREA would place it at a waterway it does not concern.
                unplaced_reason = "position_unreadable"
            elif area is None:
                unplaced_reason = "unknown_area_code"
            elif not area["single_navarea"]:
                unplaced_reason = "multi_navarea_series"
            else:
                navarea = area["navareas"][0]
                for ww in STRATEGIC_WATERWAYS:
                    if navarea in _WATERWAY_NAVAREAS.get(ww["name"], ()):
                        matches.append((ww["name"], "navarea", None))
            if unplaced_reason and is_naval:
                unplaced_naval.append(
                    {
                        "id": warning.get("id"),
                        "navarea": warning.get("navarea"),
                        "series": area["series"] if area else None,
                        "issue_date": warning.get("issue_date"),
                        "reason": unplaced_reason,
                        "text_snippet": text[:200],
                    }
                )

        for name, basis, distance in matches:
            entry = credited[name]
            entry["nearby"][basis] += 1
            if is_naval:
                entry["naval"].append(
                    {
                        "id": warning.get("id"),
                        "text_snippet": text[:200],
                        "navarea": warning.get("navarea"),
                        "issue_date": warning.get("issue_date"),
                        "match": basis,
                        "distance_km": distance,
                    }
                )

    waterways: list[dict] = []

    for ww in STRATEGIC_WATERWAYS:
        entry = credited[ww["name"]]
        # Nearest stated positions first, then NAVAREA-only matches.
        naval_warnings = sorted(
            entry["naval"],
            key=lambda d: (d["match"] != "proximity", d["distance_km"] or 0.0),
        )
        naval_count = len(naval_warnings)

        if feed_error:
            status = "unknown"
        elif naval_count >= 3:
            status = "critical"
        elif naval_count >= 2:
            status = "elevated"
        elif naval_count >= 1:
            status = "advisory"
        else:
            status = "clear"

        waterways.append(
            {
                "name": ww["name"],
                "lat": ww["lat"],
                "lon": ww["lon"],
                "throughput": ww.get("throughput"),
                "naval_warnings": naval_count,
                "status": status,
                "warning_details": naval_warnings[:5],
                "total_nearby": sum(entry["nearby"].values()),
                "nearby_by_match": dict(entry["nearby"]),
                "naval_by_match": {
                    basis: sum(1 for d in naval_warnings if d["match"] == basis)
                    for basis in ("proximity", "navarea")
                },
            }
        )

    data_gaps = [f"NGA feed: {gap}" for gap in nav_data.get("data_gaps") or []]
    if feed_error and not data_gaps:
        data_gaps.append(f"NGA feed: {feed_error}")
    if unplaced_naval:
        data_gaps.append(
            f"{len(unplaced_naval)} naval warning(s) state no readable position "
            "and no single-NAVAREA area code, so no waterway is credited with "
            "them (unplaced_naval_warnings): "
            + ", ".join(f"{w['id']} ({w['reason']})" for w in unplaced_naval)
        )
    if truncated:
        data_gaps.append(
            f"{truncated} warning text(s) were cut at {maritime._MAX_TEXT_LENGTH} "
            "characters; positions after the cut are not read"
        )

    response = {
        "waterways": waterways,
        "count": len(waterways),
        "total_nav_warnings": len(all_warnings),
        "total_naval_warnings": naval_total,
        "unplaced_naval_warnings": unplaced_naval,
        "matching_rule": {
            "proximity_km": _WATERWAY_PROXIMITY_KM,
            "rule": _VESSEL_MATCHING_RULE,
            "waterway_navareas": {k: list(v) for k, v in _WATERWAY_NAVAREAS.items()},
            "area_codes": maritime.area_codes_view(),
        },
        "newest_issue_date": nav_data.get("newest_issue_date"),
        "newest_issue_age_days": nav_data.get("newest_issue_age_days"),
        "stale_after_days": nav_data.get("stale_after_days"),
        "data_gaps": data_gaps,
        "source": "nga-msi-vessel-snapshot",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if feed_error:
        # Every waterway used to read "clear" when NGA was down.
        response["error"] = f"NGA navigational warnings unavailable: {feed_error}"
        response["degraded"] = True
        response["reason"] = nav_data.get("reason") or "nav_warnings_unavailable"
    elif nav_data.get("degraded"):
        response["degraded"] = True
    return response


# ---------------------------------------------------------------------------
# Function 12: Infrastructure Cascade Analysis
# ---------------------------------------------------------------------------


async def fetch_cascade_analysis(
    fetcher: Fetcher,
    corridor: str | None = None,
) -> dict:
    """Simulate infrastructure cascade from corridor disruption.

    1. Fetch current cable health (existing fetch_cable_health)
    2. If corridor specified, simulate that corridor disrupted
    3. If not, simulate each at_risk/disrupted corridor
    4. Run analysis.cascade.simulate_cascade()

    Args:
        fetcher: Shared HTTP fetcher with caching and circuit breaking.
        corridor: Optional specific corridor to simulate disruption of.

    Returns:
        Dict with scenarios, current health, source, and timestamp.
    """
    now = datetime.now(timezone.utc)

    from . import infrastructure

    health_data = await infrastructure.fetch_cable_health(fetcher)
    corridors_health = health_data.get("corridors", {})

    scenarios: list[dict] = []

    if corridor:
        # Simulate specific corridor disruption
        result = simulate_cascade([corridor], current_health=corridors_health)
        scenarios.append(
            {
                "scenario": f"Disruption of {corridor}",
                "corridors": [corridor],
                **result,
            }
        )
    else:
        # Simulate each at_risk or disrupted corridor
        at_risk_corridors = [
            name
            for name, info in corridors_health.items()
            if info.get("status_score", 0) >= 2
        ]

        if at_risk_corridors:
            # Individual scenarios
            for c in at_risk_corridors:
                result = simulate_cascade([c], current_health=corridors_health)
                scenarios.append(
                    {
                        "scenario": f"Disruption of {c}",
                        "corridors": [c],
                        **result,
                    }
                )

            # Combined worst-case scenario
            if len(at_risk_corridors) >= 2:
                result = simulate_cascade(
                    at_risk_corridors, current_health=corridors_health
                )
                scenarios.append(
                    {
                        "scenario": "Combined disruption (worst case)",
                        "corridors": at_risk_corridors,
                        **result,
                    }
                )
        else:
            # No at-risk corridors; simulate red_sea as a common scenario
            result = simulate_cascade(["red_sea"], current_health=corridors_health)
            scenarios.append(
                {
                    "scenario": "Hypothetical: Red Sea corridor disruption",
                    "corridors": ["red_sea"],
                    **result,
                }
            )

    return {
        "scenarios": scenarios,
        "scenario_count": len(scenarios),
        "current_health": {
            name: {
                "status_score": info.get("status_score"),
                "status_label": info.get("status_label"),
            }
            for name, info in corridors_health.items()
        },
        "source": "cascade-analysis",
        "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
