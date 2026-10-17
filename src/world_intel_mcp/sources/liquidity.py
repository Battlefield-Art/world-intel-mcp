"""Liquidity and money: Fed balance sheet, Treasury General Account,
overnight reverse repo, net liquidity, US M2, euro area M1/M2/M3 and the
Eurosystem balance sheet. Keyless sources only.

- FRED graph CSV (``fred.stlouisfed.org/graph/fredgraph.csv``): the keyless
  download behind every FRED chart; robots.txt allows it with
  ``Crawl-delay: 1``, honored by the fetcher's ``fred-graph`` rate floor.
  The CSV carries observation dates and values only, so units, frequency
  and release come from ``FRED_SERIES`` below (each checked against the
  series page on 2026-10-17) and ``published_at`` is always null.
- ECB Data Portal SDMX-JSON (``data-api.ecb.europa.eu``): units, scale,
  titles and provisional flags come from the response itself.

Every point carries ``observed_at`` (the period the value describes),
``value`` and ``units``; every series says whether it is an amount
outstanding, a flow (transactions within the period), a growth rate or an
index. A blank FRED cell is a missing observation, never a zero, and a
failed component is an error, never a zero term in the net liquidity sum.

Blocked upstreams (measured 2026-10-17, see ROADMAP Phase 29 and
``BLOCKED_SOURCES``): ICI money market fund assets and ETF net issuance
answer 403 on every path, robots.txt included; FINRA margin statistics
answer 200 but the finra.org Terms of Use (Restrictions d and e) forbid
building a database from the site and robots; the BLS public API answers
without a key but api.bls.gov/robots.txt disallows every path. No proxy is
substituted for any of them.
"""

import asyncio
import calendar
import copy
import csv
import io
import logging
import re
from datetime import date, datetime, timezone
from typing import Any

from ..fetcher import Fetcher

logger = logging.getLogger("world-intel-mcp.sources.liquidity")

_FRED_GRAPH_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
_ECB_DATA_URL = "https://data-api.ecb.europa.eu/service/data/{dataset}/{key}"

FRED_SOURCE = "fred-graph"
ECB_SOURCE = "ecb-data-portal"

# FRED drops connections that carry the Fetcher's generic default UA
# (measured 2026-10-17: ReadTimeout after 18 s, 200 in 0.06 s with this
# one), so both upstreams get the project's descriptive UA.
_HEADERS = {"User-Agent": "world-intel-mcp (https://github.com/marc-shade/world-intel-mcp)"}

_FRED_TTL = 3600
_ECB_TTL = 3 * 3600

DEFAULT_LIMIT = 52
_MAX_ECB_SERIES = 50

# Units, frequency and release as stated on each FRED series page
# (checked 2026-10-17). ``max_age_days`` is how old the newest observation
# may be before the response flags it stale: weekly H.4.1 data are as of
# Wednesday and released the next day; RRP is daily on business days;
# M2 for a month is released late in the following month.
FRED_SERIES: dict[str, dict[str, Any]] = {
    "WALCL": {
        "label": "Federal Reserve total assets",
        "detail": "Total assets less eliminations from consolidation, Wednesday level",
        "units": "USD millions",
        "scale": 1_000_000,
        "frequency": "weekly, as of Wednesday",
        "seasonal_adjustment": "not seasonally adjusted",
        "measure": "amount_outstanding",
        "release": "H.4.1 Factors Affecting Reserve Balances",
        "max_age_days": 10,
    },
    "WDTGAL": {
        "label": "Treasury General Account (Wednesday level)",
        "detail": "U.S. Treasury, General Account deposits with F.R. Banks, Wednesday level",
        "units": "USD millions",
        "scale": 1_000_000,
        "frequency": "weekly, as of Wednesday",
        "seasonal_adjustment": "not seasonally adjusted",
        "measure": "amount_outstanding",
        "release": "H.4.1 Factors Affecting Reserve Balances",
        "max_age_days": 10,
    },
    "WTREGEN": {
        "label": "Treasury General Account (week average)",
        "detail": "U.S. Treasury, General Account deposits with F.R. Banks, average of the week ending Wednesday",
        "units": "USD millions",
        "scale": 1_000_000,
        "frequency": "weekly, ending Wednesday (week average)",
        "seasonal_adjustment": "not seasonally adjusted",
        "measure": "amount_outstanding",
        "release": "H.4.1 Factors Affecting Reserve Balances",
        "max_age_days": 10,
    },
    "RRPONTSYD": {
        "label": "Fed overnight reverse repo",
        "detail": (
            "Treasury securities sold by the Federal Reserve in overnight reverse "
            "repurchase operations; each matures the next business day, so a day's "
            "take-up is the amount outstanding overnight"
        ),
        "units": "USD billions",
        "scale": 1_000_000_000,
        "frequency": "daily (business days)",
        "seasonal_adjustment": "not seasonally adjusted",
        "measure": "amount_outstanding",
        "release": "Temporary Open Market Operations",
        "max_age_days": 6,
    },
    "M2SL": {
        "label": "US M2 money stock",
        "detail": "M2, monthly average",
        "units": "USD billions",
        "scale": 1_000_000_000,
        "frequency": "monthly",
        "seasonal_adjustment": "seasonally adjusted",
        "measure": "amount_outstanding",
        "release": "H.6 Money Stock Measures",
        "max_age_days": 100,
    },
}

US_M3_NOTE = (
    "US M3 is discontinued: the Federal Reserve stopped publishing it in 2006 "
    "(FRED M3SL ends at 2006-02-01). There is no current US M3 series."
)
NESTING_NOTE = (
    "M1 is contained in M2 and M2 in M3: the aggregates nest, so they are "
    "never added together."
)

# Net liquidity components and the unit each is converted to before the
# subtraction. RRP may come from up to this many days before the
# Wednesday (a holiday leaves a blank FRED cell); never from after it.
_NET_LIQUIDITY_RRP_LOOKBACK_DAYS = 5
NET_LIQUIDITY_FORMULA = (
    "net_liquidity = WALCL - WDTGAL - RRPONTSYD, in USD billions: WALCL and "
    "WDTGAL are USD millions divided by 1,000; RRPONTSYD is already USD billions"
)
NET_LIQUIDITY_ALIGNMENT = (
    "One point per WALCL Wednesday. WDTGAL must be reported for that same "
    "Wednesday. RRPONTSYD is taken from that Wednesday, or when that day is "
    f"blank, from the latest earlier business day within "
    f"{_NET_LIQUIDITY_RRP_LOOKBACK_DAYS} days; each point names the RRP date "
    "used. Weeks without an aligned TGA or RRP value are skipped and counted, "
    "never filled with zero."
)
TGA_CHOICE_NOTE = (
    "The Treasury General Account term is WDTGAL (Wednesday level), not "
    "WTREGEN (week average), so all three terms describe the same Wednesday."
)
# Do not swap WDTGAL for WTREGEN: WTREGEN averages the week ending
# Wednesday, while WALCL and WDTGAL are Wednesday levels, so the swap
# would subtract a weekly average from a point-in-time balance sheet.
TGA_WTREGEN_NOTE = (
    "WTREGEN is not a drop-in replacement: subtracting a week average from "
    "Wednesday-level total assets mixes two different measures. WTREGEN is "
    "served on its own by intel_fed_liquidity_series."
)
NET_LIQUIDITY_METHOD: dict[str, Any] = {
    "formula": NET_LIQUIDITY_FORMULA,
    "components": {
        "WALCL": {
            "term": "Federal Reserve total assets",
            "observed": "Wednesday level",
            "units": "USD millions",
            "conversion": "divided by 1,000 to USD billions",
        },
        "WDTGAL": {
            "term": "Treasury General Account",
            "observed": "Wednesday level",
            "units": "USD millions",
            "conversion": "divided by 1,000 to USD billions",
        },
        "RRPONTSYD": {
            "term": "Fed overnight reverse repo",
            "observed": "daily, business days",
            "units": "USD billions",
            "conversion": "none: already USD billions",
            "alignment": (
                "the value for the WALCL Wednesday; when that day is blank, the "
                f"latest earlier day within {_NET_LIQUIDITY_RRP_LOOKBACK_DAYS} days, "
                "reported per point as reverse_repo_observed_at; never a later day"
            ),
        },
    },
    "alignment": NET_LIQUIDITY_ALIGNMENT,
    "tga_choice": f"{TGA_CHOICE_NOTE} {TGA_WTREGEN_NOTE}",
}

# Measures the Phase 29 brief named that this module does not serve, with
# the status measured 2026-10-17. No proxy is substituted for any of them.
BLOCKED_SOURCES: list[dict[str, str]] = [
    {
        "measure": "money market fund assets, weekly",
        "publisher": "ICI",
        "status": "blocked",
        "measured": (
            "2026-10-17: HTTP 403 (Akamai 'Access Denied') on "
            "www.ici.org/research/stats/mmf and on /robots.txt"
        ),
    },
    {
        "measure": "ETF net issuance, monthly actuals and weekly estimates",
        "publisher": "ICI",
        "status": "blocked",
        "measured": (
            "2026-10-17: HTTP 403 (Akamai 'Access Denied') on "
            "www.ici.org/research/stats/etf and on /robots.txt"
        ),
    },
    {
        "measure": "customer margin balances (debit and free credit), monthly",
        "publisher": "FINRA",
        "status": "blocked",
        "measured": (
            "2026-10-17: the margin-statistics XLSX answers 200, but the finra.org "
            "Terms of Use (Restrictions d and e) forbid creating a database from the "
            "site and using data mining, scraping or harvesting tools, robots included"
        ),
    },
    {
        "measure": "BLS series (for example CPI-U, nonfarm payrolls)",
        "publisher": "BLS",
        "status": "blocked",
        "measured": (
            "2026-10-17: public API v1 answers 200 without a key, but "
            "api.bls.gov/robots.txt is 'Disallow: /' and the www.bls.gov terms page "
            "answers 403 to this server's User-Agent"
        ),
    },
]

# ECB Data Portal series used by the money-supply view and the board.
# Data types: 1 = outstanding amounts (stocks), 4 = transactions (flows),
# I + suffix A = annual growth rate of the notional-stock index.
_ECB_MONEY_KEY = "M.U2.Y.V.M10+M20+M30.X.1+4+I.U2.2300.Z01.E+A"
_ECB_AGGREGATE_CODES = {"M10": "M1", "M20": "M2", "M30": "M3"}
_ECB_EUROSYSTEM_KEY = "W.U2.C.T000000.Z5.Z01"
ECB_DATASETS = {
    "BSI": "Balance sheet items of euro area monetary financial institutions (monetary aggregates)",
    "ILM": "Eurosystem weekly financial statement (internal liquidity management)",
}
_ECB_MAX_AGE_DAYS = {"M": 75, "W": 16, "D": 6, "Q": 200, "A": 500}

_ECB_KEY_RE = re.compile(r"^[A-Za-z0-9_+.]{1,200}$")
_FRED_ID_RE = re.compile(r"^[A-Z0-9]{2,30}$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _today() -> date:
    return datetime.now(timezone.utc).date()


def _month_end(d: date) -> date:
    return d.replace(day=calendar.monthrange(d.year, d.month)[1])


def _round(value: float | None, ndigits: int = 3) -> float | None:
    return None if value is None else round(value, ndigits)


def _validate_limit(limit: Any) -> int:
    """``limit`` newest points; 0 returns the full history."""
    if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
        raise ValueError(f"limit must be a non-negative integer (0 = full history), got {limit!r}")
    return limit


def _validate_start(start: Any) -> date | None:
    if start in (None, ""):
        return None
    if not isinstance(start, str):
        raise ValueError(f"start must be an ISO date YYYY-MM-DD, got {start!r}")
    try:
        return date.fromisoformat(start)
    except ValueError as exc:
        raise ValueError(f"start must be an ISO date YYYY-MM-DD, got {start!r}") from exc


def _change(latest: dict | None, previous: dict | None, relative: bool = True) -> dict | None:
    """Change between the two newest observations (same units).

    ``relative=False`` (growth rates) reports the absolute change only:
    a percent change of a percent is not meaningful.
    """
    if not latest or not previous:
        return None
    a, b = latest["value"], previous["value"]
    return {
        "absolute": _round(a - b),
        "pct": _round((a - b) / abs(b) * 100, 4) if b and relative else None,
        "from": previous["observed_at"],
        "to": latest["observed_at"],
        "basis": "vs previous observation",
    }


def _freshness(period_end: date | None, max_age_days: int) -> dict:
    if period_end is None:
        return {"latest_age_days": None, "expected_max_age_days": max_age_days, "stale": None}
    age = (_today() - period_end).days
    return {
        "latest_age_days": age,
        "expected_max_age_days": max_age_days,
        "stale": age > max_age_days,
    }


def _stale_gap(name: str, observed_at: str, fresh: dict) -> str | None:
    if fresh.get("stale"):
        return (
            f"{name}: newest observation {observed_at} is {fresh['latest_age_days']} "
            f"days old, past the expected {fresh['expected_max_age_days']} "
            "(publisher delay, a served stale cache copy, or an outage)"
        )
    return None


# ---------------------------------------------------------------------------
# FRED graph CSV
# ---------------------------------------------------------------------------


def parse_fred_csv(text: str, series_id: str) -> list[tuple[date, float | None]]:
    """Parse ``observation_date,<ID>`` rows, oldest first.

    A blank (or ".") value is a missing observation and stays ``None``.
    Anything else that is not a number, a header for another series, or
    an HTML error page raises ``ValueError``.
    """
    reader = csv.reader(io.StringIO(text))
    header = next(reader, None)
    if header is None or [h.strip() for h in header] != ["observation_date", series_id]:
        raise ValueError(f"unexpected header {header!r:.80}")
    rows: list[tuple[date, float | None]] = []
    for row in reader:
        if not row:
            continue
        if len(row) != 2:
            raise ValueError(f"unexpected row {row!r:.80}")
        day = date.fromisoformat(row[0].strip())
        raw = row[1].strip()
        rows.append((day, None if raw in ("", ".") else float(raw)))
    return rows


async def _fred_rows(
    fetcher: Fetcher, series_id: str
) -> tuple[list[tuple[date, float | None]] | None, str | None]:
    text = await fetcher.get_text(
        _FRED_GRAPH_URL,
        source=FRED_SOURCE,
        cache_key=f"liquidity:fred:{series_id}",
        cache_ttl=_FRED_TTL,
        headers=_HEADERS,
        params={"id": series_id},
        timeout=20.0,
    )
    if text is None:
        return None, f"FRED graph CSV for {series_id} unavailable (fetch failed, no cached copy)"
    try:
        rows = parse_fred_csv(text, series_id)
    except ValueError as exc:
        return None, f"FRED graph CSV for {series_id} not in the expected format: {exc}"
    if not any(v is not None for _, v in rows):
        return None, f"FRED graph CSV for {series_id} has no observations"
    return rows, None


def _fred_period_end(series_id: str, d: date) -> date:
    return _month_end(d) if FRED_SERIES[series_id]["frequency"] == "monthly" else d


def _build_fred_series(
    series_id: str,
    rows: list[tuple[date, float | None]],
    limit: int,
    start: date | None,
) -> dict:
    meta = FRED_SERIES[series_id]
    units = meta["units"]
    present = [(d, v) for d, v in rows if v is not None]
    blank = len(rows) - len(present)
    window = [(d, v) for d, v in present if start is None or d >= start]
    newest_first = list(reversed(window))
    if limit:
        newest_first = newest_first[:limit]
    points = [
        {"observed_at": d.isoformat(), "value": v, "units": units} for d, v in newest_first
    ]
    latest_d, latest_v = present[-1]
    latest = {"observed_at": latest_d.isoformat(), "value": latest_v, "units": units}
    previous = (
        {"observed_at": present[-2][0].isoformat(), "value": present[-2][1], "units": units}
        if len(present) > 1
        else None
    )
    fresh = _freshness(_fred_period_end(series_id, latest_d), meta["max_age_days"])
    return {
        "series_id": series_id,
        "dataset": f"FRED {series_id}",
        "label": meta["label"],
        "detail": meta["detail"],
        "units": units,
        "scale": meta["scale"],
        "frequency": meta["frequency"],
        "seasonal_adjustment": meta["seasonal_adjustment"],
        "measure": meta["measure"],
        "release": meta["release"],
        "published_at": None,
        "latest": latest,
        "previous": previous,
        "change": _change(latest, previous),
        "freshness": fresh,
        "first_observed_at": present[0][0].isoformat(),
        "blank_observations": blank,
        "order": "newest_first",
        "count": len(points),
        "points": points,
    }


_FRED_GAP = (
    "FRED's keyless CSV carries observation dates only: published_at is null, "
    "and units/frequency come from the server's series catalog, not the payload."
)


async def fetch_fed_liquidity_series(
    fetcher: Fetcher,
    series_id: str = "WALCL",
    limit: int = DEFAULT_LIMIT,
    start: str | None = None,
) -> dict:
    """One FRED liquidity series (keyless), newest first; ``limit=0`` is full history."""
    sid = str(series_id or "").strip().upper()
    if sid not in FRED_SERIES:
        return {
            "error": f"unknown series {series_id!r}; choose one of {sorted(FRED_SERIES)}",
            "series_available": sorted(FRED_SERIES),
            "timestamp": _utc_now_iso(),
        }
    try:
        n = _validate_limit(limit)
        start_d = _validate_start(start)
    except ValueError as exc:
        return {"error": str(exc), "timestamp": _utc_now_iso()}

    rows, err = await _fred_rows(fetcher, sid)
    if rows is None:
        return {
            "error": err,
            "series_id": sid,
            "source": FRED_SOURCE,
            "data_gaps": [err],
            "timestamp": _utc_now_iso(),
        }
    series = _build_fred_series(sid, rows, n, start_d)
    gaps = [_FRED_GAP]
    stale = _stale_gap(sid, series["latest"]["observed_at"], series["freshness"])
    if stale:
        gaps.append(stale)
    if series["blank_observations"]:
        gaps.append(
            f"{series['blank_observations']} blank {sid} dates in the source "
            "(no value published) are omitted, not read as zero"
        )
    return {**series, "source": FRED_SOURCE, "data_gaps": gaps, "timestamp": _utc_now_iso()}


# ---------------------------------------------------------------------------
# Net liquidity
# ---------------------------------------------------------------------------


def compute_net_liquidity(
    walcl: list[tuple[date, float | None]],
    tga: list[tuple[date, float | None]],
    rrp: list[tuple[date, float | None]],
) -> tuple[list[dict], list[str]]:
    """Aligned weekly net liquidity in USD billions, oldest first.

    Returns ``(points, skipped)`` where ``skipped`` lists the WALCL dates
    that had no aligned TGA or RRP value.
    """
    tga_by_day = {d: v for d, v in tga if v is not None}
    rrp_days = sorted((d, v) for d, v in rrp if v is not None)
    rrp_by_day = dict(rrp_days)
    points: list[dict] = []
    skipped: list[str] = []
    for day, assets in walcl:
        if assets is None:
            continue
        tga_v = tga_by_day.get(day)
        rrp_day: date | None = None
        for back in range(_NET_LIQUIDITY_RRP_LOOKBACK_DAYS + 1):
            candidate = date.fromordinal(day.toordinal() - back)
            if candidate in rrp_by_day:
                rrp_day = candidate
                break
        if tga_v is None or rrp_day is None:
            skipped.append(day.isoformat())
            continue
        assets_bn = assets / 1000.0
        tga_bn = tga_v / 1000.0
        rrp_bn = rrp_by_day[rrp_day]
        points.append(
            {
                "observed_at": day.isoformat(),
                "value": round(assets_bn - tga_bn - rrp_bn, 3),
                "units": "USD billions",
                "fed_total_assets": round(assets_bn, 3),
                "treasury_general_account": round(tga_bn, 3),
                "reverse_repo": round(rrp_bn, 3),
                "reverse_repo_observed_at": rrp_day.isoformat(),
            }
        )
    return points, skipped


async def fetch_net_liquidity(
    fetcher: Fetcher,
    limit: int = DEFAULT_LIMIT,
    start: str | None = None,
) -> dict:
    """Fed balance sheet minus TGA minus overnight RRP, weekly, newest first."""
    try:
        n = _validate_limit(limit)
        start_d = _validate_start(start)
    except ValueError as exc:
        return {"error": str(exc), "timestamp": _utc_now_iso()}

    ids = ("WALCL", "WDTGAL", "RRPONTSYD")
    fetched = await asyncio.gather(*(_fred_rows(fetcher, sid) for sid in ids))
    failures = {sid: err for sid, (rows, err) in zip(ids, fetched) if rows is None}
    base = {
        "source": FRED_SOURCE,
        "formula": NET_LIQUIDITY_FORMULA,
        "alignment": NET_LIQUIDITY_ALIGNMENT,
        "method": copy.deepcopy(NET_LIQUIDITY_METHOD),
        "units": "USD billions",
        "measure": "amount_outstanding",
        "frequency": "weekly, as of Wednesday",
    }
    if failures:
        gaps = list(failures.values())
        return {
            **base,
            "error": (
                "net liquidity not computed: missing component(s) "
                f"{', '.join(sorted(failures))}; a missing term is never treated as zero"
            ),
            "components_failed": sorted(failures),
            "data_gaps": gaps,
            "timestamp": _utc_now_iso(),
        }
    walcl, tga, rrp = (rows for rows, _ in fetched)
    all_points, skipped = compute_net_liquidity(walcl, tga, rrp)
    if not all_points:
        return {
            **base,
            "error": "net liquidity not computed: no week had all three components aligned",
            "skipped_weeks": len(skipped),
            "data_gaps": [f"{len(skipped)} WALCL weeks lacked an aligned TGA or RRP value"],
            "timestamp": _utc_now_iso(),
        }

    latest = all_points[-1]
    previous = all_points[-2] if len(all_points) > 1 else None
    window = [p for p in all_points if start_d is None or p["observed_at"] >= start_d.isoformat()]
    newest_first = list(reversed(window))
    if n:
        newest_first = newest_first[:n]
    keep = ("label", "units", "frequency", "measure", "release", "latest", "freshness")
    components = {}
    for sid, rows in zip(ids, (walcl, tga, rrp)):
        built = _build_fred_series(sid, rows, 1, None)
        components[sid] = {k: built[k] for k in keep}
    gaps = [_FRED_GAP, TGA_CHOICE_NOTE]
    for sid, comp in components.items():
        stale = _stale_gap(sid, comp["latest"]["observed_at"], comp["freshness"])
        if stale:
            gaps.append(stale)
    if skipped:
        gaps.append(
            f"{len(skipped)} of {len(skipped) + len(all_points)} WALCL weeks skipped "
            f"(no aligned TGA or RRP value), first {skipped[0]}, last {skipped[-1]}"
        )
    return {
        **base,
        "latest": latest,
        "previous": previous,
        "change": _change(latest, previous),
        "components": components,
        "first_observed_at": all_points[0]["observed_at"],
        "skipped_weeks": len(skipped),
        "order": "newest_first",
        "count": len(newest_first),
        "points": newest_first,
        "published_at": None,
        "data_gaps": gaps,
        "timestamp": _utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# ECB Data Portal (SDMX-JSON)
# ---------------------------------------------------------------------------


def _attr_lookup(defs: list[dict], indexes: list[Any]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for pos, vi in enumerate(indexes):
        if vi is None or pos >= len(defs):
            continue
        values = defs[pos].get("values") or []
        if isinstance(vi, int) and 0 <= vi < len(values):
            out[defs[pos]["id"]] = values[vi]
    return out


def _ecb_units(attrs: dict[str, dict]) -> tuple[str | None, int | None]:
    unit = attrs.get("UNIT", {})
    mult = attrs.get("UNIT_MULT", {})
    unit_id = unit.get("id")
    try:
        exponent = int(mult.get("id")) if mult.get("id") is not None else None
    except ValueError:
        exponent = None
    scale = 10**exponent if exponent is not None else None
    if unit_id is None:
        return None, scale
    if unit_id == "PCCH":
        return "percent", scale
    mult_name = (mult.get("name") or "").lower()
    if mult_name and mult_name != "units":
        return f"{unit_id} {mult_name}", scale
    return unit_id, scale


def _ecb_measure(dataset: str, dims: dict[str, str]) -> str:
    if dataset == "ILM":
        return "amount_outstanding"
    data_type = dims.get("DATA_TYPE")
    if data_type == "1":
        return "amount_outstanding"
    if data_type == "4":
        return "flow"
    if data_type == "I":
        return "growth_rate" if dims.get("BS_SUFFIX") == "A" else "index"
    return "unknown"


def _period_end(value: dict) -> date | None:
    end = value.get("end")
    if isinstance(end, str) and len(end) >= 10:
        try:
            return date.fromisoformat(end[:10])
        except ValueError:
            return None
    return None


def parse_sdmx_json(data: dict, dataset: str) -> list[dict]:
    """Every series in an ECB SDMX-JSON 1.0 response, points oldest first."""
    structure = data["structure"]
    dims = structure["dimensions"]["series"]
    periods = structure["dimensions"]["observation"][0]["values"]
    series_attr_defs = structure["attributes"]["series"]
    obs_attr_defs = structure["attributes"]["observation"]
    status_pos = next(
        (i for i, a in enumerate(obs_attr_defs) if a.get("id") == "OBS_STATUS"), None
    )
    out: list[dict] = []
    for skey, body in data["dataSets"][0].get("series", {}).items():
        idx = [int(i) for i in skey.split(":")]
        dim_ids = {dims[i]["id"]: dims[i]["values"][j]["id"] for i, j in enumerate(idx)}
        key = ".".join(dims[i]["values"][j]["id"] for i, j in enumerate(idx))
        attrs = _attr_lookup(series_attr_defs, body.get("attributes", []))
        units, scale = _ecb_units(attrs)
        points = []
        for oi, ov in body.get("observations", {}).items():
            if not ov or ov[0] is None:
                continue
            period = periods[int(oi)]
            status = None
            if status_pos is not None and len(ov) > status_pos + 1 and ov[status_pos + 1] is not None:
                values = obs_attr_defs[status_pos].get("values") or []
                if 0 <= ov[status_pos + 1] < len(values):
                    status = values[ov[status_pos + 1]].get("name")
            end = _period_end(period)
            points.append(
                {
                    "observed_at": period["id"],
                    "period_end": end.isoformat() if end else None,
                    "value": float(ov[0]),
                    "units": units,
                    "status": status,
                }
            )
        points.sort(key=lambda p: (p["period_end"] or "", p["observed_at"]))
        out.append(
            {
                "key": f"{dataset}.{key}",
                "title": attrs.get("TITLE", {}).get("name"),
                "title_full": attrs.get("TITLE_COMPL", {}).get("name"),
                "dimensions": dim_ids,
                "units": units,
                "scale": scale,
                "measure": _ecb_measure(dataset, dim_ids),
                "frequency": dim_ids.get("FREQ"),
                "points": points,
            }
        )
    out.sort(key=lambda s: s["key"])
    return out


async def _ecb_fetch(
    fetcher: Fetcher, dataset: str, key: str, last_n: int, start: date | None
) -> tuple[list[dict] | None, str | None]:
    params: dict[str, Any] = {"format": "jsondata"}
    if last_n:
        params["lastNObservations"] = last_n
    if start is not None:
        params["startPeriod"] = start.isoformat()
    data = await fetcher.get_json(
        _ECB_DATA_URL.format(dataset=dataset, key=key),
        source=ECB_SOURCE,
        cache_key=f"liquidity:ecb:{dataset}:{key}:{last_n}:{start}",
        cache_ttl=_ECB_TTL,
        headers=_HEADERS,
        params=params,
        timeout=30.0,
    )
    if not isinstance(data, dict):
        return None, (
            f"ECB Data Portal returned no data for {dataset}/{key} "
            "(outage, or no series matches the key)"
        )
    try:
        series = parse_sdmx_json(data, dataset)
    except (KeyError, IndexError, TypeError, ValueError) as exc:
        return None, f"ECB Data Portal response for {dataset}/{key} not in the expected SDMX-JSON shape: {exc!r:.80}"
    if not any(s["points"] for s in series):
        return None, f"ECB Data Portal returned no observations for {dataset}/{key}"
    return series, None


def _finish_ecb_series(series: dict, limit: int) -> dict:
    """Newest-first window plus latest/previous/change/freshness."""
    pts = series["points"]
    latest = pts[-1] if pts else None
    previous = pts[-2] if len(pts) > 1 else None
    end = date.fromisoformat(latest["period_end"]) if latest and latest["period_end"] else None
    max_age = _ECB_MAX_AGE_DAYS.get(series.get("frequency") or "", 75)
    newest_first = list(reversed(pts))
    if limit:
        newest_first = newest_first[:limit]
    return {
        **{k: v for k, v in series.items() if k != "points"},
        "published_at": None,
        "latest": latest,
        "previous": previous,
        # A change between two flows is not a liquidity change; growth
        # rates change in percentage points, not percent.
        "change": (
            None
            if series["measure"] == "flow"
            else _change(latest, previous, relative=series["measure"] != "growth_rate")
        ),
        "freshness": _freshness(end, max_age),
        "order": "newest_first",
        "count": len(newest_first),
        "points": newest_first,
    }


_ECB_GAP = (
    "The ECB Data Portal response carries no release dates: published_at is "
    "null; observation status (normal / provisional) is per point."
)


async def fetch_ecb_series(
    fetcher: Fetcher,
    dataset: str = "BSI",
    key: str = "M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
    limit: int = 24,
    start: str | None = None,
) -> dict:
    """ECB Data Portal series (BSI money and banking, ILM Eurosystem balance sheet)."""
    ds = str(dataset or "").strip().upper()
    if ds not in ECB_DATASETS:
        return {
            "error": f"dataset must be one of {sorted(ECB_DATASETS)}, got {dataset!r}",
            "datasets": ECB_DATASETS,
            "timestamp": _utc_now_iso(),
        }
    key = str(key or "").strip()
    if not _ECB_KEY_RE.match(key):
        return {
            "error": "key must be an SDMX series key: letters, digits, '.', '+' and '_' only",
            "timestamp": _utc_now_iso(),
        }
    try:
        n = _validate_limit(limit)
        start_d = _validate_start(start)
    except ValueError as exc:
        return {"error": str(exc), "timestamp": _utc_now_iso()}

    # One extra observation so the oldest returned point still has a change.
    series, err = await _ecb_fetch(fetcher, ds, key, n + 1 if n else 0, start_d)
    if series is None:
        return {
            "error": err,
            "dataset": ds,
            "key": key,
            "source": ECB_SOURCE,
            "data_gaps": [err],
            "timestamp": _utc_now_iso(),
        }
    gaps = [_ECB_GAP]
    if len(series) > _MAX_ECB_SERIES:
        gaps.append(f"{len(series)} series matched; the first {_MAX_ECB_SERIES} by key are returned")
        series = series[:_MAX_ECB_SERIES]
    finished = [_finish_ecb_series(s, n) for s in series]
    for s in finished:
        if s["measure"] == "unknown":
            gaps.append(f"{s['key']}: measure type not recognized (not typed as stock or flow)")
        if s["latest"]:
            stale = _stale_gap(s["key"], s["latest"]["observed_at"], s["freshness"])
            if stale:
                gaps.append(stale)
    return {
        "dataset": ds,
        "dataset_description": ECB_DATASETS[ds],
        "key": key,
        "source": ECB_SOURCE,
        "series_count": len(finished),
        "series": finished,
        "data_gaps": gaps,
        "timestamp": _utc_now_iso(),
    }


# ---------------------------------------------------------------------------
# Money supply
# ---------------------------------------------------------------------------


def _ecb_money_view(series: list[dict], limit: int) -> dict:
    aggregates: dict[str, dict] = {}
    for s in series:
        name = _ECB_AGGREGATE_CODES.get(s["dimensions"].get("BS_ITEM", ""))
        role = {
            "amount_outstanding": "stock",
            "flow": "flow",
            "growth_rate": "annual_growth_rate",
        }.get(s["measure"])
        if name is None or role is None:
            continue
        aggregates.setdefault(name, {})[role] = _finish_ecb_series(s, limit)
    return dict(sorted(aggregates.items()))


async def fetch_money_supply(
    fetcher: Fetcher,
    region: str = "all",
    limit: int = 13,
) -> dict:
    """US M2 (FRED) and euro area M1/M2/M3 (ECB), reported separately, never summed."""
    reg = str(region or "all").strip().lower()
    if reg not in ("all", "us", "euro_area"):
        return {"error": f"region must be all, us or euro_area, got {region!r}", "timestamp": _utc_now_iso()}
    try:
        n = _validate_limit(limit)
    except ValueError as exc:
        return {"error": str(exc), "timestamp": _utc_now_iso()}

    want_us = reg in ("all", "us")
    want_ea = reg in ("all", "euro_area")

    async def _none() -> tuple[None, None]:
        return None, None

    us_task = _fred_rows(fetcher, "M2SL") if want_us else _none()
    ea_task = _ecb_fetch(fetcher, "BSI", _ECB_MONEY_KEY, n + 1 if n else 0, None) if want_ea else _none()
    (us_rows, us_err), (ea_series, ea_err) = await asyncio.gather(us_task, ea_task)

    gaps: list[str] = []
    out: dict[str, Any] = {"region": reg, "note": NESTING_NOTE}
    if want_us:
        us: dict[str, Any] = {"M3": {"available": False, "note": US_M3_NOTE}}
        if us_rows is None:
            us["M2"] = {"error": us_err}
            gaps.append(us_err)
        else:
            m2 = _build_fred_series("M2SL", us_rows, n, None)
            us["M2"] = {**m2, "source": FRED_SOURCE}
            gaps.append(_FRED_GAP)
            stale = _stale_gap("M2SL", m2["latest"]["observed_at"], m2["freshness"])
            if stale:
                gaps.append(stale)
        out["us"] = us
    if want_ea:
        if ea_series is None:
            out["euro_area"] = {"error": ea_err}
            gaps.append(ea_err)
        else:
            aggregates = _ecb_money_view(ea_series, n)
            out["euro_area"] = {
                "source": ECB_SOURCE,
                "key": f"BSI.{_ECB_MONEY_KEY}",
                "aggregates": aggregates,
                "note": (
                    "stock = outstanding amount at end of month; flow = transactions "
                    "during the month (excludes reclassifications and revaluations, so "
                    "stock changes differ from flows); annual_growth_rate = percent"
                ),
            }
            gaps.append(_ECB_GAP)
            for name, roles in aggregates.items():
                for role, s in roles.items():
                    if s["latest"]:
                        stale = _stale_gap(f"euro area {name} {role}", s["latest"]["observed_at"], s["freshness"])
                        if stale:
                            gaps.append(stale)
            missing = sorted(set(_ECB_AGGREGATE_CODES.values()) - set(aggregates))
            if missing:
                gaps.append(f"euro area aggregates missing from the ECB response: {missing}")

    us_failed = want_us and "error" in out.get("us", {}).get("M2", {})
    ea_failed = want_ea and "error" in out.get("euro_area", {})
    if (not want_us or us_failed) and (not want_ea or ea_failed):
        out["error"] = "money supply unavailable: " + "; ".join(gaps)
    out["data_gaps"] = gaps
    out["timestamp"] = _utc_now_iso()
    return out


# ---------------------------------------------------------------------------
# Liquidity board
# ---------------------------------------------------------------------------


def _row(
    indicator: str,
    label: str,
    dataset: str,
    source: str,
    measure: str,
    frequency: str,
    units: str | None,
    latest: dict | None,
    previous: dict | None,
    period_end: date | None,
    freshness: dict,
) -> dict:
    change = _change(latest, previous)
    return {
        "indicator": indicator,
        "label": label,
        "dataset": dataset,
        "source": source,
        "measure": measure,
        "frequency": frequency,
        "units": units,
        "latest_value": latest["value"] if latest else None,
        "observed_at": latest["observed_at"] if latest else None,
        "period_end": period_end.isoformat() if period_end else None,
        "previous_value": previous["value"] if previous else None,
        "previous_observed_at": previous["observed_at"] if previous else None,
        "change": change["absolute"] if change else None,
        "change_pct": change["pct"] if change else None,
        "change_basis": "vs previous observation",
        "freshness": freshness,
    }


_BOARD_FRED = (
    ("fed_total_assets", "WALCL"),
    ("treasury_general_account", "WDTGAL"),
    ("reverse_repo", "RRPONTSYD"),
    ("us_m2", "M2SL"),
)


async def fetch_liquidity_board(fetcher: Fetcher, sort: str = "biggest_move") -> dict:
    """Latest reading and change per liquidity indicator across FRED and the ECB."""
    sort = str(sort or "biggest_move").strip().lower()
    if sort not in ("biggest_move", "newest"):
        return {"error": f"sort must be biggest_move or newest, got {sort!r}", "timestamp": _utc_now_iso()}

    fred_ids = [sid for _, sid in _BOARD_FRED]
    results = await asyncio.gather(
        *(_fred_rows(fetcher, sid) for sid in fred_ids),
        _ecb_fetch(fetcher, "BSI", _ECB_MONEY_KEY, 2, None),
        _ecb_fetch(fetcher, "ILM", _ECB_EUROSYSTEM_KEY, 2, None),
    )
    fred_results = dict(zip(fred_ids, results[: len(fred_ids)]))
    (bsi, bsi_err), (ilm, ilm_err) = results[len(fred_ids):]

    rows: list[dict] = []
    unavailable: list[dict] = []
    gaps: list[str] = []

    for indicator, sid in _BOARD_FRED:
        fr, err = fred_results[sid]
        if fr is None:
            unavailable.append({"indicator": indicator, "dataset": f"FRED {sid}", "error": err})
            gaps.append(err)
            continue
        s = _build_fred_series(sid, fr, 1, None)
        rows.append(
            _row(
                indicator, s["label"], s["dataset"], FRED_SOURCE, s["measure"], s["frequency"],
                s["units"], s["latest"], s["previous"],
                _fred_period_end(sid, date.fromisoformat(s["latest"]["observed_at"])),
                s["freshness"],
            )
        )

    nl_parts = [fred_results[s] for s in ("WALCL", "WDTGAL", "RRPONTSYD")]
    if all(rows_ is not None for rows_, _ in nl_parts):
        points, _ = compute_net_liquidity(*(rows_ for rows_, _ in nl_parts))
        if points:
            latest = points[-1]
            previous = points[-2] if len(points) > 1 else None
            end = date.fromisoformat(latest["observed_at"])
            rows.append(
                _row(
                    "net_liquidity", "US net liquidity (WALCL - WDTGAL - RRPONTSYD)",
                    "derived from FRED WALCL, WDTGAL, RRPONTSYD", FRED_SOURCE,
                    "amount_outstanding", "weekly, as of Wednesday", "USD billions",
                    {"observed_at": latest["observed_at"], "value": latest["value"]},
                    {"observed_at": previous["observed_at"], "value": previous["value"]} if previous else None,
                    end, _freshness(end, FRED_SERIES["WALCL"]["max_age_days"]),
                )
            )
        else:
            unavailable.append({"indicator": "net_liquidity", "error": "no aligned week"})
            gaps.append("net liquidity: no week had all three components aligned")
    else:
        unavailable.append(
            {"indicator": "net_liquidity", "error": "a component failed; never computed with a zero term"}
        )
        gaps.append("net liquidity not computed: a component series failed")

    for err, payload, wanted in (
        (bsi_err, bsi, "BSI"),
        (ilm_err, ilm, "ILM"),
    ):
        if payload is None:
            name = "euro area M1/M2/M3" if wanted == "BSI" else "Eurosystem total assets"
            unavailable.append({"indicator": name, "dataset": f"ECB {wanted}", "error": err})
            gaps.append(err)
            continue
        for s in payload:
            if wanted == "BSI":
                agg = _ECB_AGGREGATE_CODES.get(s["dimensions"].get("BS_ITEM", ""))
                if agg is None or s["measure"] != "amount_outstanding":
                    continue
                indicator, label = f"euro_area_{agg.lower()}", f"Euro area {agg} (stock)"
            else:
                indicator, label = "eurosystem_total_assets", s["title"] or "Eurosystem total assets"
            f = _finish_ecb_series(s, 1)
            latest = f["latest"]
            end = date.fromisoformat(latest["period_end"]) if latest and latest["period_end"] else None
            rows.append(
                _row(
                    indicator, label, s["key"], ECB_SOURCE, s["measure"],
                    {"M": "monthly", "W": "weekly"}.get(s["frequency"], s["frequency"]),
                    s["units"], latest, f["previous"], end, f["freshness"],
                )
            )

    for r in rows:
        stale = _stale_gap(r["indicator"], r["observed_at"], r["freshness"])
        if stale:
            gaps.append(stale)

    if sort == "newest":
        rows.sort(key=lambda r: (r["period_end"] or "", r["indicator"]), reverse=True)
    else:
        rows.sort(key=lambda r: (r["change_pct"] is None, -abs(r["change_pct"] or 0.0), r["indicator"]))

    gaps.append(
        "Changes are vs each indicator's previous observation, so horizons differ "
        "(daily RRP, weekly Fed and Eurosystem data, monthly money supply)."
    )
    gaps.append(
        "published_at is null for every row: neither FRED's keyless CSV nor the "
        "ECB payload carries release dates. FRED units come from the server's "
        "series catalog; ECB units come from the response."
    )
    gaps.append(
        "Not covered (see blocked): ICI money market fund assets and ETF net "
        "issuance, FINRA margin statistics and BLS series; no proxy is "
        "substituted for them."
    )
    out: dict[str, Any] = {
        "sort": sort,
        "count": len(rows),
        "indicators": rows,
        "unavailable": unavailable,
        "blocked": copy.deepcopy(BLOCKED_SOURCES),
        "net_liquidity_formula": NET_LIQUIDITY_FORMULA,
        "note": NESTING_NOTE,
        "sources": [FRED_SOURCE, ECB_SOURCE],
        "data_gaps": gaps,
        "timestamp": _utc_now_iso(),
    }
    if not rows:
        out["error"] = "liquidity board unavailable: every source failed"
    return out
