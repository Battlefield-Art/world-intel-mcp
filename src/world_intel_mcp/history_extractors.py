"""History extractors for more collector sources (ROADMAP Phase 29).

``history.py`` holds the extractor registry and its first extractors; this
module registers extractors for further collector sources whose payloads
carry dated numeric values. ``history.py`` imports it at its end, so these
are registered wherever the store classifies a capture. Every point is
dated only from a date in its payload, never from the collection time:

- ``liquidity_board``: each indicator row's latest and previous value
  (FRED WALCL, WDTGAL, RRPONTSYD and M2SL; ECB euro area M1, M2, M3 and
  Eurosystem total assets), in the row's own units. The derived net
  liquidity row is skipped: the ``net_liquidity`` source stores that series
  with its three terms.
- ``net_liquidity``: per Wednesday, net liquidity and its three terms in
  USD billions. The reverse repo term is dated by the day it was taken from
  (``reverse_repo_observed_at``), which can be the Tuesday before.
- ``net_migration``: World Bank SM.POP.NETM for every country or region in
  the payload's rankings (persons, dated by the year). The most recent
  years are UN World Population Prospects modelled values or projections,
  so a later World Bank release can revise them.

Period forms: FRED dates are kept as written (FRED dates a monthly value at
the first day of its month); ECB monthly periods (``2026-08``) are kept as
written; an ECB weekly period (``2026-W41``) is stored as the Sunday that
ends that ISO week (``2026-10-11``), the period end the ECB response itself
gives for its weekly series, because the store keeps calendar dates. A
value whose period cannot be read is emitted as written so the store skips
it and counts it in the capture's ``extract_error``, never stores it
undated.

Gaps / not covered:
- Collector sources with dated values left without an extractor:
  ``energy_prices``, ``residential_natgas`` and ``electricity_rates`` (EIA)
  need ``EIA_API_KEY``, so no real payload could be captured to build their
  tests; ``migration_surges`` and ``displacement`` (UNHCR yearly totals)
  were not examined in depth.
- Collector sources whose payloads carry no observation date and so got no
  extractor: crypto, stablecoin, ETF, sector, macro signal and shipping
  quotes (Yahoo or CoinGecko values without a quote time), AAA gas prices
  (today's average, undated), and the computed BTC technicals.
- Values served from a stale cache copy are stored like any other: the
  store's ``collected_at`` is when this server saved the value, which for a
  stale copy is later than when the copy was fetched.
"""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from .history import Observation, normalize_observed_at, register_extractor

_ISO_WEEK = re.compile(r"(\d{4})-W(\d{2})")


def period_date(value: Any) -> str | None:
    """A payload period in the store's date forms, or None if unreadable.

    Dates, months, years and offset instants go through
    ``normalize_observed_at``; an ISO week (``2026-W41``) becomes the Sunday
    that ends it.
    """
    observed = normalize_observed_at(value)
    if observed is not None:
        return observed
    match = _ISO_WEEK.fullmatch(str(value or "").strip())
    if match is None:
        return None
    try:
        return date.fromisocalendar(int(match[1]), int(match[2]), 7).isoformat()
    except ValueError:
        return None


def _dated(entry: dict, date_key: str) -> str:
    """``observed_at`` for a point, or the raw period when it cannot be
    read: the store then skips the point and counts it, which keeps an
    undated value out of a dated series."""
    raw = entry.get(date_key)
    return period_date(raw) or ("" if raw is None else str(raw))


# Board rows computed by this server from other rows, not read upstream.
_DERIVED_BOARD_ROWS = frozenset({"net_liquidity"})


@register_extractor("liquidity_board")
def _extract_liquidity_board(payload: dict) -> list[Observation]:
    """Latest and previous value of each upstream indicator row."""
    points = []
    for row in payload.get("indicators") or []:
        indicator = row.get("indicator")
        if not indicator or indicator in _DERIVED_BOARD_ROWS:
            continue
        for value_key, date_key in (
            ("latest_value", "observed_at"),
            ("previous_value", "previous_observed_at"),
        ):
            if row.get(value_key) is None:
                continue
            points.append(
                Observation(indicator, row[value_key], observed_at=_dated(row, date_key),
                            unit=row.get("units"), label=row.get("label"))
            )
    return points


# (series suffix, value key, date key, label) per net liquidity point.
_NET_LIQUIDITY_TERMS = (
    ("net_liquidity", "value", "observed_at",
     "US net liquidity (WALCL - WDTGAL - RRPONTSYD)"),
    ("fed_total_assets", "fed_total_assets", "observed_at",
     "Fed total assets (WALCL, Wednesday level)"),
    ("treasury_general_account", "treasury_general_account", "observed_at",
     "Treasury General Account (WDTGAL, Wednesday level)"),
    ("reverse_repo", "reverse_repo", "reverse_repo_observed_at",
     "Fed overnight reverse repo (RRPONTSYD, the day used for the week)"),
)


@register_extractor("net_liquidity")
def _extract_net_liquidity(payload: dict) -> list[Observation]:
    """Each week's net liquidity and the three terms it was computed from."""
    unit = payload.get("units")
    points = []
    for point in payload.get("points") or []:
        for suffix, value_key, date_key, label in _NET_LIQUIDITY_TERMS:
            if point.get(value_key) is None:
                continue
            points.append(
                Observation(suffix, point[value_key], observed_at=_dated(point, date_key),
                            unit=unit, label=label)
            )
    return points


_MIGRATION_LISTS = (
    "top_net_immigration",
    "top_net_emigration",
    "highest_rate_immigration",
    "highest_rate_emigration",
    "world_regions",
)


@register_extractor("net_migration")
def _extract_net_migration(payload: dict) -> list[Observation]:
    """Net migration per country or region in the rankings, once each."""
    points = []
    seen: set[str] = set()
    for list_key in _MIGRATION_LISTS:
        for entry in payload.get(list_key) or []:
            iso = entry.get("iso3")
            if not iso or iso in seen or entry.get("net_migration") is None:
                continue
            seen.add(iso)
            points.append(
                Observation(iso, entry["net_migration"], observed_at=_dated(entry, "year"),
                            unit="persons",
                            label=f"{entry.get('name') or iso}: net migration (World Bank SM.POP.NETM)")
            )
    return points
