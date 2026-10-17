"""Extractors registered by history_extractors.py, pinned on real payloads.

Fixtures under fixtures/history/ are live payloads captured 2026-10-17
under an isolated HOME by the project's own fetch functions
(``liquidity.fetch_liquidity_board``, ``liquidity.fetch_net_liquidity``,
``migration.fetch_net_migration``), then trimmed: the board is complete;
net_liquidity keeps its newest 3 of 52 points (``count`` set to match);
net_migration keeps the first 4 entries of each ranking, so Ukraine (UKR)
and Syria (SYR) stay in two rankings each. No key or personal data is in
any of them. Every expected value below is read off those files by eye.

Gaps / not covered: payloads served from a stale cache copy (stored like
any other, see the module docstring); the EIA, UNHCR and quote sources
that got no extractor.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from world_intel_mcp import history_extractors
from world_intel_mcp.history import HistoryStore, SourceOutcome, classify

_FIX = Path(__file__).parent / "fixtures" / "history"
T0 = 1_792_000_000.0


def _payload(name: str) -> dict:
    return json.loads((_FIX / f"{name}.json").read_text())


def _points(source: str, payload: dict) -> dict[str, tuple]:
    c = classify(SourceOutcome(source, T0, payload=payload))
    assert c.status == "ok", c.reason
    out = {o.series_key: [] for o in c.observations}
    for o in c.observations:
        out[o.series_key].append((o.observed_at, o.value, o.unit))
    return out


def test_period_date_forms() -> None:
    pd = history_extractors.period_date
    assert pd("2026-W41") == "2026-10-11"  # Sunday ending ISO week 41
    assert pd("2026-W01") == "2026-01-04"
    assert pd("2020-W53") == "2021-01-03"  # 2020 has 53 ISO weeks
    assert pd("2026-W54") is None and pd("2026-W00") is None
    assert pd("2026-08") == "2026-08" and pd("2026-08-01") == "2026-08-01"
    assert pd(2025) == "2025"
    assert pd("2026-Q2") is None and pd(None) is None and pd("") is None


def test_liquidity_board_rows_latest_and_previous() -> None:
    points = _points("liquidity_board", _payload("liquidity_board"))
    assert points == {
        "us_m2": [("2026-08-01", 23342.8, "USD billions"), ("2026-07-01", 23217.9, "USD billions")],
        "euro_area_m3": [("2026-08", 17646463.0, "EUR millions"),
                         ("2026-07", 17615092.0, "EUR millions")],
        "euro_area_m2": [("2026-08", 16447405.0, "EUR millions"),
                         ("2026-07", 16434747.0, "EUR millions")],
        "fed_total_assets": [("2026-10-14", 6755639.0, "USD millions"),
                             ("2026-10-07", 6747560.0, "USD millions")],
        # ECB weeks 41 and 40 end on these Sundays (the response's period end).
        "eurosystem_total_assets": [("2026-10-11", 5943911.0, "EUR millions"),
                                    ("2026-10-04", 5947826.0, "EUR millions")],
        "euro_area_m1": [("2026-08", 11293328.0, "EUR millions"),
                         ("2026-07", 11291831.0, "EUR millions")],
        "reverse_repo": [("2026-10-16", 0.1, "USD billions"), ("2026-10-15", 1.201, "USD billions")],
        "treasury_general_account": [("2026-10-14", 885823.0, "USD millions"),
                                     ("2026-10-07", 885783.0, "USD millions")],
    }
    # The derived row is the net_liquidity source's series, not stored twice.
    assert "net_liquidity" not in points


def test_net_liquidity_points_and_terms() -> None:
    points = _points("net_liquidity", _payload("net_liquidity"))
    usd = "USD billions"
    assert points == {
        "net_liquidity": [("2026-10-14", 5869.615, usd), ("2026-10-07", 5859.439, usd),
                          ("2026-09-30", 5747.446, usd)],
        "fed_total_assets": [("2026-10-14", 6755.639, usd), ("2026-10-07", 6747.56, usd),
                             ("2026-09-30", 6743.031, usd)],
        "treasury_general_account": [("2026-10-14", 885.823, usd), ("2026-10-07", 885.783, usd),
                                     ("2026-09-30", 984.046, usd)],
        "reverse_repo": [("2026-10-14", 0.201, usd), ("2026-10-07", 2.338, usd),
                         ("2026-09-30", 11.539, usd)],
    }


def test_reverse_repo_term_is_dated_by_the_day_it_was_taken_from() -> None:
    payload = _payload("net_liquidity")
    payload["points"][0]["reverse_repo_observed_at"] = "2026-10-13"  # a Tuesday fallback
    points = _points("net_liquidity", payload)
    assert points["reverse_repo"][0] == ("2026-10-13", 0.201, "USD billions")
    assert points["net_liquidity"][0][0] == "2026-10-14"


def test_net_migration_once_per_country_dated_by_year() -> None:
    c = classify(SourceOutcome("net_migration", T0, payload=_payload("net_migration")))
    assert c.status == "ok" and c.extract_error is None
    got = {o.series_key: (o.observed_at, o.value, o.unit) for o in c.observations}
    assert len(c.observations) == len(got) == 18  # 20 entries, UKR and SYR twice
    assert got == {
        iso: ("2025", float(value), "persons")
        for iso, value in {
            "UKR": 1702358, "USA": 1230663, "SYR": 421693, "GBR": 389911,
            "PAK": -1235336, "IND": -495753, "BGD": -402100, "NPL": -364699,
            "OMN": 113734, "ARE": 158634,
            "MAF": -1170, "MHL": -1698, "TUV": -255, "JOR": -303267,
            "NAC": 1556865, "ECS": 1180849, "WLD": 0, "EAS": -37630,
        }.items()
    }
    labels = {o.series_key: o.label for o in c.observations}
    assert labels["UKR"] == "Ukraine: net migration (World Bank SM.POP.NETM)"


def test_unreadable_or_missing_period_is_skipped_and_counted_never_stored_undated() -> None:
    payload = _payload("liquidity_board")
    rows = {r["indicator"]: r for r in payload["indicators"]}
    rows["us_m2"]["observed_at"] = "2026-Q9"
    rows["euro_area_m1"]["previous_observed_at"] = None
    c = classify(SourceOutcome("liquidity_board", T0, payload=payload))
    assert c.extract_error == "2 invalid point(s) skipped"
    kept = {(o.series_key, o.observed_at) for o in c.observations}
    assert ("us_m2", "2026-07-01") in kept and ("euro_area_m1", "2026-08") in kept
    assert len(c.observations) == 14
    assert all(o.observed_at for o in c.observations)


def test_record_cycle_stores_extracted_series_once(tmp_path: Path) -> None:
    store = HistoryStore(tmp_path / "h.db")
    try:
        outcomes = [
            SourceOutcome(name, T0, payload=_payload(name))
            for name in ("liquidity_board", "net_liquidity", "net_migration")
        ]
        first = store.record_cycle(outcomes, now=T0)
        assert first["observations_stored"] == 16 + 12 + 18
        again = store.record_cycle(
            [SourceOutcome(o.source, T0 + 300, payload=o.payload) for o in outcomes], now=T0 + 300
        )
        assert again["observations_stored"] == 0
        weekly = store.series_history("liquidity_board:eurosystem_total_assets")
        assert [(o["observed_at"], o["value"]) for o in weekly["observations"]] == [
            ("2026-10-11", 5943911.0), ("2026-10-04", 5947826.0),
        ]
    finally:
        store.close()


def test_history_module_registers_these_extractors_on_import() -> None:
    """A fresh interpreter that imports only the store sees them, so the
    collector's classify finds them whatever imported history first."""
    code = (
        "import world_intel_mcp.history as h; "
        "print(sorted(set(h.registered_extractors()) & "
        "{'liquidity_board', 'net_liquidity', 'net_migration'}))"
    )
    out = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, check=True,
        env=os.environ.copy(), timeout=60,
    ).stdout
    assert out.strip() == "['liquidity_board', 'net_liquidity', 'net_migration']"
