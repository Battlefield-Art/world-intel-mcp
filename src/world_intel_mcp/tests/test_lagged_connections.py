"""Tests for analysis/lagged_connections.py, the intel_lagged_connections
tool and ``intel history connections``.

Reference values (P_CASES, BETA_CASES, PEARSON_*) were computed once with
scipy 1.18.1 and numpy 2.5.4 in a throwaway venv outside the repo and are
hardcoded here; nothing in the repo imports scipy:

- P_CASES: ``2 * scipy.stats.t.sf(abs(r) * sqrt(df / (1 - r**2)), df)`` with
  ``df = n - 2`` (fractional n stands in for an effective sample size).
- BETA_CASES: ``scipy.special.betainc(a, b, x)``.
- PEARSON_R, PEARSON_P: ``scipy.stats.pearsonr(PEARSON_X, PEARSON_Y)``.

The Benjamini-Hochberg examples are worked by hand in their test's
docstring. Synthetic series come from ``random.Random(seed).random()``
through Box-Muller, so they are identical on every supported Python (the
module promises ``random()`` stays stable across versions; ``gauss`` does
not). Series are written through ``HistoryStore.record_cycle`` with a
test-only extractor, so the analysis reads what the production write path
stored.
"""

import importlib
import json
import math
import os
import random
import statistics
import types
from datetime import date, timedelta

import pytest
from click.testing import CliRunner

from world_intel_mcp import cli, history
from world_intel_mcp.analysis import lagged_connections as lc
from world_intel_mcp.history import HistoryStore, Observation, SourceOutcome

from .test_server_registry import _TMP_CACHE

T0 = 1_760_000_000.0  # 2025-10-09T08:53:20Z
D0 = date(2025, 1, 6)  # a Monday

P_CASES = [
    (0.0, 30, 1.0),
    (0.05, 30, 0.7930219578171515),
    (0.3, 30, 0.10724594805795433),
    (0.5, 30, 0.004899933667068084),
    (0.9, 30, 1.316606070026386e-11),
    (-0.5, 30, 0.004899933667068084),
    (0.2, 100, 0.04603628646005413),
    (0.1, 1000, 0.0015441161074010938),
    (0.999, 50, 1.9009987456813015e-66),
    (0.7, 3, 0.5063666222132699),
    (0.5, 4, 0.5),
    (0.3, 3000, 1.9407172086957736e-63),
    (0.08, 3000, 1.1493171414190742e-05),
    (0.5, 21.5, 0.019331374409111864),
    (0.35, 7.25, 0.42927655756801303),
    (0.62, 12, 0.031508181347306076),
]
BETA_CASES = [
    (0.5, 0.5, 0.3, 0.36901011956554536),
    (2.5, 0.5, 0.9, 0.4895897445644278),
    (10.0, 3.0, 0.7, 0.25281534785499993),
    (100.0, 0.5, 0.99, 0.15677586542444089),
    (0.5, 50.0, 0.01, 0.6826956021258024),
    (14.0, 0.5, 0.75, 0.004899933667068084),
    (3.0, 7.0, 0.2, 0.26180249600000016),
    (1.0, 1.0, 0.42, 0.42),
]
PEARSON_X = [1.2, 2.3, 2.9, 4.1, 5.0, 6.2, 6.8, 8.1, 9.0, 9.9, 11.3, 12.0]
PEARSON_Y = [2.0, 1.1, 3.9, 3.0, 6.1, 4.2, 7.9, 6.0, 8.1, 11.9, 9.0, 10.2]
PEARSON_R = 0.9036537837015148
PEARSON_P = 5.551095396255053e-05


def _normals(seed: int, n: int) -> list[float]:
    rng = random.Random(seed)
    out: list[float] = []
    while len(out) < n:
        u1, u2 = rng.random(), rng.random()
        radius = math.sqrt(-2.0 * math.log(1.0 - u1))
        out += [radius * math.cos(2 * math.pi * u2), radius * math.sin(2 * math.pi * u2)]
    return out[:n]


def _walk(seed: int, n: int) -> list[float]:
    level, out = 0.0, []
    for step in _normals(seed, n):
        level += step
        out.append(level)
    return out


def _days(values, offset: int = 0) -> list[tuple[str, float]]:
    return [((D0 + timedelta(days=i + offset)).isoformat(), v) for i, v in enumerate(values)]


@pytest.fixture
def store(tmp_path, monkeypatch):
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    history.register_extractor(
        "lagtest",
        lambda payload: [
            Observation(p["key"], p["value"], observed_at=p["observed_at"])
            for p in payload["points"]
        ],
    )
    s = HistoryStore(tmp_path / "history.db")
    yield s
    s.close()


def _write(store, series: dict, at: float = T0) -> None:
    """One collector capture of source 'lagtest' holding every given point."""
    points = [
        {"key": key, "observed_at": observed_at, "value": value}
        for key, rows in series.items()
        for observed_at, value in rows
    ]
    store.record_cycle([SourceOutcome("lagtest", at, 1.0, {"points": points})], now=at)


def _run(store, keys, **kw):
    return lc.lagged_connections(store, [f"lagtest:{k}" if ":" not in k else k for k in keys], **kw)


def _planted(store, at: float = T0) -> None:
    """x is a random walk; y_t = x_(t-3) + noise, 200 days."""
    x = _walk(11, 200)
    noise = _normals(12, 200)
    y = [x[t - 3] + 0.3 * noise[t] for t in range(3, 200)]
    _write(store, {"x": _days(x), "y": _days(y, offset=3)}, at)


def _ar1(seed: int, n: int, phi: float) -> list[float]:
    """n unit-variance AR(1) values: e_t = phi e_(t-1) + sqrt(1 - phi^2) z_t."""
    z = _normals(seed, n)
    out = [z[0]]
    for t in range(1, n):
        out.append(phi * out[-1] + math.sqrt(1.0 - phi * phi) * z[t])
    return out


def _levels(changes: list[float]) -> list[float]:
    """Levels starting at 0 whose day-to-day changes are ``changes``."""
    level, out = 0.0, [0.0]
    for step in changes:
        level += step
        out.append(level)
    return out


# ---------------------------------------------------------------------------
# Statistics against independent reference values
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("r,n,expected", P_CASES)
def test_p_value_matches_scipy_reference(r, n, expected) -> None:
    assert lc.correlation_p_value(r, n) == pytest.approx(expected, rel=1e-9, abs=1e-300)


@pytest.mark.parametrize("a,b,x,expected", BETA_CASES)
def test_incomplete_beta_matches_scipy_reference(a, b, x, expected) -> None:
    assert lc.regularized_incomplete_beta(a, b, x) == pytest.approx(expected, rel=1e-12)


def test_incomplete_beta_edges_and_bad_arguments() -> None:
    assert lc.regularized_incomplete_beta(2.0, 3.0, 0.0) == 0.0
    assert lc.regularized_incomplete_beta(2.0, 3.0, 1.0) == 1.0
    for bad in [(0.0, 1.0, 0.5), (1.0, -1.0, 0.5), (1.0, 1.0, 1.5)]:
        with pytest.raises(ValueError):
            lc.regularized_incomplete_beta(*bad)
    with pytest.raises(ValueError):
        lc.correlation_p_value(0.5, 2)
    assert lc.correlation_p_value(1.0, 10) == 0.0
    assert lc.correlation_p_value(-1.0, 10) == 0.0


def test_pearson_matches_scipy_reference() -> None:
    r = lc.pearson_r(PEARSON_X, PEARSON_Y)
    assert r == pytest.approx(PEARSON_R, rel=1e-12)
    assert lc.correlation_p_value(r, len(PEARSON_X)) == pytest.approx(PEARSON_P, rel=1e-9)
    # statistics.correlation is a second, independent Pearson implementation.
    assert r == pytest.approx(statistics.correlation(PEARSON_X, PEARSON_Y), rel=1e-12)
    assert lc.pearson_r([1.0, 1.0, 1.0], [1.0, 2.0, 3.0]) is None
    assert lc.pearson_r([0.1, 0.1, 0.1, 0.1], [1.0, 2.0, 3.0, 4.0]) is None
    with pytest.raises(ValueError):
        lc.pearson_r([1.0, 2.0], [1.0])


def test_benjamini_hochberg_hand_worked() -> None:
    """m = 4, p = (0.02, 0.03, 0.035, 0.5). Sorted ranks give m p / rank of
    0.08, 0.06, 0.04667 and 0.5; stepping down from the largest rank keeps
    the running minimum, so q = (0.04667, 0.04667, 0.04667, 0.5). The first
    p alone (0.08 > 0.05) would not pass; the step-up rescues it.

    m = 5, p = (0.01, 0.04, 0.03, 0.005, 0.2): m p / rank in rank order is
    0.025, 0.025, 0.05, 0.05, 0.2, already non-decreasing, so q in input
    order is (0.025, 0.05, 0.05, 0.025, 0.2).

    BY multiplies BH by 1 + 1/2 + 1/3 + 1/4 = 25/12 for m = 4: 0.04667 x
    25/12 = 0.09722, and 0.5 x 25/12 caps at 1."""
    assert lc.benjamini_hochberg([0.02, 0.03, 0.035, 0.5]) == pytest.approx(
        [0.14 / 3, 0.14 / 3, 0.14 / 3, 0.5]
    )
    assert lc.benjamini_hochberg([0.01, 0.04, 0.03, 0.005, 0.2]) == pytest.approx(
        [0.025, 0.05, 0.05, 0.025, 0.2]
    )
    assert lc.benjamini_yekutieli([0.02, 0.03, 0.035, 0.5]) == pytest.approx(
        [0.14 / 3 * 25 / 12] * 3 + [1.0]
    )
    assert lc.benjamini_hochberg([0.9, 0.95]) == pytest.approx([0.95, 0.95])
    assert lc.benjamini_hochberg([]) == []


# ---------------------------------------------------------------------------
# Grid, sampling, transform
# ---------------------------------------------------------------------------


def test_period_rules_for_each_grid() -> None:
    day = date(2026, 10, 12).toordinal()
    assert lc.period_of("2026-10-12", "day") == (day, None)
    assert lc.period_of("2026-10-12T23:59:59Z", "day") == (day, None)
    assert lc.period_of("2026-10-13", "day") == (day + 1, None)
    # Weeks run Monday (2026-10-12) to Sunday (2026-10-18).
    monday = lc.period_of("2026-10-12", "week")[0]
    assert lc.period_of("2026-10-18T22:00:00Z", "week")[0] == monday
    assert lc.period_of("2026-10-19", "week")[0] == monday + 1
    assert lc.period_label(lc.period_of("2026-10-15", "week")[0], "week") == "2026-10-12"
    december = lc.period_of("2025-12-31", "month")[0]
    assert lc.period_of("2026-01-01", "month")[0] == december + 1
    assert lc.period_of("2026-01", "month")[0] == december + 1
    assert lc.period_label(december + 1, "month") == "2026-01"
    assert lc.period_label(day, "day") == "2026-10-12"
    assert lc.period_of("2026-01", "day") == (None, "coarser")
    assert lc.period_of("2026-01", "week") == (None, "coarser")
    assert lc.period_of("2026", "month") == (None, "coarser")
    for bad in ("2026-13", "2026-02-30", "yesterday", "2026-10-12T03:00:00+02:00"):
        assert lc.period_of(bad, "day")[1] == "unreadable"


def test_changes_are_never_taken_across_a_gap() -> None:
    levels = {1: 10.0, 2: 12.0, 3: 11.0, 5: 20.0, 6: 26.0}
    assert lc.apply_transform(levels, "changes") == {2: 2.0, 3: -1.0, 6: 6.0}
    assert lc.apply_transform(levels, "levels") == levels


def test_lag1_autocorrelation_uses_adjacent_periods_only() -> None:
    values = {p: v for p, v in enumerate([3.0, 1.0, 4.0, 1.0, 5.0, 9.0, 2.0, 6.0, 5.0, 3.0, 5.0, 8.0])}
    values[40] = 100.0  # isolated: no neighbour, so it joins no pair
    phi, pairs = lc.lag1_autocorrelation(values)
    seq = [values[p] for p in range(12)]
    assert pairs == 11
    assert phi == pytest.approx(statistics.correlation(seq[:-1], seq[1:]), rel=1e-12)
    assert lc.lag1_autocorrelation({p: float(p % 3) for p in range(5)}) == (None, 4)


def test_week_grid_takes_each_weeks_last_observation_and_latest_vintage(store) -> None:
    # Business days 2025-01-06 (Mon) to 2025-01-16 (Thu), valued 0..8.
    weekdays = [D0 + timedelta(days=i) for i in range(11) if (D0 + timedelta(days=i)).weekday() < 5]
    _write(store, {"w": [(d.isoformat(), float(i)) for i, d in enumerate(weekdays)]})
    friday = weekdays[4].isoformat()  # 2025-01-10
    _write(store, {"w": [(friday, 99.0)]}, at=T0 + 600)  # a later capture revises Friday
    info = lc._sample_on_grid("lagtest:w", lc._read_rows(store, "lagtest:w"), "week")
    first_week = lc.period_of(D0.isoformat(), "week")[0]
    # Week 1 ends on Friday, read at its revised value; week 2 ends on Thursday (8).
    assert info["_levels"] == {first_week: 99.0, first_week + 1: 8.0}
    assert info["revised_observed_at"] == 1
    assert (info["rows"], info["dated_observations"]) == (10, 9)


# ---------------------------------------------------------------------------
# What the tests find
# ---------------------------------------------------------------------------


def test_planted_lag_is_found_at_the_predicted_lag_and_survives_fdr(store) -> None:
    _planted(store)
    result = _run(store, ["x", "y"])
    (pair,) = result["pairs"]
    (best,) = pair["strongest"]
    # y_t = x_(t-3): x's value at t is compared with y's at t+3, so +3.
    assert best["lag"] == 3
    assert best["timing"] == "A leads B by 3 days"
    assert best["direction"] == "same"
    assert best["r"] > 0.9
    # 199 changes of x (days 1-199), 196 of y (days 4-199); at +3 they meet on 196.
    assert best["n"] == 196
    assert best["q"] < 1e-50 and best["significant"] is True
    assert [e["lag"] for e in pair["significant"]] == [3]
    assert result["tests_run"] == 11 and result["significant_tests"] == 1
    assert result["method"]["q_threshold"] == 0.05
    assert "A leads B by k" in result["method"]["lag_convention"]
    assert result["method"]["transform"] == "changes"


def test_lag_sign_follows_the_order_the_series_are_given(store) -> None:
    """B_t = A_(t-2) exactly: given as (A, B) the connection sits at +2 with
    r = 1; given as (B, A) it sits at -2, read as B leads A."""
    a = _walk(5, 60)
    _write(store, {"a": _days(a), "b": _days(a[:-2], offset=2)})
    forward = _run(store, ["a", "b"])["pairs"][0]["strongest"][0]
    assert (forward["lag"], forward["timing"]) == (2, "A leads B by 2 days")
    assert forward["r"] == pytest.approx(1.0) and forward["p"] == 0.0
    backward = _run(store, ["b", "a"])["pairs"][0]["strongest"][0]
    assert (backward["lag"], backward["timing"]) == (-2, "B leads A by 2 days")


def test_independent_series_give_no_fdr_significant_result(store) -> None:
    """Four independent random walks (seeds 101-104), 250 days, default
    settings: 6 pairs x 11 lags = 66 tests, none at q <= 0.05 (the smallest
    q measured is 0.699)."""
    _write(store, {f"ind{i}": _days(_walk(100 + i, 250)) for i in range(1, 5)})
    result = _run(store, [f"ind{i}" for i in range(1, 5)])
    assert result["tests_run"] == 66
    assert result["significant_tests"] == 0
    assert all(not p["significant"] for p in result["pairs"])
    assert min(e["q"] for p in result["pairs"] for e in p["strongest"]) > 0.05
    assert result["summary"].startswith("0 of 6 pairs")


def test_random_walks_correlate_in_levels_but_not_in_changes(store) -> None:
    """Independent random walks are the textbook spurious correlation. Seeds
    1000/1001: |r| = 0.87 in levels (q ~ 1e-59 with periods treated as
    independent), nothing in changes. Over 20 seed pairs (measured: 14 with a
    level lag at q <= 0.05 but none at q_ar1 <= 0.05, 1 with a significant
    change lag, the nominal 5% rate) the default transform protects, and in
    levels the AR(1) adjustment does: seeds 1000/1001 have an effective n of
    0.9, so p_ar1 is 1 (no evidence)."""
    _write(store, {"rwa": _days(_walk(1000, 200)), "rwb": _days(_walk(1001, 200))})
    levels = _run(store, ["rwa", "rwb"], transform="levels")
    best = levels["pairs"][0]["strongest"][0]
    assert abs(best["r"]) > 0.8 and best["significant_if_independent"] is True
    assert best["significant"] is False and best["p_ar1"] == 1.0
    assert best["n_effective_ar1"] < 0.1 * best["n"]
    assert best["p_ar1"] is None or best["p_ar1"] > 100 * best["p"]
    assert all(s["lag1_autocorrelation"] > 0.9 for s in levels["series"])
    changes = _run(store, ["rwa", "rwb"])
    assert changes["significant_tests"] == 0
    assert abs(changes["pairs"][0]["strongest"][0]["r"]) < 0.2

    level_hits = level_hits_if_independent = change_hits = 0
    for s in range(1, 20):
        _write(store, {f"a{s}": _days(_walk(1000 + 2 * s, 200)),
                       f"b{s}": _days(_walk(1001 + 2 * s, 200))}, at=T0 + s)
        level = _run(store, [f"a{s}", f"b{s}"], transform="levels")
        level_hits += level["significant_tests"] > 0
        level_hits_if_independent += level["significant_tests_if_independent"] > 0
        change_hits += _run(store, [f"a{s}", f"b{s}"])["significant_tests"] > 0
    assert level_hits_if_independent >= 12
    assert level_hits == 0
    assert change_hits <= 2


def test_weekly_grid_finds_a_two_week_lead_in_daily_data(store) -> None:
    """y(d) = x(d - 14) daily; on the week grid each week takes its Sunday,
    so Y_w = X_(w-2) and the weekly changes match exactly at +2 weeks."""
    x = _walk(21, 7 * 60)
    _write(store, {"x": _days(x), "y": _days(x[:-14], offset=14)})
    result = _run(store, ["x", "y"], grid="week", max_lag=3)
    best = result["pairs"][0]["strongest"][0]
    assert (best["lag"], best["timing"]) == (2, "A leads B by 2 weeks")
    assert best["r"] == pytest.approx(1.0)
    assert result["method"]["grid_rule"].startswith("ISO weeks in UTC, Monday to Sunday")
    assert {s["series_key"]: s["first_period"] for s in result["series"]} == {
        "lagtest:x": D0.isoformat(), "lagtest:y": (D0 + timedelta(days=14)).isoformat()
    }


def test_month_periods_fit_the_month_grid_and_are_coarser_than_days(store) -> None:
    a = _walk(31, 41)
    months = [f"{2020 + m // 12}-{m % 12 + 1:02d}" for m in range(41)]
    _write(store, {"ma": list(zip(months, a)), "mb": list(zip(months[1:], a[:-1]))})
    monthly = _run(store, ["ma", "mb"], grid="month", max_lag=2)
    best = monthly["pairs"][0]["strongest"][0]
    assert (best["lag"], best["timing"]) == (1, "A leads B by 1 month")
    daily = _run(store, ["ma", "mb"])
    assert daily["pairs"] == []
    reasons = {e["series_key"]: e["reason"] for e in daily["excluded_series"]}
    assert "coarser than the day grid" in reasons["lagtest:ma"]
    assert "Use grid=month" in reasons["lagtest:ma"]
    assert "fewer than two series could be used, so nothing was tested" in daily["data_gaps"]


# ---------------------------------------------------------------------------
# Minimum sample, revisions, undated rows
# ---------------------------------------------------------------------------


def test_minimum_overlap_excludes_and_reports_each_lag_with_its_n(store) -> None:
    """33 days each, so 32 changes on the same days: lag k overlaps 32 - |k|.
    At the default minimum of 30, lags -2..+2 run and the rest are listed."""
    _write(store, {"s1": _days(_walk(41, 33)), "s2": _days(_walk(42, 33))})
    result = _run(store, ["s1", "s2"])
    (pair,) = result["pairs"]
    assert result["method"]["min_overlap"] == lc.DEFAULT_MIN_OVERLAP == 30
    assert "80% power" in result["method"]["min_overlap_reason"]
    assert pair["not_tested"] == [
        {"lag": -5, "n": 27, "reason": "below_min_overlap"},
        {"lag": -4, "n": 28, "reason": "below_min_overlap"},
        {"lag": -3, "n": 29, "reason": "below_min_overlap"},
        {"lag": 3, "n": 29, "reason": "below_min_overlap"},
        {"lag": 4, "n": 28, "reason": "below_min_overlap"},
        {"lag": 5, "n": 27, "reason": "below_min_overlap"},
    ]
    assert (pair["lags_tested"], pair["lags_not_tested"]) == (5, 6)
    assert (result["tests_run"], result["tests_not_run"]) == (5, 6)
    assert abs(pair["strongest"][0]["lag"]) <= 2
    lowered = _run(store, ["s1", "s2"], min_overlap=20)
    assert (lowered["tests_run"], lowered["tests_not_run"]) == (11, 0)


def test_pairs_without_variation_and_short_series_are_reported(store) -> None:
    _write(store, {
        "flat": _days([4.33] * 40),
        "moving": _days(_walk(51, 40)),
        "short": _days(_walk(52, 10)),
    })
    result = _run(store, ["flat", "moving", "short"], max_lag=1)
    by_pair = {(p["a"], p["b"]): p for p in result["pairs"]}
    flat = by_pair[("lagtest:flat", "lagtest:moving")]
    assert [nt["reason"] for nt in flat["not_tested"]] == ["no_variation"] * 3
    assert [nt["n"] for nt in flat["not_tested"]] == [38, 39, 38]
    assert flat["strongest"] == []
    gaps = "\n".join(result["data_gaps"])
    assert "lagtest:short: 9 period-over-period changes on the day grid" in gaps
    assert "lagtest:flat / lagtest:moving: no lag could be tested" in gaps


def test_revisions_use_the_latest_vintage(store) -> None:
    """First capture: a is unrelated noise. A later capture revises every one
    of a's 60 points to b two days later. The tool must read the revision."""
    b = _walk(61, 62)
    _write(store, {"a": _days(_normals(62, 60)), "b": _days(b)})
    before = _run(store, ["a", "b"], max_lag=3)
    assert before["significant_tests"] == 0
    _write(store, {"a": _days(b[2:])}, at=T0 + 3600)
    after = _run(store, ["a", "b"], max_lag=3)
    best = after["pairs"][0]["strongest"][0]
    assert (best["lag"], best["r"]) == (2, pytest.approx(1.0))
    a_info = next(s for s in after["series"] if s["series_key"] == "lagtest:a")
    assert a_info["revised_observed_at"] == 60
    assert a_info["rows"] == 120 and a_info["dated_observations"] == 60
    assert "latest collected value" in after["method"]["vintage"]
    assert any("60 observation dates were revised" in g for g in after["data_gaps"])


def test_series_without_observed_at_are_excluded_with_the_stated_rule(store) -> None:
    for i, price in enumerate(_walk(71, 40)):
        store.record_cycle(
            [SourceOutcome("market_quotes", T0 + 300 * i, 1.0,
                           {"quotes": [{"symbol": "^GSPC", "price": 5000 + price}]})],
            now=T0 + 300 * i,
        )
    _planted(store)
    result = _run(store, ["market_quotes:^GSPC:price", "x", "y"])
    (excluded,) = result["excluded_series"]
    assert excluded["series_key"] == "market_quotes:^GSPC:price"
    assert "no row has observed_at (40 rows carry collected_at only)" in excluded["reason"]
    assert "stored only on change" in excluded["reason"]
    assert [(p["a"], p["b"]) for p in result["pairs"]] == [("lagtest:x", "lagtest:y")]
    assert "collected_at" in result["method"]["undated_rows"]
    alone = _run(store, ["market_quotes:^GSPC:price", "x"])
    assert alone["pairs"] == [] and alone["tests_run"] == 0
    assert "fewer than two series could be used, so nothing was tested" in alone["data_gaps"]


def test_window_selects_whole_periods(store) -> None:
    _planted(store)
    result = _run(store, ["x", "y"], start="2025-03-01", end="2025-05-31")
    window = result["method"]["window"]
    assert (window["start"], window["end"], window["periods"]) == ("2025-03-01", "2025-05-31", 92)
    x_info = result["series"][0]
    assert x_info["periods"] == 92 and x_info["periods_outside_window"] == 108
    assert result["pairs"][0]["strongest"][0]["lag"] == 3
    weekly = _run(store, ["x", "y"], grid="week", start="2025-03-05", end="2025-03-05", min_overlap=20)
    assert weekly["method"]["window"]["start"] == "2025-03-03"
    empty = _run(store, ["x", "y"], start="2030-01-01")
    assert empty["pairs"] == []
    assert {e["reason"] for e in empty["excluded_series"]} == {"no observation inside the window"}


def test_window_is_capped_to_the_most_recent_periods(store, monkeypatch) -> None:
    monkeypatch.setattr(lc, "MAX_PERIODS", 120)
    _planted(store)
    result = _run(store, ["x", "y"])
    window = result["method"]["window"]
    assert window["limited_to_recent"] is True and window["periods"] == 120
    assert window["end"] == (D0 + timedelta(days=199)).isoformat()
    assert "window limited to the most recent 120 days" in result["data_gaps"]


# ---------------------------------------------------------------------------
# Serial-correlation diagnostic
# ---------------------------------------------------------------------------


def test_ar1_effective_sample_size_follows_the_reported_autocorrelations(store) -> None:
    _planted(store)
    result = _run(store, ["x", "y"], transform="levels")
    best = result["pairs"][0]["strongest"][0]
    phi_a, phi_b = (s["lag1_autocorrelation"] for s in result["series"])
    expected = min(best["n"], best["n"] * (1 - phi_a * phi_b) / (1 + phi_a * phi_b))
    assert best["n_effective_ar1"] == pytest.approx(expected, abs=0.5)
    assert best["n_effective_ar1"] < best["n"]
    assert "does not enter q" in result["method"]["serial_correlation"]
    assert any("Serial correlation inflates significance" in c for c in result["cannot_prove"])


def test_cannot_prove_says_a_small_q_is_not_a_large_effect(store) -> None:
    """The effect-size line quotes the module's own p for |r| 0.1 at n 1,000
    (P_CASES pins that p to scipy's 0.00154). The list a call returns is a
    copy: clearing it leaves the module's list whole."""
    _planted(store)
    result = _run(store, ["x", "y"])
    line = next(c for c in result["cannot_prove"] if c.startswith("A small q is not a large effect"))
    assert "an |r| of 0.1 (1% of the variance shared) already gives p = 0.0015;" in line
    result["cannot_prove"].clear()
    result["method"]["not_tested_reasons"].clear()
    assert len(lc.CANNOT_PROVE) == 7
    assert set(lc.NOT_TESTED_REASONS) == {"below_min_overlap", "no_variation"}


# ---------------------------------------------------------------------------
# Serial correlation decides significance (q_ar1)
# ---------------------------------------------------------------------------


def test_independent_random_walks_in_levels_are_not_flagged(store) -> None:
    """Two independent Gaussian random walks of 300 daily points per seed,
    levels, defaults. q treats the periods as independent and flags almost
    every seed; significant follows q_ar1 and flags about none. Measured
    through the store in a scratch run over seeds 0-99: 91 by q, 0 by q_ar1.
    Seeds 0-19 here: 19 by q, 0 by q_ar1."""
    series = {}
    for s in range(20):
        steps = _normals(s, 600)
        series[f"w{s}a"] = _days(_levels(steps[:300])[1:])
        series[f"w{s}b"] = _days(_levels(steps[300:])[1:])
    _write(store, series)
    flagged = flagged_if_independent = 0
    for s in range(20):
        result = _run(store, [f"w{s}a", f"w{s}b"], transform="levels")
        flagged += result["significant_tests"] > 0
        flagged_if_independent += result["significant_tests_if_independent"] > 0
    assert flagged_if_independent >= 15
    assert flagged <= 1


def test_autocorrelated_changes_keep_significance_near_the_nominal_rate(store) -> None:
    """Independent pairs whose daily changes are AR(1) with phi 0.8, 200
    changes each, one test per call (max_lag 0), so q_ar1 is p_ar1 and q is
    p. A scratch run of 4,000 pairs gave p <= 0.05 in 35.3% and p_ar1 <= 0.05
    in 5.1%; these 120 pairs give 44 by q and 4 by q_ar1."""
    series = {}
    for run in range(120):
        series[f"ar{run}a"] = _days(_levels(_ar1(2000 + 2 * run, 200, 0.8)))
        series[f"ar{run}b"] = _days(_levels(_ar1(2001 + 2 * run, 200, 0.8)))
    _write(store, series)
    significant = if_independent = 0
    for run in range(120):
        result = _run(store, [f"ar{run}a", f"ar{run}b"], max_lag=0)
        significant += result["significant_tests"]
        if_independent += result["significant_tests_if_independent"]
    assert if_independent >= 30
    assert significant <= 12
    gaps = _run(store, ["ar0a", "ar0b"], max_lag=0)["data_gaps"]
    assert any(g.startswith("lagtest:ar0a: lag-1 autocorrelation +0.8") for g in gaps)
    assert any("significance uses the AR(1)-adjusted p_ar1 and q_ar1" in g for g in gaps)


def test_below_an_effective_size_of_three_is_no_evidence(store) -> None:
    """Seeds 1000/1001 in levels: |r| 0.87 and q ~ 1e-59 if periods were
    independent, but both lag-1 autocorrelations are above 0.99, leaving an
    effective sample under 1. That is reported as no evidence, not as null."""
    _write(store, {"rwa": _days(_walk(1000, 200)), "rwb": _days(_walk(1001, 200))})
    result = _run(store, ["rwa", "rwb"], transform="levels")
    best = result["pairs"][0]["strongest"][0]
    assert abs(best["r"]) > 0.8 and best["q"] < 1e-50
    assert best["n_effective_ar1"] < 3 and best["p_ar1"] == 1.0 and best["q_ar1"] == 1.0
    assert best["note"].startswith("no evidence:")
    assert best["significant"] is False and best["significant_if_independent"] is True
    assert best["serial_correlation_dominates"] is True
    assert result["significant_tests"] == 0 and result["significant_tests_if_independent"] > 0
    assert result["summary"].startswith("0 of 1 pairs have a lag with q_ar1 <= 0.05")
    assert "Assuming independent periods instead, 1 of 1 pairs have a lag with q <= 0.05." in result["summary"]
    (dominates,) = [g for g in result["data_gaps"] if "serial correlation dominates" in g]
    assert dominates.startswith("lagtest:rwa / lagtest:rwb: serial correlation dominates")
    assert "no evidence either way" in dominates
    assert "significant uses q_ar1" in result["method"]["serial_correlation"]
    assert "post-selection" in result["method"]["serial_correlation"]


def test_unmeasurable_autocorrelation_is_not_counted_as_significant(store) -> None:
    """Weekly observations on the day grid, in levels: no two observations
    sit on adjacent days, so neither lag-1 autocorrelation can be measured and
    p cannot be adjusted. The near-exact lag-0 match keeps its p but is not
    counted as significant, and the response says why."""
    x = _walk(501, 40)
    noise = _normals(502, 40)
    weeks = [(D0 + timedelta(days=7 * i)).isoformat() for i in range(40)]
    _write(store, {"wx": list(zip(weeks, x)),
                   "wy": list(zip(weeks, [v + 0.1 * e for v, e in zip(x, noise)]))})
    result = _run(store, ["wx", "wy"], transform="levels")
    best = result["pairs"][0]["strongest"][0]
    assert (best["lag"], best["n"]) == (0, 40) and best["r"] > 0.99
    assert best["significant_if_independent"] is True and best["significant"] is False
    assert best["n_effective_ar1"] is None and best["p_ar1"] == 1.0
    assert best["serial_correlation_dominates"] is None
    assert best["note"].startswith("not assessed:")
    unmeasurable = [g for g in result["data_gaps"]
                    if "lag-1 autocorrelation not measurable (0 adjacent periods" in g]
    assert len(unmeasurable) == 2
    assert all(g.endswith("a median 7 days apart, so a coarser grid may fit") for g in unmeasurable)


def test_q_and_q_ar1_adjust_across_every_test_in_the_call(store) -> None:
    """Three series, 3 pairs x 11 lags = 33 tests. The planted lag's p (and
    p_ar1) is the smallest in the call by far, so Benjamini-Hochberg gives it
    m p with m = 33, every test in the call; per pair it would be 11 p.
    Measured: q / p = 33.08 and q_ar1 / p_ar1 = 32.98 after rounding to three
    significant digits."""
    _planted(store)
    _write(store, {"z": _days(_walk(13, 200))}, at=T0 + 1)
    result = _run(store, ["x", "y", "z"])
    assert result["tests_run"] == 33
    (xy,) = [p for p in result["pairs"] if (p["a"], p["b"]) == ("lagtest:x", "lagtest:y")]
    best = xy["strongest"][0]
    assert best["lag"] == 3 and 0 < best["p"] <= best["p_ar1"] < 1e-50
    # Ratios: pytest.approx's default absolute tolerance (1e-12) would call
    # any two values this small equal.
    assert best["q"] / best["p"] == pytest.approx(33, rel=0.02)
    assert best["q_ar1"] / best["p_ar1"] == pytest.approx(33, rel=0.02)
    harmonic = sum(1 / i for i in range(1, 34))
    assert best["q_by"] / best["q_ar1"] == pytest.approx(harmonic, rel=0.02)


def test_strongest_breaks_ties_on_p_by_the_larger_t(store) -> None:
    """b_t = a_(t-2) + 0.9 a_(t-3) + small noise over 3,000 days: in changes,
    r is 0.748 at +2 and 0.675 at +3 on 2,996 points, and both p underflow to
    exactly 0.0. Equal p, so the larger |t| (lag +2) is the strongest."""
    a = _walk(301, 3000)
    noise = _normals(302, 3000)
    b = [a[t - 2] + 0.9 * a[t - 3] + 0.05 * noise[t] for t in range(3, 3000)]
    _write(store, {"ta": _days(a), "tb": _days(b, offset=3)})
    pair = _run(store, ["ta", "tb"])["pairs"][0]
    assert [(e["lag"], e["p"]) for e in pair["significant"]] == [(2, 0.0), (3, 0.0)]
    (best,) = pair["strongest"]
    assert (best["lag"], best["n"]) == (2, 2996)
    assert best["r"] == pytest.approx(0.748, abs=0.005)


def test_a_q_exactly_at_the_threshold_is_significant(store, monkeypatch) -> None:
    """The rule is q_ar1 <= threshold (q <= threshold for
    significant_if_independent). With every adjusted value forced to the
    threshold itself, every test counts."""
    _planted(store)
    monkeypatch.setattr(lc, "benjamini_hochberg", lambda pvalues: [0.05] * len(pvalues))
    result = _run(store, ["x", "y"])
    assert result["tests_run"] == 11
    assert result["significant_tests"] == 11
    assert result["significant_tests_if_independent"] == 11
    entries = result["pairs"][0]["significant"]
    assert len(entries) == 11 and all(e["q_ar1"] == e["q"] == 0.05 for e in entries)


def test_effective_sample_size_is_capped_at_n(store) -> None:
    """Changes autocorrelated with opposite signs (measured phi +0.53 and
    -0.42) make (1 - phi_a phi_b) / (1 + phi_a phi_b) about 1.57; the
    effective size is capped at n, so p_ar1 equals p instead of undercutting
    it."""
    pos = _ar1(401, 300, 0.5)
    neg = [c + 0.25 * v for v, c in zip(pos, _ar1(402, 300, -0.5))]
    _write(store, {"pos": _days(_levels(pos)), "neg": _days(_levels(neg))})
    result = _run(store, ["pos", "neg"])
    phi_a, phi_b = (s["lag1_autocorrelation"] for s in result["series"])
    assert phi_a > 0.3 and phi_b < -0.3
    best = result["pairs"][0]["strongest"][0]
    assert best["lag"] == 0 and best["p"] < 1e-4
    assert best["n_effective_ar1"] == best["n"] == 300
    assert best["p_ar1"] == best["p"]
    assert best["serial_correlation_dominates"] is False


def test_week_grid_reports_unequal_change_intervals(store) -> None:
    """Each week takes its last observation. A series seen on Fridays except
    every third week (Monday only) has changes spanning 3 to 11 days; a daily
    series has 7-day changes throughout."""
    rows = [((D0 + timedelta(days=7 * w + (0 if w % 3 == 0 else 4))).isoformat(), v)
            for w, v in enumerate(_walk(601, 40))]
    _write(store, {"irregular": rows, "daily": _days(_walk(602, 280))})
    result = _run(store, ["irregular", "daily"], grid="week", min_overlap=20, max_lag=1)
    by_key = {s["series_key"]: s for s in result["series"]}
    assert by_key["lagtest:irregular"]["change_interval_days"] == {"min": 3, "max": 11}
    assert by_key["lagtest:daily"]["change_interval_days"] == {"min": 7, "max": 7}
    gaps = result["data_gaps"]
    assert any(g.startswith("lagtest:irregular: on the week grid its changes span 3 to 11 days")
               for g in gaps)
    assert not any(g.startswith("lagtest:daily: on the week grid") for g in gaps)
    assert "unequal intervals" in result["method"]["sampling"]
    levels = _run(store, ["irregular", "daily"], grid="week", transform="levels", max_lag=1)
    assert all(s["change_interval_days"] is None for s in levels["series"])


def test_month_grid_names_unequal_intervals_only_beyond_calendar_months(store) -> None:
    """Calendar months alone run 28 to 31 days, so daily data covering whole
    months (2023-01 to 2024-12) gets no unequal-interval line. Ending on
    2024-12-15 makes the last change span 15 days, and the line appears."""
    start = date(2023, 1, 1)

    def rows(seed: int, end: date) -> list[tuple[str, float]]:
        values = _walk(seed, (end - start).days + 1)
        return [((start + timedelta(days=i)).isoformat(), v) for i, v in enumerate(values)]

    _write(store, {"m1": rows(1001, date(2024, 12, 31)), "m2": rows(1002, date(2024, 12, 31)),
                   "p1": rows(1001, date(2024, 12, 15)), "p2": rows(1002, date(2024, 12, 15))})
    whole = _run(store, ["m1", "m2"], grid="month", min_overlap=20, max_lag=1)
    assert [s["change_interval_days"] for s in whole["series"]] == [{"min": 28, "max": 31}] * 2
    assert not any("unequal intervals" in g for g in whole["data_gaps"])
    partial = _run(store, ["p1", "p2"], grid="month", min_overlap=20, max_lag=1)
    assert [s["change_interval_days"] for s in partial["series"]] == [{"min": 15, "max": 31}] * 2
    assert [g for g in partial["data_gaps"] if "unequal intervals" in g] == [
        f"lagtest:{k}: on the month grid its changes span 15 to 31 days (each period takes "
        "its last observation), so changes cover unequal intervals" for k in ("p1", "p2")
    ]


def test_q_ar1_counts_the_tests_that_could_not_be_assessed(store) -> None:
    """x and y are daily (y_t = x_(t-3) + noise, 300 days); z is seen only on
    Mondays and Tuesdays, so its changes have no adjacent pairs and its 22
    tests are not assessed (p_ar1 = 1). They stay in the call's adjustment:
    the planted test's q_ar1 is 33 p_ar1 (measured 32.97), not 11 p_ar1."""
    x = _walk(701, 300)
    noise = _normals(702, 300)
    z = [((D0 + timedelta(days=i)).isoformat(), v)
         for i, v in enumerate(_walk(703, 300)) if i % 7 in (0, 1)]
    _write(store, {"x": _days(x), "y": _days([x[i - 3] + 0.3 * noise[i] for i in range(3, 300)],
                                             offset=3), "z": z})
    result = _run(store, ["x", "y", "z"])
    assert result["tests_run"] == 33
    assert [s["lag1_pairs"] for s in result["series"]] == [298, 295, 0]
    by_pair = {(p["a"], p["b"]): p for p in result["pairs"]}
    best = by_pair[("lagtest:x", "lagtest:y")]["strongest"][0]
    assert best["lag"] == 3 and "note" not in best and 0 < best["p_ar1"] < 1e-100
    assert best["q_ar1"] / best["p_ar1"] == pytest.approx(33, rel=0.02)
    for key in ("lagtest:x", "lagtest:y"):
        (zbest,) = by_pair[(key, "lagtest:z")]["strongest"]
        assert zbest["note"] == lc.NO_PHI_NOTE and zbest["p_ar1"] == 1.0


def test_an_effective_size_just_under_three_is_no_evidence_and_reads_2_9(store) -> None:
    """Random walks (seeds 3240/3241, 40 days) in levels, lag 0: measured
    phi values leave an effective size of 2.97. That is below 3, so p_ar1 is 1
    with the no-evidence note, and it is shown rounded down as 2.9, never as
    3.0."""
    _write(store, {"ra": _days(_walk(3240, 40)), "rb": _days(_walk(3241, 40))})
    result = _run(store, ["ra", "rb"], transform="levels", max_lag=0)
    best = result["pairs"][0]["strongest"][0]
    assert best["n"] == 40 and best["n_effective_ar1"] == 2.9
    assert best["p_ar1"] == 1.0 and best["note"] == lc.NO_EVIDENCE_NOTE
    assert best["significant"] is False
    (gap,) = [g for g in result["data_gaps"] if "serial correlation dominates" in g]
    assert "effective sample size of 2.9 of 40 periods" in gap
    assert gap.endswith("Below 3 the sample carries no evidence either way (p_ar1 set to 1)")


def test_one_unmeasurable_autocorrelation_leaves_the_test_unassessed(store) -> None:
    """A daily walk (210 days, phi measured) against a weekly copy of it plus
    noise (30 Mondays, no adjacent days, phi unknown), in levels on the day
    grid. Six lags reach 30 points; every one is not assessed, none is
    significant, and only the weekly series gets the unmeasurable line."""
    d = _walk(801, 210)
    weekly = [((D0 + timedelta(days=7 * i)).isoformat(), d[7 * i] + 0.2 * e)
              for i, e in enumerate(_normals(802, 30))]
    _write(store, {"daily": _days(d), "weekly": weekly})
    result = _run(store, ["daily", "weekly"], transform="levels")
    phi_daily, phi_weekly = (s["lag1_autocorrelation"] for s in result["series"])
    assert phi_daily > 0.9 and phi_weekly is None
    assert result["tests_run"] == 6
    assert result["significant_tests"] == 0 and result["significant_tests_if_independent"] == 6
    best = result["pairs"][0]["strongest"][0]
    assert (best["lag"], best["n"]) == (0, 30) and best["r"] > 0.99
    assert best["n_effective_ar1"] is None and best["p_ar1"] == 1.0
    assert best["note"] == lc.NO_PHI_NOTE
    unmeasurable = [g for g in result["data_gaps"] if "not measurable" in g]
    assert len(unmeasurable) == 1 and unmeasurable[0].startswith("lagtest:weekly:")


def test_negative_autocorrelation_gets_its_data_gaps_line(store) -> None:
    """AR(1) changes at phi -0.5 (measured -0.51) beside white-noise changes
    (measured +0.10): the line covers |phi| above 0.3 whatever its sign, and
    the white-noise series gets none."""
    _write(store, {"neg": _days(_levels(_ar1(901, 300, -0.5))),
                   "oth": _days(_levels(_normals(902, 300)))})
    result = _run(store, ["neg", "oth"])
    phis = [s["lag1_autocorrelation"] for s in result["series"]]
    assert phis[0] == pytest.approx(-0.5052, abs=1e-4) and abs(phis[1]) < 0.3
    assert [g for g in result["data_gaps"] if "lag-1 autocorrelation" in g] == [
        "lagtest:neg: lag-1 autocorrelation -0.51 after the transform; p and q assume "
        "independent periods, so significance uses the AR(1)-adjusted p_ar1 and q_ar1"
    ]


def test_serial_correlation_dominates_above_an_effective_size_of_three(store) -> None:
    """Random walks (seeds 2000/2001, 300 days) in levels, lag 0: effective
    size 5.9 of 300 (measured), above 3 so p_ar1 is computed (0.608), but
    below 0.2 n, so serial correlation dominates."""
    _write(store, {"ra": _days(_walk(2000, 300)), "rb": _days(_walk(2001, 300))})
    result = _run(store, ["ra", "rb"], transform="levels", max_lag=0)
    best = result["pairs"][0]["strongest"][0]
    assert best["n"] == 300 and best["n_effective_ar1"] == 5.9
    assert "note" not in best and best["p_ar1"] == pytest.approx(0.608, abs=0.001)
    assert best["serial_correlation_dominates"] is True
    (gap,) = [g for g in result["data_gaps"] if "serial correlation dominates" in g]
    assert "effective sample size of 5.9 of 300 periods" in gap and "no evidence" not in gap


def test_flat_adjacent_values_are_named_as_the_reason(store) -> None:
    """A rate flat at 2.5 for 99 days that moves to 2.75 on day 100: in
    changes it has 98 adjacent pairs, but the earlier side never varies, so
    phi is undefined. The note and the data_gaps line give that reason, not a
    shortage of adjacent periods."""
    _write(store, {"rate": _days([2.5] * 99 + [2.75]), "walk": _days(_walk(951, 100))})
    result = _run(store, ["rate", "walk"], max_lag=1)
    rate = result["series"][0]
    assert (rate["lag1_autocorrelation"], rate["lag1_pairs"]) == (None, 98)
    best = result["pairs"][0]["strongest"][0]
    assert best["note"] == lc.NO_PHI_FLAT_NOTE and best["p_ar1"] == 1.0
    assert [g for g in result["data_gaps"] if g.startswith("lagtest:rate: lag-1")] == [
        "lagtest:rate: lag-1 autocorrelation undefined (across its 98 adjacent-period pairs "
        "after the transform, the earlier or the later values do not vary), so its tests "
        "cannot be adjusted for serial correlation and are not counted as significant "
        "(p_ar1 set to 1)"
    ]
    assert "adjacent values that do not vary" in result["method"]["serial_correlation"]
    assert "not over each test's overlap" in result["method"]["serial_correlation"]


def test_window_cut_reports_a_given_start_as_clamped(store, monkeypatch) -> None:
    """The most-recent cut also applies when start is given; the start
    actually used is then reported (200 days, cut to the last 120: day 80)."""
    monkeypatch.setattr(lc, "MAX_PERIODS", 120)
    _planted(store)
    result = _run(store, ["x", "y"], start=D0.isoformat())
    (clamp,) = [c for c in result["inputs_clamped"] if c["input"] == "start"]
    assert clamp == {
        "input": "start", "requested": D0.isoformat(),
        "used": (D0 + timedelta(days=80)).isoformat(),
        "reason": "the window is limited to the most recent 120 days",
    }
    assert result["method"]["window"]["start"] == clamp["used"]
    inside = _run(store, ["x", "y"], start=(D0 + timedelta(days=100)).isoformat())
    assert not [c for c in inside["inputs_clamped"] if c["input"] == "start"]


# ---------------------------------------------------------------------------
# Inputs: clamps and errors
# ---------------------------------------------------------------------------


def test_numeric_inputs_are_clamped_and_reported(store) -> None:
    _planted(store)
    result = _run(store, ["x", "y"], max_lag=500, min_overlap=5, q=0.9)
    assert result["method"]["lags"] == [-30, 30]
    assert result["method"]["min_overlap"] == 20
    assert result["method"]["q_threshold"] == 0.25
    assert {c["input"]: (c["requested"], c["used"]) for c in result["inputs_clamped"]} == {
        "max_lag": (500, 30), "min_overlap": (5, 20), "q": (0.9, 0.25)
    }
    low = _run(store, ["x", "y"], max_lag=-4, q=0)
    assert low["method"]["lags"] == [0, 0] and low["tests_run"] == 1
    assert low["method"]["q_threshold"] == 0.001
    assert _run(store, ["x", "y"], max_lag=4.0)["method"]["lags"] == [-4, 4]


def test_total_tests_are_capped_by_reducing_max_lag(store) -> None:
    _write(store, {f"c{i}": _days(_walk(80 + i, 40)) for i in range(8)})
    result = _run(store, [f"c{i}" for i in range(8)], max_lag=30, min_overlap=20)
    assert result["method"]["lags"] == [-17, 17]
    assert len(result["pairs"]) == 28
    assert result["tests_run"] + result["tests_not_run"] == 28 * 35 <= lc.MAX_TESTS
    reduced = [c for c in result["inputs_clamped"] if c.get("reason")]
    assert reduced == [{
        "input": "max_lag", "requested": 30, "used": 17,
        "reason": "28 pairs x 35 lags keeps the call within 1000 tests",
    }]


@pytest.mark.parametrize(
    "kwargs,message",
    [
        ({"series_keys": ["lagtest:x"]}, "at least 2 series"),
        ({"series_keys": [f"lagtest:k{i}" for i in range(9)]}, "at most 8 series"),
        ({"series_keys": ["lagtest:x", "lagtest:x"]}, "repeats ['lagtest:x']"),
        ({"series_keys": "lagtest:x"}, "must be a list"),
        ({"series_keys": None}, "must be a list"),
        ({"series_keys": ["lagtest:x", ""]}, "non-empty strings"),
        ({"grid": "hourly"}, "unknown grid 'hourly'; allowed: day, week, month"),
        ({"transform": "logs"}, "unknown transform 'logs'; allowed: changes, levels"),
        ({"max_lag": "5"}, "max_lag must be an integer"),
        ({"max_lag": True}, "max_lag must be an integer"),
        ({"min_overlap": 25.5}, "min_overlap must be an integer"),
        ({"q": "0.05"}, "q must be a number"),
        ({"q": float("nan")}, "q must be a number"),
        ({"start": "2025/01/01"}, "start must be a date YYYY-MM-DD"),
        ({"end": "2025-02-30"}, "end must be a date YYYY-MM-DD"),
        ({"start": "2025-06-01", "end": "2025-05-01"}, "is after end"),
    ],
)
def test_input_errors_name_what_is_allowed(store, kwargs, message) -> None:
    _planted(store)
    args = {"series_keys": ["lagtest:x", "lagtest:y"], **kwargs}
    keys = args.pop("series_keys")
    result = lc.lagged_connections(store, keys, **args)
    assert set(result) == {"error"}
    assert message in result["error"]


def test_unknown_series_is_an_error_with_a_listing_hint(store) -> None:
    _planted(store)
    result = _run(store, ["x", "nope"])
    assert result["unknown_series"] == ["lagtest:nope"]
    assert "intel_series_history" in result["error"]


def test_user_facing_text_has_no_em_dashes() -> None:
    from world_intel_mcp.tools import lagged_connections as tool_mod

    texts = [tool_mod.TOOLS[0].description, json.dumps(tool_mod.TOOLS[0].inputSchema),
             cli.history_connections.help or ""]
    texts += [v for k, v in vars(lc).items() if k.isupper() and isinstance(v, str)]
    texts += lc.CANNOT_PROVE + list(lc.GRID_RULES.values()) + list(lc.TRANSFORM_RULES.values())
    texts += [p.help or "" for p in cli.history_connections.params if hasattr(p, "help")]
    assert not [t for t in texts if "—" in t]


# ---------------------------------------------------------------------------
# MCP tool through the real dispatcher
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def server(isolated_runtime):
    """server.py imported under test_server_registry's temp cache path, the
    shared runtime rebound while the override is in force (that file's
    fixture pattern)."""
    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        module = importlib.import_module("world_intel_mcp.server")
        isolated_runtime._initialize()
        yield module
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


@pytest.fixture
def tool_db(server, store, monkeypatch):
    _planted(store)
    runtime = importlib.import_module("world_intel_mcp.runtime")
    monkeypatch.setattr(runtime, "cache", types.SimpleNamespace(db_path=store.db_path))
    return store.db_path


def test_tool_is_registered_with_its_schema(server) -> None:
    (tool,) = [t for t in server.TOOLS if t.name == "intel_lagged_connections"]
    props = tool.inputSchema["properties"]
    assert tool.inputSchema["required"] == ["series_keys"]
    assert props["grid"]["enum"] == ["day", "week", "month"]
    assert props["transform"]["enum"] == ["changes", "levels"]
    assert props["transform"]["default"] == "changes"
    assert "accrues only while the collector" in tool.description
    assert "correlation is not causation" in tool.description
    assert "A leads B by k" in tool.description


async def test_tool_through_the_real_dispatcher(server, tool_db) -> None:
    args = {"series_keys": ["lagtest:x", "lagtest:y"], "max_lag": 4}
    result = await server._dispatch("intel_lagged_connections", args)
    best = result["pairs"][0]["strongest"][0]
    assert (best["lag"], best["n"], best["significant"]) == (3, 196, True)
    assert result["method"]["lags"] == [-4, 4] and result["tests_run"] == 9

    (content,) = await server.call_tool("intel_lagged_connections", args)
    assert json.loads(content.text)["pairs"][0]["strongest"][0]["lag"] == 3

    bad = await server._dispatch(
        "intel_lagged_connections", {"series_keys": ["lagtest:x", "lagtest:y"], "grid": "hourly"}
    )
    assert bad == {"error": "unknown grid 'hourly'; allowed: day, week, month"}
    missing = await server._dispatch("intel_lagged_connections", {})
    assert "must be a list" in missing["error"]


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def cli_db(tmp_path, monkeypatch):
    monkeypatch.setattr(history, "_EXTRACTORS", dict(history._EXTRACTORS))
    history.register_extractor(
        "lagtest",
        lambda payload: [
            Observation(p["key"], p["value"], observed_at=p["observed_at"])
            for p in payload["points"]
        ],
    )
    db = tmp_path / "cli-cache.db"
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(db))
    monkeypatch.setenv("COLUMNS", "240")
    monkeypatch.setattr(cli, "_fetcher", None)
    s = HistoryStore(db)
    _planted(s)
    _write(s, {"[red]odd[/red]": _days(_walk(91, 33))})
    s.close()
    yield db
    # The CLI's shared fetcher opened a Cache on this db; close it before
    # monkeypatch restores the previous fetcher.
    if cli._fetcher is not None:
        cli._fetcher.cache.close()


def _invoke(*args):
    return CliRunner().invoke(cli.main, list(args), catch_exceptions=False)


def test_cli_connections_table_states_convention_and_limits(cli_db) -> None:
    out = _invoke("history", "connections", "lagtest:x", "lagtest:y").output
    assert "A leads B by 3 days" in out and "+3" in out
    assert "Lag convention: Pairs are (A, B)" in out
    assert "Transform: Period-over-period changes" in out
    assert "Cannot prove: Correlation is not causation" in out
    assert "1 of 1 pairs have a lag with q <= 0.05" in out
    # The line above matches only the independent-periods sentence; pin both.
    assert "1 of 1 pairs have a lag with q_ar1 <= 0.05" in out
    assert "Assuming independent periods instead, 1 of 1 pairs have a lag with q <= 0.05." in out


def test_cli_connections_lists_untested_lags_and_renders_keys_literally(cli_db) -> None:
    out = _invoke("history", "connections", "lagtest:x", "lagtest:[red]odd[/red]").output
    assert "lagtest:[red]odd[/red]" in out
    assert "Not tested (minimum 30 overlapping periods)" in out
    assert "lag +3 (n 29)" in out


def test_cli_connections_shows_the_ar1_effective_sample_size(cli_db) -> None:
    args = ("history", "connections", "lagtest:x", "lagtest:y", "--transform", "levels")
    best = json.loads(_invoke("--json-output", *args).output)["pairs"][0]["strongest"][0]
    assert best["n_effective_ar1"] < best["n"] / 2
    out = _invoke(*args).output
    assert f" {best['n_effective_ar1']:g} " in out and f" {best['n']} " in out
    assert "n_eff: AR(1) effective sample size" in out and "p and q use n" in out


def test_cli_connections_json_and_errors(cli_db) -> None:
    data = json.loads(
        _invoke("--json-output", "history", "connections", "lagtest:x", "lagtest:y",
                "--grid", "week", "--min-overlap", "20").output
    )
    assert data["method"]["grid"] == "week"
    assert data["method"]["min_overlap"] == 20
    out = _invoke("history", "connections", "lagtest:x", "lagtest:nope").output
    assert out.startswith("Error:") and "lagtest:nope" in out
    bad = CliRunner().invoke(cli.main, ["history", "connections", "a", "b", "--grid", "hour"])
    assert bad.exit_code == 2 and "day" in bad.output and "month" in bad.output


def test_cli_connections_reports_each_clamp(cli_db, monkeypatch) -> None:
    out = _invoke("history", "connections", "lagtest:x", "lagtest:y",
                  "--min-overlap", "5", "--max-lag", "40", "--q", "0.5").output
    assert "Clamped: min_overlap 5 -> 20 (allowed 20 to 3000)" in out
    assert "Clamped: max_lag 40 -> 30 (allowed 0 to 30)" in out
    assert "Clamped: q 0.5 -> 0.25 (allowed 0.001 to 0.25)" in out
    monkeypatch.setattr(lc, "MAX_PERIODS", 120)
    cut = _invoke("history", "connections", "lagtest:x", "lagtest:y", "--start", D0.isoformat()).output
    used = (D0 + timedelta(days=80)).isoformat()
    assert (f"Clamped: start {D0.isoformat()} -> {used} (the window is limited to the most "
            "recent 120 days)") in cut
    assert "Clamped:" not in _invoke("history", "connections", "lagtest:x", "lagtest:y").output


def test_cli_connections_table_shows_p_ar1_and_q_ar1(cli_db) -> None:
    args = ("history", "connections", "lagtest:x", "lagtest:y", "--transform", "levels")
    best = json.loads(_invoke("--json-output", *args).output)["pairs"][0]["strongest"][0]
    out = _invoke(*args).output
    (header,) = [line for line in out.splitlines() if "n_eff" in line and "Timing" in line]
    assert header.split()[-5:] == ["p", "q", "p_ar1", "q_ar1", "Significant"]
    (row,) = [line for line in out.splitlines() if "A leads B by 3 days" in line]
    assert row.split()[-3:] == [cli._p_text(best["p_ar1"]), cli._p_text(best["q_ar1"]),
                                "yes" if best["significant"] else "no"]
    assert "p_ar1 and q_ar1 use n_eff, and Significant means q_ar1 <= 0.05" in out


def test_cli_connections_prints_notes_and_both_summary_counts(cli_db) -> None:
    """Seeds 1000/1001 in levels have an effective size of 0.9 (no evidence),
    so the CLI prints the entry's note and a q_ar1 count of 0 beside the
    independent-periods count of 1."""
    s = HistoryStore(cli_db)
    _write(s, {"rwa": _days(_walk(1000, 200)), "rwb": _days(_walk(1001, 200))}, at=T0 + 1)
    s.close()
    out = _invoke("history", "connections", "lagtest:rwa", "lagtest:rwb",
                  "--transform", "levels").output
    notes = [line for line in out.splitlines() if line.startswith("Note:")]
    assert len(notes) == 1
    assert notes[0].startswith("Note: lagtest:rwa / lagtest:rwb lag -5: no evidence: the AR(1) "
                               "effective sample size is below 3")
    flat = " ".join(out.split())
    assert "0 of 1 pairs have a lag with q_ar1 <= 0.05" in flat
    assert "Assuming independent periods instead, 1 of 1 pairs have a lag with q <= 0.05." in flat


def test_cli_gap_lines_render_series_keys_literally(cli_db) -> None:
    """Keys such as lagtest:x carry ':x:' once a colon follows them, which the
    console would otherwise turn into an emoji."""
    out = _invoke("history", "connections", "lagtest:x", "lagtest:y",
                  "--transform", "levels").output
    flat = " ".join(out.split())
    assert "Gap: lagtest:x: lag-1 autocorrelation +" in flat
    assert "Gap: lagtest:y: lag-1 autocorrelation +" in flat
    assert chr(0x274C) not in out  # the cross-mark emoji ':x:' turns into
