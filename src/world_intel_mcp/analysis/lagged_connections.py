"""Connections with timing offsets between series in the history store.

For 2 to ``MAX_SERIES`` series the collector saved (``history.py``; keys as
``intel_series_history`` lists them), the series are aligned on a UTC
calendar grid, transformed, and their Pearson correlation is tested at every
lag from ``-max_lag`` to ``+max_lag``, with a Benjamini-Hochberg
false-discovery adjustment across every test run in the call. Standard
library only.

Alignment:
- Grid periods are UTC calendar days, ISO weeks (Monday to Sunday, labelled
  by the Monday's date) or calendar months. ``observed_at`` decides the
  period: a ``YYYY-MM-DD`` date or an instant (stored in UTC) falls in the
  day, week and month containing it; a ``YYYY-MM`` month fits only the month
  grid and a ``YYYY`` year fits none. Rows coarser than the grid are not
  used, and are counted.
- Vintage: when a later capture revised the value for one ``observed_at``,
  the latest collected value is used (ties broken by insertion order).
  Earlier vintages are ignored; the number of revised observation dates is
  reported per series.
- Sampling: a period takes the value of its last observation by
  ``observed_at``, so a daily series on the week grid is its last day of the
  week. Values inside a period are not averaged: averaging a random walk
  makes its period-to-period changes serially correlated. On the week and
  month grids a change therefore runs from one period's last observation to
  the next period's, and those intervals need not be equal (a Monday close
  then the next week's Friday close is 11 days); each series reports the
  shortest and longest interval in days (dated observations only).
- Rows without ``observed_at`` (Yahoo quotes) are not used. Their only date
  is ``collected_at``, when this server saw a changed value, and rows are
  stored only when the value changes, so a day with no row cannot be told
  from a day with an unchanged value. A series with no dated rows is
  excluded with that reason.
- ``start`` / ``end`` select whole grid periods (the ones containing them and
  every period between). Without them the window is the series' combined
  span. Either way it is cut to the most recent ``MAX_PERIODS`` periods; when
  the cut moves a given ``start``, the start actually used is reported in
  ``inputs_clamped``.

Transform:
- ``changes`` (default): a period's value minus the immediately preceding
  period's, only when both exist; a change is never taken across a gap.
  Levels of trending series correlate spuriously (two independent random
  walks routinely reach |r| above 0.5), which is why changes are the default.
- ``levels``: the sampled values.

Tests:
- Pairs are (A, B) in the order the series were given. Lag +k compares A at
  period t with B at period t+k: a connection at +k means A's moves come k
  periods before B's (A leads B by k); lag -k means B leads A by k.
- n counts the periods where both transformed values exist after the shift
  (pairwise complete). A (pair, lag) with n below the minimum, or with no
  variation on either side, is not tested and is listed with its n.
- p is two-sided, from Student's t with n - 2 degrees of freedom. With
  t^2 = r^2 (n - 2) / (1 - r^2), df / (df + t^2) = 1 - r^2, so
  P(|T| >= |t|) = I_{1 - r^2}((n - 2) / 2, 1 / 2), the regularized incomplete
  beta function, evaluated by its continued fraction (DLMF 8.17.22) with the
  modified Lentz method. No permutation test is run.
- q is the Benjamini-Hochberg adjusted p over every test run in the call,
  and q_ar1 the same adjustment of ``p_ar1`` (below); ``q_by`` is the
  Benjamini-Yekutieli version of q_ar1, valid under any dependence between
  tests. A test is significant when q_ar1 <= the threshold (default
  ``DEFAULT_Q`` = 0.05); ``significant_if_independent`` applies the
  threshold to q.

Minimum sample, ``DEFAULT_MIN_OVERLAP`` = 30 overlapping points (callers may
raise it, or lower it to ``MIN_OVERLAP_FLOOR`` = 20): at n = 30 a two-sided
test at alpha 0.05 detects a true |rho| of 0.5 with about 80% power before
any multiple-testing correction (Fisher z approximation: n = ((1.960 +
0.842) / atanh 0.5)^2 + 3 = 29.0; simulated with bivariate normal draws:
0.826 at n = 30, 0.643 at n = 20). Below that, a moderate real correlation is
more often missed than found, and one that passes rests on few points. The
floor is 20 because below it a single outlying pair can dominate r and the t
reference distribution leans on near-normal values.

Serial correlation: p and q treat periods as independent, which overstates
the evidence when a series is autocorrelated after the transform. Each
series' lag-1 autocorrelation (phi) is measured, and every test carries
``n_effective_ar1``, n scaled by (1 - phi_a phi_b) / (1 + phi_a phi_b) (the
large-sample variance inflation of r for two AR(1) series, Bartlett) and
capped at n, and ``p_ar1``, p recomputed with that sample size. Significance
uses q_ar1. Below an effective size of 3 the sample carries no evidence
either way, and without a measurable phi (fewer than ``_AUTOCORR_MIN_PAIRS``
adjacent periods, or adjacent values that do not vary) the adjustment cannot
be made; both set p_ar1 to 1 and the entry says why. phi is measured once per
series over all its periods in the window (``lag1_pairs`` adjacent pairs),
not over each test's overlap, so the adjustment assumes the series has the
same autocorrelation inside every overlap; a series whose autocorrelation
changes between periods (a long white-noise stretch before a short
autocorrelated one that holds the overlap) can still have its evidence
overstated. ``serial_correlation_dominates`` marks n_effective_ar1 below
``DOMINATES_BELOW`` x n (variance of r inflated more than fivefold), and
data_gaps names those pairs and every series that took part in a test with
|phi| above ``AUTOCORR_NOTE_ABOVE``. The strongest lag is chosen by p among a pair's
lags, so its p and p_ar1 are post-selection values; q and q_ar1 carry the
adjustment for every test in the call. Measured 2026-10-17 in a scratch
simulation through the store (not part of the test suite): for two
independent Gaussian random walks of 300 daily points, levels, defaults,
seeds 0-99, 91 of 100 seeds have a lag with q <= 0.05 and none with q_ar1 <=
0.05 (conservative there: their phi is near 1). For two independent series
of 200 AR(1) changes, 4,000 runs, at lag 0 p <= 0.05 in 12.2% (phi 0.5) and
35.3% (phi 0.8) of runs and q_ar1 <= 0.05 in 4.5% and 5.1%; with the default
11 lags a call has any significant lag in 4.3% and 2.9% of runs.

Gaps / not covered:
- Pearson only: no rank correlation, so a few extreme changes can drive r.
- No prewhitening and no correction for autocorrelation beyond lag 1 or for
  volatility clustering: ``p_ar1`` is an AR(1) approximation, so series
  with longer memory (seasonal patterns, long swings) can still have their
  evidence overstated.
- Benjamini-Hochberg assumes independent or positively dependent tests;
  neighbouring lags of one pair share most of their data, so q and q_ar1 are
  approximate (``q_by`` is the any-dependence bound for q_ar1).
- The adjustment covers one call. Other calls with other series, grids or
  windows are further tests it cannot see.
- Business-day series lose their Monday changes on the day grid (Friday to
  Monday is not adjacent). Within one week or month, two series' last
  observations can fall on different days (Wednesday levels against a
  Friday close), so "same period" is not "same day".
- One vintage only (latest seen); there is no first-release view.
- Reference p-values in the tests come from scipy in a throwaway venv, not
  from an independent derivation at run time.
"""

from __future__ import annotations

import math
import re
from datetime import date
from typing import Any, Iterable, Sequence

from ..history import HISTORY_ACCRUAL_NOTE, HistoryStore

GRIDS = ("day", "week", "month")
TRANSFORMS = ("changes", "levels")
DEFAULT_GRID = "day"
DEFAULT_TRANSFORM = "changes"
DEFAULT_MAX_LAG = 5
MAX_LAG_CAP = 30
DEFAULT_MIN_OVERLAP = 30
MIN_OVERLAP_FLOOR = 20
DEFAULT_Q = 0.05
Q_FLOOR = 0.001
Q_CAP = 0.25
MAX_SERIES = 8
MAX_TESTS = 1000
MAX_PERIODS = 3000
# Fewer adjacent pairs than this and a lag-1 autocorrelation is not reported.
_AUTOCORR_MIN_PAIRS = 10
# n_effective_ar1 below this share of n: the variance of r is inflated more
# than fivefold, so serial correlation, not the sample size, sets the evidence.
DOMINATES_BELOW = 0.2
# A series whose |lag-1 autocorrelation| exceeds this gets a data_gaps line.
AUTOCORR_NOTE_ABOVE = 0.3

_UNIT = {"day": "day", "week": "week", "month": "month"}

GRID_RULES = {
    "day": "UTC calendar days",
    "week": "ISO weeks in UTC, Monday to Sunday, labelled by the Monday's date",
    "month": "UTC calendar months, labelled YYYY-MM",
}
SAMPLING_RULE = (
    "Each period takes the value of its last observation by observed_at (a daily "
    "series on the week grid is its last day of the week). Values inside a period "
    "are not averaged. On the week and month grids a change runs from one period's "
    "last observation to the next period's, so changes can span unequal intervals; "
    "each series reports change_interval_days (shortest and longest)."
)
VINTAGE_RULE = (
    "Latest vintage: where later captures revised the value for an observed_at, the "
    "latest collected value is used and earlier values are ignored."
)
UNDATED_RULE = (
    "Rows without observed_at (for example Yahoo quotes, saved with collected_at "
    "only) are not used: collected_at is when this server saw a changed value, not "
    "when it was observed, and rows are stored only on change, so a day with no row "
    "cannot be told from a day with an unchanged value."
)
TRANSFORM_RULES = {
    "changes": (
        "Period-over-period changes: a period's value minus the immediately "
        "preceding period's, only when both exist; never across a gap."
    ),
    "levels": "Levels: the sampled values, untransformed.",
}
WHY_CHANGES = (
    "Changes are the default because levels of trending series correlate "
    "spuriously: two independent random walks routinely reach |r| above 0.5 in "
    "levels and near 0 in changes."
)
LAG_CONVENTION = (
    "Pairs are (A, B) in the order the series were given. Lag +k: A's value at "
    "period t is compared with B's value at period t+k, so a connection at lag +k "
    "means A's moves come k periods before B's (A leads B by k). Lag -k: B leads A "
    "by k. Lag 0: the same period."
)
P_RULE = (
    "Two-sided p from Student's t with n - 2 degrees of freedom, computed exactly "
    "as I_(1 - r^2)((n - 2)/2, 1/2), the regularized incomplete beta function."
)
MIN_OVERLAP_REASON = (
    "30 overlapping points give about 80% power to detect a true |r| of 0.5 at "
    "alpha 0.05 (two-sided, before multiple-testing correction); fewer miss "
    "moderate real correlations more often than they find them. The floor is 20: "
    "below it one outlying pair can dominate r."
)
AR1_RULE = (
    "p and q treat periods as independent. Done: each series' lag-1 "
    "autocorrelation (phi) after the transform is measured, and every test carries "
    "n_effective_ar1 (n scaled by (1 - phi_a phi_b) / (1 + phi_a phi_b), capped at "
    "n) and p_ar1 (p recomputed with it). q_ar1 is the Benjamini-Hochberg "
    "adjustment of p_ar1 across the call, and significant uses q_ar1; p_ar1 does "
    "not enter q. Below an effective sample size of 3 the sample carries no "
    "evidence either way, and without a measurable phi (fewer than "
    f"{_AUTOCORR_MIN_PAIRS} adjacent periods, or adjacent values that do not vary) "
    "no adjustment can be made; both set p_ar1 to 1 and say why in the entry's "
    "note. phi is measured once per series over all its periods in the window "
    "(lag1_pairs), not over each test's overlap, so the adjustment assumes the "
    "same autocorrelation inside every overlap. n_effective_ar1 is shown rounded "
    "down to one decimal. serial_correlation_dominates "
    f"marks n_effective_ar1 below {DOMINATES_BELOW:g} n. The strongest lag is the "
    "smallest p among the pair's lags, so its p and p_ar1 are post-selection "
    "values; q and q_ar1 carry the adjustment for every test in the call. Not "
    "done: prewhitening, autocorrelation beyond lag 1, volatility clustering."
)
SIGNIFICANCE_RULE = (
    "A test is significant when q_ar1 <= the threshold. significant_if_independent "
    "applies the same threshold to q, which treats periods as independent and "
    "overstates the evidence for autocorrelated series."
)
NO_EVIDENCE_NOTE = (
    "no evidence: the AR(1) effective sample size is below 3, so with this much "
    "serial correlation the sample cannot tell a real correlation from chance; "
    "p_ar1 is set to 1"
)
NO_PHI_NOTE = (
    "not assessed: a series has fewer than "
    f"{_AUTOCORR_MIN_PAIRS} adjacent periods after the transform, so its lag-1 "
    "autocorrelation cannot be measured and p cannot be adjusted for it; p_ar1 is "
    "set to 1, so the test cannot be significant"
)
NO_PHI_FLAT_NOTE = (
    "not assessed: across a series' adjacent-period pairs after the transform, the "
    "earlier or the later values do not vary, so its lag-1 autocorrelation is "
    "undefined and p cannot be adjusted "
    "for it; p_ar1 is set to 1, so the test cannot be significant"
)
NOT_TESTED_REASONS = {
    "below_min_overlap": "fewer overlapping periods than min_overlap",
    "no_variation": "one side is constant over the overlap, so r is undefined",
}
CANNOT_PROVE = [
    "Correlation is not causation: a strong r at any lag shows the two series moved "
    "together in this sample, not that one moved the other.",
    "A lead is not evidence of causal direction: when A's moves come k periods "
    "before B's, B can still drive A (through expectations or release timing), or "
    "a third factor can reach A first.",
    "Serial correlation inflates significance even after differencing: p and q "
    "treat periods as independent. What was done: lag-1 autocorrelations are "
    "measured, every test carries p_ar1 with an AR(1) effective sample size, and "
    "significant uses q_ar1, the false-discovery adjustment of p_ar1. What was "
    "not: prewhitening, autocorrelation beyond lag 1, volatility clustering, so "
    "series with longer memory than AR(1) can still have their evidence "
    "overstated. Each series' phi comes from its whole window, not from each "
    "test's overlap, so a series whose autocorrelation differs inside the "
    "overlap can too.",
    "A shared common driver produces correlation: two series that both respond to "
    "a third (the business cycle, a policy date, a holiday calendar) correlate "
    "with no link between them.",
    "The store holds only what the collector recorded. " + HISTORY_ACCRUAL_NOTE
    + " The sample is the collection period, not the series' full history, and "
    "periods the collector missed are absent, not zero.",
    "Benjamini-Hochberg controls the false discovery rate for independent or "
    "positively dependent tests; neighbouring lags of one pair share most of "
    "their data, so q and q_ar1 are approximate (q_by, the Benjamini-Yekutieli "
    "version of q_ar1, holds under any dependence). The adjustment covers this "
    "call only, not other calls with other series, grids or windows.",
]


# ---------------------------------------------------------------------------
# Statistics (standard library only)
# ---------------------------------------------------------------------------

_CF_EPS = 1e-15
_CF_TINY = 1e-300
_CF_MAX_ITER = 10_000


def _beta_continued_fraction(a: float, b: float, x: float) -> float:
    """Denominator f of I_x(a, b) = x^a (1-x)^b / (a B(a, b)) / f, where
    f = 1 + d1 / (1 + d2 / (1 + ...)) with DLMF 8.17.22's coefficients
    d_(2m+1) = -(a+m)(a+b+m)x / ((a+2m)(a+2m+1)) and
    d_(2m) = m(b-m)x / ((a+2m-1)(a+2m)), by the modified Lentz method."""
    f = c = 1.0
    d = 0.0
    for j in range(1, _CF_MAX_ITER):
        m = j // 2
        if j % 2:
            coef = -(a + m) * (a + b + m) * x / ((a + 2 * m) * (a + 2 * m + 1))
        else:
            coef = m * (b - m) * x / ((a + 2 * m - 1) * (a + 2 * m))
        d = 1.0 + coef * d
        if abs(d) < _CF_TINY:
            d = _CF_TINY
        d = 1.0 / d
        c = 1.0 + coef / c
        if abs(c) < _CF_TINY:
            c = _CF_TINY
        delta = c * d
        f *= delta
        if abs(delta - 1.0) < _CF_EPS:
            return f
    raise ArithmeticError(f"incomplete beta did not converge (a={a}, b={b}, x={x})")


def _ibeta(a: float, b: float, x: float, y: float) -> float:
    """I_x(a, b), with y = 1 - x supplied by the caller so that whichever of
    the two is small keeps its precision."""
    if x <= 0.0:
        return 0.0
    if y <= 0.0:
        return 1.0
    if x > (a + 1.0) / (a + b + 2.0):
        # The fraction converges fast only below this point; use
        # I_x(a, b) = 1 - I_(1-x)(b, a).
        return 1.0 - _ibeta(b, a, y, x)
    log_front = (
        a * math.log(x) + b * math.log(y) - math.log(a)
        - (math.lgamma(a) + math.lgamma(b) - math.lgamma(a + b))
    )
    return math.exp(log_front) / _beta_continued_fraction(a, b, x)


def regularized_incomplete_beta(a: float, b: float, x: float) -> float:
    """I_x(a, b) for a, b > 0 and 0 <= x <= 1."""
    if not (a > 0 and b > 0) or not 0.0 <= x <= 1.0:
        raise ValueError(f"need a, b > 0 and 0 <= x <= 1, got a={a}, b={b}, x={x}")
    return _ibeta(a, b, x, 1.0 - x)


def correlation_p_value(r: float, n: float) -> float:
    """Two-sided p of H0 rho = 0 from Student's t with n - 2 degrees of
    freedom. ``n`` may be fractional (an effective sample size)."""
    df = n - 2.0
    if not df > 0:
        raise ValueError(f"need n > 2, got {n}")
    r = abs(r)
    if r >= 1.0:
        return 0.0
    # t^2 = r^2 df / (1 - r^2), so df / (df + t^2) = 1 - r^2 exactly.
    return min(1.0, _ibeta(df / 2.0, 0.5, (1.0 - r) * (1.0 + r), r * r))


def ar1_adjusted(
    r: float, n: int, phi_a: float | None, phi_b: float | None
) -> tuple[float | None, float, str | None]:
    """(n_effective_ar1, p_ar1, note) for one test: n scaled by
    (1 - phi_a phi_b) / (1 + phi_a phi_b) and capped at n, and p recomputed
    with it. p_ar1 is 1, with a note, when either phi is unknown or the
    effective size is below 3."""
    if phi_a is None or phi_b is None:
        return None, 1.0, NO_PHI_NOTE
    prod = phi_a * phi_b
    if prod >= 1.0:
        n_eff = 0.0
    elif prod <= -1.0:
        n_eff = float(n)
    else:
        n_eff = min(float(n), n * (1.0 - prod) / (1.0 + prod))
    if n_eff < 3:
        return n_eff, 1.0, NO_EVIDENCE_NOTE
    return n_eff, correlation_p_value(r, n_eff), None


# Quotes a p from correlation_p_value, so it joins the list once that exists.
CANNOT_PROVE.append(
    "A small q is not a large effect: q says r is unlikely to be zero in this "
    "sample, not that it is big. With 1,000 overlapping periods an |r| of 0.1 "
    f"(1% of the variance shared) already gives p = {correlation_p_value(0.1, 1000):.2g}; "
    "read r and n alongside q."
)


def pearson_r(xs: Sequence[float], ys: Sequence[float]) -> float | None:
    """Pearson correlation, or None when either side does not vary."""
    n = len(xs)
    if n != len(ys):
        raise ValueError("xs and ys differ in length")
    if n < 2 or min(xs) == max(xs) or min(ys) == max(ys):
        return None
    mx = math.fsum(xs) / n
    my = math.fsum(ys) / n
    dx = [x - mx for x in xs]
    dy = [y - my for y in ys]
    sxx = math.fsum(d * d for d in dx)
    syy = math.fsum(d * d for d in dy)
    if sxx <= 0.0 or syy <= 0.0:
        return None
    r = math.fsum(u * v for u, v in zip(dx, dy)) / (math.sqrt(sxx) * math.sqrt(syy))
    return max(-1.0, min(1.0, r))


def benjamini_hochberg(pvalues: Sequence[float]) -> list[float]:
    """BH adjusted p-values (q), in the input order: q at rank i is the
    minimum over ranks j >= i of m p_(j) / j, capped at 1."""
    m = len(pvalues)
    order = sorted(range(m), key=lambda i: pvalues[i])
    adjusted = [1.0] * m
    running = 1.0
    for rank in range(m, 0, -1):
        i = order[rank - 1]
        running = min(running, pvalues[i] * m / rank)
        adjusted[i] = running
    return adjusted


def benjamini_yekutieli(pvalues: Sequence[float]) -> list[float]:
    """BY adjusted p-values: BH scaled by the harmonic sum 1 + 1/2 + ... + 1/m,
    which keeps the false discovery rate under any dependence."""
    harmonic = math.fsum(1.0 / i for i in range(1, len(pvalues) + 1))
    return [min(1.0, q * harmonic) for q in benjamini_hochberg(pvalues)]


# ---------------------------------------------------------------------------
# Grid
# ---------------------------------------------------------------------------

_DAY = re.compile(r"(\d{4})-(\d{2})-(\d{2})(?:T\d{2}:\d{2}:\d{2}Z)?")
_MONTH = re.compile(r"(\d{4})-(\d{2})")
_YEAR = re.compile(r"\d{4}")
_ISO_DATE = re.compile(r"\d{4}-\d{2}-\d{2}")


def period_of(observed_at: str, grid: str) -> tuple[int | None, str | None]:
    """Integer period index of a stored ``observed_at`` on ``grid`` (adjacent
    periods differ by 1), or ``(None, "coarser" | "unreadable")``."""
    text = str(observed_at)
    m = _DAY.fullmatch(text)
    if m:
        try:
            day = date(int(m[1]), int(m[2]), int(m[3]))
        except ValueError:
            return None, "unreadable"
        if grid == "day":
            return day.toordinal(), None
        if grid == "week":
            # Ordinal 1 (0001-01-01) is a Monday.
            return (day.toordinal() - 1) // 7, None
        return day.year * 12 + day.month - 1, None
    m = _MONTH.fullmatch(text)
    if m:
        if not 1 <= int(m[2]) <= 12:
            return None, "unreadable"
        if grid == "month":
            return int(m[1]) * 12 + int(m[2]) - 1, None
        return None, "coarser"
    if _YEAR.fullmatch(text):
        return None, "coarser"
    return None, "unreadable"


def period_label(index: int, grid: str) -> str:
    if grid == "day":
        return date.fromordinal(index).isoformat()
    if grid == "week":
        return date.fromordinal(7 * index + 1).isoformat()
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def apply_transform(levels: dict[int, float], transform: str) -> dict[int, float]:
    if transform == "levels":
        return dict(levels)
    return {p: v - levels[p - 1] for p, v in levels.items() if p - 1 in levels}


def lag1_autocorrelation(values: dict[int, float]) -> tuple[float | None, int]:
    """Pearson r between each value and the next period's, with the number of
    adjacent pairs; None below ``_AUTOCORR_MIN_PAIRS`` pairs or without
    variation."""
    xs = [values[p] for p in sorted(values) if p + 1 in values]
    ys = [values[p + 1] for p in sorted(values) if p + 1 in values]
    if len(xs) < _AUTOCORR_MIN_PAIRS:
        return None, len(xs)
    return pearson_r(xs, ys), len(xs)


def overlap_at_lag(
    a: dict[int, float], b: dict[int, float], lag: int
) -> tuple[list[float], list[float]]:
    """A at t paired with B at t + lag, over every t where both exist."""
    xs: list[float] = []
    ys: list[float] = []
    for p in sorted(a):
        other = b.get(p + lag)
        if other is not None:
            xs.append(a[p])
            ys.append(other)
    return xs, ys


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


class _InputError(ValueError):
    pass


def _integer(name: str, value: Any, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, bool):
        raise _InputError(f"{name} must be an integer, got {value!r}")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    raise _InputError(f"{name} must be an integer, got {value!r}")


def _clamp(name: str, value: Any, lo: Any, hi: Any, clamps: list[dict]) -> Any:
    used = max(lo, min(hi, value))
    if used != value:
        clamps.append({"input": name, "requested": value, "used": used, "allowed": [lo, hi]})
    return used


def _iso_date(name: str, value: Any) -> date | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not _ISO_DATE.fullmatch(value.strip()):
        raise _InputError(f"{name} must be a date YYYY-MM-DD, got {value!r}")
    try:
        return date.fromisoformat(value.strip())
    except ValueError as exc:
        raise _InputError(f"{name} must be a date YYYY-MM-DD, got {value!r}") from exc


def _series_keys(value: Any) -> list[str]:
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        raise _InputError(
            "series_keys must be a list of 2 to "
            f"{MAX_SERIES} series keys (as intel_series_history lists them)"
        )
    keys = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise _InputError(f"series_keys entries must be non-empty strings, got {item!r}")
        keys.append(item.strip())
    if len(keys) < 2:
        raise _InputError(f"series_keys needs at least 2 series, got {len(keys)}")
    if len(keys) > MAX_SERIES:
        raise _InputError(f"series_keys takes at most {MAX_SERIES} series per call, got {len(keys)}")
    duplicates = sorted({k for k in keys if keys.count(k) > 1})
    if duplicates:
        raise _InputError(f"series_keys repeats {duplicates}")
    return keys


# ---------------------------------------------------------------------------
# Store reads and per-series preparation
# ---------------------------------------------------------------------------


def _read_rows(store: HistoryStore, series_key: str) -> list[tuple]:
    # HistoryStore has no bulk read for one series' every vintage, so this
    # reads its observations table directly (parameterized, read-only).
    return store._conn.execute(
        "SELECT observed_at, value, source, unit, label FROM history_observations "
        "WHERE series_key = ? ORDER BY observed_at, collected_at, id",
        (series_key,),
    ).fetchall()


def _sample_on_grid(key: str, rows: list[tuple], grid: str) -> dict[str, Any]:
    latest: dict[str, float] = {}
    distinct: dict[str, set[float]] = {}
    undated = 0
    for observed_at, value, _source, _unit, _label in rows:
        if observed_at is None:
            undated += 1
            continue
        latest[observed_at] = float(value)  # rows run oldest vintage first
        distinct.setdefault(observed_at, set()).add(float(value))
    coarser = unreadable = 0
    coarser_kinds: set[str] = set()
    levels: dict[int, float] = {}
    days: dict[int, int | None] = {}
    for observed_at in sorted(latest):
        period, why = period_of(observed_at, grid)
        if period is None:
            if why == "coarser":
                coarser += 1
                coarser_kinds.add("month" if len(observed_at) == 7 else "year")
            else:
                unreadable += 1
            continue
        levels[period] = latest[observed_at]  # sorted, so the last one wins
        # The sampled observation's day; a YYYY-MM row on the month grid has none.
        day = _DAY.fullmatch(str(observed_at))
        days[period] = date(int(day[1]), int(day[2]), int(day[3])).toordinal() if day else None
    last_row = rows[-1] if rows else (None, None, None, None, None)
    return {
        "series_key": key,
        "source": last_row[2],
        "unit": next((r[3] for r in reversed(rows) if r[3]), None),
        "label": next((r[4] for r in reversed(rows) if r[4]), None),
        "rows": len(rows),
        "rows_without_observed_at": undated,
        "rows_coarser_than_grid": coarser,
        "rows_unreadable_observed_at": unreadable,
        "revised_observed_at": sum(1 for values in distinct.values() if len(values) > 1),
        "dated_observations": len(latest),
        "_levels": levels,
        "_days": days,
        "_coarser_kinds": coarser_kinds,
    }


def change_interval_days(levels: dict[int, float], days: dict[int, int | None]) -> list[int]:
    """Days spanned by each period-over-period change, from the earlier
    period's sampled observation to the later one's (both ends dated)."""
    out = []
    for p in sorted(levels):
        if p - 1 in levels and days.get(p) is not None and days.get(p - 1) is not None:
            out.append(days[p] - days[p - 1])
    return out


def _exclusion_reason(info: dict, grid: str) -> str | None:
    if info["dated_observations"] == 0:
        return (
            f"no row has observed_at ({info['rows_without_observed_at']} rows carry "
            "collected_at only). " + UNDATED_RULE
        )
    if not info["_levels"]:
        if info["rows_coarser_than_grid"]:
            kinds = " and ".join(sorted(info["_coarser_kinds"]))
            hint = " Use grid=month." if info["_coarser_kinds"] == {"month"} else ""
            return (
                f"every observation describes a {kinds}, coarser than the {grid} grid."
                + hint
            )
        return "no observed_at could be read as a date"
    return None


def _median_gap(periods: Iterable[int]) -> float | None:
    ordered = sorted(periods)
    gaps = sorted(b - a for a, b in zip(ordered, ordered[1:]))
    if not gaps:
        return None
    mid = len(gaps) // 2
    return float(gaps[mid]) if len(gaps) % 2 else (gaps[mid - 1] + gaps[mid]) / 2.0


def _sig(value: float) -> float:
    return float(f"{value:.3g}")


def _floor1(value: float) -> float:
    # Rounded down, so an effective size just under 3 never reads as 3.0.
    return math.floor(value * 10 + 1e-9) / 10


# ---------------------------------------------------------------------------
# The analysis
# ---------------------------------------------------------------------------


def lagged_connections(
    store: HistoryStore,
    series_keys: Any,
    grid: Any = None,
    transform: Any = None,
    max_lag: Any = None,
    min_overlap: Any = None,
    q: Any = None,
    start: Any = None,
    end: Any = None,
) -> dict:
    """Correlation between stored series at leads and lags (module docstring)."""
    clamps: list[dict] = []
    try:
        keys = _series_keys(series_keys)
        grid = DEFAULT_GRID if grid is None else grid
        if grid not in GRIDS:
            raise _InputError(f"unknown grid {grid!r}; allowed: {', '.join(GRIDS)}")
        transform = DEFAULT_TRANSFORM if transform is None else transform
        if transform not in TRANSFORMS:
            raise _InputError(
                f"unknown transform {transform!r}; allowed: {', '.join(TRANSFORMS)}"
            )
        requested_lag = _clamp(
            "max_lag", _integer("max_lag", max_lag, DEFAULT_MAX_LAG), 0, MAX_LAG_CAP, clamps
        )
        min_n = _clamp(
            "min_overlap",
            _integer("min_overlap", min_overlap, DEFAULT_MIN_OVERLAP),
            MIN_OVERLAP_FLOOR, MAX_PERIODS, clamps,
        )
        if q is None:
            q = DEFAULT_Q
        if isinstance(q, bool) or not isinstance(q, (int, float)) or not math.isfinite(q):
            raise _InputError(f"q must be a number, got {q!r}")
        q_threshold = _clamp("q", float(q), Q_FLOOR, Q_CAP, clamps)
        start_d = _iso_date("start", start)
        end_d = _iso_date("end", end)
        if start_d and end_d and start_d > end_d:
            raise _InputError(f"start {start_d} is after end {end_d}")
    except _InputError as exc:
        return {"error": str(exc)}

    rows = {key: _read_rows(store, key) for key in keys}
    unknown = [key for key in keys if not rows[key]]
    if unknown:
        return {
            "error": f"no observations stored for {unknown}; call intel_series_history "
            "without series_key to list available series",
            "unknown_series": unknown,
        }

    infos = [_sample_on_grid(key, rows[key], grid) for key in keys]
    excluded = []
    usable = []
    for info in infos:
        reason = _exclusion_reason(info, grid)
        if reason:
            excluded.append({"series_key": info["series_key"], "reason": reason})
        else:
            usable.append(info)

    unit = _UNIT[grid]
    # One shared window of whole grid periods.
    window: dict[str, Any] = {"start": None, "end": None, "limited_to_recent": False}
    lo = period_of(start_d.isoformat(), grid)[0] if start_d else None
    hi = period_of(end_d.isoformat(), grid)[0] if end_d else None
    if usable:
        first = min(min(i["_levels"]) for i in usable)
        last = max(max(i["_levels"]) for i in usable)
        lo = first if lo is None else lo
        hi = last if hi is None else hi
        if hi - lo + 1 > MAX_PERIODS:
            lo = hi - MAX_PERIODS + 1
            window["limited_to_recent"] = True
            if start_d:
                clamps.append({
                    "input": "start", "requested": start_d.isoformat(),
                    "used": period_label(lo, grid),
                    "reason": f"the window is limited to the most recent {MAX_PERIODS} {unit}s",
                })
        window.update(start=period_label(lo, grid), end=period_label(hi, grid),
                      periods=max(0, hi - lo + 1))

    still_usable = []
    for info in usable:
        inside = {p: v for p, v in info["_levels"].items() if lo <= p <= hi}
        info["periods_outside_window"] = len(info["_levels"]) - len(inside)
        info["_levels"] = inside
        if not inside:
            excluded.append({"series_key": info["series_key"],
                             "reason": "no observation inside the window"})
            continue
        info["_values"] = apply_transform(inside, transform)
        info["_intervals"] = (
            change_interval_days(inside, info["_days"]) if transform == "changes" else []
        )
        info["_phi"], info["_phi_pairs"] = lag1_autocorrelation(info["_values"])
        still_usable.append(info)
    usable = still_usable

    pair_index = [(i, j) for i in range(len(usable)) for j in range(i + 1, len(usable))]
    lag = requested_lag
    if pair_index and len(pair_index) * (2 * lag + 1) > MAX_TESTS:
        lag = max(0, (MAX_TESTS // len(pair_index) - 1) // 2)
        clamps.append({
            "input": "max_lag", "requested": requested_lag, "used": lag,
            "reason": f"{len(pair_index)} pairs x {2 * lag + 1} lags keeps the call "
            f"within {MAX_TESTS} tests",
        })

    tests: list[dict] = []
    pairs: list[dict] = []
    for i, j in pair_index:
        a, b = usable[i], usable[j]
        pair = {"a": a["series_key"], "b": b["series_key"], "not_tested": [], "_tests": []}
        for k in range(-lag, lag + 1):
            xs, ys = overlap_at_lag(a["_values"], b["_values"], k)
            n = len(xs)
            if n < min_n:
                pair["not_tested"].append({"lag": k, "n": n, "reason": "below_min_overlap"})
                continue
            r = pearson_r(xs, ys)
            if r is None:
                pair["not_tested"].append({"lag": k, "n": n, "reason": "no_variation"})
                continue
            t_abs = math.inf if abs(r) >= 1.0 else abs(r) * math.sqrt((n - 2) / (1 - r * r))
            n_eff, p_ar1, note = ar1_adjusted(r, n, a["_phi"], b["_phi"])
            if note == NO_PHI_NOTE and all(
                s["_phi_pairs"] >= _AUTOCORR_MIN_PAIRS for s in (a, b) if s["_phi"] is None
            ):
                note = NO_PHI_FLAT_NOTE
            test = {"lag": k, "r": r, "n": n, "p": correlation_p_value(r, n), "_t": t_abs,
                    "n_eff": n_eff, "p_ar1": p_ar1, "note": note}
            tests.append(test)
            pair["_tests"].append(test)
        pairs.append(pair)

    pvalues = [t["p"] for t in tests]
    p_ar1_values = [t["p_ar1"] for t in tests]
    for test, q_bh, q_ar1, q_by in zip(
        tests, benjamini_hochberg(pvalues), benjamini_hochberg(p_ar1_values),
        benjamini_yekutieli(p_ar1_values),
    ):
        test["q"], test["q_ar1"], test["q_by"] = q_bh, q_ar1, q_by
        test["significant"] = q_ar1 <= q_threshold
        test["significant_if_independent"] = q_bh <= q_threshold

    gaps: list[str] = []

    def _entry(test: dict) -> dict:
        k = test["lag"]
        plural = "" if abs(k) == 1 else "s"
        timing = (
            f"A leads B by {k} {unit}{plural}" if k > 0
            else f"B leads A by {-k} {unit}{plural}" if k < 0
            else f"same {unit}"
        )
        n_eff = test["n_eff"]
        out = {
            "lag": k,
            "timing": timing,
            "r": round(test["r"], 4),
            "direction": "same" if test["r"] > 0 else "opposite" if test["r"] < 0 else "none",
            "n": test["n"],
            "p": _sig(test["p"]),
            "q": _sig(test["q"]),
            "n_effective_ar1": None if n_eff is None else _floor1(n_eff),
            "p_ar1": _sig(test["p_ar1"]),
            "q_ar1": _sig(test["q_ar1"]),
            "q_by": _sig(test["q_by"]),
            "significant": test["significant"],
            "significant_if_independent": test["significant_if_independent"],
            "serial_correlation_dominates": (
                None if n_eff is None else n_eff < DOMINATES_BELOW * test["n"]
            ),
        }
        if test["note"]:
            out["note"] = test["note"]
        return out

    pair_out = []
    by_key = {info["series_key"]: info for info in usable}
    for pair in pairs:
        a, b = by_key[pair["a"]], by_key[pair["b"]]
        ranked = sorted(pair["_tests"], key=lambda t: (t["p"], -t["_t"]))
        strongest = [t for t in ranked if (t["p"], t["_t"]) == (ranked[0]["p"], ranked[0]["_t"])] if ranked else []
        pair_out.append({
            "a": pair["a"],
            "b": pair["b"],
            "lags_tested": len(pair["_tests"]),
            "lags_not_tested": len(pair["not_tested"]),
            "strongest": [_entry(t) for t in strongest],
            "significant": [_entry(t) for t in ranked if t["significant"]],
            "not_tested": pair["not_tested"],
        })
        if not ranked:
            biggest = max((nt["n"] for nt in pair["not_tested"]), default=0)
            gaps.append(
                f"{pair['a']} / {pair['b']}: no lag could be tested (largest overlap "
                f"{biggest} periods; minimum {min_n})"
            )
        elif strongest[0]["n_eff"] is not None and (
            strongest[0]["n_eff"] < DOMINATES_BELOW * strongest[0]["n"]
        ):
            best = strongest[0]
            gap = (
                f"{pair['a']} / {pair['b']}: serial correlation dominates: lag-1 "
                f"autocorrelations {a['_phi']:+.2f} and {b['_phi']:+.2f} leave an AR(1) "
                f"effective sample size of {_floor1(best['n_eff']):g} of {best['n']} periods "
                "at the strongest lag, so p and q overstate the evidence; significance uses q_ar1"
            )
            if best["n_eff"] < 3:
                gap += ". Below 3 the sample carries no evidence either way (p_ar1 set to 1)"
            gaps.append(gap)

    tested_keys = {key for p in pairs if p["_tests"] for key in (p["a"], p["b"])}
    series_out = []
    for info in usable:
        values = info["_values"]
        intervals = info["_intervals"]
        series_out.append({
            "series_key": info["series_key"],
            "source": info["source"],
            "label": info["label"],
            "unit": info["unit"],
            "rows": info["rows"],
            "rows_without_observed_at": info["rows_without_observed_at"],
            "rows_coarser_than_grid": info["rows_coarser_than_grid"],
            "rows_unreadable_observed_at": info["rows_unreadable_observed_at"],
            "revised_observed_at": info["revised_observed_at"],
            "dated_observations": info["dated_observations"],
            "periods": len(info["_levels"]),
            "periods_outside_window": info["periods_outside_window"],
            "first_period": period_label(min(info["_levels"]), grid),
            "last_period": period_label(max(info["_levels"]), grid),
            "median_period_gap": _median_gap(info["_levels"]),
            "points_after_transform": len(values),
            "lag1_autocorrelation": None if info["_phi"] is None else round(info["_phi"], 4),
            "lag1_pairs": info["_phi_pairs"],
            "change_interval_days": (
                {"min": min(intervals), "max": max(intervals)} if intervals else None
            ),
        })
        key = info["series_key"]
        phi = info["_phi"]
        tested = key in tested_keys
        if tested and phi is not None and abs(phi) > AUTOCORR_NOTE_ABOVE:
            gaps.append(
                f"{key}: lag-1 autocorrelation {phi:+.2f} after the transform; p and q "
                "assume independent periods, so significance uses the AR(1)-adjusted "
                "p_ar1 and q_ar1"
            )
        elif tested and phi is None and info["_phi_pairs"] >= _AUTOCORR_MIN_PAIRS:
            gaps.append(
                f"{key}: lag-1 autocorrelation undefined (across its {info['_phi_pairs']} "
                "adjacent-period pairs after the transform, the earlier or the later "
                "values do not vary), so its tests "
                "cannot be adjusted for serial correlation and are not counted as "
                "significant (p_ar1 set to 1)"
            )
        elif tested and phi is None:
            gap = (
                f"{key}: lag-1 autocorrelation not measurable ({info['_phi_pairs']} adjacent "
                f"periods after the transform, at least {_AUTOCORR_MIN_PAIRS} needed), so its "
                "tests cannot be adjusted for serial correlation and are not counted as "
                "significant (p_ar1 set to 1)"
            )
            spacing = _median_gap(info["_levels"])
            if spacing is not None and spacing > 1:
                gap += (f"; its observations are a median {spacing:g} {unit}s apart, so a "
                        "coarser grid may fit")
            gaps.append(gap)
        # A week is 7 days; calendar months alone vary from 28 to 31.
        if grid != "day" and intervals and max(intervals) - min(intervals) > (
            0 if grid == "week" else 3
        ):
            gaps.append(
                f"{key}: on the {grid} grid its changes span {min(intervals)} to "
                f"{max(intervals)} days (each period takes its last observation), so "
                "changes cover unequal intervals"
            )
        if info["rows_without_observed_at"]:
            gaps.append(f"{key}: {info['rows_without_observed_at']} rows without observed_at not used")
        if info["rows_coarser_than_grid"]:
            gaps.append(f"{key}: {info['rows_coarser_than_grid']} rows coarser than the {grid} grid not used")
        if info["rows_unreadable_observed_at"]:
            gaps.append(f"{key}: {info['rows_unreadable_observed_at']} rows with an unreadable observed_at not used")
        if info["revised_observed_at"]:
            gaps.append(
                f"{key}: {info['revised_observed_at']} observation dates were revised between "
                "captures; the latest collected value is used"
            )
        if len(values) < min_n:
            what = "period-over-period changes" if transform == "changes" else "periods"
            gap = (f"{key}: {len(values)} {what} on the {grid} grid, fewer than the "
                   f"{min_n}-point minimum, so no test involving it can run")
            spacing = _median_gap(info["_levels"])
            if transform == "changes" and spacing is not None and spacing > 1:
                gap += (f"; its observations are a median {spacing:g} {unit}s apart, "
                        "and changes need adjacent periods, so a coarser grid may fit")
            gaps.append(gap)
    if window["limited_to_recent"]:
        gaps.append(f"window limited to the most recent {MAX_PERIODS} {unit}s")
    if len(usable) < 2:
        gaps.append("fewer than two series could be used, so nothing was tested")

    significant = sum(1 for t in tests if t["significant"])
    significant_independent = sum(1 for t in tests if t["significant_if_independent"])
    with_signal = sum(1 for p in pair_out if p["significant"])
    with_signal_independent = sum(
        1 for p in pairs if any(t["significant_if_independent"] for t in p["_tests"])
    )
    not_run = sum(p["lags_not_tested"] for p in pair_out)
    return {
        "summary": (
            f"{with_signal} of {len(pair_out)} pairs have a lag with q_ar1 <= {q_threshold:g} "
            f"({significant} of {len(tests)} tests; {not_run} pair/lag combinations not "
            f"tested). Assuming independent periods instead, {with_signal_independent} of "
            f"{len(pair_out)} pairs have a lag with q <= {q_threshold:g}."
        ),
        "series": series_out,
        "excluded_series": excluded,
        "pairs": pair_out,
        "tests_run": len(tests),
        "tests_not_run": not_run,
        "significant_tests": significant,
        "significant_tests_if_independent": significant_independent,
        "method": {
            "grid": grid,
            "grid_rule": GRID_RULES[grid],
            "window": window,
            "sampling": SAMPLING_RULE,
            "vintage": VINTAGE_RULE,
            "undated_rows": UNDATED_RULE,
            "transform": transform,
            "transform_rule": TRANSFORM_RULES[transform],
            "why_changes_default": WHY_CHANGES,
            "lags": [-lag, lag],
            "lag_convention": LAG_CONVENTION,
            "overlap": "Pairwise: each (pair, lag) test uses every period where both "
            "transformed values exist; n is reported per test.",
            "correlation": "Pearson r",
            "p_value": P_RULE,
            "multiple_testing": f"Benjamini-Hochberg across all {len(tests)} tests run in "
            "this call: q over p (periods treated as independent) and q_ar1 over p_ar1; "
            "q_by is the Benjamini-Yekutieli version of q_ar1, valid under any dependence "
            "between tests.",
            "significance": SIGNIFICANCE_RULE,
            "q_threshold": q_threshold,
            "q_threshold_default": DEFAULT_Q,
            "strongest": "Per pair, the tested lag(s) with the smallest p (ties broken by "
            "the larger |t|).",
            "min_overlap": min_n,
            "min_overlap_default": DEFAULT_MIN_OVERLAP,
            "min_overlap_reason": MIN_OVERLAP_REASON,
            "not_tested_reasons": dict(NOT_TESTED_REASONS),
            "serial_correlation": AR1_RULE,
            "limits": {
                "max_series": MAX_SERIES, "max_lag": MAX_LAG_CAP,
                "max_tests": MAX_TESTS, "max_periods": MAX_PERIODS,
                "min_overlap_floor": MIN_OVERLAP_FLOOR, "q_range": [Q_FLOOR, Q_CAP],
            },
        },
        "inputs_clamped": clamps,
        "cannot_prove": list(CANNOT_PROVE),
        "data_gaps": gaps,
        "note": HISTORY_ACCRUAL_NOTE,
    }
