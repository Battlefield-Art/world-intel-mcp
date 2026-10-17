"""Static check: the dashboard's Trends panel never shows an unknown count as 0.

``fetch_weekly_trends`` reports ``trend_count`` as None when the baseline
database is missing or unreadable and ``current_anomaly_count`` as None
when anomaly detection fails, naming each failure in ``data_gaps`` (and in
``error`` when the baselines could not be read). The dashboard turns a
fetch that timed out or raised into ``{"error": ...}`` with no counts at
all. The Trends panel rendered both counts as ``(x || 0)``, so a failed
anomaly check read as "0 anomalies", and it hid itself whenever ``error``
was set, so a missing baseline database left no trace on the page.

These tests read index.html as text and assert the panel's contract: both
counts go through ``countOrUnknown`` (the number, else "?"), the panel is
not hidden on error, and it lists the data gaps (or the error when there
are none) through ``esc``.

Gaps / not covered:
- Static text analysis, not execution: it proves the block is written this
  way, not that a browser renders it so. The block was executed in node
  against five payloads (healthy, failed anomaly detection, missing
  baselines, a dashboard timeout, a gap containing markup) before and after
  the change, outside this suite; it was not exercised in a browser.
- Only the Trends panel is checked. Other panels that render a missing
  count are not.
"""

import re
from pathlib import Path

_INDEX = Path(__file__).resolve().parents[1] / "dashboard" / "index.html"
_SECTION = "// ── "


def _trends_block(src: str) -> str:
    start = src.index(_SECTION + "TRENDS ──")
    end = src.index(_SECTION, start + len(_SECTION))
    return src[start:end]


def test_trends_counts_render_unknown_instead_of_zero() -> None:
    block = _trends_block(_INDEX.read_text())
    for field in ("trend_count", "current_anomaly_count"):
        assert re.search(rf"wt\.{field}\s*\|\|", block) is None, (
            f"{field} still falls back with ||, so null reads as 0"
        )
        assert f"countOrUnknown(wt.{field})" in block, f"{field} is not rendered through countOrUnknown"


def test_count_or_unknown_shows_only_numbers() -> None:
    match = re.search(r"^function countOrUnknown\(n\) \{(.*)\}$", _INDEX.read_text(), re.M)
    assert match, "countOrUnknown is not defined"
    body = match.group(1)
    assert "typeof n === 'number'" in body
    assert re.search(r":\s*'\?'", body), "a non-number does not render as '?'"


def test_trends_panel_shows_when_the_fetch_failed() -> None:
    block = _trends_block(_INDEX.read_text())
    gate = re.search(r"^\s*if \((.+)\) \{\s*$", block, re.M)
    assert gate, "the Trends block has no opening if"
    assert "weekly_trends" in gate.group(1)
    assert "error" not in gate.group(1), "the panel is hidden when the fetch reports an error"


def test_trends_panel_lists_gaps_or_error_escaped() -> None:
    block = _trends_block(_INDEX.read_text())
    assert "wt.data_gaps" in block, "the panel never reads data_gaps"
    assert "wt.error" in block, "a fetch error with no data_gaps is not shown"
    assert "Data Gaps" in block
    assert "esc(String(g))" in block, "gap text is not escaped"
