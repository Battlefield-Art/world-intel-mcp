"""Static routing check for the dashboard's two detail renderers.

index.html renders item details through two functions: map clicks call
``showDetail(type, d)`` and drawer rows call ``showDrawerDetail(id)``, each
an ``if (type === ...)`` chain. A click whose type has no branch in the
function it reaches opens an empty panel (category label, no title, no
fields) with no console error. The AOI layer shipped that way: its branch
sat in ``showDrawerDetail`` while the map click went to ``showDetail``.

These tests read index.html as text and assert that every type a caller
dispatches has a branch in the renderer it reaches.

Gaps / not covered:
- Static text analysis, not execution. It proves a branch exists, not that
  the branch renders the right fields; the AOI panel itself was verified
  by clicking circle, polygon and corridor AOIs in headless Chromium.
- Only literal dispatches (``showDetail('x', ...)``, ``_storeItem('x', ...)``)
  are seen; a type passed through a variable is not.
- ``_KNOWN_UNROUTED_*`` lists types that are broken today in the same way
  (found while fixing AOI, not fixed in that change). The sets are strict:
  fixing one fails the test until the set is shrunk, so they cannot hide a
  new regression or outlive the fix.
"""

import re
from pathlib import Path

_INDEX = Path(__file__).resolve().parents[1] / "dashboard" / "index.html"

# Map clicks with no showDetail branch: pipeline's branch lives only in
# showDrawerDetail; trade_route has no branch anywhere.
_KNOWN_UNROUTED_MAP = {"pipeline", "trade_route"}
# Drawer rows with no showDrawerDetail branch: UCDP conflict rows.
_KNOWN_UNROUTED_DRAWER = {"conflict"}


def _function_body(src: str, signature: str) -> str:
    start = src.index(signature)
    end = src.index("\n}\n", start)
    return src[start:end]


def _branch_types(body: str) -> set[str]:
    return set(re.findall(r"type === '(\w+)'", body))


def test_every_map_click_type_has_a_show_detail_branch() -> None:
    src = _INDEX.read_text()
    dispatched = set(re.findall(r"showDetail\('(\w+)'", src))
    handled = _branch_types(_function_body(src, "function showDetail(type, d) {"))
    assert "aoi" in dispatched, "AOI layer no longer dispatches to showDetail"
    assert "aoi" in handled, "AOI click reaches showDetail, which has no aoi branch"
    assert dispatched - handled == _KNOWN_UNROUTED_MAP


def test_every_drawer_item_type_has_a_show_drawer_detail_branch() -> None:
    src = _INDEX.read_text()
    stored = set(re.findall(r"_storeItem\('(\w+)'", src))
    handled = _branch_types(_function_body(src, "function showDrawerDetail(id) {"))
    assert stored, "no drawer items found; the extraction regex is stale"
    assert stored - handled == _KNOWN_UNROUTED_DRAWER
