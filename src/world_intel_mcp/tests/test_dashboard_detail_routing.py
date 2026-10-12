"""Static routing check for the dashboard's two detail renderers.

index.html renders item details through two functions: map clicks call
``showDetail(type, d)`` and drawer rows call ``showDrawerDetail(id)``, each
an ``if (type === ...)`` chain. A click whose type has no branch in the
function it reaches opens an empty panel (category label, no title, no
fields) with no console error; ``showDrawerDetail`` instead falls back to a
raw dump of the first 12 keys. The AOI layer shipped with an empty panel:
its branch sat in ``showDrawerDetail`` while the map click went to
``showDetail``. Pipelines and trade routes (map) were empty the same way,
and the drawer's UCDP and conflict-zone rows got the raw dump.

These tests read index.html as text and assert that every type a caller
dispatches has a branch in the renderer it reaches.

Gaps / not covered:
- Static text analysis, not execution. It proves a branch exists, not that
  the branch renders the right fields. The panels were verified by real
  clicks in headless Chromium (AOI circle/polygon/corridor, pipeline and
  trade route on the map, pipeline/UCDP/conflict-zone drawer rows).
- The raw-dump fallback means a missing drawer branch is not an empty
  panel, only an unformatted one; this test is what catches it.
- Only literal dispatches (``showDetail('x', ...)``, ``_storeItem('x', ...)``)
  are seen; a type passed through a variable is not.
- A delegating branch (the drawer's ``conflict`` rows hand off to
  ``showDetail``) counts as handled; the delegate's own branch is checked
  by the map-side test because the hand-off is a literal ``showDetail('conflict'``.
"""

import re
from pathlib import Path

_INDEX = Path(__file__).resolve().parents[1] / "dashboard" / "index.html"


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
    assert dispatched - handled == set()


def test_every_drawer_item_type_has_a_show_drawer_detail_branch() -> None:
    src = _INDEX.read_text()
    stored = set(re.findall(r"_storeItem\('(\w+)'", src))
    handled = _branch_types(_function_body(src, "function showDrawerDetail(id) {"))
    assert stored, "no drawer items found; the extraction regex is stale"
    assert stored - handled == set()
