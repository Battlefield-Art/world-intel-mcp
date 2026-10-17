"""Import-based tests for server.py's tool registry and dispatcher.

Every other test file reads server.py as *text* because its shared
``Cache()``/``AOIStore()`` open at the default on-disk path. Here we
import it for real, under a temp cache path (``WORLD_INTEL_CACHE_DB`` is
honored by ``Cache``; the fixture binds the shared infrastructure while
the override is in force), which is what lets these tests verify the
registry structurally instead of by regex: the TOOLS/_dispatch parity
invariant, schema well-formedness, and the actual dispatch path for the
pure (no-network) tools.
"""

import importlib
import inspect
import os
import re
import tempfile
from pathlib import Path

import pytest

_TMP_CACHE = Path(tempfile.mkdtemp(prefix="wim-server-test-")) / "cache.db"


@pytest.fixture(scope="module")
def server():
    """Import server.py exactly once, cache pointed at a temp file."""
    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        module = importlib.import_module("world_intel_mcp.server")
        # The shared infrastructure binds its cache path on first access,
        # and an earlier test module may already have touched it under a
        # different path. Rebind it here, under the override, so this
        # file's premise holds whatever order the modules run in.
        importlib.import_module("world_intel_mcp.runtime")._initialize()
        yield module
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


def test_server_cache_landed_in_temp_path(server) -> None:
    """Guard for this file's own premise: if the env override stopped
    working, every test here would be silently exercising (and writing
    to) the developer's real cache."""
    assert Path(server.cache.db_path) == _TMP_CACHE


def test_tool_names_unique_and_conventional(server) -> None:
    names = [t.name for t in server.TOOLS]
    assert len(names) == len(set(names)), "duplicate tool name in TOOLS"
    non_conforming = [n for n in names if not n.startswith("intel_")]
    assert non_conforming == [], f"tools outside intel_* convention: {non_conforming}"


def test_every_tool_has_description_and_object_schema(server) -> None:
    for tool in server.TOOLS:
        assert tool.description and tool.description.strip(), tool.name
        assert isinstance(tool.inputSchema, dict), tool.name
        assert tool.inputSchema.get("type") == "object", tool.name


def test_tools_and_dispatch_are_in_one_to_one_parity(server) -> None:
    """The invariant ROADMAP.md carries ('TOOLS and _dispatch are
    aligned'). Since the Phase 26 split, dispatch is the union of the
    modular handler registry (tools.ALL_HANDLERS, whose per-module
    parity is enforced at import by tools.aggregate) and the shrinking
    legacy match/case; every registered tool must resolve through
    exactly that union, and nothing may be dispatchable without being
    registered."""
    from world_intel_mcp import tools as tools_pkg

    registered = {t.name for t in server.TOOLS}
    dispatch_src = inspect.getsource(server._dispatch)
    legacy_cases = set(re.findall(r'case "(intel_[a-z0-9_]+)"', dispatch_src))
    handled = legacy_cases | set(tools_pkg.ALL_HANDLERS)
    assert registered - handled == set(), "registered but never dispatched"
    assert handled - registered == set(), "dispatched but never registered"
    overlap = legacy_cases & set(tools_pkg.ALL_HANDLERS)
    assert overlap == set(), f"tool handled twice (legacy + modular): {overlap}"


def test_aggregate_refuses_drift_and_collisions() -> None:
    """The import-time guarantee has to be falsifiable: a module whose
    TOOLS and HANDLERS disagree, or that collides with an existing
    name, must refuse to aggregate."""
    import types

    from mcp.types import Tool as _Tool

    from world_intel_mcp import tools as tools_pkg

    drifty = types.SimpleNamespace(
        __name__="drifty",
        TOOLS=[
            _Tool(
                name="intel_fake",
                description="x",
                inputSchema={"type": "object", "properties": {}},
            )
        ],
        HANDLERS={},
    )
    with pytest.raises(RuntimeError, match="drift"):
        tools_pkg.aggregate([drifty])

    async def _h(arguments):
        return {}

    colliding = types.SimpleNamespace(
        __name__="colliding",
        TOOLS=[
            _Tool(
                name="intel_status",
                description="x",
                inputSchema={"type": "object", "properties": {}},
            )
        ],
        HANDLERS={"intel_status": _h},
    )
    from world_intel_mcp.tools import system as system_mod

    with pytest.raises(RuntimeError, match="collision"):
        tools_pkg.aggregate([system_mod, colliding])


def test_tool_count_matches_documented_surface(server) -> None:
    """139 = 138 intelligence tools + intel_status. If this fails after
    adding a tool, update README.md, ROADMAP.md, and this number in the
    same change: the repo's history shows doc counts drifting stale
    within weeks when nothing enforced them."""
    assert len(server.TOOLS) == 139


@pytest.mark.asyncio
async def test_dispatch_unknown_tool_returns_error(server) -> None:
    result = await server._dispatch("intel_no_such_tool", {})
    assert result == {"error": "Unknown tool: intel_no_such_tool"}


@pytest.mark.asyncio
async def test_dispatch_reaches_real_aoi_store_round_trip(server) -> None:
    """Drive define -> list -> update -> delete through the real
    dispatcher and the module-level AOIStore (temp-backed): the tools
    exist end-to-end, not just as registry entries."""
    defined = await server._dispatch(
        "intel_aoi_define",
        {"name": "Registry Test", "lat": 40.44, "lon": -79.99, "radius_km": 25},
    )
    assert defined.get("aoi", {}).get("name") == "Registry Test"

    listed = await server._dispatch("intel_aoi_list", {})
    assert any(a["name"] == "Registry Test" for a in listed["aois"])

    updated = await server._dispatch(
        "intel_aoi_update", {"name": "Registry Test", "radius_km": 50}
    )
    assert updated.get("aoi", {}).get("radius_km") == 50.0

    deleted = await server._dispatch("intel_aoi_delete", {"name": "Registry Test"})
    assert deleted.get("deleted") == "Registry Test"


@pytest.mark.asyncio
async def test_dispatch_static_dataset_tool_works(server) -> None:
    """A pure static-config tool through the real dispatcher: no
    network, real data contract."""
    result = await server._dispatch("intel_military_bases", {})
    assert result.get("count", 0) >= 70
    assert any(
        b.get("name") == "Norfolk Naval Station" for b in result.get("bases", [])
    )


@pytest.mark.asyncio
async def test_dispatch_polygon_and_corridor_round_trip(server) -> None:
    """Shape AOIs exist end-to-end through the real dispatcher."""
    poly = await server._dispatch(
        "intel_aoi_define_polygon",
        {
            "name": "Registry Poly",
            "vertices": [[39.5, -81.0], [39.5, -79.0], [41.3, -79.0], [41.3, -81.0]],
        },
    )
    assert poly.get("aoi", {}).get("kind") == "polygon"

    corr = await server._dispatch(
        "intel_aoi_define_corridor",
        {
            "name": "Registry Corridor",
            "waypoints": [[40.0, -80.0], [41.0, -78.0]],
            "width_km": 40,
        },
    )
    assert corr.get("aoi", {}).get("kind") == "corridor"

    listed = await server._dispatch("intel_aoi_list", {})
    kinds = {a["name"]: a["kind"] for a in listed["aois"]}
    assert kinds.get("Registry Poly") == "polygon"
    assert kinds.get("Registry Corridor") == "corridor"

    for name in ("Registry Poly", "Registry Corridor"):
        deleted = await server._dispatch("intel_aoi_delete", {"name": name})
        assert deleted.get("deleted") == name


_REPO = Path(__file__).resolve().parents[3]


def test_readme_documents_every_registered_tool(server) -> None:
    """The README's per-tool reference had drifted to 126 of 135 tools by
    2026-10-11 while every headline count said 135. Pin the reference to
    the registry itself: same names, and each section's "(n)" matches its
    rows."""
    readme = (_REPO / "README.md").read_text()
    ref = readme.split("## MCP Tools Reference", 1)[1].split("\n## ", 1)[0]
    registered = {t.name for t in server.TOOLS}
    listed: list[str] = []
    for section in re.split(r"^### ", ref, flags=re.M)[1:]:
        header, _, body = section.partition("\n")
        rows = re.findall(r"^\| `(intel_[a-z0-9_]+)`", body, re.M)
        count = re.search(r"\((\d+)\)\s*$", header)
        assert count, f"README section without a (n) count: {header}"
        assert int(count.group(1)) == len(rows), f"README '{header}' says {count.group(1)}, lists {len(rows)}"
        listed.extend(rows)
    assert len(listed) == len(set(listed)), "a tool is listed twice in the README reference"
    assert set(listed) == registered, {
        "undocumented": sorted(registered - set(listed)),
        "documented_but_not_registered": sorted(set(listed) - registered),
    }
    n = len(server.TOOLS)
    assert f"**{n} MCP tools**" in readme
    assert f"**Total: {n} tools**" in readme


@pytest.mark.asyncio
async def test_dispatch_news_signal_tools_pass_arguments_and_mark_outages(
    server, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Phase 29 news-signal tools through the real dispatcher, with
    RSS and GDELT failing at the source boundary: the arguments reach the
    analysis, and an outage comes back marked, never as an empty ranking
    or a zero-hostility timeline."""
    from world_intel_mcp.analysis import news_signals as ns
    from world_intel_mcp.sources import news

    async def _rss_down(fetcher, category=None, limit=50):
        return {
            "items": [], "count": 0, "feeds_attempted": 119, "feeds_ok": 0,
            "feeds_failed": ["BBC World"], "error": "All 119 RSS feeds failed or timed out",
            "degraded": True, "reason": "all_feeds_failed",
        }

    async def _gdelt_down(fetcher, query, mode, days):
        return None

    monkeypatch.setattr(news, "fetch_news_feed", _rss_down)
    monkeypatch.setattr(ns, "_gdelt_timeline", _gdelt_down)
    monkeypatch.setattr(ns, "_GDELT_RETRY_WAIT_S", 0.0)

    sit = await server._dispatch("intel_situations", {"sort": "hostile", "kind": "between"})
    assert (sit["sort"], sit["kind"], sit["situations"]) == ("hostile", "between", [])
    assert sit["degraded"] is True and sit["reason"] == "all_feeds_failed"

    top = await server._dispatch("intel_top_stories", {"min_outlets": 4})
    assert top["min_outlets"] == 4 and top["stories"] == []
    assert top["degraded"] is True

    host = await server._dispatch(
        "intel_hostility_timeline", {"country": "IRN", "other": "Israel", "days": 3}
    )
    assert [p["iso3"] for p in host["parties"]] == ["IRN", "ISR"]
    assert host["days_requested"] == 3
    assert host["days"] == [] and host["reason"] == "gdelt_fetch_failed"
