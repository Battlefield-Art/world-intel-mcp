"""The /api/stream conflict-zone fallback.

The page reads only /api/stream. The hotspot fallback for the conflict
layer (used when ACLED and UCDP both have nothing) used to be built only
in /api/overview, so with no conflict feed the map layer and the drawer's
"Active Conflict Zones" section stayed empty. Both endpoints now share
``_conflict_zones_fallback``; these tests drive one real stream tick with
stubbed sources and read the SSE events it yields.

Gaps / not covered: only one tick of the generator is consumed (the
30 s re-tick loop is not exercised); the page-side rendering of the
payload is verified in headless Chromium, not here.
"""

import json

import pytest

from world_intel_mcp.dashboard import app as dashboard_app


async def _one_tick(monkeypatch: pytest.MonkeyPatch, sources: dict) -> list[dict]:
    """Run one stream tick over ``sources`` (name -> payload) and return
    the decoded SSE events up to and including the _complete event."""

    def _defs(_fetcher):
        def _factory(payload):
            async def _coro():
                return payload

            return _coro

        return {name: _factory(payload) for name, payload in sources.items()}

    monkeypatch.setattr(dashboard_app, "_ensure_fetcher", lambda: None)
    monkeypatch.setattr(dashboard_app, "_stream_source_defs", _defs)
    resp = await dashboard_app.api_stream(None)
    events = []
    it = resp.body_iterator
    try:
        while True:
            chunk = await it.__anext__()
            text = chunk if isinstance(chunk, str) else chunk.decode()
            assert text.startswith("data: ")
            ev = json.loads(text[len("data: "):])
            events.append(ev)
            if ev.get("_complete"):
                break
    finally:
        await it.aclose()
    return events


def _zones_events(events: list[dict]) -> list[dict]:
    return [e for e in events if "conflict_zones" in e]


async def test_stream_sends_hotspot_fallback_when_no_conflict_feed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = await _one_tick(
        monkeypatch,
        {
            "earthquakes": {"earthquakes": [], "count": 0},
            "acled_events": {"error": "ACLED credentials not configured"},
            "ucdp_events": {"events": [], "count": 0, "source": "ucdp"},
        },
    )
    zones = _zones_events(events)
    assert len(zones) == 1, "exactly one conflict_zones event per tick"
    cz = zones[0]["conflict_zones"]
    expected = dashboard_app._conflict_zones_fallback(
        {"error": "ACLED credentials not configured"}, {"events": [], "count": 0}
    )
    assert cz["source"] == "intel-hotspots"
    assert cz["count"] == expected["count"] == len(dashboard_app.INTEL_HOTSPOTS) > 0
    assert [e["location"] for e in cz["events"]] == [
        e["location"] for e in expected["events"]
    ]
    # The extra event is not a source: progress counters stay on the source count.
    assert zones[0]["_progressive"] is True
    assert zones[0]["_total"] == 3
    assert max(e.get("_done", 0) for e in events) == 3


async def test_stream_clears_fallback_when_a_conflict_feed_has_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events = await _one_tick(
        monkeypatch,
        {
            "acled_events": {"error": "ACLED credentials not configured"},
            "ucdp_events": {"events": [{"id": 1}], "count": 1, "source": "ucdp"},
        },
    )
    zones = _zones_events(events)
    assert len(zones) == 1
    assert zones[0]["conflict_zones"] is None


def test_fallback_builder_returns_none_when_acled_has_events() -> None:
    assert (
        dashboard_app._conflict_zones_fallback({"events": [{}], "count": 1}, None)
        is None
    )
    assert dashboard_app._conflict_zones_fallback(None, None)["count"] > 0
