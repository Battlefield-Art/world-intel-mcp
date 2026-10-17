"""Tests for the dashboard research panels (ROADMAP Phase 29, wave 3).

Four routes back four panels: /api/liquidity-board, /api/situations,
/api/top-stories and /api/collection-health. Each calls the function the
matching MCP tool calls. The route tests stub that function (collection
health reads a real temporary history store written with ``record_cycle``)
and pin the JSON each route answers: the payload passed through unchanged
on success, partial failures left inside it, a whole-call failure as a 503
with the payload or reason intact, a timeout as a 504, bad parameters as a
400 before anything is fetched.

The page's renderers are pure functions between the ``research-render``
markers in index.html. The JS tests run that block under node, deno or bun
and parse the markup they return: remote strings escaped, links limited to
http and https, a never-loaded or failed panel showing no figures, the
liquidity board keeping units and keeping amounts apart from flows, and the
collection health panel listing problems and its cannot_prove text.

Uses Starlette's TestClient without the context manager so lifespan
(default-path Cache, vector store) never runs.

Gaps / not covered: the controller half of the page (fetch, tabs, refresh
timer, Esc) runs only in a browser and is checked by driving the dashboard
live, not here; the JS tests skip when no JavaScript runtime is on PATH; no
route is load tested.
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
import re
import shutil
import subprocess
import sys
import threading
import time
import types
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

import httpx
import pytest
from starlette.testclient import TestClient

from world_intel_mcp import history
from world_intel_mcp.analysis import news_signals
from world_intel_mcp.dashboard import app as dashboard_app
from world_intel_mcp.history import HistoryStore, SourceOutcome
from world_intel_mcp.sources import liquidity, news

_INDEX = Path(dashboard_app.__file__).with_name("index.html")
_METHOD = liquidity.NET_LIQUIDITY_METHOD


@pytest.fixture
def client() -> TestClient:
    return TestClient(dashboard_app.app)


@pytest.fixture
def fetcher(monkeypatch: pytest.MonkeyPatch) -> object:
    sentinel = object()
    monkeypatch.setattr(dashboard_app, "_ensure_fetcher", lambda: sentinel)
    return sentinel


def _stub(monkeypatch, module, name, result=None, exc=None, delay=None) -> list:
    """Replace ``module.name`` with a coroutine that records its call and
    returns a deep copy of ``result`` (or raises ``exc``)."""
    calls: list = []

    async def fake(fetcher, **kwargs):
        calls.append((fetcher, kwargs))
        if delay:
            await asyncio.sleep(delay)
        if exc is not None:
            raise exc
        return copy.deepcopy(result)

    monkeypatch.setattr(module, name, fake)
    return calls


# ---------------------------------------------------------------------------
# Payloads in the shapes the functions return (measured live 2026-10-17)
# ---------------------------------------------------------------------------


def _row(indicator, label, units, value, previous, change, pct, measure="amount_outstanding", **extra):
    return {
        "indicator": indicator, "label": label, "dataset": f"FRED {indicator.upper()}",
        "source": "fred-graph", "measure": measure, "frequency": "weekly", "units": units,
        "latest_value": value, "status": None, "observed_at": "2026-10-08",
        "period_end": "2026-10-08", "previous_value": previous,
        "previous_observed_at": "2026-10-01", "change": change, "change_pct": pct,
        "change_basis": "vs previous observation",
        "freshness": {"latest_age_days": 9, "expected_max_age_days": 14, "stale": False},
        **extra,
    }


_BOARD = {
    "sort": "biggest_move",
    "ranking_rule": "biggest_move ranks absolute changes in billions of each row currency",
    "count": 2,
    "indicators": [
        _row("fed_total_assets", "Fed total assets", "USD millions", 6_600_000.0, 6_612_345.0, -12_345.0, -0.1867),
        _row(
            "reverse_repo", "Overnight reverse repo", "USD billions", 210.5, 205.0, 5.5, 2.6829,
            served_from_stale_cache=True, cache_age_seconds=4000,
        ),
    ],
    "unavailable": [{"indicator": "euro_area_m1", "dataset": "ECB BSI", "error": "ECB HTTP 503"}],
    "blocked": copy.deepcopy(liquidity.BLOCKED_SOURCES),
    "net_liquidity_formula": liquidity.NET_LIQUIDITY_FORMULA,
    "note": "M1 is contained in M2 and M2 in M3.",
    "sources": ["fred-graph", "ecb-data-portal"],
    "data_gaps": ["euro_area_m1: ECB HTTP 503"],
    "timestamp": "2026-10-17T19:04:08+00:00",
}

_BOARD_OUTAGE = {
    **_BOARD,
    "count": 0,
    "indicators": [],
    "unavailable": [
        {"indicator": "fed_total_assets", "dataset": "FRED WALCL", "error": "FRED WALCL: HTTP 503"},
        {"indicator": "net_liquidity", "error": "a component failed; never computed with a zero term"},
    ],
    "data_gaps": ["FRED WALCL: HTTP 503", "net liquidity not computed: a component series failed"],
    "error": "liquidity board unavailable: every source failed",
}

_WINDOW = {
    "hours": 24.0,
    "current": {"start": "2026-10-16T19:04:08Z", "end": "2026-10-17T19:04:08Z"},
    "prior": {"start": "2026-10-15T19:04:08Z", "end": "2026-10-16T19:04:08Z"},
}
_COVERAGE = {
    "items_read": 2632, "items_current_window": 632, "items_current_window_comparable": 185,
    "items_prior_window": 487, "undated_items": 50, "future_dated_items": 0, "feeds_ok": 94,
    "feeds_failed": ["CFR", "HRW"], "feeds_comparable": 55, "feeds_not_comparable": ["SCMP"],
    "feeds_stale": [],
}

_SITUATIONS = {
    "situations": [
        {
            "kind": "between", "key": "IRN|ISR",
            "countries": [{"iso3": "IRN", "name": "Iran"}, {"iso3": "ISR", "name": "Israel"}],
            "articles": 12, "hostile_articles": 7, "hostile_share": 0.583,
            "by_category": {"threat": 4, "sanction": 0, "force": 3, "criticism": 0},
            "outlet_count": 8, "current_comparable": 9, "prior": 4, "change": 5, "change_ratio": 2.0,
            "sample_headlines": [
                {"title": "Israel and Iran trade warnings", "outlet": "BBC",
                 "link": "https://www.bbc.co.uk/news/1", "published": "2026-10-17T10:00:00Z",
                 "hostility": ["threat"]},
            ],
        }
    ],
    "count": 1, "sort": "covered", "kind": "all", "min_articles": 2,
    "source": "rss-aggregator+news-signals", "timestamp": "2026-10-17T19:04:08Z",
    "window": _WINDOW, "coverage": _COVERAGE,
    "definitions": {"between": "a headline naming two or more countries"},
    "method": "RSS headlines from every configured feed.",
    "data_gaps": ["2 RSS feeds failed or timed out and are absent: CFR, HRW"],
}

# What fetch_situations answers when every RSS feed failed (its own shape).
_SITUATIONS_DEGRADED = {
    "situations": [], "count": 0, "sort": "covered", "kind": "all",
    "source": "rss-aggregator+news-signals", "timestamp": "2026-10-17T19:04:08Z",
    "error": "RSS feed read failed", "degraded": True, "reason": "rss_unavailable",
    "data_gaps": ["every RSS feed failed: no coverage could be read, which is not the same as no coverage"],
}

_TOP = {
    "stories": [
        {
            "headline": "Pope returns to Vatican after surgery", "outlet_count": 4,
            "outlets": ["BBC", "NDTV", "SCMP", "Al Jazeera"], "articles": 5,
            "keywords": ["pope", "surgery"], "first_seen": "2026-10-17T10:26:27Z",
            "last_seen": "2026-10-17T17:37:52Z",
            "items": [{"title": "Pope returns to Vatican", "outlet": "BBC",
                       "link": "https://www.bbc.co.uk/news/2", "published": "2026-10-17T17:37:52Z"}],
        }
    ],
    "count": 1,
    "attention": [{"type": "country", "name": "Yemen", "iso3": "YEM", "current_comparable": 4,
                   "prior": 0, "change": 4, "change_ratio": 5.0}],
    "min_outlets": 3, "source": "rss-aggregator+news-signals", "timestamp": "2026-10-17T19:04:08Z",
    "window": _WINDOW, "coverage": _COVERAGE,
    "definitions": {"outlet": "a publisher"}, "method": "Clustering is lexical.",
    "data_gaps": ["2 RSS feeds failed or timed out and are absent: CFR, HRW"],
}


# ---------------------------------------------------------------------------
# /api/liquidity-board
# ---------------------------------------------------------------------------


def test_liquidity_board_passes_payload_through_with_the_method_block(client, fetcher, monkeypatch) -> None:
    calls = _stub(monkeypatch, liquidity, "fetch_liquidity_board", _BOARD)
    r = client.get("/api/liquidity-board")
    assert r.status_code == 200
    assert "access-control-allow-origin" not in r.headers
    # Partial outage (unavailable), stale-cache row and blocked list all stay in the payload.
    assert r.json() == {**_BOARD, "net_liquidity_method": _METHOD}
    assert calls == [(fetcher, {"sort": "biggest_move"})]


def test_liquidity_board_sort_is_passed_and_validated(client, fetcher, monkeypatch) -> None:
    calls = _stub(monkeypatch, liquidity, "fetch_liquidity_board", _BOARD)
    assert client.get("/api/liquidity-board?sort=newest").status_code == 200
    assert calls == [(fetcher, {"sort": "newest"})]
    r = client.get("/api/liquidity-board?sort=pct")
    assert r.status_code == 400
    assert r.json() == {"error": "sort must be one of biggest_move, newest, got 'pct'"}
    assert len(calls) == 1  # nothing fetched for a refused request


def test_liquidity_board_total_outage_is_503_with_reasons_intact(client, fetcher, monkeypatch) -> None:
    _stub(monkeypatch, liquidity, "fetch_liquidity_board", _BOARD_OUTAGE)
    r = client.get("/api/liquidity-board")
    assert r.status_code == 503
    assert r.json() == {**_BOARD_OUTAGE, "net_liquidity_method": _METHOD}


def test_liquidity_board_exception_is_503_with_reason(client, fetcher, monkeypatch) -> None:
    _stub(monkeypatch, liquidity, "fetch_liquidity_board", exc=RuntimeError("socket closed"))
    r = client.get("/api/liquidity-board")
    assert r.status_code == 503
    assert r.json() == {
        "error": "liquidity board failed: RuntimeError: socket closed",
        "net_liquidity_method": _METHOD,
    }


def test_liquidity_board_timeout_is_504(client, fetcher, monkeypatch) -> None:
    monkeypatch.setattr(dashboard_app, "_RESEARCH_TIMEOUT_S", 0.05)
    _stub(monkeypatch, liquidity, "fetch_liquidity_board", _BOARD, delay=5)
    r = client.get("/api/liquidity-board")
    assert r.status_code == 504
    assert r.json() == {
        "error": "liquidity board timed out after 0.05 s",
        "timeout": True,
        "net_liquidity_method": _METHOD,
    }


def test_timeout_raised_inside_the_function_is_a_failure_not_the_budget(client, fetcher, monkeypatch) -> None:
    _stub(monkeypatch, liquidity, "fetch_liquidity_board", exc=TimeoutError("ECB read timed out"))
    r = client.get("/api/liquidity-board")
    assert r.status_code == 503
    assert r.json() == {
        "error": "liquidity board failed: TimeoutError: ECB read timed out",
        "net_liquidity_method": _METHOD,
    }


# ---------------------------------------------------------------------------
# /api/situations and /api/top-stories
# ---------------------------------------------------------------------------


def test_situations_pass_through_with_default_arguments(client, fetcher, monkeypatch) -> None:
    calls = _stub(monkeypatch, news_signals, "fetch_situations", _SITUATIONS)
    r = client.get("/api/situations")
    assert r.status_code == 200
    assert "access-control-allow-origin" not in r.headers
    assert r.json() == _SITUATIONS
    assert calls == [(fetcher, {"hours": 24.0, "sort": "covered", "kind": "all", "limit": 20, "min_articles": 2})]


def test_situations_arguments_are_parsed(client, fetcher, monkeypatch) -> None:
    calls = _stub(monkeypatch, news_signals, "fetch_situations", _SITUATIONS)
    r = client.get("/api/situations?hours=6&sort=hostile&kind=between&limit=5&min_articles=3")
    assert r.status_code == 200
    assert calls == [(fetcher, {"hours": 6.0, "sort": "hostile", "kind": "between", "limit": 5, "min_articles": 3})]


@pytest.mark.parametrize(
    ("query", "error"),
    [
        ("hours=0", "hours must be between 1 and 168, got '0'"),
        ("hours=169", "hours must be between 1 and 168, got '169'"),
        ("hours=nan", "hours must be between 1 and 168, got 'nan'"),
        ("hours=abc", "hours must be a number between 1 and 168, got 'abc'"),
        ("sort=loudest", "sort must be one of covered, hostile, change, got 'loudest'"),
        ("kind=near", "kind must be one of all, between, inside, got 'near'"),
        ("limit=0", "limit must be between 1 and 100, got '0'"),
        ("limit=2.5", "limit must be a number between 1 and 100, got '2.5'"),
        ("min_articles=0", "min_articles must be between 1 and 1000, got '0'"),
    ],
)
def test_situations_bad_arguments_are_400_before_any_fetch(client, fetcher, monkeypatch, query, error) -> None:
    calls = _stub(monkeypatch, news_signals, "fetch_situations", _SITUATIONS)
    r = client.get(f"/api/situations?{query}")
    assert r.status_code == 400
    assert r.json() == {"error": error}
    assert calls == []


def test_situations_every_feed_failed_is_503_not_an_empty_list(client, fetcher, monkeypatch) -> None:
    _stub(monkeypatch, news_signals, "fetch_situations", _SITUATIONS_DEGRADED)
    r = client.get("/api/situations")
    assert r.status_code == 503
    assert r.json() == _SITUATIONS_DEGRADED


def test_situations_quiet_window_is_200_with_its_coverage(client, fetcher, monkeypatch) -> None:
    empty = {**_SITUATIONS, "situations": [], "count": 0}
    _stub(monkeypatch, news_signals, "fetch_situations", empty)
    r = client.get("/api/situations")
    assert r.status_code == 200
    assert r.json() == empty


def test_situations_route_surfaces_the_real_function_outage(client, fetcher, monkeypatch) -> None:
    """Not a stub of the analysis: the real fetch_situations runs over a
    feed reader that failed, and its own degraded answer reaches the page."""

    async def dead_feed(fetcher, **kwargs):
        return {"error": "all 119 RSS feeds failed", "degraded": True, "reason": "rss_unavailable"}

    monkeypatch.setattr(news, "fetch_news_feed", dead_feed)
    r = client.get("/api/situations")
    assert r.status_code == 503
    body = r.json()
    assert body["error"] == "all 119 RSS feeds failed"
    assert body["degraded"] is True and body["reason"] == "rss_unavailable"


def test_top_stories_pass_through_with_default_arguments(client, fetcher, monkeypatch) -> None:
    calls = _stub(monkeypatch, news_signals, "fetch_top_stories", _TOP)
    r = client.get("/api/top-stories")
    assert r.status_code == 200
    assert r.json() == _TOP
    assert calls == [(fetcher, {"hours": 24.0, "min_outlets": 3, "limit": 10, "attention_limit": 10})]


def test_top_stories_arguments_are_parsed_and_validated(client, fetcher, monkeypatch) -> None:
    calls = _stub(monkeypatch, news_signals, "fetch_top_stories", _TOP)
    assert client.get("/api/top-stories?hours=72&min_outlets=5&limit=3&attention_limit=4").status_code == 200
    assert calls == [(fetcher, {"hours": 72.0, "min_outlets": 5, "limit": 3, "attention_limit": 4})]
    for query, error in (
        ("min_outlets=0", "min_outlets must be between 1 and 50, got '0'"),
        ("attention_limit=51", "attention_limit must be between 1 and 50, got '51'"),
        ("hours=x", "hours must be a number between 1 and 168, got 'x'"),
    ):
        r = client.get(f"/api/top-stories?{query}")
        assert r.status_code == 400, query
        assert r.json() == {"error": error}
    assert len(calls) == 1


def test_top_stories_outage_exception_and_timeout(client, fetcher, monkeypatch) -> None:
    degraded = {"stories": [], "count": 0, "attention": [], "min_outlets": 3,
                "error": "RSS feed read failed", "degraded": True, "reason": "rss_unavailable",
                "data_gaps": ["every RSS feed failed"]}
    _stub(monkeypatch, news_signals, "fetch_top_stories", degraded)
    r = client.get("/api/top-stories")
    assert (r.status_code, r.json()) == (503, degraded)

    _stub(monkeypatch, news_signals, "fetch_top_stories", exc=ValueError("bad cluster"))
    r = client.get("/api/top-stories")
    assert (r.status_code, r.json()) == (503, {"error": "top stories failed: ValueError: bad cluster"})

    monkeypatch.setattr(dashboard_app, "_RESEARCH_TIMEOUT_S", 0.05)
    _stub(monkeypatch, news_signals, "fetch_top_stories", _TOP, delay=5)
    r = client.get("/api/top-stories")
    assert (r.status_code, r.json()) == (504, {"error": "top stories timed out after 0.05 s", "timeout": True})


def test_top_stories_empty_result_is_200(client, fetcher, monkeypatch) -> None:
    empty = {**_TOP, "stories": [], "count": 0, "attention": []}
    _stub(monkeypatch, news_signals, "fetch_top_stories", empty)
    r = client.get("/api/top-stories")
    assert (r.status_code, r.json()) == (200, empty)


async def _probe_loop(path: str, gaps: list, done: dict) -> httpx.Response:
    """GET ``path`` on the app in this event loop (no portal thread), while
    a ticker records the longest stretch the loop could not run it."""

    async def ticker() -> None:
        last = time.monotonic()
        while not done:
            await asyncio.sleep(0.01)
            now = time.monotonic()
            gaps.append(now - last)
            last = now

    transport = httpx.ASGITransport(app=dashboard_app.app)
    async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as c:
        tick = asyncio.create_task(ticker())
        try:
            return await c.get(path)
        finally:
            done["at"] = time.monotonic()
            await tick


async def test_top_stories_clustering_runs_off_the_event_loop_and_under_the_budget(monkeypatch) -> None:
    """The real fetch_top_stories over a feed read that answered. Clustering
    is seconds of CPU over a 7 d window; it runs in a worker thread, so the
    dashboard keeps serving meanwhile, and the route's budget answers 504
    with its reason instead of waiting for it."""
    loop_thread = threading.get_ident()
    threads: list[int] = []

    def slow_cluster(rows, similarity_threshold):
        threads.append(threading.get_ident())
        time.sleep(0.8)
        return []

    async def feed(fetcher, **kwargs):
        return {"items": [], "feeds_ok": 119, "feeds_failed": []}

    monkeypatch.setattr(news, "fetch_news_feed", feed)
    monkeypatch.setattr(news_signals, "cluster_articles", slow_cluster)
    monkeypatch.setattr(dashboard_app, "_ensure_fetcher", lambda: object())

    gaps: list[float] = []
    r = await _probe_loop("/api/top-stories?hours=168", gaps, {})
    assert r.status_code == 200 and r.json()["stories"] == []
    assert threads and threads[0] != loop_thread
    assert max(gaps) < 0.4, max(gaps)  # the 0.8 s of clustering never held the loop

    monkeypatch.setattr(dashboard_app, "_RESEARCH_TIMEOUT_S", 0.2)
    r = await _probe_loop("/api/top-stories?hours=168", [], {})
    assert (r.status_code, r.json()) == (504, {"error": "top stories timed out after 0.2 s", "timeout": True})


# ---------------------------------------------------------------------------
# /api/collection-health
# ---------------------------------------------------------------------------

_ROSTER = ["quakes", "fires", "storms", "ports", "tides"]
_T0 = 1_760_700_000.0  # last cycle
_CHECK = _T0 + 120.0  # when the route reads the store


def _populate(db_path: Path) -> None:
    store = HistoryStore(db_path)
    try:
        store.record_cycle(
            [SourceOutcome("ports", _T0 - 3600, 0.4, payload={"ports": [{"name": "Rotterdam"}]})],
            now=_T0 - 3600,
        )
        store.record_cycle(
            [SourceOutcome("fires", _T0 - 300, 2.0, payload={"fires": [{"lat": 1.0}]}),
             SourceOutcome("legacy_feed", _T0 - 300, 0.1, payload={"items": [{"id": 1}]})],
            now=_T0 - 300,
        )
        store.record_cycle(
            [SourceOutcome("quakes", _T0, 0.5, payload={"earthquakes": [{"magnitude": 5.1}]}),
             SourceOutcome("fires", _T0, 12.0, error="HTTP 503 from FIRMS"),
             SourceOutcome("storms", _T0, 0.3, payload={"storms": []})],
            now=_T0,
        )
    finally:
        store.close()


def _expected(db_path: Path, source: str | None = None) -> dict:
    store = HistoryStore(db_path)
    try:
        return store.collection_health(_ROSTER, source=source, now=_CHECK)
    finally:
        store.close()


@pytest.fixture
def pinned_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """Default cadence and retention, and the clock collection_health reads
    (time.time() when the route calls it) pinned to _CHECK."""
    for var in ("WORLD_INTEL_HISTORY_CADENCE_S", "WORLD_INTEL_HISTORY_CADENCES",
                "WORLD_INTEL_HISTORY_PAYLOAD_DAYS", "WORLD_INTEL_HISTORY_CAPTURE_DAYS",
                "WORLD_INTEL_HISTORY_DAYS"):
        monkeypatch.delenv(var, raising=False)
    monkeypatch.setattr(history, "time", types.SimpleNamespace(time=lambda: _CHECK))
    monkeypatch.setattr(dashboard_app, "_collector_roster", lambda: list(_ROSTER))


@pytest.fixture
def health_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, pinned_history) -> Path:
    db_path = tmp_path / "cache.db"
    monkeypatch.setattr(dashboard_app, "_history_db_path", lambda: db_path)
    return db_path


def test_collection_health_reads_the_store_and_returns_the_report_unchanged(client, health_db) -> None:
    _populate(health_db)
    r = client.get("/api/collection-health")
    assert r.status_code == 200
    assert "access-control-allow-origin" not in r.headers
    body = r.json()
    assert body == _expected(health_db)
    states = {s["source"]: s["state"] for s in body["sources"]}
    assert states == {
        "quakes": "ok", "fires": "failing", "storms": "degraded",
        "ports": "not_attempted_recently", "tides": "never_run", "legacy_feed": "ok",
    }
    by = {s["source"]: s for s in body["sources"]}
    assert by["fires"]["consecutive_failures"] == 1
    assert by["fires"]["last_reason"] == "HTTP 503 from FIRMS"
    assert by["ports"]["overdue"] is True
    assert by["legacy_feed"]["in_collector_roster"] is False
    assert by["tides"]["attempts"] == 0
    assert body["cannot_prove"] and body["storage"]["db_path"] == str(health_db)


def test_collection_health_one_source_and_unknown_source(client, health_db) -> None:
    _populate(health_db)
    r = client.get("/api/collection-health?source=fires")
    assert r.status_code == 200
    assert r.json() == _expected(health_db, source="fires")
    assert [s["source"] for s in r.json()["sources"]] == ["fires"]
    r = client.get("/api/collection-health?source=nope")
    assert r.status_code == 404
    assert r.json() == _expected(health_db, source="nope")
    assert "known_sources" in r.json()


def test_collection_health_store_never_written_reads_never_run(client, health_db) -> None:
    assert not health_db.exists()
    r = client.get("/api/collection-health")
    assert r.status_code == 200
    body = r.json()
    assert body == _expected(health_db)
    assert {s["state"] for s in body["sources"]} == {"never_run"}
    assert body["state_counts"] == {"never_run": len(_ROSTER)}
    assert body["storage"]["captures"] == 0


def test_collection_health_corrupt_store_is_503(client, health_db) -> None:
    health_db.write_bytes(b"this is not a sqlite database " * 64)
    r = client.get("/api/collection-health")
    assert r.status_code == 503
    body = r.json()
    assert set(body) == {"error", "db_path"}
    assert body["error"].startswith("history store unavailable: DatabaseError: ")
    assert body["db_path"] == str(health_db)


def test_collection_health_store_that_cannot_be_created_is_503(client, tmp_path, monkeypatch, pinned_history) -> None:
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("a file where the cache directory should be")
    db_path = blocker / "cache.db"
    monkeypatch.setattr(dashboard_app, "_history_db_path", lambda: db_path)
    r = client.get("/api/collection-health")
    assert r.status_code == 503
    body = r.json()
    assert set(body) == {"error", "db_path"}
    assert body["error"].startswith("history store unavailable: ")
    assert body["db_path"] == str(db_path)


def test_collection_health_opens_the_dashboard_cache_database(client, tmp_path, monkeypatch, pinned_history) -> None:
    """Unpatched _history_db_path: the store is the one the dashboard's own
    Fetcher cache uses, the database the collector writes in the same
    environment."""
    db_path = tmp_path / "dash-cache.db"
    _populate(db_path)
    monkeypatch.setattr(
        dashboard_app, "_ensure_fetcher",
        lambda: types.SimpleNamespace(cache=types.SimpleNamespace(db_path=db_path)),
    )
    assert dashboard_app._history_db_path() == db_path
    r = client.get("/api/collection-health")
    assert r.status_code == 200
    assert r.json() == _expected(db_path)
    assert {s["source"]: s["state"] for s in r.json()["sources"]}["fires"] == "failing"


def test_no_research_answer_is_readable_by_another_origin(client, fetcher, monkeypatch, tmp_path, pinned_history) -> None:
    """The panel fetches from its own origin. No answer of the four routes,
    success or failure, carries Access-Control-Allow-Origin, so a page from
    another site cannot read the run log or the cache database path."""
    _stub(monkeypatch, liquidity, "fetch_liquidity_board", _BOARD_OUTAGE)
    _stub(monkeypatch, news_signals, "fetch_situations", _SITUATIONS)
    _stub(monkeypatch, news_signals, "fetch_top_stories", _TOP)
    db_path = tmp_path / "cache.db"
    _populate(db_path)
    monkeypatch.setattr(dashboard_app, "_history_db_path", lambda: db_path)
    origin = {"Origin": "https://other.example"}
    answers = {
        path: client.get(path, headers=origin)
        for path in (
            "/api/liquidity-board", "/api/liquidity-board?sort=x", "/api/situations",
            "/api/top-stories", "/api/collection-health", "/api/collection-health?source=nope",
        )
    }
    assert {p: r.status_code for p, r in answers.items()} == {
        "/api/liquidity-board": 503, "/api/liquidity-board?sort=x": 400, "/api/situations": 200,
        "/api/top-stories": 200, "/api/collection-health": 200, "/api/collection-health?source=nope": 404,
    }
    leaking = [p for p, r in answers.items() if any(h.startswith("access-control-") for h in r.headers)]
    assert leaking == []


def test_roster_is_the_collector_sources() -> None:
    from world_intel_mcp.collector import SOURCES

    assert dashboard_app._collector_roster() == [name for name, _, _, _ in SOURCES]


def test_reading_the_roster_leaves_root_logging_as_it_was(monkeypatch) -> None:
    """Importing the collector runs logging.basicConfig at INFO. In the
    dashboard process nothing configures the root logger, so without the
    guard the first collection-health request would start printing every
    upstream request line. Force a fresh import against an unconfigured
    root logger and check nothing was left on it."""
    import world_intel_mcp
    from world_intel_mcp import collector

    root = logging.getLogger()
    monkeypatch.setattr(root, "handlers", [])
    monkeypatch.setattr(root, "level", logging.WARNING)
    monkeypatch.delitem(sys.modules, "world_intel_mcp.collector")
    monkeypatch.setattr(world_intel_mcp, "collector", collector)
    roster = dashboard_app._collector_roster()
    assert root.handlers == []
    assert root.level == logging.WARNING
    assert roster == [name for name, _, _, _ in collector.SOURCES]


# ---------------------------------------------------------------------------
# Page: static checks (no JavaScript runtime needed)
# ---------------------------------------------------------------------------


def _section(src: str, marker: str) -> str:
    return src[src.index(f"// {marker}:begin"):src.index(f"// {marker}:end")]


def test_research_markup_is_assigned_through_the_sanitizer() -> None:
    src = _INDEX.read_text()
    controller = _section(src, "research-panel")
    assignments = re.findall(r"innerHTML\s*=\s*([^;]+);", controller)
    assert assignments, "no innerHTML assignment found; the extraction is stale"
    assert all(a.startswith("safe(") for a in assignments), assignments


def test_renderers_touch_no_dom_or_network() -> None:
    block = _section(_INDEX.read_text(), "research-render")
    for banned in ("document.", "window.", "innerHTML", "fetch(", "$(", "localStorage", "eval("):
        assert banned not in block, banned


def test_page_calls_the_four_research_routes() -> None:
    controller = _section(_INDEX.read_text(), "research-panel")
    paths = {r.path for r in dashboard_app.app.routes}
    for path in ("/api/liquidity-board", "/api/situations", "/api/top-stories", "/api/collection-health"):
        assert f"'{path}" in controller, path
        assert path in paths, path


# ---------------------------------------------------------------------------
# Page: the renderers, run under a JavaScript runtime
# ---------------------------------------------------------------------------

_T_MS = 1_760_727_848_000
_CLOCK = datetime.fromtimestamp(_T_MS / 1000, tz=timezone.utc).strftime("%H:%M:%SZ")
_PAGE_TAGS = {"div", "span", "table", "thead", "tbody", "tr", "th", "td", "a", "br"}
_PAGE_ATTRS = {"class", "href", "data-external"}
_HOSTILE = '<img src=x onerror="alert(1)">'
_SCRIPT = "<script>alert(2)</script>"


def _js_command() -> list[str]:
    for name, args in (("node", []), ("deno", ["run", "--quiet", "--no-prompt"]), ("bun", ["run"])):
        path = shutil.which(name)
        if path:
            return [path, *args]
    pytest.skip("no JavaScript runtime (node, deno or bun) on PATH")


def _js(tmp_path: Path, calls: list) -> list:
    """Run the page's render block; each call is [function name, *args]."""
    script = tmp_path / "research_render.mjs"
    script.write_text(
        _section(_INDEX.read_text(), "research-render")
        + "\nvar FNS = { rEsc: rEsc, rHref: rHref, renderResearch: renderResearch,"
        " researchAttemptError: researchAttemptError };\n"
        + "var CALLS = " + json.dumps(calls) + ";\n"
        + "console.log(JSON.stringify(CALLS.map(function(c) { return FNS[c[0]].apply(null, c.slice(1)); })));\n"
    )
    proc = subprocess.run(
        [*_js_command(), str(script)], capture_output=True, text=True, timeout=60, check=False
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout)


def _norm(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


class _Markup(HTMLParser):
    """Tags, attributes, hrefs and the visible text (entities decoded,
    whitespace collapsed) of a rendered panel."""

    def __init__(self, html: str) -> None:
        super().__init__(convert_charrefs=True)
        self.tags: set[str] = set()
        self.attrs: set[str] = set()
        self.hrefs: list[str] = []
        self._text: list[str] = []
        self.feed(html)
        self.close()
        self.text = _norm(" ".join(self._text))

    def handle_starttag(self, tag, attrs) -> None:
        self.tags.add(tag)
        for name, value in attrs:
            self.attrs.add(name)
            if name == "href":
                self.hrefs.append(value)

    def handle_data(self, data) -> None:
        self._text.append(data)


def _assert_inert(html: str) -> _Markup:
    m = _Markup(html)
    assert m.tags <= _PAGE_TAGS, m.tags - _PAGE_TAGS
    assert m.attrs <= _PAGE_ATTRS, m.attrs - _PAGE_ATTRS
    assert all(h.startswith(("http://", "https://")) for h in m.hrefs), m.hrefs
    return m


def _loaded(data: dict, http: int = 200) -> dict:
    attempt = {"at": _T_MS, "http": http, "data": data, "error": None}
    return {"lastAttempt": attempt, "lastGood": attempt}


def _failed(data, error: str, http: int | None = 503) -> dict:
    return {"lastAttempt": {"at": _T_MS, "http": http, "data": data, "error": error}}


def _poison(value, mark: str):
    """Every string in a payload, prefixed with markup."""
    if isinstance(value, str):
        return mark + value
    if isinstance(value, list):
        return [_poison(v, mark) for v in value]
    if isinstance(value, dict):
        return {k: _poison(v, mark) for k, v in value.items()}
    return value


def test_escape_helper_and_link_filter(tmp_path) -> None:
    out = _js(tmp_path, [
        ["rEsc", "<a href=\"x\">'&"],
        ["rEsc", None],
        ["rHref", "https://example.org/a?b=1&c=2"],
        ["rHref", "javascript:alert(1)"],
        ["rHref", " JAVASCRIPT:alert(1)"],
        ["rHref", "data:text/html,<script>alert(1)</script>"],
        ["rHref", "//evil.example/x"],
        ["rHref", "https://example.org/\" onmouseover=\"alert(1)"],
    ])
    assert out == [
        "&lt;a href=&quot;x&quot;&gt;&#39;&amp;", "",
        "https://example.org/a?b=1&c=2", None, None, None, None, None,
    ]


def test_headline_markup_renders_as_text_not_markup(tmp_path) -> None:
    top = copy.deepcopy(_TOP)
    story = top["stories"][0]
    story["headline"] = _HOSTILE
    story["outlets"] = ["<b>Evil</b>"]
    story["keywords"] = [_SCRIPT]
    story["items"] = [{"title": _HOSTILE, "outlet": _SCRIPT, "link": "javascript:alert(3)", "published": "x"}]
    top["attention"][0]["name"] = _SCRIPT
    sit = copy.deepcopy(_SITUATIONS)
    sit["situations"][0]["sample_headlines"][0].update(title=_HOSTILE, link="javascript:alert(4)")
    sit["situations"][0]["countries"][0]["name"] = _SCRIPT
    html_top, html_sit = _js(tmp_path, [
        ["renderResearch", "stories", _loaded(top)],
        ["renderResearch", "situations", _loaded(sit)],
    ])
    for html in (html_top, html_sit):
        m = _assert_inert(html)
        assert _HOSTILE in m.text and _SCRIPT in m.text
        assert "javascript:" not in html
        assert m.hrefs == []  # the javascript: links became plain text
    assert "&lt;img src=x onerror=&quot;alert(1)&quot;&gt;" in html_top
    # An http(s) link stays a link.
    assert _Markup(_js(tmp_path, [["renderResearch", "stories", _loaded(_TOP)]])[0]).hrefs == [
        "https://www.bbc.co.uk/news/2"
    ]


@pytest.mark.parametrize("panel", ["liquidity", "situations", "stories", "health"])
def test_every_payload_string_is_escaped(tmp_path, panel) -> None:
    payload = {
        "liquidity": {**_BOARD, "net_liquidity_method": _METHOD},
        "situations": _SITUATIONS,
        "stories": _TOP,
        "health": None,
    }[panel]
    if payload is None:
        db = tmp_path / "h.db"
        _populate(db)
        payload = _expected(db)
    mark = '"\'><i data-x="1">'
    html_ok, html_failed = _js(tmp_path, [
        ["renderResearch", panel, _loaded(_poison(payload, mark))],
        ["renderResearch", panel, _failed(_poison({**payload, "error": "down"}, mark), mark + "down")],
    ])
    for html in (html_ok, html_failed):
        m = _assert_inert(html)
        assert mark in m.text  # shown, as text


def test_panel_that_never_loaded_says_so(tmp_path) -> None:
    never, never_state, loading = _js(tmp_path, [
        ["renderResearch", "liquidity", None],
        ["renderResearch", "health", {}],
        ["renderResearch", "situations", {"inFlight": True, "requestedAt": _T_MS}],
    ])
    for html in (never, never_state):
        assert _Markup(html).text == "Not loaded yet: no request has been made for this panel, so nothing is shown."
    assert _Markup(loading).text == (
        f"Loading: first request sent {_CLOCK}. Nothing has loaded yet, so no figures are shown."
    )


def test_failed_call_never_reads_as_no_data(tmp_path) -> None:
    html, not_json, offline = _js(tmp_path, [
        ["renderResearch", "situations", _failed(_SITUATIONS_DEGRADED, "RSS feed read failed")],
        ["renderResearch", "stories", _failed(None, "response was not JSON", http=502)],
        ["renderResearch", "health", _failed(None, "request failed: TypeError: Failed to fetch", http=None)],
    ])
    m = _assert_inert(html)
    assert "UNAVAILABLE HTTP 503: RSS feed read failed" in m.text
    assert "Reason: rss_unavailable" in m.text
    assert _SITUATIONS_DEGRADED["data_gaps"][0] in m.text
    assert "No situation" not in m.text and "0 articles" not in m.text and "rs-story" not in html
    assert "UNAVAILABLE HTTP 502: response was not JSON" in _Markup(not_json).text
    assert "No story" not in not_json
    assert "UNAVAILABLE request failed: TypeError: Failed to fetch" in _Markup(offline).text
    assert "mini-box" not in offline  # no zero counts for a store that was never read


def test_failed_refresh_keeps_the_earlier_copy_and_says_how_old(tmp_path) -> None:
    st = {
        "lastAttempt": {"at": _T_MS + 60_000, "http": 504, "data": {"error": "situations timed out after 90 s", "timeout": True},
                        "error": "situations timed out after 90 s"},
        "lastGood": {"at": _T_MS, "http": 200, "data": _SITUATIONS, "error": None},
    }
    (html,) = _js(tmp_path, [["renderResearch", "situations", st]])
    m = _assert_inert(html)
    assert "REFRESH FAILED HTTP 504: situations timed out after 90 s" in m.text
    assert f"Showing the copy loaded {_CLOCK}, 60 s before that attempt." in m.text
    assert "Israel and Iran trade warnings" in m.text


def test_empty_result_states_its_threshold_and_coverage(tmp_path) -> None:
    sit, top = _js(tmp_path, [
        ["renderResearch", "situations", _loaded({**_SITUATIONS, "situations": [], "count": 0})],
        ["renderResearch", "stories", _loaded({**_TOP, "stories": [], "count": 0, "attention": []})],
    ])
    s = _Markup(sit).text
    assert "No situation reached 2 articles in this window, over the coverage stated above." in s
    assert "Read 2,632 items from 94 feeds" in s
    assert "Partial coverage: 2 feeds failed or timed out and are absent: CFR, HRW." in s
    t = _Markup(top).text
    assert "No story was carried by 3 or more distinct outlets in this window" in t
    assert "No named country or organization grew against the prior window" in t


def test_liquidity_keeps_units_and_keeps_amounts_apart_from_flows(tmp_path) -> None:
    board = copy.deepcopy(_BOARD)
    board["indicators"].append(
        _row("euro_area_m3_flow", "Euro area M3 (flow)", "EUR millions", 1234.5, None, None, None, measure="flow")
    )
    board["indicators"][0]["freshness"]["stale"] = True
    (html,) = _js(tmp_path, [["renderResearch", "liquidity", _loaded({**board, "net_liquidity_method": _METHOD})]])
    m = _assert_inert(html)
    text = m.text
    amounts, flows = text.index("Amounts outstanding (stocks)"), text.index("Flows (transactions within the period)")
    assert amounts < text.index("Fed total assets") < text.index("Overnight reverse repo") < flows
    assert flows < text.index("Euro area M3 (flow)")
    assert "it is not a flow" in text
    # Values as the payload states them: USD millions stay millions, no conversion.
    assert '<td class="rs-num">6,600,000</td><td>USD millions</td>' in html
    assert '<td class="rs-num">210.5</td><td>USD billions</td>' in html
    assert '<td class="rs-num">1,234.5</td><td>EUR millions</td>' in html
    assert "-12,345" in text and "-0.19%" in text
    assert "STALE" in text and "STALE CACHE, 67 min old" in text
    assert "1 indicator unavailable: shown as missing, never as zero." in text and "ECB HTTP 503" in text
    for blocked in liquidity.BLOCKED_SOURCES:
        assert _norm(blocked["measured"]) in text
    for series, comp in _METHOD["components"].items():
        assert series in text and comp["units"] in text and _norm(comp["conversion"]) in text
    assert _norm(_METHOD["alignment"]) in text and _norm(_METHOD["tga_choice"]) in text
    assert board["ranking_rule"] in text


def test_liquidity_total_outage_shows_reasons_and_no_board(tmp_path) -> None:
    (html,) = _js(tmp_path, [[
        "renderResearch", "liquidity",
        _failed({**_BOARD_OUTAGE, "net_liquidity_method": _METHOD}, _BOARD_OUTAGE["error"]),
    ]])
    m = _assert_inert(html)
    assert "UNAVAILABLE HTTP 503: liquidity board unavailable: every source failed" in m.text
    assert "FRED WALCL: HTTP 503" in m.text
    assert "a component failed; never computed with a zero term" in m.text
    assert "Amounts outstanding" not in m.text and "no indicator rows" not in m.text


def test_collection_health_lists_problems_and_cannot_prove(tmp_path, monkeypatch) -> None:
    for var in ("WORLD_INTEL_HISTORY_CADENCE_S", "WORLD_INTEL_HISTORY_CADENCES"):
        monkeypatch.delenv(var, raising=False)
    db = tmp_path / "h.db"
    _populate(db)
    report = _expected(db)
    fresh = tmp_path / "fresh.db"
    never = _expected(fresh)
    html, html_never, html_down = _js(tmp_path, [
        ["renderResearch", "health", _loaded(report)],
        ["renderResearch", "health", _loaded(never)],
        ["renderResearch", "health", _failed(
            {"error": "history store unavailable: DatabaseError: file is not a database", "db_path": "/x/cache.db"},
            "history store unavailable: DatabaseError: file is not a database",
        )],
    ])
    text = _assert_inert(html).text
    for line in report["cannot_prove"]:
        assert _norm(line) in text
    for state, meaning in report["states_explained"].items():
        assert _norm(f"{state}: {meaning}") in text
    assert text.index("failing (1)") < text.index("HTTP 503 from FIRMS") < text.index("ok (2)")
    assert "OVERDUE" in text and "not attempted recently (1)" in text
    assert "never run (1)" in text and "tides" in text
    assert "not in roster" in text  # legacy_feed: logged, but no longer collected
    assert "No collector capture is recorded" not in text
    never_text = _Markup(html_never).text
    assert "No collector capture is recorded in this database: every source reads never_run, which is not healthy." in never_text
    down = _Markup(html_down).text
    assert "UNAVAILABLE HTTP 503: history store unavailable: DatabaseError: file is not a database" in down
    assert "Database: /x/cache.db" in down
    assert "mini-box" not in html_down
