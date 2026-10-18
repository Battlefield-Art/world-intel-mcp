"""News signals: the second review's findings outside the GDELT floor.

Second review of 2026-10-17. Each test names its finding: demonyms the
whole-word dossier matcher stopped counting (NS-1), the CLI partial-outage
line that never printed for situations and top stories (NS-7 / F11), a
searched span sorted as text (NS-8), empty and one-letter countries sent
to GDELT (NS-9), and the boundaries no test pinned (F6, F7, F8), plus an
event-loop test that does not count ticks (F12).

Gaps / not covered: real feeds and real GDELT (no network); the demonym
rule is checked on the forms below, not on every headline form.
"""

import asyncio
import threading

import httpx
import pytest
import respx
from click.testing import CliRunner

from world_intel_mcp import cli
from world_intel_mcp.analysis import clustering, dossier
from world_intel_mcp.analysis import news_signals as ns
from world_intel_mcp.sources import elections, intelligence, markets, news, sanctions
from world_intel_mcp.sources.news import _RSS_FEEDS, fetch_news_feed
from world_intel_mcp.tests.test_news_signals import (
    _DERIVED,
    _NOW,
    _NOW_GDELT,
    _SITUATION_ITEMS,
    _column,
    _feed,
    _gdelt_router,
    _patch_feed,
    _top_items,
    _volraw_dropping,
)

_GDELT_URL = r".*api\.gdeltproject\.org.*"


@pytest.fixture
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _skip(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", _skip)


@pytest.fixture
def _other_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    """The dossier's four non-news sources, answered locally."""

    async def _brief(*args, **kwargs) -> dict:
        return {"supporting_data": {}, "brief": ""}

    async def _stocks(*args, **kwargs) -> dict:
        return {"error": "not under test"}

    async def _elections(*args, **kwargs) -> dict:
        return {"elections": []}

    async def _sanctions(*args, **kwargs) -> dict:
        return {"results": [], "count": 0}

    monkeypatch.setattr(intelligence, "fetch_country_brief", _brief)
    monkeypatch.setattr(markets, "fetch_country_stocks", _stocks)
    monkeypatch.setattr(elections, "fetch_election_calendar", _elections)
    monkeypatch.setattr(sanctions, "fetch_sanctions_search", _sanctions)


def _news_item(title: str, published) -> dict:
    return {
        "title": title,
        "link": "https://news.example/x",
        "published": published,
        "summary": "",
        "feed_name": "BBC World",
        "category": "geopolitics",
    }


# ---------------------------------------------------------------------------
# Dossier
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "iso3,title,expected",
    [
        # NS-1: counted by the substring matcher before a5ced4a, lost by the
        # whole-word one.
        ("PRK", "North Korean troops cross into Russia", True),
        ("PRK", "North Koreans fighting for Russia captured", True),
        ("PSE", "Gazans flee south as strikes resume", True),
        ("PSE", "Gazan hospital runs out of fuel", True),
        ("AFG", "Afghans queue at the border", True),
        # The new endings must not reopen the traps the matcher closed.
        ("IRN", "Tirana mayor resigns", False),
        ("IND", "Indiana governor signs bill", False),
        ("IND", "Storm builds over the Indian Ocean", False),
        ("PRK", "South Korea holds drills", False),
    ],
)
def test_dossier_matcher_counts_demonym_endings(iso3: str, title: str, expected: bool) -> None:
    assert dossier._names_country(title, iso3) is expected


async def test_dossier_mentions_not_capped_when_every_match_is_listed(
    fetcher, _other_sections, monkeypatch: pytest.MonkeyPatch
) -> None:
    # F8 (M20): with `>=`, three matches all listed read as capped.
    items = [_news_item(f"Iran talks round {i}", f"2026-10-17T1{i}:00:00Z") for i in range(3)]
    _patch_feed(monkeypatch, _feed(items))
    section = (await dossier.fetch_country_dossier(fetcher, country="IR"))["news"]
    assert section["matching_items"] == 3
    assert section["mention_count"] == 3
    assert section["mention_count_capped"] is False


async def test_dossier_searched_span_uses_parsed_times_only(
    fetcher, _other_sections, monkeypatch: pytest.MonkeyPatch
) -> None:
    # NS-8: the span sorted raw strings, so a date the feed could not parse
    # ("Okt 17, 2026 (Ortszeit)") was reported as the newest item.
    items = [
        _news_item("Iran talks resume", "2026-10-16T09:00:00Z"),
        _news_item("Iran envoy arrives", "Okt 17, 2026 (Ortszeit)"),
        _news_item("Iran deal signed", "2026-10-17T10:00:00Z"),
        _news_item("Pope returns", None),
    ]
    _patch_feed(monkeypatch, _feed(items))
    section = (await dossier.fetch_country_dossier(fetcher, country="IR"))["news"]
    assert section["searched_span"] == {
        "oldest": "2026-10-16T09:00:00Z",
        "newest": "2026-10-17T10:00:00Z",
    }
    assert section["items_without_parsed_time"] == 2
    assert section["items_searched"] == 4


# ---------------------------------------------------------------------------
# RSS outage threshold
# ---------------------------------------------------------------------------


_SECURITY = _RSS_FEEDS["security"]
_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Feed</title>
<item>
  <title>Sanctions talks resume in Geneva</title>
  <link>https://news.example/1</link>
  <pubDate>Sat, 17 Oct 2026 14:00:00 GMT</pubDate>
</item>
</channel></rss>"""


@respx.mock
async def test_fetch_news_feed_three_of_eight_feeds_live_is_degraded(fetcher, _no_sleep) -> None:
    # F7 (M19): only 4 of 8 (not degraded) was pinned, which holds for any
    # threshold up to one half; 3 of 8 must be marked.
    assert len(_SECURITY) == 8
    for _, url in _SECURITY[:5]:
        respx.get(url).mock(return_value=httpx.Response(503))
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))
    result = await fetch_news_feed(fetcher, category="security")
    assert (result["feeds_ok"], result["feeds_attempted"]) == (3, 8)
    assert result["degraded"] is True
    assert result["reason"] == "partial_feeds_failed"
    assert "error" not in result


# ---------------------------------------------------------------------------
# Hostility timeline
# ---------------------------------------------------------------------------


@respx.mock
async def test_hostility_timeline_hours_missing_is_the_union_of_partial_columns(
    fetcher, _no_sleep, monkeypatch: pytest.MonkeyPatch
) -> None:
    # F6 (M15): two columns partial on disjoint hours; the last column's
    # hours alone would tell a reader 00-05 were complete.
    monkeypatch.setattr(ns, "_GDELT_RETRY_WAIT_S", 0.0)
    route, _ = _gdelt_router()
    drops = {
        "threat": lambda d: d.startswith("20261016") and int(d[9:11]) < 6,
        "force": lambda d: d.startswith("20261016") and int(d[9:11]) >= 12,
    }

    def _answer(request: httpx.Request) -> httpx.Response:
        col = _column(request)
        if col in drops:
            return httpx.Response(200, json=_volraw_dropping(
                request.url.params["query"], _DERIVED[col], drops[col],
            ))
        return route(request)

    respx.get(url__regex=_GDELT_URL).mock(side_effect=_answer)
    r = await ns.fetch_hostility_timeline(fetcher, "Iran", "Israel", days=3, now=_NOW_GDELT)
    d16 = next(d for d in r["days"] if d["date"] == "2026-10-16")
    assert d16["partial_columns"] == ["threat", "force"]
    assert d16["hours_missing"] == [f"{h:02d}" for h in (*range(0, 6), *range(12, 24))]


@respx.mock
async def test_hostility_timeline_refuses_empty_and_one_letter_countries(
    fetcher, _no_sleep
) -> None:
    # NS-9: '' or 'X' went out as nine GDELT requests and could open the
    # breaker every GDELT caller shares.
    route, calls = _gdelt_router()
    respx.get(url__regex=_GDELT_URL).mock(side_effect=route)
    for country, other in (("", None), ("   ", None), ("X", None), ("Iran", "  ")):
        r = await ns.fetch_hostility_timeline(fetcher, country, other, now=_NOW_GDELT)
        assert "too short to search" in r["error"], r
        assert r["days"] == []
    assert calls == []


# ---------------------------------------------------------------------------
# CLI partial-outage line
# ---------------------------------------------------------------------------


@pytest.fixture
def _cli_env(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setenv("COLUMNS", "200")
    monkeypatch.setattr(cli, "_fetcher", None)


@pytest.mark.parametrize("command,items", [("situations", _SITUATION_ITEMS), ("top-stories", None)])
def test_cli_situations_and_top_stories_print_the_partial_outage_line(
    _cli_env, monkeypatch: pytest.MonkeyPatch, command: str, items
) -> None:
    # NS-7 / F11: the line read a top-level feeds_attempted these two
    # results did not carry, so only `intel news` and `intel trending`
    # ever printed it.
    marker = {"degraded": True, "reason": "partial_feeds_failed", "feeds_ok": 3,
              "feeds_attempted": 119, "feeds_failed": ["CFR"]}
    _patch_feed(monkeypatch, _feed(items if items is not None else _top_items(), **marker))
    result = CliRunner().invoke(cli.main, [command], catch_exceptions=False)
    assert result.exit_code == 0
    assert "Partial outage: only 3 of 119 RSS feeds answered live" in result.output


async def test_situations_and_top_stories_carry_the_feed_counts(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    marker = {"degraded": True, "reason": "partial_feeds_failed", "feeds_ok": 1,
              "feeds_attempted": 3}
    _patch_feed(monkeypatch, _feed(_SITUATION_ITEMS, **marker))
    r = await ns.fetch_situations(fetcher, min_articles=1, now=_NOW)
    assert (r["feeds_ok"], r["feeds_attempted"]) == (1, 3)
    _patch_feed(monkeypatch, _feed(_top_items(), **marker))
    t = await ns.fetch_top_stories(fetcher, now=_NOW)
    assert (t["feeds_ok"], t["feeds_attempted"]) == (1, 3)


# ---------------------------------------------------------------------------
# Clustering off the event loop, without counting ticks
# ---------------------------------------------------------------------------


def _gated_clusterer(monkeypatch: pytest.MonkeyPatch, module, gate: threading.Event,
                     seen: list[bool]) -> None:
    real = module.cluster_articles

    def _waits_for_the_loop(*args, **kwargs):
        # Released by a coroutine on the event loop: if clustering ran on
        # the loop, that coroutine could not run and the wait times out.
        seen.append(gate.wait(timeout=5.0))
        return real(*args, **kwargs)

    monkeypatch.setattr(module, "cluster_articles", _waits_for_the_loop)


async def _with_loop_release(gate: threading.Event, coro):
    async def _release() -> None:
        await asyncio.sleep(0.05)
        gate.set()

    releaser = asyncio.create_task(_release())
    try:
        return await coro
    finally:
        await releaser


async def test_clustering_lets_the_event_loop_run_meanwhile(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    # F12: the earlier test counted 10 ms ticks and failed under machine
    # load (8 ticks counted); this one asks only whether the loop ran.
    _patch_feed(monkeypatch, _feed(_top_items()))
    gate, seen = threading.Event(), []
    _gated_clusterer(monkeypatch, ns, gate, seen)
    r = await _with_loop_release(gate, ns.fetch_top_stories(fetcher, now=_NOW))
    assert r["count"] == 1
    assert seen == [True]

    gate, seen = threading.Event(), []
    _gated_clusterer(monkeypatch, clustering, gate, seen)
    c = await _with_loop_release(gate, clustering.fetch_news_clusters(fetcher))
    assert c["cluster_count"] >= 1
    assert seen == [True]
