"""RSS outage markers and feed-content checks in sources/news.py.

Covers the cloud review of 2026-10-17 (findings M5, L4, L6): a near-total
RSS outage used to read as a normal result (fetch_news_feed marked only a
total outage, and fetch_trending_keywords passed on only error/degraded/
reason); feed links were returned whatever their scheme and titles had no
length cap. Feeds are served by respx in the live RSS item shape.

Gaps / not covered: the 50 % live-share threshold is pinned at its edges
(2 of 8 and 4 of 8 feeds), not tuned against a history of real outages;
feedparser's own handling of malformed links beyond the schemes below.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx
from click.testing import CliRunner

from world_intel_mcp import cli
from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import news
from world_intel_mcp.sources.news import _RSS_FEEDS, fetch_news_feed, fetch_trending_keywords

_RSS = """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Feed</title>
<item>
  <title>Sanctions talks resume in Geneva</title>
  <link>https://news.example/1</link>
  <pubDate>Sat, 17 Oct 2026 14:00:00 GMT</pubDate>
</item>
<item>
  <title>Sanctions relief on the table</title>
  <link>https://news.example/2</link>
  <pubDate>Sat, 17 Oct 2026 13:00:00 GMT</pubDate>
</item>
</channel></rss>"""

_SECURITY = _RSS_FEEDS["security"]
_ALL_FEEDS = [entry for feeds in _RSS_FEEDS.values() for entry in feeds]


@pytest.fixture
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _skip(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _skip)


def _serve(dead_urls: set[str], body: str = _RSS) -> None:
    for url in dead_urls:
        respx.get(url).mock(return_value=httpx.Response(503))
    respx.route().mock(return_value=httpx.Response(200, text=body))


@respx.mock
async def test_fetch_news_feed_minority_of_feeds_answering_is_degraded(
    fetcher: Fetcher, _no_sleep
) -> None:
    dead = [name for name, _ in _SECURITY[:6]]
    _serve({url for _, url in _SECURITY[:6]})

    result = await fetch_news_feed(fetcher, category="security")
    assert result["feeds_ok"] == 2
    assert result["feeds_attempted"] == len(_SECURITY)
    assert result["feeds_failed"] == sorted(dead)
    assert result["degraded"] is True
    assert result["reason"] == "partial_feeds_failed"
    # Not a total outage: the items that did arrive are kept, and no error.
    assert "error" not in result
    assert result["count"] == 4


@respx.mock
async def test_fetch_news_feed_half_of_the_feeds_answering_is_not_degraded(
    fetcher: Fetcher, _no_sleep
) -> None:
    _serve({url for _, url in _SECURITY[:4]})

    result = await fetch_news_feed(fetcher, category="security")
    assert result["feeds_ok"] == 4
    assert "degraded" not in result
    assert "reason" not in result


@respx.mock
async def test_fetch_news_feed_all_feeds_down_names_the_count_and_no_stale_copy(
    fetcher: Fetcher, _no_sleep
) -> None:
    # Review L6: the all-feeds-down test checked neither the error text nor
    # that no feed was counted as an expired copy.
    respx.route().mock(return_value=httpx.Response(500))

    result = await fetch_news_feed(fetcher, category="security")
    assert result["error"] == f"All {len(_SECURITY)} RSS feeds failed or timed out"
    assert result["feeds_stale"] == []
    assert result["reason"] == "all_feeds_failed"


@respx.mock
async def test_trending_passes_feed_counts_and_a_near_total_outage(
    fetcher: Fetcher, _no_sleep
) -> None:
    # Review M5: with 118 of 119 feeds down, trending returned a normal
    # table with no marker and no feed counts.
    live_name, live_url = _ALL_FEEDS[0]
    _serve({url for _, url in _ALL_FEEDS[1:]})

    result = await fetch_trending_keywords(fetcher, min_count=1)
    assert result["feeds_attempted"] == len(_ALL_FEEDS)
    assert result["feeds_ok"] == 1
    assert len(result["feeds_failed"]) == len(_ALL_FEEDS) - 1
    assert live_name not in result["feeds_failed"]
    assert result["feeds_stale"] == []
    assert result["degraded"] is True
    assert result["reason"] == "partial_feeds_failed"
    assert {k["word"] for k in result["keywords"]} >= {"sanctions"}


@respx.mock
async def test_fetch_news_feed_keeps_only_web_links_and_caps_titles(fetcher: Fetcher) -> None:
    # Review L4: links were returned whatever their scheme (javascript:,
    # data:) and titles had no length cap; feedparser 6.0.14 passes both
    # through unchanged (checked 2026-10-17).
    long_title = "Sanctions " + "x" * 2000
    body = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Feed</title>
<item><title>{long_title}</title><link>javascript:alert(1)</link>
  <pubDate>Sat, 17 Oct 2026 14:00:00 GMT</pubDate></item>
<item><title>Data link</title><link>data:text/html;base64,PHNjcmlwdD4=</link>
  <pubDate>Sat, 17 Oct 2026 13:00:00 GMT</pubDate></item>
<item><title>Upper-case scheme</title><link>HTTPS://news.example/ok</link>
  <pubDate>Sat, 17 Oct 2026 12:00:00 GMT</pubDate></item>
<item><title>Plain http</title><link>http://news.example/plain</link>
  <pubDate>Sat, 17 Oct 2026 11:00:00 GMT</pubDate></item>
</channel></rss>"""
    respx.route().mock(return_value=httpx.Response(200, text=body))

    result = await fetch_news_feed(fetcher, category="security")
    by_pub = {}
    for item in result["items"]:
        by_pub.setdefault(item["published"], item)
    assert by_pub["2026-10-17T14:00:00Z"]["link"] == ""
    assert by_pub["2026-10-17T14:00:00Z"]["title"] == long_title[:500] + "..."
    assert by_pub["2026-10-17T13:00:00Z"]["link"] == ""
    assert by_pub["2026-10-17T12:00:00Z"]["link"] == "HTTPS://news.example/ok"
    assert by_pub["2026-10-17T11:00:00Z"]["link"] == "http://news.example/plain"


def _cli(monkeypatch: pytest.MonkeyPatch, tmp_path, target: str, payload: dict, *args: str):
    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setattr(cli, "_fetcher", None)

    async def _fake(*a, **kw) -> dict:
        return payload

    monkeypatch.setattr(news, target, _fake)
    return CliRunner().invoke(cli.main, list(args), env={"COLUMNS": "200"})


_PARTIAL = {
    "feeds_attempted": 119,
    "feeds_ok": 1,
    "feeds_failed": ["BBC World"],
    "feeds_stale": [],
    "degraded": True,
    "reason": "partial_feeds_failed",
}


def test_cli_news_and_trending_say_when_most_feeds_failed(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    feed = dict(_PARTIAL, items=[{"title": "Sanctions talks resume", "feed_name": "DW News",
                                  "category": "europe", "published": "2026-10-17T14:00:00Z"}],
                count=1, categories_fetched=["europe"])
    result = _cli(monkeypatch, tmp_path, "fetch_news_feed", feed, "news")
    assert result.exit_code == 0
    assert "Partial outage: only 1 of 119 RSS feeds answered live" in result.output
    assert "Sanctions talks resume" in result.output

    trend = dict(_PARTIAL, keywords=[{"word": "sanctions", "count": 2}], total_items_analyzed=2)
    result = _cli(monkeypatch, tmp_path, "fetch_trending_keywords", trend, "trending")
    assert result.exit_code == 0
    assert "Partial outage: only 1 of 119 RSS feeds answered live" in result.output
    assert "sanctions" in result.output
