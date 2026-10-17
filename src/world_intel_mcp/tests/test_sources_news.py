"""Tests for sources/news.py — RSS aggregation, trending keywords, GDELT.

Complements test_sources.py, which already covers the GDELT artlist
degraded-vs-zero distinction (issue #17). This file covers the RSS feed
aggregation contract, keyword trending, the GDELT artlist/timeline success
paths, and the timeline degraded shape.

Gaps / not covered: live feed availability across the 119 configured
feeds; the asyncio.wait_for per-feed hard timeout (needs a slow fake
transport); the 25-way semaphore cap (behavioral, not observable from the
result shape). Stale detection reads the breaker's per-source stale-serve
count around each fetch, so a stale serve of the same feed by a separate,
non-shared read in the same process at the same moment would be
attributed to this one; no test drives that race.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import news
from world_intel_mcp.sources.news import (
    _RSS_FEEDS,
    _parse_published,
    _truncate,
    fetch_gdelt_search,
    fetch_news_feed,
    fetch_trending_keywords,
)

_LONG_SUMMARY = "S" * 250

_RSS = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Feed</title>
<item>
  <title>Newer headline</title>
  <link>https://news.example/1</link>
  <pubDate>Mon, 31 Aug 2026 14:00:00 GMT</pubDate>
  <description>{_LONG_SUMMARY}</description>
</item>
<item>
  <title>Older headline</title>
  <link>https://news.example/2</link>
  <pubDate>Sun, 30 Aug 2026 10:00:00 GMT</pubDate>
  <description>Short summary.</description>
</item>
</channel></rss>"""


@respx.mock
@pytest.mark.asyncio
async def test_fetch_news_feed_single_category(fetcher: Fetcher) -> None:
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    result = await fetch_news_feed(fetcher, category="security")

    assert result["source"] == "rss-aggregator"
    assert result["categories_fetched"] == ["security"]
    # Every security feed returns the same 2-item RSS document.
    assert result["count"] == 2 * len(_RSS_FEEDS["security"])

    item = result["items"][0]
    assert item["category"] == "security"
    assert item["feed_name"] in {name for name, _ in _RSS_FEEDS["security"]}
    assert "source_tier" in item
    # Sorted by published descending: every "Newer headline" copy precedes
    # every "Older headline" copy.
    titles = [i["title"] for i in result["items"]]
    assert titles.index("Older headline") == len(_RSS_FEEDS["security"])
    # 250-char summary truncated to 200 + ellipsis.
    newer = next(i for i in result["items"] if i["title"] == "Newer headline")
    assert newer["summary"] == "S" * 200 + "..."
    assert newer["link"] == "https://news.example/1"
    older = next(i for i in result["items"] if i["title"] == "Older headline")
    assert older["summary"] == "Short summary."


@respx.mock
@pytest.mark.asyncio
async def test_fetch_news_feed_limit(fetcher: Fetcher) -> None:
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    result = await fetch_news_feed(fetcher, category="security", limit=3)
    assert result["count"] == 3
    assert len(result["items"]) == 3


@pytest.mark.asyncio
async def test_fetch_news_feed_unknown_category(fetcher: Fetcher) -> None:
    result = await fetch_news_feed(fetcher, category="astrology")
    assert "Unknown category" in result["error"]
    assert result["items"] == []
    assert result["count"] == 0
    assert result["categories_fetched"] == []


@respx.mock
@pytest.mark.asyncio
async def test_fetch_news_feed_all_feeds_down(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ROADMAP Phase 24.5: a category-wide outage used to return count=0
    with no marker, shape-identical to a slow news day. It must now say
    every feed failed, and name them."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)

    respx.route().mock(return_value=httpx.Response(500))

    result = await fetch_news_feed(fetcher, category="security")
    assert result["items"] == []
    assert result["count"] == 0
    assert result["categories_fetched"] == ["security"]
    assert result["degraded"] is True
    assert result["reason"] == "all_feeds_failed"
    assert "error" in result
    n_feeds = len(_RSS_FEEDS["security"])
    assert result["feeds_attempted"] == n_feeds
    assert result["feeds_ok"] == 0
    assert sorted(result["feeds_failed"]) == sorted(
        name for name, _ in _RSS_FEEDS["security"]
    )


@respx.mock
@pytest.mark.asyncio
async def test_fetch_news_feed_partial_outage_is_counted_not_fatal(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One dead feed among live ones: items still flow, the dead feed is
    named, and the response is not an outage."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    dead_name, dead_url = _RSS_FEEDS["security"][0]
    respx.get(dead_url).mock(return_value=httpx.Response(503))
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    result = await fetch_news_feed(fetcher, category="security")
    n_feeds = len(_RSS_FEEDS["security"])
    assert result["count"] == 2 * (n_feeds - 1)
    assert result["feeds_failed"] == [dead_name]
    assert result["feeds_ok"] == n_feeds - 1
    assert result["feeds_attempted"] == n_feeds
    assert "degraded" not in result
    assert "error" not in result


@respx.mock
@pytest.mark.asyncio
async def test_fetch_news_feed_reachable_but_empty_feeds_are_not_an_outage(
    fetcher: Fetcher,
) -> None:
    """Every feed answered with a valid, empty channel: a genuinely quiet
    result, so no outage marker."""
    empty = '<?xml version="1.0"?><rss version="2.0"><channel><title>F</title></channel></rss>'
    respx.route().mock(return_value=httpx.Response(200, text=empty))

    result = await fetch_news_feed(fetcher, category="security")
    assert result["count"] == 0
    assert result["feeds_ok"] == len(_RSS_FEEDS["security"])
    assert result["feeds_failed"] == []
    assert "degraded" not in result


def _rss_cache_key(feed_name: str) -> str:
    return f"news:rss:{feed_name.lower().replace(' ', '_')}"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_news_feed_stale_fallback_is_not_a_live_feed(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A feed whose live fetch fails but whose expired copy is still cached
    comes back as that copy (the fetcher's stale fallback), and text
    payloads carry no inline marker. It must be named stale, not counted
    as a live feed."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    stale_name, stale_url = _RSS_FEEDS["security"][0]
    fetcher.cache.set(_rss_cache_key(stale_name), _RSS, ttl_seconds=-1)
    respx.get(stale_url).mock(return_value=httpx.Response(503))
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    result = await fetch_news_feed(fetcher, category="security")
    n_feeds = len(_RSS_FEEDS["security"])
    assert result["feeds_stale"] == [stale_name]
    assert result["feeds_ok"] == n_feeds - 1
    assert result["feeds_failed"] == []
    # The old copy's items are kept: they carry their own publish times.
    assert result["count"] == 2 * n_feeds
    assert "degraded" not in result


@respx.mock
@pytest.mark.asyncio
async def test_fetch_news_feed_outage_behind_a_warm_cache_is_still_an_outage(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """In a running deployment the cache is warm, so when every feed fails
    most come back as expired copies. That is still an outage."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)
    names = [name for name, _ in _RSS_FEEDS["security"]]
    for name in names:
        fetcher.cache.set(_rss_cache_key(name), _RSS, ttl_seconds=-1)
    respx.route().mock(return_value=httpx.Response(503))

    result = await fetch_news_feed(fetcher, category="security")
    assert result["degraded"] is True
    assert result["reason"] == "all_feeds_failed"
    assert result["feeds_ok"] == 0
    assert result["feeds_stale"] == sorted(names)
    assert "expired cache copy" in result["error"]


def _gated_get_xml(calls: list[str], gate: asyncio_mod.Event):
    async def _get_xml(url, source, cache_key=None, cache_ttl=300, timeout=None):
        calls.append(url)
        await gate.wait()
        return _RSS

    return _get_xml


async def _let_tasks_start(n: int = 5) -> None:
    for _ in range(n):
        await asyncio_mod.sleep(0)


@pytest.mark.asyncio
async def test_fetch_news_feed_concurrent_reads_share_one_fetch_per_feed(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The collector starts news_feed, trending_keywords, situations and
    top_stories in one gather; each used to request every feed itself.
    Concurrent reads of the same feed set now share one request per feed,
    while each caller keeps its own limit and its own item dicts."""
    calls: list[str] = []
    gate = asyncio_mod.Event()
    monkeypatch.setattr(fetcher, "get_xml", _gated_get_xml(calls, gate))

    first = asyncio_mod.ensure_future(fetch_news_feed(fetcher, category="security", limit=3))
    second = asyncio_mod.ensure_future(fetch_news_feed(fetcher, category="security", limit=500))
    await _let_tasks_start()
    gate.set()
    a, b = await asyncio_mod.gather(first, second)

    n_feeds = len(_RSS_FEEDS["security"])
    assert len(calls) == n_feeds
    assert a["count"] == 3
    assert b["count"] == 2 * n_feeds
    assert a["feeds_ok"] == b["feeds_ok"] == n_feeds
    a["items"][0]["title"] = "mutated by the first caller"
    assert all(i["title"] != "mutated by the first caller" for i in b["items"])
    assert news._INFLIGHT_READS == {}


@pytest.mark.asyncio
async def test_fetch_news_feed_cancelled_reader_leaves_the_shared_read_running(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A caller cancelled mid-read (the collector's per-source timeout)
    must not cancel the read other callers are waiting on."""
    calls: list[str] = []
    gate = asyncio_mod.Event()
    monkeypatch.setattr(fetcher, "get_xml", _gated_get_xml(calls, gate))

    first = asyncio_mod.ensure_future(fetch_news_feed(fetcher, category="security"))
    await _let_tasks_start()
    second = asyncio_mod.ensure_future(fetch_news_feed(fetcher, category="security"))
    await _let_tasks_start()
    first.cancel()
    gate.set()
    b = await second
    assert b["feeds_ok"] == len(_RSS_FEEDS["security"])
    assert "degraded" not in b
    with pytest.raises(asyncio_mod.CancelledError):
        await first


@pytest.mark.asyncio
async def test_fetch_trending_keywords(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    items = [
        {"title": "Ukraine's grid attacked"},
        {"title": "Drone raid hits Ukraine depot"},
        {"title": "Ukraine counteroffensive stalls; drone losses mount"},
        {"title": ""},  # empty title tolerated
    ]

    async def _fake_feed(f, category=None, limit=50):
        return {"items": items}

    monkeypatch.setattr(news, "fetch_news_feed", _fake_feed)

    result = await fetch_trending_keywords(fetcher, min_count=3)

    assert result["source"] == "keyword-analysis"
    assert result["total_items_analyzed"] == 4
    # "ukraine" appears 3x (possessive punctuation stripped); "drone" only
    # 2x so it must fall below min_count=3.
    assert result["keywords"] == [{"word": "ukraine", "count": 3}]
    assert "degraded" not in result


@pytest.mark.asyncio
async def test_fetch_trending_keywords_rss_outage_is_not_no_trends(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _down(f, category=None, limit=50):
        return {
            "items": [],
            "count": 0,
            "feeds_attempted": 119,
            "feeds_ok": 0,
            "feeds_failed": ["BBC World"],
            "error": "All 119 RSS feeds failed or timed out",
            "degraded": True,
            "reason": "all_feeds_failed",
        }

    monkeypatch.setattr(news, "fetch_news_feed", _down)
    result = await fetch_trending_keywords(fetcher)
    assert result["keywords"] == []
    assert result["degraded"] is True
    assert result["reason"] == "all_feeds_failed"
    assert result["error"] == "All 119 RSS feeds failed or timed out"


@pytest.mark.asyncio
async def test_fetch_trending_keywords_min_count_filter(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_feed(f, category=None, limit=50):
        return {"items": [{"title": "Taiwan strait tension"}] * 2}

    monkeypatch.setattr(news, "fetch_news_feed", _fake_feed)

    result = await fetch_trending_keywords(fetcher, min_count=2)
    words = {k["word"]: k["count"] for k in result["keywords"]}
    # Stopwords and <3-char tokens never appear.
    assert words == {"taiwan": 2, "strait": 2, "tension": 2}


@respx.mock
@pytest.mark.asyncio
async def test_fetch_gdelt_search_artlist_success(fetcher: Fetcher) -> None:
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(
        return_value=httpx.Response(
            200,
            json={
                "articles": [
                    {
                        "title": "Border clash reported",
                        "url": "https://example.com/a1",
                        "seendate": "20260831T120000Z",
                        "socialimage": "",
                        "domain": "example.com",
                        "language": "English",
                        "sourcecountry": "France",
                    }
                ]
            },
        )
    )

    result = await fetch_gdelt_search(fetcher, query="border clash", limit=10)

    assert result["source"] == "gdelt"
    assert result["mode"] == "artlist"
    assert result["query"] == "border clash"
    assert result["count"] == 1
    article = result["articles"][0]
    assert article["title"] == "Border clash reported"
    assert article["url"] == "https://example.com/a1"
    assert article["domain"] == "example.com"
    assert article["sourcecountry"] == "France"
    assert result.get("error") is None
    assert result.get("degraded") is None


@respx.mock
@pytest.mark.asyncio
async def test_fetch_gdelt_search_timeline_mode(fetcher: Fetcher) -> None:
    timeline = [
        {"series": "Volume Intensity", "data": [{"date": "20260830", "value": 1.2}]}
    ]
    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(
        return_value=httpx.Response(200, json={"timeline": timeline})
    )

    result = await fetch_gdelt_search(fetcher, query="coup", mode="timelinevol")
    assert result["mode"] == "timelinevol"
    assert result["timeline"] == timeline
    assert result["count"] == 1


@respx.mock
@pytest.mark.asyncio
async def test_fetch_gdelt_search_timeline_degraded_shape(
    fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Issue-#17 contract in timeline mode: a failed fetch keeps timeline
    as its normal empty-list shape while flagging degradation."""

    async def _no_sleep(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _no_sleep)

    respx.get(url__regex=r".*api\.gdeltproject\.org.*").mock(
        return_value=httpx.Response(429, text="rate limited")
    )

    result = await fetch_gdelt_search(fetcher, query="coup", mode="timelinevol")
    assert result["degraded"] is True
    assert result["reason"] == "gdelt_fetch_failed"
    assert result["timeline"] == []
    assert result["articles"] is None
    assert result["count"] == 0


def test_truncate() -> None:
    assert _truncate(None) == ""
    assert _truncate("") == ""
    assert _truncate("short") == "short"
    assert _truncate("x" * 200) == "x" * 200  # exactly at the limit
    assert _truncate("x" * 201) == "x" * 200 + "..."


def test_parse_published_fallbacks() -> None:
    import time as time_mod

    parsed = _parse_published({"published_parsed": time_mod.gmtime(1700000000)})
    assert parsed is not None and parsed.endswith("Z")

    updated = _parse_published({"updated_parsed": time_mod.gmtime(1700000000)})
    assert updated is not None and updated.endswith("Z")

    assert _parse_published({"published": "raw"}) == "raw"
    assert _parse_published({"updated": "raw2"}) == "raw2"
    assert _parse_published({}) is None


def test_parse_published_is_utc_on_a_non_utc_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """feedparser's *_parsed structs are UTC. time.mktime read them as
    local time, so on mac-studio (EDT) every published stamp came out
    four hours in the future (measured live 2026-10-17: a feed's newest
    item stamped 20:18Z at 16:20Z)."""
    import time as time_mod

    monkeypatch.setenv("TZ", "America/New_York")
    time_mod.tzset()
    try:
        utc_struct = time_mod.gmtime(1700000000)  # 2023-11-14 22:13:20 UTC
        assert _parse_published({"published_parsed": utc_struct}) == "2023-11-14T22:13:20Z"
        assert _parse_published({"updated_parsed": utc_struct}) == "2023-11-14T22:13:20Z"
    finally:
        monkeypatch.delenv("TZ")
        time_mod.tzset()


@pytest.mark.asyncio
async def test_gdelt_search_uses_extended_timeout(fetcher, monkeypatch) -> None:
    """Measured live 2026-09-02 from mac-studio: api.gdeltproject.org
    TCP-connects in 0.03 s but its TLS handshake took 19.7 s, 17.4 s,
    then >45 s across three samples, while Nominatim and USGS handshake
    in 0.05 s. The Fetcher's 15 s default counts TLS inside connect, so
    every live GDELT fetch was a ConnectTimeout hidden behind stale
    cache. GDELT must be given its own budget."""
    seen: dict = {}

    async def _record(url, source, **kwargs):
        seen.update(kwargs)
        return {"articles": []}

    monkeypatch.setattr(fetcher, "get_json", _record)
    await news.fetch_gdelt_search(fetcher, query="test", mode="artlist", limit=1)

    assert seen.get("timeout") is not None, "no timeout passed to get_json"
    assert seen["timeout"] >= 30.0
