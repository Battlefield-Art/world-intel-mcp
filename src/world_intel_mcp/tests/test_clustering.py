"""Tests for analysis/clustering.py — Jaccard news clustering.

Similarity values in comments are hand-computed from the token sets after
stopword removal (e.g. "new" is a stopword and must not count).
"""

import pytest

from world_intel_mcp.analysis.clustering import (
    cluster_articles,
    fetch_news_clusters,
    jaccard_similarity,
)
from world_intel_mcp.sources import news


def test_jaccard_identical_sets() -> None:
    assert jaccard_similarity({"a", "b", "c"}, {"a", "b", "c"}) == 1.0


def test_jaccard_disjoint_sets() -> None:
    assert jaccard_similarity({"a", "b"}, {"c", "d"}) == 0.0


def test_jaccard_partial_overlap() -> None:
    # {a,b} vs {b,c}: intersection 1, union 3.
    assert jaccard_similarity({"a", "b"}, {"b", "c"}) == pytest.approx(1 / 3)


def test_jaccard_empty_set_is_zero() -> None:
    assert jaccard_similarity(set(), {"a"}) == 0.0
    assert jaccard_similarity({"a"}, set()) == 0.0


def test_similar_titles_cluster_dissimilar_stay_apart() -> None:
    """The load-bearing pair: near-duplicates merge, the unrelated story
    does not get pulled in."""
    articles = [
        {"title": "Russia launches missile strikes on Kyiv energy grid"},
        # Token overlap 6/7 with the first title ("new" is a stopword).
        {"title": "Russia launches new missile strikes on Kyiv grid"},
        {"title": "Chocolate festival delights visitors in Belgium"},
    ]
    clusters = cluster_articles(articles, similarity_threshold=0.3)
    assert len(clusters) == 2
    # Sorted by member count: the pair first.
    assert clusters[0]["member_count"] == 2
    assert clusters[0]["member_indices"] == [0, 1]
    assert clusters[0]["representative"] is articles[0]
    assert clusters[1]["member_count"] == 1
    assert clusters[1]["representative"] is articles[2]


def test_threshold_boundary_is_inclusive() -> None:
    # {border,camps,flood} vs {border,camps,burn}: J = 2/4 = 0.5 exactly.
    articles = [
        {"title": "border camps flood"},
        {"title": "border camps burn"},
    ]
    merged = cluster_articles(articles, similarity_threshold=0.5)
    assert merged[0]["member_count"] == 2  # sim >= threshold merges
    split = cluster_articles(articles, similarity_threshold=0.51)
    assert all(c["member_count"] == 1 for c in split)


def test_empty_input_returns_empty_list() -> None:
    assert cluster_articles([]) == []


def test_missing_or_none_titles_become_singletons() -> None:
    articles = [{"title": None}, {}, {"title": "Real headline about earthquakes"}]
    clusters = cluster_articles(articles)
    assert len(clusters) == 3
    assert all(c["member_count"] == 1 for c in clusters)


def test_custom_title_field() -> None:
    articles = [
        {"headline": "Drought grips southern Europe farms"},
        {"headline": "Drought grips southern Europe vineyards"},
    ]
    clusters = cluster_articles(articles, title_field="headline")
    assert clusters[0]["member_count"] == 2


async def test_fetch_news_clusters_reports_clusters_and_singletons(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    items = [
        {
            "title": "Russia launches missile strikes on Kyiv energy grid",
            "source_name": "BBC",
            "link": "https://example.com/1",
        },
        {
            "title": "Russia missile strikes hit Kyiv grid overnight",
            "source": "Reuters",
            "link": "https://example.com/2",
        },
        {
            "title": "Missile strikes on Kyiv grid continue as Russia presses",
            "source_name": "BBC",
            "link": "https://example.com/3",
        },
        {"title": "Chocolate festival delights visitors in Brussels"},
        {"title": "Quantum computing startup raises funding round"},
    ]

    async def _fake_feed(fetcher, category=None, limit=100):
        return {"items": items}

    monkeypatch.setattr(news, "fetch_news_feed", _fake_feed)
    result = await fetch_news_clusters(fetcher)

    # Only multi-member clusters are surfaced; the two unrelated stories
    # are counted as singletons, not clusters.
    assert result["cluster_count"] == 1
    assert result["singleton_count"] == 2
    assert result["total_items"] == 5
    assert result["threshold"] == 0.25
    assert result["source"] == "jaccard-clustering"

    cluster = result["clusters"][0]
    assert cluster["size"] == 3
    assert cluster["headline"] == items[0]["title"]
    assert "kyiv" in cluster["keywords"]
    assert "missile" in cluster["keywords"]
    assert sorted(cluster["sources"]) == ["BBC", "Reuters"]
    assert len(cluster["items"]) == 3
    assert cluster["items"][0] == {
        "title": items[0]["title"],
        "source": "BBC",
        "link": "https://example.com/1",
    }


# Real producer shape: items as sources/news.fetch_news_feed emits them,
# captured live 2026-10-17 (one story carried by seven outlets, plus two
# items from the same publisher's sister feeds). fetch_news_feed names the
# outlet "feed_name"; the clusterer used to read only source_name/source,
# so every live cluster reported sources == [].
_LIVE_POPE_ITEMS = [
    {"title": "Pope Leo leaves hospital after lung cancer surgery", "link": "https://www.aljazeera.com/video/newsfeed/2026/10/17/pope-leo-leaves-hospital-after-lung-cancer-surgery?traffic_source=rss", "published": "2026-10-17T16:47:52Z", "feed_name": "Al Jazeera", "category": "geopolitics", "source_tier": "major"},
    {"title": "Smiling pope returns to Vatican after cancer surgery", "link": "https://www.dawn.com/news/2037823/smiling-pope-returns-to-vatican-after-cancer-surgery", "published": "2026-10-17T16:20:04Z", "feed_name": "Dawn Pakistan", "category": "south_asia", "source_tier": "major"},
    {"title": "Pope Leo XIV leaves hospital days after lung cancer surgery", "link": "https://www.npr.org/2026/10/17/nx-s1-6006762/pope-leo-xiv-leaves-hospital-days-after-lung-cancer-surgery", "published": "2026-10-17T16:10:14Z", "feed_name": "NPR World", "category": "geopolitics", "source_tier": "major"},
    {"title": "Pope discharged from hospital days after lung cancer surgery", "link": "https://www.bbc.co.uk/news/articles/c5x2vkddrkrko?at_medium=RSS&at_campaign=rss", "published": "2026-10-17T14:51:36Z", "feed_name": "BBC World", "category": "geopolitics", "source_tier": "major"},
    {"title": "Pope Leo discharged from hospital after cancer operation", "link": "https://www.theguardian.com/world/2026/oct/17/pope-leo-discharged-from-hospital-after-cancer-operation", "published": "2026-10-17T14:41:49Z", "feed_name": "The Guardian World", "category": "geopolitics", "source_tier": "major"},
    {"title": "Vatican says Pope Leo to be released from the hospital after lung cancer surgery", "link": "https://www.timesofisrael.com/vatican-says-pope-leo-to-be-released-from-the-hospital-after-lung-cancer-surgery/", "published": "2026-10-17T12:18:32Z", "feed_name": "Times of Israel", "category": "middle_east", "source_tier": "unknown"},
    {"title": "Pope Leo leaves hospital 4 days after lung cancer surgery", "link": "https://www.scmp.com/news/world/europe/article/3371256/pope-leo-leave-hospital-saturday-after-lung-cancer-surgery?utm_source=rss_feed", "published": "2026-10-17T11:26:27Z", "feed_name": "SCMP", "category": "asia_pacific", "source_tier": "unknown"},
    # Sister feeds of one publisher: two feeds, one outlet.
    {"title": "Pope Leo leaves hospital after lung cancer surgery, Vatican says", "link": "https://www.bbc.com/mundo/a1", "published": "2026-10-17T11:00:00Z", "feed_name": "BBC Mundo", "category": "multilingual", "source_tier": "major"},
    {"title": "Unrelated: chess olympiad opens in Budapest", "link": "https://example.org/x", "published": "2026-10-17T10:00:00Z", "feed_name": "SCMP", "category": "asia_pacific", "source_tier": "unknown"},
]


async def test_fetch_news_clusters_reads_the_real_producer_outlet_key(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_feed(fetcher, category=None, limit=100):
        return {"items": list(_LIVE_POPE_ITEMS)}

    monkeypatch.setattr(news, "fetch_news_feed", _fake_feed)
    result = await fetch_news_clusters(fetcher)

    assert result["cluster_count"] == 1
    cluster = result["clusters"][0]
    assert cluster["size"] == 8
    assert sorted(cluster["sources"]) == sorted(
        i["feed_name"] for i in _LIVE_POPE_ITEMS[:8]
    )
    assert cluster["items"][0]["source"] == "Al Jazeera"
    # BBC World and BBC Mundo are one outlet (one publisher, two feeds).
    assert cluster["outlet_count"] == 7
    assert cluster["outlets"] == sorted(
        ["Al Jazeera", "BBC", "Dawn Pakistan", "NPR World", "The Guardian World",
         "Times of Israel", "SCMP"]
    )


async def test_fetch_news_clusters_marks_an_rss_outage(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An all-feeds-down feed must not come back as a quiet, cluster-free
    news day."""

    async def _down(fetcher, category=None, limit=100):
        return {
            "items": [],
            "count": 0,
            "feeds_attempted": 3,
            "feeds_ok": 0,
            "feeds_failed": ["A", "B", "C"],
            "error": "All 3 RSS feeds failed or timed out",
            "degraded": True,
            "reason": "all_feeds_failed",
        }

    monkeypatch.setattr(news, "fetch_news_feed", _down)
    result = await fetch_news_clusters(fetcher)
    assert result["clusters"] == []
    assert result["degraded"] is True
    assert result["reason"] == "all_feeds_failed"
    assert result["feeds_failed"] == ["A", "B", "C"]


async def test_fetch_news_clusters_healthy_feed_has_no_outage_marker(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_feed(fetcher, category=None, limit=100):
        return {"items": list(_LIVE_POPE_ITEMS), "feeds_failed": ["X"], "feeds_ok": 9}

    monkeypatch.setattr(news, "fetch_news_feed", _fake_feed)
    result = await fetch_news_clusters(fetcher)
    assert "degraded" not in result
    assert result["feeds_failed"] == ["X"]


async def test_fetch_news_clusters_names_feeds_served_stale(
    fetcher, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _fake_feed(fetcher, category=None, limit=100):
        return {"items": list(_LIVE_POPE_ITEMS), "feeds_failed": [], "feeds_ok": 8,
                "feeds_stale": ["SCMP"]}

    monkeypatch.setattr(news, "fetch_news_feed", _fake_feed)
    result = await fetch_news_clusters(fetcher)
    assert result["feeds_stale"] == ["SCMP"]
    assert result["clusters"][0]["outlet_count"] == 7
