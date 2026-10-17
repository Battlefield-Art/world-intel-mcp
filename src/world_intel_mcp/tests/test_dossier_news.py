"""Tests for the country dossier's news section (analysis/dossier.py).

The section is driven through the real ``sources.news.fetch_news_feed``
(RSS XML served by respx), so the items it reads carry the producer's real
keys. Headlines were captured live 2026-10-17; links and publish times are
fixture values.

Until 2026-10-17 the section was always empty with no marker: it asked for
category="all" (not a category, so fetch_news_feed answered with an error
dict), read "articles" (the producer emits "items") and "source" (items
carry "feed_name"), and checked only its own "_error", never the
producer's "error".

Gaps / not covered: headline matching is a substring test on the tier-1
keyword lists ("american" also matches "Latin American", "iran" matches
"Tirana"); only the 200 newest items are searched; real feed depth.
"""

import asyncio as asyncio_mod

import httpx
import pytest
import respx

from world_intel_mcp.analysis import dossier
from world_intel_mcp.sources import elections, intelligence, markets, news, sanctions
from world_intel_mcp.sources.news import _RSS_FEEDS

_FEED_NAMES = {name for feeds in _RSS_FEEDS.values() for name, _ in feeds}
_N_FEEDS = len(_FEED_NAMES)
_RUSSIA = "Trump’s pause on Russia sanctions is ‘reward’ for Putin, Zelenskyy says"
_POPE = "Pope Leo leaves hospital after lung cancer surgery"
_RSS = f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Feed</title>
<item>
  <title>{_POPE}</title>
  <link>https://news.example/pope</link>
  <pubDate>Sat, 17 Oct 2026 16:47:52 GMT</pubDate>
</item>
<item>
  <title>{_RUSSIA}</title>
  <link>https://news.example/russia-sanctions</link>
  <pubDate>Sat, 17 Oct 2026 14:43:52 GMT</pubDate>
</item>
</channel></rss>"""


@pytest.fixture
def _other_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    """The four non-news sources, answered locally: only news is under test."""

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


@pytest.fixture
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _skip(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _skip)


@respx.mock
async def test_dossier_news_reads_the_real_feed_shape(fetcher, _other_sections) -> None:
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    r = await dossier.fetch_country_dossier(fetcher, country="RU")
    section = r["news"]
    # Every feed serves both items; the Russia headline matches, the pope
    # one does not, and the dossier keeps 15.
    assert section["mention_count"] == 15
    assert {m["title"] for m in section["mentions"]} == {_RUSSIA}
    assert all(m["source"] in _FEED_NAMES for m in section["mentions"])
    assert section["mentions"][0]["published"] == "2026-10-17T14:43:52Z"
    assert section["mentions"][0]["link"] == "https://news.example/russia-sanctions"
    assert section["data_gaps"] == []
    assert "error" not in section


@respx.mock
async def test_dossier_news_outage_is_a_gap_not_zero_mentions(
    fetcher, _other_sections, _no_sleep
) -> None:
    respx.route().mock(return_value=httpx.Response(503))

    r = await dossier.fetch_country_dossier(fetcher, country="RU")
    section = r["news"]
    assert section["mention_count"] is None
    assert section["mentions"] == []
    assert section["degraded"] is True
    assert section["reason"] == "all_feeds_failed"
    assert section["error"] == f"All {_N_FEEDS} RSS feeds failed or timed out"
    assert section["data_gaps"] == [
        f"news feed: All {_N_FEEDS} RSS feeds failed or timed out; mentions not measured"
    ]


@respx.mock
async def test_dossier_news_names_failed_feeds(fetcher, _other_sections, _no_sleep) -> None:
    dead_name, dead_url = _RSS_FEEDS["geopolitics"][0]
    respx.get(dead_url).mock(return_value=httpx.Response(503))
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    r = await dossier.fetch_country_dossier(fetcher, country="RUS")
    section = r["news"]
    assert section["mention_count"] == 15
    assert dead_name not in {m["source"] for m in section["mentions"]}
    assert section["data_gaps"] == [
        f"1 of {_N_FEEDS} RSS feeds failed or timed out; their items are missing: {dead_name}"
    ]


async def test_dossier_news_exception_is_a_gap(
    fetcher, _other_sections, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def _boom(*args, **kwargs) -> dict:
        raise RuntimeError("feed parser crashed")

    monkeypatch.setattr(news, "fetch_news_feed", _boom)

    r = await dossier.fetch_country_dossier(fetcher, country="RU")
    section = r["news"]
    assert section["mention_count"] is None
    assert section["error"] == "feed parser crashed"
    assert section["data_gaps"] == ["news feed failed: feed parser crashed; mentions not measured"]


@respx.mock
async def test_dossier_news_without_keywords_is_not_searched(fetcher, _other_sections) -> None:
    # GB is accepted but has no tier-1 keyword list; searching headlines
    # for "gbr" used to return a quiet zero.
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    r = await dossier.fetch_country_dossier(fetcher, country="GB")
    section = r["news"]
    assert section["mention_count"] is None
    assert section["mentions"] == []
    assert section["data_gaps"] == [
        "no headline keywords for GBR (only the 22 tier-1 countries have them); "
        "mentions not searched"
    ]


def test_cli_dossier_prints_a_news_outage_as_not_measured(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from click.testing import CliRunner

    from world_intel_mcp import cli

    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setattr(cli, "_fetcher", None)
    gap = f"news feed: All {_N_FEEDS} RSS feeds failed or timed out; mentions not measured"

    async def _dossier(*args, **kwargs) -> dict:
        return {
            "overview": {"country": "Russia", "iso2": "RU", "iso3": "RUS"},
            "news": {"mentions": [], "mention_count": None, "degraded": True, "data_gaps": [gap]},
        }

    monkeypatch.setattr(dossier, "fetch_country_dossier", _dossier)
    result = CliRunner().invoke(cli.main, ["dossier", "-c", "RU"], env={"COLUMNS": "200"})
    assert result.exit_code == 0
    assert "News: not measured" in result.output
    assert f"Gap: {gap}" in result.output
    assert "recent mentions" not in result.output
