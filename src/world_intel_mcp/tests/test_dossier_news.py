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

Gaps / not covered: headline matching is a whole-word test on the tier-1
keyword lists with a short list of compound names excluded (the trap
headlines below); compounds outside that list ("Lagos, Portugal", the
Republic of the Congo) still count; only the 200 newest items are
searched; real feed depth.
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


# ---------------------------------------------------------------------------
# Review fixes (cloud review of 3c0ebff, 2026-10-17)
# ---------------------------------------------------------------------------

_TIRANA = "Tirana mayor resigns after corruption probe"
_IRAN = "Iran's foreign minister visits Beijing"


def _rss_doc(items: list[tuple[str, str, str]]) -> str:
    body = "".join(
        f"<item><title>{title}</title><link>{link}</link><pubDate>{pub}</pubDate></item>"
        for title, link, pub in items
    )
    return (
        '<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>'
        f"<title>Feed</title>{body}</channel></rss>"
    )


@pytest.mark.parametrize(
    "iso3,title,expected",
    [
        # The review's false mentions, then true ones beside them.
        ("IRN", "Tirana mayor resigns", False),
        ("IRN", _IRAN, True),
        ("IND", "Indiana governor signs bill", False),
        ("IND", "Storm builds over the Indian Ocean", False),
        ("IND", "Indian navy deploys to the Gulf", True),
        ("IND", "Indians vote in state polls", True),
        ("SDN", "South Sudan holds vote", False),
        ("SDN", "South Sudanese refugees cross the border", False),
        ("SDN", "Sudan army retakes Khartoum", True),
        ("USA", "Latin American leaders meet", False),
        ("USA", "Americans head to the polls", True),
        ("USA", "American and Latin American envoys meet", True),
        ("MEX", "New Mexico wildfire spreads", False),
        ("MEX", "Mexican peso slides", True),
    ],
)
def test_dossier_headline_country_match_is_whole_word(iso3: str, title: str, expected: bool) -> None:
    assert dossier._names_country(title, iso3) is expected


@respx.mock
async def test_dossier_news_matches_whole_words_and_reports_its_reach(
    fetcher, _other_sections
) -> None:
    # Review M4: the match was a substring test, so "Tirana" counted as
    # Iran; the response also hid the 15-mention cap and how far back the
    # 200 searched items reached.
    respx.route().mock(return_value=httpx.Response(200, text=_rss_doc([
        (_TIRANA, "https://news.example/tirana", "Sat, 17 Oct 2026 17:00:00 GMT"),
        (_IRAN, "https://news.example/iran", "Sat, 17 Oct 2026 16:00:00 GMT"),
        (_POPE, "https://news.example/pope", "Sat, 17 Oct 2026 15:00:00 GMT"),
    ])))
    r = await dossier.fetch_country_dossier(fetcher, country="IR")
    section = r["news"]
    assert {m["title"] for m in section["mentions"]} == {_IRAN}
    assert section["mention_count"] == 15
    # The 200 newest items: every Tirana copy, then the newest Iran copies.
    assert section["items_searched"] == 200
    assert section["matching_items"] == 200 - _N_FEEDS
    assert section["mention_count_capped"] is True
    assert section["searched_span"] == {
        "oldest": "2026-10-17T16:00:00Z",
        "newest": "2026-10-17T17:00:00Z",
    }
    assert "whole-word" in section["match_method"]
    assert section["data_gaps"] == []


@respx.mock
async def test_dossier_news_names_feeds_served_from_an_expired_copy(
    fetcher, _other_sections, _no_sleep
) -> None:
    # Review mutant M27: nothing pinned the dossier's stale-feed gap.
    stale_name, stale_url = _RSS_FEEDS["geopolitics"][0]
    fetcher.cache.set(
        f"news:rss:{stale_name.lower().replace(' ', '_')}", _RSS, ttl_seconds=-1
    )
    respx.get(stale_url).mock(return_value=httpx.Response(503))
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    r = await dossier.fetch_country_dossier(fetcher, country="RU")
    section = r["news"]
    assert section["mention_count"] == 15
    assert section["data_gaps"] == [
        f"1 RSS feeds were served from an expired cache copy: {stale_name}"
    ]


@respx.mock
async def test_dossier_news_minority_of_feeds_is_degraded_but_measured(
    fetcher, _other_sections, _no_sleep
) -> None:
    # Review M5: only the geopolitics feeds answer; mentions are still
    # counted over them, and the section says the count rests on a
    # minority of the feeds.
    live = {url for _, url in _RSS_FEEDS["geopolitics"]}
    for feeds in _RSS_FEEDS.values():
        for _, url in feeds:
            if url not in live:
                respx.get(url).mock(return_value=httpx.Response(503))
    respx.route().mock(return_value=httpx.Response(200, text=_RSS))

    r = await dossier.fetch_country_dossier(fetcher, country="RU")
    section = r["news"]
    n_live = len(_RSS_FEEDS["geopolitics"])
    assert section["mention_count"] == n_live
    assert section["degraded"] is True
    assert section["reason"] == "partial_feeds_failed"
    assert "error" not in section
    assert any(
        g.startswith(f"only {n_live} of {_N_FEEDS} RSS feeds answered live")
        for g in section["data_gaps"]
    )


def test_cli_dossier_says_when_mentions_are_capped(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from click.testing import CliRunner

    from world_intel_mcp import cli

    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setattr(cli, "_fetcher", None)

    async def _dossier(*args, **kwargs) -> dict:
        return {
            "overview": {"country": "Iran", "iso2": "IR", "iso3": "IRN"},
            "news": {"mentions": [{"title": _IRAN}], "mention_count": 15,
                     "mention_count_capped": True, "matching_items": 81,
                     "items_searched": 200, "data_gaps": []},
        }

    monkeypatch.setattr(dossier, "fetch_country_dossier", _dossier)
    result = CliRunner().invoke(cli.main, ["dossier", "-c", "IR"], env={"COLUMNS": "200"})
    assert result.exit_code == 0
    assert "News: 81 matching headlines in the 200 newest items (15 listed)" in result.output
