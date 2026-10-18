"""Tests for the country dossier's sanctions section (analysis/dossier.py).

The section is driven through the real ``sources.sanctions`` search, with
the OFAC SDN CSV served by respx. Until 2026-10-17 it searched entity names
for the ISO-3 code (so Sudan's search was the letters "SDN"), read
"results" where the producer emits "matches" (so the list was always
empty), and a failed SDN download read as zero matches.

Gaps / not covered: the rows are fixture values in the SDN CSV layout, not
the live list, so how often real remarks name a country in the form the
search uses is not measured here.
"""

import httpx
import pytest
import respx

from world_intel_mcp.analysis import dossier
from world_intel_mcp.sources import elections, intelligence, markets, news
from world_intel_mcp.sources.sanctions import _OFAC_CSV_URL


def _row(n: int, name: str, remarks: str) -> str:
    # ent_num, SDN_Name, SDN_Type, Program, Title, Call_Sign, Vess_type,
    # Tonnage, GRT, Vess_flag, Vess_owner, Remarks
    return f'{n},"{name}","individual","SDGT",-0-,-0-,-0-,-0-,-0-,-0-,-0-,"{remarks}"'


@pytest.fixture
def _other_sections(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every section but sanctions, answered locally."""

    async def _brief(*args, **kwargs) -> dict:
        return {"supporting_data": {}, "brief": ""}

    async def _stocks(*args, **kwargs) -> dict:
        return {"error": "not under test"}

    async def _elections(*args, **kwargs) -> dict:
        return {"elections": []}

    async def _news(*args, **kwargs) -> dict:
        return {"items": []}

    monkeypatch.setattr(intelligence, "fetch_country_brief", _brief)
    monkeypatch.setattr(markets, "fetch_country_stocks", _stocks)
    monkeypatch.setattr(elections, "fetch_election_calendar", _elections)
    monkeypatch.setattr(news, "fetch_news_feed", _news)


@respx.mock
async def test_dossier_sanctions_screens_the_country_not_the_code(fetcher, _other_sections) -> None:
    rows = [
        _row(1, "AHMED, Ali", "nationality Sudan; DOB 1970"),
        _row(2, "SDN TRADING CO", "Address Tehran, Iran"),
        _row(3, "OMAR, Musa", "citizen Sudan"),
        _row(4, "NILE HOLDINGS", "Organization Established Date 2001; Khartoum, Sudan"),
    ]
    respx.get(_OFAC_CSV_URL).mock(return_value=httpx.Response(200, text="\n".join(rows)))

    section = (await dossier.fetch_country_dossier(fetcher, country="SD"))["sanctions"]

    assert [m["name"] for m in section["matches"]] == ["AHMED, Ali", "OMAR, Musa", "NILE HOLDINGS"]
    assert section["match_count"] == 3
    assert section["more_matches"] is False
    assert section["total_entities"] == 4
    assert section["country_searched"] == "Sudan"
    assert "Not an exposure screen" in section["match_method"]
    assert "error" not in section


@respx.mock
async def test_dossier_sanctions_outage_is_a_gap_not_zero_matches(fetcher, _other_sections) -> None:
    respx.get(_OFAC_CSV_URL).mock(return_value=httpx.Response(503))

    section = (await dossier.fetch_country_dossier(fetcher, country="SDN"))["sanctions"]

    assert section["match_count"] is None
    assert section["total_entities"] == 0
    assert (section["degraded"], section["reason"]) == (True, "sdn_unavailable")
    assert section["error"] == "The OFAC SDN list could not be read"
    assert section["data_gaps"] == [
        "OFAC SDN list could not be read; sanctions not screened, not zero"
    ]


@respx.mock
async def test_dossier_sanctions_says_when_the_list_was_cut_off(fetcher, _other_sections) -> None:
    rows = [_row(n, f"PERSON {n}", "nationality Iran") for n in range(12)]
    respx.get(_OFAC_CSV_URL).mock(return_value=httpx.Response(200, text="\n".join(rows)))

    section = (await dossier.fetch_country_dossier(fetcher, country="IR"))["sanctions"]

    assert section["match_count"] == 10
    assert len(section["matches"]) == 10
    assert section["more_matches"] is True
    assert section["country_searched"] == "Iran"


@respx.mock
async def test_dossier_sanctions_uses_english_names_beyond_tier_1(fetcher, _other_sections) -> None:
    rows = [_row(1, "BERLIN GMBH", "Address Berlin, Germany"), _row(2, "X", "nationality France")]
    respx.get(_OFAC_CSV_URL).mock(return_value=httpx.Response(200, text="\n".join(rows)))

    section = (await dossier.fetch_country_dossier(fetcher, country="DE"))["sanctions"]

    assert section["country_searched"] == "Germany"
    assert [m["name"] for m in section["matches"]] == ["BERLIN GMBH"]


def test_cli_dossier_says_sanctions_were_not_screened(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from click.testing import CliRunner

    from world_intel_mcp import cli

    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli-cache.db"))
    monkeypatch.setattr(cli, "_fetcher", None)

    async def _dossier(*args, **kwargs) -> dict:
        return {
            "overview": {"country": "Sudan", "iso2": "SD", "iso3": "SDN"},
            "sanctions": {"matches": [], "match_count": None, "total_entities": 0},
        }

    monkeypatch.setattr(dossier, "fetch_country_dossier", _dossier)
    result = CliRunner().invoke(cli.main, ["dossier", "-c", "SD"], env={"COLUMNS": "200"})
    assert result.exit_code == 0, result.output
    assert "Sanctions: not screened (SDN list unavailable)" in result.output
