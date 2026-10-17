"""Tests for the research guide: MCP resources, prompts and ``intel guide``.

The registry is imported under test_server_registry's temp cache path
(the shared override every registry-importing test file uses), so
nothing here opens the developer's real cache database. The live stdio
test spawns the installed console script with its own temp cache.
"""

import importlib
import json
import os
import re
import sys
import types
from pathlib import Path

import anyio
import pytest
from click.testing import CliRunner
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server.lowlevel import NotificationOptions
from mcp.shared.exceptions import McpError
from mcp.types import Tool

from world_intel_mcp import guide, guide_prompts
from world_intel_mcp.cache import Cache
from world_intel_mcp.circuit_breaker import CircuitBreaker

from .test_server_registry import _TMP_CACHE

_SOURCES_DIR = Path(guide.__file__).resolve().parent / "sources"
_TOOL_LINE = re.compile(r"^- `(intel_[a-z0-9_]+)`", re.M)


def _import_under_temp_cache(name: str):
    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        return importlib.import_module(name)
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


@pytest.fixture(scope="module")
def tools_pkg():
    return _import_under_temp_cache("world_intel_mcp.tools")


@pytest.fixture(scope="module")
def server():
    return _import_under_temp_cache("world_intel_mcp.server")


@pytest.fixture(scope="module")
def registered(tools_pkg) -> set[str]:
    return {t.name for t in tools_pkg.ALL_TOOLS}


def _tools_section(markdown: str) -> str:
    return markdown.split("## Tools by domain", 1)[1]


def _sample_args(prompt: guide_prompts.ResearchPrompt) -> dict[str, str]:
    return {a.name: (a.choices[0] if a.choices else "Sudan") for a in prompt.arguments if a.required}


def _prompt_problems(prompt: guide_prompts.ResearchPrompt, registered: set[str]) -> list[str]:
    """Every way a prompt can send a model to a tool that is not there."""
    problems = [f"lists unregistered {t}" for t in prompt.tools if t not in registered]
    text = guide_prompts.render(prompt, _sample_args(prompt))
    if "{" in text or "}" in text:
        problems.append("text keeps an unfilled placeholder")
    for name in set(guide_prompts.TOOL_NAME.findall(text)):
        if name not in prompt.tools:
            problems.append(f"text names {name}, which is not in its tool list")
        if name not in registered:
            problems.append(f"text names unregistered {name}")
    return problems


# ---------------------------------------------------------------------------
# Guide resource: generated from the live registry
# ---------------------------------------------------------------------------


def test_guide_lists_every_registered_tool_exactly_once(tools_pkg, registered) -> None:
    text = guide.render_guide()
    listed = _TOOL_LINE.findall(_tools_section(text))
    assert len(listed) == len(set(listed)), "a tool is listed twice"
    assert set(listed) == registered, {
        "missing_from_guide": sorted(registered - set(listed)),
        "not_registered": sorted(set(listed) - registered),
    }
    n_tools, n_domains = len(tools_pkg.ALL_TOOLS), len(tools_pkg._MODULES)
    assert f"server: {n_tools}\ntools in {n_domains} domains" in text


def test_guide_groups_tools_under_their_own_module(tools_pkg) -> None:
    section = _tools_section(guide.render_guide())
    blocks = re.split(r"^### ", section, flags=re.M)[1:]
    assert len(blocks) == len(tools_pkg._MODULES)
    for module, block in zip(tools_pkg._MODULES, blocks):
        header = block.split("\n", 1)[0]
        names = _TOOL_LINE.findall(block)
        assert names == [t.name for t in module.TOOLS], module.__name__
        assert header.endswith(f"({len(module.TOOLS)})"), header


def test_guide_follows_the_registry_it_is_given() -> None:
    """A tool added to a module shows up and a removed one disappears:
    the list is computed, never hand-kept."""
    fake = types.SimpleNamespace(
        __name__="world_intel_mcp.tools.newdomain",
        __doc__="New domain tools: one probe.\n\nDetail paragraph.",
        TOOLS=[
            Tool(
                name="intel_probe",
                description="Probe something — carefully. Second sentence.",
                inputSchema={
                    "type": "object",
                    "properties": {"where": {}, "when": {}},
                    "required": ["where"],
                },
            )
        ],
    )
    text = guide.render_guide([fake], prompts=())
    assert "### Newdomain (1)" in text
    assert "New domain tools: one probe." in text
    assert "Detail paragraph" not in text
    assert "- `intel_probe` [where (required), when]: Probe something - carefully." in text
    assert "Second sentence" not in text
    assert "1\ntools in 1 domains" in text
    assert _TOOL_LINE.findall(_tools_section(guide.render_guide([], prompts=()))) == []


def test_guide_states_how_to_ask_what_answers_hold_and_limits() -> None:
    text = guide.render_guide()
    for heading in (
        "## How to ask a strong question",
        "## What an answer from this server contains",
        "## What this server will not do",
        "## Worked research prompts",
    ):
        assert heading in text
    for phrase in ("Name the period", "Name the comparison", "Name the measure", "Name the place"):
        assert phrase in text
    for phrase in ("Trade, place orders", "Forecast prices", "Prove causation", "Show history it did not record"):
        assert phrase in text
    for resource in guide.RESOURCES:
        if str(resource.uri) != guide.GUIDE_URI:
            assert str(resource.uri) in text
    for prompt in guide_prompts.PROMPTS:
        assert f"- `{prompt.name}`" in text


def test_published_guide_text_has_no_em_dashes() -> None:
    assert "—" not in guide.render_guide()
    assert "—" not in guide.MEASURES
    for prompt in guide_prompts.PROMPTS:
        assert "—" not in guide_prompts.render(prompt, _sample_args(prompt)), prompt.name


def test_measures_glossary_covers_the_named_measures() -> None:
    for heading in (
        "## Amount outstanding versus flow",
        "## Growth versus acceleration",
        "## Percent versus percentage points",
        "## Seasonally adjusted",
        "## Observation, publication and collection dates",
        "## Correlation and timing offsets",
        "## Missing data is not zero",
    ):
        assert heading in guide.MEASURES
    assert "None of this shows that one thing\ncaused the other" in guide.MEASURES


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------


def test_prompt_set_size_names_and_domain_coverage() -> None:
    prompts = guide_prompts.PROMPTS
    assert len(prompts) >= 20
    names = [p.name for p in prompts]
    assert len(names) == len(set(names))
    assert all(re.fullmatch(r"[a-z0-9]+(-[a-z0-9]+)*", n) for n in names), names
    domains = {p.domain for p in prompts}
    assert {"conflict", "markets", "hazards", "migration", "aviation", "cyber", "aoi"} <= domains
    for p in prompts:
        assert p.steps and p.limits, p.name


def test_every_prompt_names_only_registered_tools(registered) -> None:
    problems = {p.name: _prompt_problems(p, registered) for p in guide_prompts.PROMPTS}
    assert {k: v for k, v in problems.items() if v} == {}


def test_prompt_check_catches_a_nonexistent_tool(registered) -> None:
    """The check above has to be able to fail."""
    bad = guide_prompts.ResearchPrompt(
        name="bad",
        title="Bad",
        domain="test",
        question="Use intel_status and also intel_no_such_tool in {Country}.",
        steps=(("intel_status", "health"), ("intel_gone_tool", "was renamed")),
        limits=("none",),
    )
    problems = _prompt_problems(bad, registered)
    assert "lists unregistered intel_gone_tool" in problems
    assert "text names unregistered intel_no_such_tool" in problems
    assert "text names intel_no_such_tool, which is not in its tool list" in problems
    assert "text keeps an unfilled placeholder" in problems


def test_prompt_text_names_tools_limits_and_arguments() -> None:
    prompt = guide_prompts.get("conflict-trend")
    text = guide_prompts.render(prompt, {"country": "Sudan", "days": "14"})
    assert "How has organized violence in Sudan changed over the last 14 days" in text
    assert "1. intel_ucdp_events:" in text
    assert "2. intel_acled_events:" in text
    assert "Limits to respect:" in text
    assert "Missing data is never zero" in text
    assert "{" not in text and "}" not in text


def test_prompt_defaults_fill_optional_arguments() -> None:
    prompt = guide_prompts.get("conflict-trend")
    text = guide_prompts.render(prompt, {"country": "Sudan", "days": "  "})
    assert "over the last 30 days" in text


def test_prompt_argument_errors() -> None:
    prompt = guide_prompts.get("displacement-history")
    with pytest.raises(guide_prompts.PromptArgumentError, match="needs argument 'country'"):
        guide_prompts.render(prompt, {})
    with pytest.raises(guide_prompts.PromptArgumentError, match="does not take colour"):
        guide_prompts.render(prompt, {"country": "Syria", "colour": "red"})
    with pytest.raises(guide_prompts.PromptArgumentError, match="must be one of origin, host"):
        guide_prompts.render(prompt, {"country": "Syria", "role": "both"})
    long_value = "x" * 1000
    text = guide_prompts.render(prompt, {"country": long_value})
    assert "x" * 200 in text and "x" * 201 not in text


def test_prompt_without_arguments_rejects_any() -> None:
    with pytest.raises(guide_prompts.PromptArgumentError, match="accepted: none"):
        guide_prompts.render(guide_prompts.get("hotspot-escalation"), {"country": "x"})


def test_readme_documents_every_prompt_and_resource() -> None:
    readme = (Path(__file__).resolve().parents[3] / "README.md").read_text()
    section = readme.split("## Research Guide: MCP Prompts and Resources", 1)[1].split("\n## ", 1)[0]
    documented = set(re.findall(r"`([a-z0-9]+(?:-[a-z0-9]+)+)`", section))
    names = {p.name for p in guide_prompts.PROMPTS}
    assert names <= documented, sorted(names - documented)
    assert f"**{len(guide_prompts.PROMPTS)} prompts**" in section
    for resource in guide.RESOURCES:
        assert f"`{resource.uri}`" in section


def test_list_prompts_mirrors_prompt_data() -> None:
    listed = guide.list_prompts()
    assert [p.name for p in listed] == [p.name for p in guide_prompts.PROMPTS]
    by_name = {p.name: p for p in listed}
    aoi = by_name["aoi-watch-setup"]
    assert {a.name: a.required for a in aoi.arguments} == {
        "name": True,
        "lat": True,
        "lon": True,
        "radius_km": True,
    }
    assert "intel_aoi_define" in aoi.description


def test_get_prompt_returns_one_user_message() -> None:
    result = guide.get_prompt("fx-move", {"symbol": "JPY"})
    assert len(result.messages) == 1
    msg = result.messages[0]
    assert msg.role == "user"
    assert "How has JPY moved against USD over the last 30 days" in msg.content.text


def test_get_prompt_errors_are_mcp_errors() -> None:
    with pytest.raises(McpError, match="Unknown prompt 'nope'"):
        guide.get_prompt("nope", None)
    with pytest.raises(McpError, match="needs argument 'ticker'"):
        guide.get_prompt("company-events", {})


# ---------------------------------------------------------------------------
# Source health resource
# ---------------------------------------------------------------------------


def test_source_health_reports_breakers_freshness_and_keys(tmp_path) -> None:
    cache = Cache(db_path=tmp_path / "health.db")
    cache.set("markets:quotes:^GSPC", {"v": 1}, ttl_seconds=300)
    cache.set("seismology:usgs:24", {"v": 2}, ttl_seconds=-10)
    breaker = CircuitBreaker(failure_threshold=2, cooldown_seconds=300)
    breaker.record_failure("gdelt")
    breaker.record_failure("gdelt")
    breaker.record_failure("ucdp")
    breaker.record_stale_serve("ucdp")
    env = {"FRED_API_KEY": "secret-fred-value", "OPENSKY_CLIENT_ID": "id-value"}

    try:
        health = guide.source_health(breaker, cache, environ=env, now=0)
    finally:
        cache.close()

    assert health["generated_at"] == "1970-01-01T00:00:00+00:00"
    assert health["open_breakers"] == ["gdelt"]
    assert health["stale_serves"] == {"ucdp": 1}
    assert health["circuit_breakers"]["ucdp"]["failures"] == 1
    assert health["stale_cache_prefixes"] == ["seismology"]
    assert set(health["cache_freshness"]) == {"markets", "seismology"}
    assert health["cache"]["total_entries"] == 2
    keyed = {k["source"]: k for k in health["keyed_sources"]}
    assert keyed["FRED"]["configured"] == ["FRED_API_KEY"]
    assert keyed["FRED"]["missing"] == []
    assert keyed["OpenSky"]["configured"] == ["OPENSKY_CLIENT_ID"]
    assert "OPENSKY_CLIENT_SECRET" in keyed["OpenSky"]["missing"]
    assert keyed["ACLED"]["configured"] == []
    dumped = json.dumps(health)
    assert "secret-fred-value" not in dumped and "id-value" not in dumped
    assert any("not that every source is healthy" in s for s in health["scope"])


def test_keyed_sources_match_what_the_source_modules_read() -> None:
    """A source that starts reading a new credential, or stops reading
    one, must change the health resource in the same commit."""
    pattern = re.compile(
        r"os\.(?:environ\.get|getenv)\(\s*[\"']([A-Z0-9_]*(?:KEY|TOKEN|SECRET|PASSWORD|EMAIL|USERNAME|CLIENT_ID))[\"']"
    )
    read = set()
    for path in _SOURCES_DIR.glob("*.py"):
        read |= set(pattern.findall(path.read_text()))
    listed = {name for ks in guide.KEYED_SOURCES for name in ks.env}
    assert read, "scan found no credential reads at all; the pattern is broken"
    assert listed == read, {
        "read_but_not_listed": sorted(read - listed),
        "listed_but_not_read": sorted(listed - read),
    }


def test_reading_health_resource_never_fetches(server, monkeypatch) -> None:
    import httpx

    def _no_network(*args, **kwargs):
        raise AssertionError("reading source health made an HTTP request")

    monkeypatch.setattr(httpx.AsyncClient, "send", _no_network)
    monkeypatch.setattr(httpx.Client, "send", _no_network)
    # A copy of the shared breaker's state, so the seeded failure does
    # not outlive this test.
    monkeypatch.setattr(server.breaker, "_states", dict(server.breaker._states))
    server.breaker.record_failure("guide-test-source")
    (content,) = guide.read_resource(guide.HEALTH_URI)
    assert content.mime_type == "application/json"
    health = json.loads(content.content)
    assert "guide-test-source" in health["circuit_breakers"]
    assert Path(health["cache"]["db_path"]) == _TMP_CACHE


def test_unknown_resource_is_an_mcp_error() -> None:
    with pytest.raises(McpError, match="Unknown resource intel://nope"):
        guide.read_resource("intel://nope")


# ---------------------------------------------------------------------------
# Server wiring (in process)
# ---------------------------------------------------------------------------


def test_server_advertises_prompts_and_resources(server) -> None:
    caps = server.server.get_capabilities(NotificationOptions(), {})
    assert caps.prompts is not None
    assert caps.resources is not None
    assert caps.tools is not None


@pytest.mark.asyncio
async def test_server_handlers_delegate_to_guide(server) -> None:
    resources = await server.list_resources()
    assert [str(r.uri) for r in resources] == [guide.GUIDE_URI, guide.HEALTH_URI, guide.MEASURES_URI]
    (content,) = await server.read_resource(guide.MEASURES_URI)
    assert content.content == guide.MEASURES
    (content,) = await server.read_resource(guide.GUIDE_URI)
    assert content.mime_type == "text/markdown"
    assert content.content == guide.render_guide()
    prompts = await server.list_prompts()
    assert len(prompts) == len(guide_prompts.PROMPTS)
    result = await server.get_prompt("story-breadth", {"topic": "Red Sea shipping"})
    assert "query Red Sea shipping" in result.messages[0].content.text


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def test_cli_guide_prints_registry_generated_markdown(tools_pkg, monkeypatch, tmp_path) -> None:
    from world_intel_mcp import cli

    monkeypatch.setenv("WORLD_INTEL_CACHE_DB", str(tmp_path / "cli.db"))
    result = CliRunner().invoke(cli.main, ["guide"], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output.startswith("# World Intelligence MCP: research guide")
    assert set(_TOOL_LINE.findall(_tools_section(result.output))) == {
        t.name for t in tools_pkg.ALL_TOOLS
    }


def test_cli_guide_measures_and_json(tools_pkg) -> None:
    from world_intel_mcp import cli

    result = CliRunner().invoke(cli.main, ["guide", "--measures"], catch_exceptions=False)
    assert result.exit_code == 0
    assert result.output == guide.MEASURES
    result = CliRunner().invoke(cli.main, ["--json-output", "guide"], catch_exceptions=False)
    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["uri"] == guide.GUIDE_URI
    assert payload["text"] == guide.render_guide()


# ---------------------------------------------------------------------------
# Live stdio session against the installed console script
# ---------------------------------------------------------------------------

_CONSOLE_SCRIPT = Path(sys.executable).with_name("world-intel-mcp")


@pytest.mark.skipif(not _CONSOLE_SCRIPT.exists(), reason="console script not installed in this env")
@pytest.mark.asyncio
async def test_live_stdio_session_serves_resources_and_prompts(tmp_path, registered) -> None:
    env = dict(os.environ)
    env.update(
        {
            "WORLD_INTEL_CACHE_DB": str(tmp_path / "stdio-cache.db"),
            "XDG_CACHE_HOME": str(tmp_path / "xdg"),
            "WORLD_INTEL_LOG_LEVEL": "WARNING",
        }
    )
    params = StdioServerParameters(command=str(_CONSOLE_SCRIPT), args=[], env=env)
    with anyio.fail_after(60):
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as session:
                init = await session.initialize()
                assert init.capabilities.prompts is not None
                assert init.capabilities.resources is not None

                listed = await session.list_resources()
                uris = [str(r.uri) for r in listed.resources]
                assert uris == [guide.GUIDE_URI, guide.HEALTH_URI, guide.MEASURES_URI]

                read_guide = await session.read_resource(guide.GUIDE_URI)
                text = read_guide.contents[0].text
                assert read_guide.contents[0].mimeType == "text/markdown"
                assert set(_TOOL_LINE.findall(_tools_section(text))) == registered

                read_health = await session.read_resource(guide.HEALTH_URI)
                health = json.loads(read_health.contents[0].text)
                assert health["circuit_breakers"] == {}
                assert Path(health["cache"]["db_path"]) == tmp_path / "stdio-cache.db"

                prompts = await session.list_prompts()
                assert [p.name for p in prompts.prompts] == [p.name for p in guide_prompts.PROMPTS]

                got = await session.get_prompt("conflict-trend", {"country": "Sudan"})
                assert "organized violence in Sudan" in got.messages[0].content.text

                with pytest.raises(McpError, match="needs argument 'country'"):
                    await session.get_prompt("conflict-trend", {})

                tools = await session.list_tools()
                assert {t.name for t in tools.tools} == registered
