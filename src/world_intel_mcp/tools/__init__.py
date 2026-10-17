"""Domain-modular tool registry (Phase 26 server.py split).

Each domain module in this package exports two names:

- ``TOOLS: list[mcp.types.Tool]`` — the domain's tool definitions.
- ``HANDLERS: dict[str, Handler]`` — tool name to handler, where a
  handler is ``async def h(arguments: dict) -> Any`` and reaches shared
  infrastructure through ``world_intel_mcp.runtime`` (fetcher, cache,
  aoi_store, vector_store).

``aggregate()`` builds the combined registry and enforces, at import
time, the invariant ROADMAP.md has carried since Phase 0: every
registered tool has exactly one handler and every handler has exactly
one registration, with no name collisions across modules. A drift that
the old text-scan parity check could only catch in CI now prevents the
server from importing at all.
"""

from typing import Any, Awaitable, Callable

from mcp.types import Tool

Handler = Callable[[dict[str, Any]], Awaitable[Any]]

from . import (  # noqa: E402
    aoi,
    conflict,
    finance,
    geospatial,
    hazards,
    history,
    infrastructure,
    intelligence,
    lagged_connections,
    liquidity,
    markets,
    news_market,
    news_signals,
    society,
    synthesis,
    system,
    vector,
)

# Module order defines list_tools order, approximating the pre-split
# registry order (markets first ... system last).
_MODULES = [
    markets,
    hazards,
    conflict,
    infrastructure,
    society,
    intelligence,
    geospatial,
    synthesis,
    news_signals,
    finance,
    liquidity,
    news_market,
    vector,
    aoi,
    history,
    lagged_connections,
    system,
]


def aggregate(
    modules: list,
) -> tuple[list[Tool], dict[str, Handler]]:
    """Combine per-domain registries, refusing drift and collisions."""
    all_tools: list[Tool] = []
    all_handlers: dict[str, Handler] = {}
    for module in modules:
        tools: list[Tool] = module.TOOLS
        handlers: dict[str, Handler] = module.HANDLERS
        tool_names = {t.name for t in tools}
        if tool_names != set(handlers):
            raise RuntimeError(
                f"{module.__name__}: TOOLS/HANDLERS drift - "
                f"registered-without-handler {sorted(tool_names - set(handlers))}, "
                f"handler-without-registration {sorted(set(handlers) - tool_names)}"
            )
        collisions = tool_names & set(all_handlers)
        if collisions:
            raise RuntimeError(
                f"{module.__name__}: tool name collision across modules: "
                f"{sorted(collisions)}"
            )
        all_tools.extend(tools)
        all_handlers.update(handlers)
    return all_tools, all_handlers


ALL_TOOLS, ALL_HANDLERS = aggregate(_MODULES)
