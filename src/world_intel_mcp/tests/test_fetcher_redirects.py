"""Tests for Fetcher.get_bytes(follow_redirects=False).

The shared client follows redirects. A caller that has checked a URL
against an allowlist passes ``follow_redirects=False`` so the body can
only come from that URL: a redirect comes back as its status and
resolved Location, the target is never requested, and the redirect is
an answer (not retried, not a source failure).

Gaps / not covered: get_text, get_json and get_xml have no such option
and keep following redirects.
"""

import inspect

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher


@respx.mock
@pytest.mark.asyncio
async def test_get_bytes_without_following_reports_the_redirect_unrequested(
    fetcher: Fetcher,
) -> None:
    origin = respx.get("https://a.test/x").mock(
        return_value=httpx.Response(302, headers={"location": "http://10.0.0.5:8080/y"})
    )
    target = respx.get("http://10.0.0.5:8080/y").mock(
        return_value=httpx.Response(200, content=b"inner")
    )

    result = await fetcher.get_bytes("https://a.test/x", source="t", follow_redirects=False)

    assert result.ok is False
    assert result.status == 302
    assert result.content is None
    assert result.location == "http://10.0.0.5:8080/y"
    assert result.error == "redirect to http://10.0.0.5:8080/y not followed"
    assert target.called is False
    assert origin.call_count == 1
    assert fetcher.breaker.status()["t"]["total_failures"] == 0


@respx.mock
@pytest.mark.asyncio
async def test_get_bytes_reports_a_relative_redirect_as_an_absolute_url(
    fetcher: Fetcher,
) -> None:
    respx.get("https://a.test/x").mock(
        return_value=httpx.Response(301, headers={"location": "/y?z=1"})
    )

    result = await fetcher.get_bytes("https://a.test/x", source="t", follow_redirects=False)

    assert result.status == 301
    assert result.location == "https://a.test/y?z=1"


@respx.mock
@pytest.mark.asyncio
async def test_get_bytes_without_following_still_returns_a_direct_body(
    fetcher: Fetcher,
) -> None:
    respx.get("https://a.test/x").mock(return_value=httpx.Response(200, content=b"body"))

    result = await fetcher.get_bytes("https://a.test/x", source="t", follow_redirects=False)

    assert result.ok is True
    assert result.content == b"body"
    assert result.location is None


@pytest.mark.asyncio
async def test_follow_redirects_is_keyword_only(fetcher: Fetcher) -> None:
    """Keyword-only, so other keyword parameters of get_bytes can be added
    without moving it. The call fills every other positional parameter and
    passes one more, so the check holds whatever positional parameters
    get_bytes gains, and fails if follow_redirects is one of them."""
    positional = [
        p for p in inspect.signature(fetcher.get_bytes).parameters.values()
        if p.kind is p.POSITIONAL_OR_KEYWORD and p.name != "follow_redirects"
    ]
    assert positional[:2] == list(inspect.signature(fetcher.get_bytes).parameters.values())[:2]
    with pytest.raises(TypeError):
        await fetcher.get_bytes("https://a.test/x", "t", *[None] * (len(positional) - 2), False)


@respx.mock
@pytest.mark.asyncio
async def test_get_bytes_follows_redirects_by_default(fetcher: Fetcher) -> None:
    respx.get("https://a.test/x").mock(
        return_value=httpx.Response(301, headers={"location": "https://a.test/y"})
    )
    moved = respx.get("https://a.test/y").mock(return_value=httpx.Response(200, content=b"moved"))

    result = await fetcher.get_bytes("https://a.test/x", source="t")

    assert result.ok is True
    assert result.status == 200
    assert result.content == b"moved"
    assert result.location is None
    assert moved.call_count == 1
