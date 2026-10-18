"""Fetcher.get_bytes reads a body as it arrives and stops as soon as the
decoded size passes ``max_bytes``.

Before, the whole body was buffered, and a gzip body fully decoded,
before the cap was checked, so a small compressed response could make
the process hold hundreds of MiB first. The bodies here are served in
compressed pieces by an iterator that counts how many the client pulls,
so "stopped reading" is observed directly, without timing or memory
measurement.

Gaps / not covered: peak memory is not measured here (the CHANGELOG
cites a one-machine measurement); one network read can still decode to
much more than ``max_bytes`` before the check runs.
"""

import gzip

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher

_URL = "https://a.test/body"
_MiB = 1024 * 1024


class _CountingBody:
    """A streamed response body that counts the pieces read from it."""

    def __init__(self, data: bytes, piece: int) -> None:
        self.pieces = [data[i : i + piece] for i in range(0, len(data), piece)]
        self.pulled = 0

    async def __aiter__(self):
        for piece in self.pieces:
            self.pulled += 1
            yield piece


@respx.mock
@pytest.mark.asyncio
async def test_a_gzip_body_past_the_cap_is_refused_without_reading_the_rest(
    fetcher: Fetcher,
) -> None:
    """16 MiB of zeros gzip to about 16 KB; each 512-byte piece decodes
    to about 0.5 MiB, so a 1 MiB cap is passed within the first few
    pieces and the rest is never read or decoded."""
    body = _CountingBody(gzip.compress(b"\0" * (16 * _MiB)), piece=512)
    respx.get(_URL).mock(
        return_value=httpx.Response(200, headers={"content-encoding": "gzip"}, content=body)
    )

    result = await fetcher.get_bytes(_URL, source="t", max_bytes=_MiB)

    assert result.ok is False
    assert result.status == 200
    assert result.content is None
    assert result.error.startswith(f"body exceeds cap {_MiB}: stopped after ")
    assert len(body.pieces) > 25
    assert body.pulled <= 4
    # An over-cap body is an answer, not an outage.
    assert fetcher.breaker.status()["t"]["total_failures"] == 0


@respx.mock
@pytest.mark.asyncio
async def test_an_uncompressed_body_past_the_cap_is_not_read_to_the_end(
    fetcher: Fetcher,
) -> None:
    body = _CountingBody(b"x" * (64 * 1024), piece=4096)
    respx.get(_URL).mock(return_value=httpx.Response(200, content=body))

    result = await fetcher.get_bytes(_URL, source="t", max_bytes=10_000)

    assert result.content is None
    assert result.error.startswith("body exceeds cap 10000: stopped after ")
    assert body.pulled < len(body.pieces)


@pytest.mark.parametrize(("length", "ok"), [(10_000, True), (10_001, False)])
@respx.mock
@pytest.mark.asyncio
async def test_a_body_of_exactly_max_bytes_is_returned_whole(
    fetcher: Fetcher, length: int, ok: bool
) -> None:
    respx.get(_URL).mock(return_value=httpx.Response(200, content=b"y" * length))

    result = await fetcher.get_bytes(_URL, source="t", max_bytes=10_000)

    assert result.ok is ok
    assert result.content == (b"y" * length if ok else None)


@respx.mock
@pytest.mark.asyncio
async def test_the_cap_counts_decoded_bytes_not_compressed_ones(fetcher: Fetcher) -> None:
    data = b"forecast track " * 1000
    respx.get(_URL).mock(
        return_value=httpx.Response(
            200, headers={"content-encoding": "gzip"}, content=gzip.compress(data)
        )
    )

    whole = await fetcher.get_bytes(_URL, source="t", max_bytes=len(data))
    short = await fetcher.get_bytes(_URL, source="t", max_bytes=len(data) - 1)

    assert whole.content == data
    assert short.content is None
    assert short.error.startswith(f"body exceeds cap {len(data) - 1}: ")
