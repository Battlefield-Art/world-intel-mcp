"""When a source's answer was really fetched, and whether it is an expired copy.

The fetcher answers from its cache while an entry is live, and when a
live fetch fails (or the source's circuit is open) it serves the expired
entry instead, marking a dict answer with ``_stale`` and
``_stale_age_seconds``. A source wrapper that rebuilds the answer drops
both marks unless it carries them, and its own ``timestamp`` is the time
it assembled the reply, not when upstream answered. Anything that scores
current conditions needs both facts, so these helpers recover them from
the cache entry the answer came from.
"""

import time
from datetime import datetime, timezone

from ..fetcher import Fetcher


def _utc(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _cache_entry(fetcher: Fetcher, cache_key: str) -> tuple[object, float] | None:
    """``(value, written_at)`` of the cache entry under ``cache_key``, live
    or expired, or None when there is none."""
    meta = fetcher.cache.get_stale_meta(cache_key)
    if meta is None or not isinstance(meta[1], (int, float)):
        return None
    return meta[0], float(meta[1])


def json_answer_freshness(fetcher: Fetcher, cache_key: str, data: object) -> dict:
    """``fetched_at`` for an answer ``fetcher.get_json`` returned under
    ``cache_key``, plus the fetcher's ``_stale`` / ``_stale_age_seconds``
    when the answer was its expired copy.

    ``fetched_at`` is when this server received the answer from upstream:
    the cache entry's write time, so a cached answer reports its original
    fetch, not the moment it was served. With no entry (the cache write
    failed) the answer was just fetched."""
    now = time.time()
    if isinstance(data, dict) and data.get("_stale"):
        age = data.get("_stale_age_seconds")
        age = float(age) if isinstance(age, (int, float)) else 0.0
        return {
            "fetched_at": _utc(now - age),
            "_stale": True,
            "_stale_age_seconds": round(age, 1),
        }
    entry = _cache_entry(fetcher, cache_key)
    return {"fetched_at": _utc(entry[1] if entry is not None else now)}


def text_answer_freshness(fetcher: Fetcher, cache_key: str, text: str) -> dict:
    """``json_answer_freshness`` for ``fetcher.get_text``, which cannot mark
    a stale ``str``: an answer equal to an expired cache entry is that
    expired copy."""
    now = time.time()
    entry = _cache_entry(fetcher, cache_key)
    if entry is None or entry[0] != text:
        return {"fetched_at": _utc(now)}
    written_at = entry[1]
    out: dict = {"fetched_at": _utc(written_at)}
    if fetcher.cache.get(cache_key) is None:
        out["_stale"] = True
        out["_stale_age_seconds"] = round(max(0.0, now - written_at), 1)
    return out
