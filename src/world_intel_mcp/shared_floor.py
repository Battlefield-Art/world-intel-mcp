"""Cross-process spacing for rate-floored upstreams.

The fetcher's per-source floor (``fetcher._SOURCE_RATE_LIMITS``) is an
asyncio lock plus a timestamp, so on its own it spaces calls made inside
one process only. GDELT limits per IP address ("Please limit requests to
one every 5 seconds", its 429 body, measured 2026-10-17), and this server
routinely runs as several processes on one host against one cache DB: the
collector daemon, one MCP stdio server per client, the dashboard, and
every ``intel`` CLI run. For the sources in ``SHARED_SOURCES`` each live
request therefore also reserves a slot in a one-row-per-source table of
that cache DB. The reservation runs under ``BEGIN IMMEDIATE``, so two
processes can never read the same last slot.

A process's own earlier reservation is left to its in-process floor:
only another process's slot pushes this one back. A slot more than
``MAX_AHEAD_S`` ahead of the clock is treated as a clock jump or a corrupt
row, not a queue, and ignored.

Gaps / not covered: processes that resolve different cache DB paths
(``WORLD_INTEL_CACHE_DB`` / ``XDG_CACHE_HOME`` decide it) do not see each
other's slots; other hosts are separate IP addresses with their own DBs;
a reservation that fails (database locked past the timeout, read-only
file) is logged by the fetcher and the call goes ahead on the in-process
floor alone; sources outside ``SHARED_SOURCES`` keep a per-process floor.
"""

from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path

# Sources whose floor holds across processes. Others keep the in-process
# floor only.
SHARED_SOURCES = frozenset({"gdelt"})

# Ten minutes of queued 5 s slots is far beyond any real queue (one waiter
# per process at a time); a slot further ahead than this is not one.
MAX_AHEAD_S = 600.0

_LOCK_TIMEOUT_S = 5.0


def reserve(
    db_path: str | Path, source: str, interval: float, now: float | None = None
) -> float:
    """Reserve the next request slot for ``source``; return its Unix time.

    The slot is ``now`` unless another process reserved one less than
    ``interval`` before ``now`` (or one still ahead of it), in which case it
    is that slot plus ``interval``. Raises ``sqlite3.Error`` when the DB
    cannot be locked or written.
    """
    now = time.time() if now is None else now
    pid = os.getpid()
    conn = sqlite3.connect(str(db_path), timeout=_LOCK_TIMEOUT_S, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "CREATE TABLE IF NOT EXISTS source_floor ("
            "source TEXT PRIMARY KEY, slot REAL NOT NULL, pid INTEGER NOT NULL)"
        )
        row = conn.execute(
            "SELECT slot, pid FROM source_floor WHERE source = ?", (source,)
        ).fetchone()
        slot = now
        if row is not None:
            last, last_pid = float(row[0]), int(row[1])
            if last_pid != pid and last - now <= MAX_AHEAD_S:
                slot = max(now, last + interval)
        conn.execute(
            "INSERT OR REPLACE INTO source_floor (source, slot, pid) VALUES (?, ?, ?)",
            (source, slot, pid),
        )
        conn.execute("COMMIT")
        return slot
    finally:
        # Closing with the transaction still open (an error above) rolls it back.
        conn.close()
