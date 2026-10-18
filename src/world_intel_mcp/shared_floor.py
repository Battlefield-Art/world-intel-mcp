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
processes can never read the same last slot, and the slot is stamped only
once the write lock is held, so a wait for the lock never shortens the gap.

Another process's slot pushes this one to that slot plus the interval. A
process's own earlier slot is left to its in-process floor, except that
a slot of its own still ahead of the clock (a reservation whose wait was
cancelled) is kept, so the next call cannot jump the queue it joined. A
slot more than ``MAX_AHEAD_S`` ahead of the clock is not a queue: it is
treated as a clock jump, overwritten with the current time and logged. A
row that cannot be read as numbers is overwritten the same way.

Gaps / not covered: processes that resolve different cache DB paths
(``WORLD_INTEL_CACHE_DB`` / ``XDG_CACHE_HOME`` decide it) do not see each
other's slots; other hosts are separate IP addresses with their own DBs;
a reservation that fails (database locked past the timeout, a file that
is not a database, read-only file) is logged by the fetcher and the call
goes ahead on the in-process floor alone; sources outside
``SHARED_SOURCES`` keep a per-process floor; a backward clock step smaller
than ``MAX_AHEAD_S`` is still waited out.
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
from pathlib import Path

logger = logging.getLogger("world-intel-mcp.shared_floor")

# Sources whose floor holds across processes. Others keep the in-process
# floor only.
SHARED_SOURCES = frozenset({"gdelt"})

# A real queue holds at most one waiter per process: eight processes on one
# host (collector, a few MCP servers, dashboard, CLI runs) at GDELT's 5 s
# is 40 s. A slot further ahead than this is a clock jump, not a queue.
MAX_AHEAD_S = 40.0

_LOCK_TIMEOUT_S = 5.0


def reserve(
    db_path: str | Path, source: str, interval: float, now: float | None = None
) -> float:
    """Reserve the next request slot for ``source``; return its Unix time.

    The slot is the current time (``now`` when given, else the clock once
    the write lock is held), pushed to another process's slot plus
    ``interval`` or kept at this process's own slot when either is still
    ahead. Raises ``sqlite3.Error`` when the DB cannot be locked or written.
    """
    pid = os.getpid()
    conn = sqlite3.connect(str(db_path), timeout=_LOCK_TIMEOUT_S, isolation_level=None)
    try:
        conn.execute("BEGIN IMMEDIATE")
        # Sampled with the lock held: a slot never predates its reservation.
        now = time.time() if now is None else now
        conn.execute(
            "CREATE TABLE IF NOT EXISTS source_floor ("
            "source TEXT PRIMARY KEY, slot REAL NOT NULL, pid INTEGER NOT NULL)"
        )
        row = conn.execute(
            "SELECT slot, pid FROM source_floor WHERE source = ?", (source,)
        ).fetchone()
        slot = now
        if row is not None:
            try:
                last, last_pid = float(row[0]), int(row[1])
            except (TypeError, ValueError):
                logger.warning(
                    "Unreadable %s floor row %r; overwriting it with the current time",
                    source,
                    tuple(row),
                )
            else:
                if last - now > MAX_AHEAD_S:
                    logger.warning(
                        "%s floor slot is %.0f s ahead of the clock (a clock jump, not a "
                        "queue); overwriting it with the current time",
                        source,
                        last - now,
                    )
                elif last_pid != pid:
                    slot = max(now, last + interval)
                else:
                    slot = max(now, last)
        conn.execute(
            "INSERT OR REPLACE INTO source_floor (source, slot, pid) VALUES (?, ?, ?)",
            (source, slot, pid),
        )
        conn.execute("COMMIT")
        return slot
    finally:
        # Closing with the transaction still open (an error above) rolls it back.
        conn.close()
