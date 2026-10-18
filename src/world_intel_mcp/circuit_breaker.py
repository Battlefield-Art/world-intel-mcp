"""Per-source circuit breaker for world-intel-mcp.

Tracks failures per data source. Trips after N consecutive failures,
blocks calls for a cooldown period, then allows a single probe.
"""

import logging
import time
from dataclasses import dataclass

logger = logging.getLogger("world-intel-mcp.circuit_breaker")


@dataclass
class _State:
    failures: int = 0
    last_failure: float = 0.0
    tripped_at: float = 0.0
    is_open: bool = False
    total_trips: int = 0
    total_successes: int = 0
    total_failures: int = 0
    total_stale_serves: int = 0


class CircuitBreaker:
    """Circuit breaker with configurable thresholds per source."""

    def __init__(
        self,
        failure_threshold: int = 3,
        cooldown_seconds: float = 300.0,
        per_source_config: dict[str, dict] | None = None,
    ):
        self.failure_threshold = failure_threshold
        self.cooldown_seconds = cooldown_seconds
        self._per_source: dict[str, dict] = per_source_config or {}
        self._states: dict[str, _State] = {}

    def _get(self, source: str) -> _State:
        if source not in self._states:
            self._states[source] = _State()
        return self._states[source]

    def _threshold_for(self, source: str) -> int:
        """Get failure threshold for a specific source."""
        return self._per_source.get(source, {}).get(
            "failure_threshold", self.failure_threshold
        )

    def _cooldown_for(self, source: str) -> float:
        """Get cooldown seconds for a specific source."""
        return self._per_source.get(source, {}).get(
            "cooldown_seconds", self.cooldown_seconds
        )

    def is_available(self, source: str) -> bool:
        """Check if source is available (circuit closed or cooldown elapsed)."""
        state = self._get(source)
        if not state.is_open:
            return True
        if time.time() - state.tripped_at >= self._cooldown_for(source):
            return True  # allow probe
        return False

    def record_success(self, source: str) -> None:
        """Record successful call — resets failure count, closes circuit."""
        state = self._get(source)
        state.failures = 0
        state.is_open = False
        state.total_successes += 1

    def record_failure(self, source: str) -> None:
        """Record failed call — increments counter, may trip breaker.

        If the breaker is already open, this failure came from the single
        probe ``is_available()`` let through once the previous cooldown
        elapsed. Re-open it and restart the cooldown from now — otherwise
        ``tripped_at`` stays stuck at the original trip time, cooldown
        reads as already-elapsed on every subsequent check, and the
        breaker allows an unthrottled probe on every call ("half-open
        forever") instead of enforcing backoff.
        """
        state = self._get(source)
        state.failures += 1
        state.last_failure = time.time()
        state.total_failures += 1
        if state.is_open:
            state.tripped_at = time.time()
            state.total_trips += 1
            logger.warning(
                "Circuit breaker probe failed for %s, re-opening (cooldown=%.0fs)",
                source,
                self._cooldown_for(source),
            )
        elif state.failures >= self._threshold_for(source):
            state.is_open = True
            state.tripped_at = time.time()
            state.total_trips += 1
            logger.warning(
                "Circuit breaker TRIPPED for %s (failures=%d, cooldown=%.0fs)",
                source,
                state.failures,
                self._cooldown_for(source),
            )

    def record_stale_serve(self, source: str) -> None:
        """Record that a response was served from stale cache.

        The fetcher counts every stale copy it serves, whatever the payload
        type. Dict responses also carry an inline ``_stale`` marker
        (fetcher.py); other payload shapes (raw text/XML, lists) cannot, so
        for them this counter, surfaced via ``status()``, is the only signal
        that a caller received old data for that source. A stale copy
        served without the Fetcher's stale path is counted only when its
        code calls this (sources/liquidity.py does).
        """
        state = self._get(source)
        state.total_stale_serves += 1

    def status(self) -> dict[str, dict]:
        """Return status of all tracked sources."""
        now = time.time()
        result = {}
        for source, state in self._states.items():
            if state.is_open:
                remaining = max(
                    0, self._cooldown_for(source) - (now - state.tripped_at)
                )
                status = "open" if remaining > 0 else "half-open"
            else:
                remaining = 0
                status = "closed"
            result[source] = {
                "status": status,
                "failures": state.failures,
                "cooldown_remaining_s": round(remaining, 1),
                "total_trips": state.total_trips,
                "total_successes": state.total_successes,
                "total_failures": state.total_failures,
                "total_stale_serves": state.total_stale_serves,
            }
        return result
