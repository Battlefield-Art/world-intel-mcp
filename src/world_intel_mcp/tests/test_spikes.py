"""Tests for analysis/spikes.py — Welford keyword spike detection.

The detector persists baselines in SQLite; every test uses a tmp_path
database (never the real ~/.cache file). Expected z-scores are hand-computed
from Welford's recurrence: for observations [10, 20, 30, 20] the running
state is n=4, mean=20, M2=200, so variance=50 and stddev=sqrt(50)=7.0711.
"""

from collections.abc import Iterator
from pathlib import Path

import pytest

from world_intel_mcp.analysis import spikes as spikes_mod
from world_intel_mcp.analysis.spikes import KeywordSpikeDetector, fetch_keyword_spikes
from world_intel_mcp.sources import news


@pytest.fixture
def detector(tmp_path: Path) -> Iterator[KeywordSpikeDetector]:
    detector = KeywordSpikeDetector(db_path=str(tmp_path / "spikes.db"))
    try:
        yield detector
    finally:
        if detector._conn is not None:
            detector._conn.close()


def _feed(detector: KeywordSpikeDetector, keyword: str, values: list[int]) -> None:
    for v in values:
        detector.detect_spikes({keyword: v})


def test_no_spikes_during_warmup(detector: KeywordSpikeDetector) -> None:
    # Fewer than 3 baseline observations: even a huge count is not a spike.
    assert detector.detect_spikes({"war": 500}) == []
    assert detector.detect_spikes({"war": 500}) == []
    assert detector.detect_spikes({"war": 500}) == []


def test_ratio_spike_on_flat_baseline(detector: KeywordSpikeDetector) -> None:
    _feed(detector, "gaza", [10, 10, 10])  # zero variance baseline
    result = detector.detect_spikes({"gaza": 40})
    assert len(result) == 1
    spike = result[0]
    assert spike["keyword"] == "gaza"
    assert spike["detection"] == "ratio"
    assert spike["z_score"] is None
    assert spike["ratio"] == 4.0
    assert spike["baseline_mean"] == 10.0
    assert spike["current_count"] == 40


def test_no_ratio_spike_below_threshold(detector: KeywordSpikeDetector) -> None:
    """Load-bearing pair with the test above: 2.5x on a flat baseline is
    NOT a spike (the ratio path requires > 3.0)."""
    _feed(detector, "gaza", [10, 10, 10])
    assert detector.detect_spikes({"gaza": 25}) == []


def test_z_score_spike_flags_surge_not_noise(detector: KeywordSpikeDetector) -> None:
    for v in [10, 20, 30, 20]:
        detector.detect_spikes({"surging": v, "steady": v})
    result = detector.detect_spikes({"surging": 40, "steady": 25})
    # surging: z = (40-20)/7.0711 = 2.83 >= 2.0 -> spike.
    # steady:  z = (25-20)/7.0711 = 0.71 -> silence.
    assert [s["keyword"] for s in result] == ["surging"]
    spike = result[0]
    assert spike["detection"] == "z_score"
    assert spike["z_score"] == 2.83
    assert spike["stddev"] == 7.07
    assert spike["baseline_mean"] == 20.0
    assert spike["ratio"] == 2.0


def test_baseline_absorbs_the_spike(detector: KeywordSpikeDetector) -> None:
    """A sustained plateau stops alarming: the spike observation is folded
    into the baseline, so the same count again is no longer anomalous."""
    _feed(detector, "gaza", [10, 10, 10])
    assert len(detector.detect_spikes({"gaza": 40})) == 1
    # Baseline is now [10,10,10,40]: mean 17.5, stddev ~12.99 -> z ~1.73.
    assert detector.detect_spikes({"gaza": 40}) == []


def test_spikes_sorted_by_magnitude_descending(detector: KeywordSpikeDetector) -> None:
    for v in [10, 20, 30, 20]:
        detector.detect_spikes({"big": v, "small": v})
    result = detector.detect_spikes({"big": 100, "small": 40})
    assert [s["keyword"] for s in result] == ["big", "small"]
    assert result[0]["z_score"] > result[1]["z_score"]


def test_baselines_persist_across_instances(tmp_path: Path, request) -> None:
    db = str(tmp_path / "persist.db")
    first = KeywordSpikeDetector(db_path=db)
    request.addfinalizer(lambda: first._conn.close() if first._conn is not None else None)
    _feed(first, "coup", [10, 10, 10])

    second = KeywordSpikeDetector(db_path=db)
    request.addfinalizer(lambda: second._conn.close() if second._conn is not None else None)
    result = second.detect_spikes({"coup": 40})
    assert len(result) == 1  # baseline survived the restart
    assert result[0]["baseline_mean"] == 10.0


async def test_fetch_keyword_spikes_composes_news_sources(
    fetcher, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, request
) -> None:
    async def _fake_trending(fetcher, min_count=3):
        return {"keywords": [{"keyword": "gaza", "count": 30}]}

    async def _fake_feed(fetcher, limit=100, category=None):
        return {
            "items": [
                {"title": "CVE-2026-1111 exploited in the wild", "summary": ""},
                {"title": "Volt Typhoon activity reported", "summary": ""},
            ]
        }

    monkeypatch.setattr(news, "fetch_trending_keywords", _fake_trending)
    monkeypatch.setattr(news, "fetch_news_feed", _fake_feed)
    # Point the module singleton at a throwaway database.
    detector = KeywordSpikeDetector(db_path=str(tmp_path / "kw.db"))
    request.addfinalizer(lambda: detector._conn.close() if detector._conn is not None else None)
    monkeypatch.setattr(spikes_mod, "_detector", detector)

    result = await fetch_keyword_spikes(fetcher, min_count=3, z_threshold=2.0)
    assert result["keywords_analyzed"] == 1
    assert result["spikes"] == []  # first observation ever: warm-up
    assert result["cve_mentions"] == ["CVE-2026-1111"]
    assert result["apt_mentions"] == ["volt typhoon"]  # lowercased
    assert result["cve_count"] == 1
    assert result["apt_count"] == 1
    assert result["z_threshold"] == 2.0
    assert result["source"] == "keyword-spike-detector"
