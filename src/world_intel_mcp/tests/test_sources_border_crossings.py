"""Tests for sources/border_crossings.py: CBP encounters and UK small-boat arrivals.

Every figure asserted here comes from data captured live on 2026-10-17.

* CBP: the dataset page https://www.cbp.gov/document/stats/nationwide-encounters
  (its "Last Modified: Sep 16, 2026") and its newest file,
  nationwide-encounters-fy23-fy26-aug-aor_0.csv (6,730,588 bytes, 52,986 rows).
  ``_CBP_NORTHERN_USBP_AUG`` is every row of that file for U.S. Border Patrol on
  the Northern Land Border in August FY2025 and August FY2026 (167 rows,
  verbatim). Their sums, 586 and 563, are the figures CBP itself publishes as
  "Northern Border Total Apprehensions" for Aug-25 and Aug-26 on its FY2025 and
  FY2026 encounter pages, so those assertions check the parser against the
  publisher, not against the fixture. ``_CBP_FEATURE_ROWS`` are nine more
  verbatim rows chosen to exercise the "2026 (FYTD)" label, October rows
  (calendar year FY-1), the "Remaining" month grouping, a Title 42 expulsion,
  a quoted citizenship and CBP's OTHER bucket; sums over them are fixture sums.
* UK: the GOV.UK Content API item for "Small boat activity in the English
  Channel" (public_updated_at 2026-10-16T13:13:46+01:00) and its ODS time series
  (16_October_2026_Small_boats_-_time_series.ods, 76,787 bytes). An ODS is a
  zip, which a text diff cannot carry, so ``_ods`` rebuilds one from verbatim
  values (every daily row from 1 January 2025 to 15 October 2026, ten weekly
  rows, three of the nine notes) using the cell markup of the live
  content.xml, including the trailing blank repeats that pad every sheet to
  16,384 columns and 1,048,576 rows. The latest week (1,066 migrants, 15
  boats, 245 prevented, 12 events prevented) and the last 7 days (all 0) are
  the figures the Home Office publishes on its weekly-summary and last-7-days
  pages.

Gaps / not covered: the live publishers are reached only by the two smoke
tests at the end (deselected by default; ``pytest -m smoke -k live_``), whose
assertions are structural because live figures move; CBP's by-state file; an
ODS written by an application other than the one the Home Office uses;
GOV.UK redirects; a CBP CSV that is not UTF-8.
"""

from __future__ import annotations

import asyncio as asyncio_mod
import copy
import io
import zipfile
from collections import Counter
from contextlib import contextmanager
from datetime import date

import httpx
import pytest
import respx

from world_intel_mcp.fetcher import Fetcher
from world_intel_mcp.sources import border_crossings as bc


def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _instant(*args, **kwargs) -> None:
        return None

    monkeypatch.setattr(asyncio_mod, "sleep", _instant)


def _no_numbers(result: dict) -> bool:
    """True when a response carries no numeric figure at any top-level key."""
    return not any(isinstance(v, (int, float)) and not isinstance(v, bool) for v in result.values())


# ---------------------------------------------------------------------------
# CBP: captured shapes
# ---------------------------------------------------------------------------

_CBP_CSV_URL = "https://www.cbp.gov/sites/default/files/2026-09/nationwide-encounters-fy23-fy26-aug-aor_0.csv"

_CBP_HEADER = (
    "Fiscal Year,Month Grouping,Month (abbv),Component,Land Border Region,Area of Responsibility,"
    "AOR (Abbv),Demographic,Citizenship,Title of Authority,Encounter Type,Encounter Count"
)

# Every row of the live file for U.S. Border Patrol, Northern Land Border, August FY2025 and FY2026.
_CBP_NORTHERN_USBP_AUG = """\
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,FMUA,INDIA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,FMUA,ROMANIA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,CANADA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,EL SALVADOR,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,GUATEMALA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,INDIA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,MEXICO,Title 8,Apprehensions,5
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,OTHER,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,VENEZUELA,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,FMUA,GUATEMALA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,BRAZIL,Title 8,Apprehensions,6
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,CANADA,Title 8,Apprehensions,30
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,COLOMBIA,Title 8,Apprehensions,11
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,ECUADOR,Title 8,Apprehensions,28
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,EL SALVADOR,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,GUATEMALA,Title 8,Apprehensions,22
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,HAITI,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,HONDURAS,Title 8,Apprehensions,8
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,MEXICO,Title 8,Apprehensions,32
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,NICARAGUA,Title 8,Apprehensions,9
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,OTHER,Title 8,Apprehensions,7
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,PERU,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,RUSSIA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,TURKEY,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,VENEZUELA,Title 8,Apprehensions,8
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,UAC,ECUADOR,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,UAC,GUATEMALA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,UAC,MEXICO,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,FMUA,MEXICO,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,"CHINA, PEOPLES REPUBLIC OF",Title 8,Apprehensions,4
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,COLOMBIA,Title 8,Apprehensions,5
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,CUBA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,ECUADOR,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,EL SALVADOR,Title 8,Apprehensions,4
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,GUATEMALA,Title 8,Apprehensions,12
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,HONDURAS,Title 8,Apprehensions,23
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,MEXICO,Title 8,Apprehensions,89
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,NICARAGUA,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,OTHER,Title 8,Apprehensions,9
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,PERU,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,ROMANIA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,TURKEY,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,VENEZUELA,Title 8,Apprehensions,20
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,UAC,MEXICO,Title 8,Apprehensions,4
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,CANADA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,ECUADOR,Title 8,Apprehensions,5
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,EL SALVADOR,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,GUATEMALA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,HONDURAS,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,MEXICO,Title 8,Apprehensions,21
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,NICARAGUA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,PERU,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,VENEZUELA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,GUATEMALA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,MEXICO,Title 8,Apprehensions,13
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,UAC,MEXICO,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,FMUA,GUATEMALA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,BRAZIL,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,COLOMBIA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,ECUADOR,Title 8,Apprehensions,19
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,EL SALVADOR,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,GUATEMALA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,HONDURAS,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,INDIA,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,MEXICO,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,OTHER,Title 8,Apprehensions,9
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,TURKEY,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,FMUA,GUATEMALA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,FMUA,VENEZUELA,Title 8,Apprehensions,4
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,EL SALVADOR,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,GUATEMALA,Title 8,Apprehensions,7
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,HONDURAS,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,MEXICO,Title 8,Apprehensions,12
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,NICARAGUA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,OTHER,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,VENEZUELA,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,FMUA,INDIA,Title 8,Apprehensions,4
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,CANADA,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,"CHINA, PEOPLES REPUBLIC OF",Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,COLOMBIA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,ECUADOR,Title 8,Apprehensions,9
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,EL SALVADOR,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,GUATEMALA,Title 8,Apprehensions,10
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,HAITI,Title 8,Apprehensions,3
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,HONDURAS,Title 8,Apprehensions,7
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,INDIA,Title 8,Apprehensions,4
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,MEXICO,Title 8,Apprehensions,11
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,NICARAGUA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,OTHER,Title 8,Apprehensions,8
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,PERU,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,RUSSIA,Title 8,Apprehensions,2
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,TURKEY,Title 8,Apprehensions,1
2025,FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,VENEZUELA,Title 8,Apprehensions,5
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,"CHINA, PEOPLES REPUBLIC OF",Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,EL SALVADOR,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,GUATEMALA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,INDIA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,MEXICO,Title 8,Apprehensions,15
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,NICARAGUA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Blaine Sector,BLW,Single Adults,RUSSIA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,BRAZIL,Title 8,Apprehensions,5
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,CANADA,Title 8,Apprehensions,13
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,"CHINA, PEOPLES REPUBLIC OF",Title 8,Apprehensions,9
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,COLOMBIA,Title 8,Apprehensions,7
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,CUBA,Title 8,Apprehensions,3
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,ECUADOR,Title 8,Apprehensions,16
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,EL SALVADOR,Title 8,Apprehensions,4
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,GUATEMALA,Title 8,Apprehensions,35
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,HONDURAS,Title 8,Apprehensions,24
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,MEXICO,Title 8,Apprehensions,51
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,NICARAGUA,Title 8,Apprehensions,7
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,OTHER,Title 8,Apprehensions,10
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,RUSSIA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Buffalo Sector,BUN,Single Adults,VENEZUELA,Title 8,Apprehensions,23
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,FMUA,GUATEMALA,Title 8,Apprehensions,3
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,FMUA,HONDURAS,Title 8,Apprehensions,7
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,COLOMBIA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,ECUADOR,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,EL SALVADOR,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,GUATEMALA,Title 8,Apprehensions,23
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,HAITI,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,HONDURAS,Title 8,Apprehensions,25
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,MEXICO,Title 8,Apprehensions,67
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,NICARAGUA,Title 8,Apprehensions,6
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,ROMANIA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,Single Adults,VENEZUELA,Title 8,Apprehensions,12
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,UAC,CANADA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Detroit Sector,DTM,UAC,HONDURAS,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,COLOMBIA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,GUATEMALA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,HONDURAS,Title 8,Apprehensions,3
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,MEXICO,Title 8,Apprehensions,8
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,NICARAGUA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Grand Forks Sector,GFN,Single Adults,OTHER,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,FMUA,COLOMBIA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,FMUA,MEXICO,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,CANADA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,COLOMBIA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,EL SALVADOR,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,GUATEMALA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,HONDURAS,Title 8,Apprehensions,10
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,MEXICO,Title 8,Apprehensions,28
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,OTHER,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,Single Adults,VENEZUELA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Havre Sector,HVM,UAC,MEXICO,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,FMUA,OTHER,Title 8,Apprehensions,6
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,BRAZIL,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,ECUADOR,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Houlton Sector,HLT,Single Adults,HONDURAS,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,CANADA,Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,GUATEMALA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Spokane Sector,SPW,Single Adults,MEXICO,Title 8,Apprehensions,14
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,FMUA,BRAZIL,Title 8,Apprehensions,4
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,FMUA,OTHER,Title 8,Apprehensions,6
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,FMUA,VENEZUELA,Title 8,Apprehensions,4
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,CANADA,Title 8,Apprehensions,6
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,"CHINA, PEOPLES REPUBLIC OF",Title 8,Apprehensions,1
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,ECUADOR,Title 8,Apprehensions,3
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,GUATEMALA,Title 8,Apprehensions,3
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,HAITI,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,HONDURAS,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,INDIA,Title 8,Apprehensions,17
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,MEXICO,Title 8,Apprehensions,5
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,OTHER,Title 8,Apprehensions,29
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,Single Adults,VENEZUELA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,UAC,INDIA,Title 8,Apprehensions,2
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Northern Land Border,Swanton Sector,SWB,UAC,OTHER,Title 8,Apprehensions,1
"""

_CBP_FEATURE_ROWS = """\
2026 (FYTD),FYTD,AUG,U.S. Border Patrol,Southwest Land Border,Big Bend Sector,BBT,Single Adults,MEXICO,Title 8,Apprehensions,86
2025,FYTD,AUG,U.S. Border Patrol,Southwest Land Border,Big Bend Sector,BBT,Single Adults,MEXICO,Title 8,Apprehensions,123
2026 (FYTD),FYTD,AUG,Office of Field Operations,Southwest Land Border,El Paso Field Office,El Paso,Single Adults,"CHINA, PEOPLES REPUBLIC OF",Title 8,Inadmissibles,6
2026 (FYTD),FYTD,AUG,Office of Field Operations,Other,Atlanta Field Office,Atlanta,Accompanied Minors,INDIA,Title 8,Inadmissibles,2
2026 (FYTD),FYTD,AUG,Office of Field Operations,Other,Atlanta Field Office,Atlanta,Accompanied Minors,OTHER,Title 8,Inadmissibles,3
2026 (FYTD),FYTD,AUG,Office of Field Operations,Southwest Land Border,Tucson Field Office,Tucson,UAC,GUATEMALA,Title 8,Inadmissibles,1
2026 (FYTD),FYTD,OCT,U.S. Border Patrol,Southwest Land Border,Big Bend Sector,BBT,Single Adults,MEXICO,Title 8,Apprehensions,105
2023,Remaining,SEP,U.S. Border Patrol,Southwest Land Border,Big Bend Sector,BBT,Single Adults,MEXICO,Title 8,Apprehensions,361
2023,FYTD,OCT,U.S. Border Patrol,Southwest Land Border,Big Bend Sector,BBT,Single Adults,MEXICO,Title 42,Expulsions,700
"""

# Excerpt of the dataset page, markup verbatim: its two caveat paragraphs, two
# listings of the same files (the list, and the table with Size and Date), and its date.
_CBP_PAGE = """<html><body><main>
<p>Encounter data includes U.S. Border Patrol Title 8 apprehensions, Office of Field Operations Title 8 inadmissibles, and all Title 42 expulsions for fiscal years 2020 to date. Data is available for the Northern Land Border, Southwest Land Border, and Nationwide (i.e., air, land, and sea modes of transportation) encounters.</p><p>Data is extracted from live CBP systems and data sources. Statistical information is subject to change due to corrections, systems changes, change in data definition, additional information, or encounters pending final review. Final statistics are available at the conclusion of each fiscal year.</p><h2 class="usa-prose">Dashboard Dataset</h2><ul class="undecorated list">
<li data-list-item-id="e1ecbc5891880c7a7d8acd71071175f63"><a href="/sites/default/files/2026-09/nationwide-encounters-fy23-fy26-aug-aor_0.csv">FY23 - FY26 (FYTD) Nationwide Encounters by Area of Responsibility - August</a></li>
<li data-list-item-id="e0df05b7a56b969b8b26877053451d6f5"><a href="/sites/default/files/2026-09/nationwide-encounters-fy23-fy26-aug-state.csv">FY23 - FY26 (FYTD) Nationwide Encounters by State - August</a></li>
<li data-list-item-id="e4cae405a56918198ee5dbf1fc5ec7d53"><a href="/sites/default/files/2026-08/nationwide-encounters-fy23-fy26-jul-aor.csv">FY23 - FY26 (FYTD) Nationwide Encounters by Area of Responsibility - July</a></li>
<li data-list-item-id="e5a208075bf353a5ada9645c00e4d8c4d"><a href="/sites/default/files/2025-11/nationwide-encounters-fy22-fy25-aor.csv">FY22 - FY25 Nationwide Encounters by Area of Responsibility</a></li>
<li data-list-item-id="e56e6b8ae6cb5e3b64889e44724858fdf"><a href="/sites/default/files/2024-10/nationwide-encounters-fy21-fy24-aor.csv">FY21 - FY24 Nationwide Encounters by Area of Responsibility</a></li>
<li data-list-item-id="e1fbf2f1de91328f2c6b5881685a722fd"><a href="/sites/default/files/assets/documents/2023-Nov/nationwide-encounters-fy20-fy23-aor.csv">FY20 - FY23 Nationwide Encounters by Area of Responsibility</a></li>
</ul>
<table><tbody>
<tr scope="row">
    <th scope="row" role="rowheader" data-sort-value=" Nationwide Encounters Dataset FY23-FY26 by Area of Responsibility - August">
    <a href="/sites/default/files/2026-09/nationwide-encounters-fy23-fy26-aug-aor_0.csv">
     Nationwide Encounters Dataset FY23-FY26 by Area of Responsibility - August
    </a>
    </th>
    <td data-sort-value="CSV">CSV</td>
    <td data-sort-value="6730588">
            6.42 MB
    </td>
    <td data-sort-value="09/04/2026">
                09/04/2026
    </td>
</tr>
<tr scope="row">
    <th scope="row" role="rowheader" data-sort-value="Nationwide Encounters Dataset FY23-FY26 by Area of Responsibility - July">
    <a href="/sites/default/files/2026-08/nationwide-encounters-fy23-fy26-jul-aor.csv">
    Nationwide Encounters Dataset FY23-FY26 by Area of Responsibility - July
    </a>
    </th>
    <td data-sort-value="CSV">CSV</td>
    <td data-sort-value="6616104">
            6.31 MB
    </td>
    <td data-sort-value="08/05/2026">
                08/05/2026
    </td>
</tr>
</tbody></table>
<div class="margin-top-8 margin-bottom-0 "><span class="text-bold">Last Modified: Sep 16, 2026</span></div>
</main></body></html>"""


@contextmanager
def _cbp_mock(rows: str, csv_response: httpx.Response | None = None, page: str = _CBP_PAGE):
    with respx.mock(assert_all_called=False) as mock:
        mock.get(bc.CBP_DATASET_PAGE).mock(return_value=httpx.Response(
            200, text=page, headers={"content-type": "text/html; charset=utf-8"}))
        mock.get(_CBP_CSV_URL).mock(return_value=csv_response or httpx.Response(
            200, text=_CBP_HEADER + "\n" + rows, headers={"content-type": "text/csv"}))
        yield mock


# ---------------------------------------------------------------------------
# CBP: file discovery and calendar mapping
# ---------------------------------------------------------------------------

def test_cbp_newest_aor_file_and_its_dates_come_from_the_dataset_page() -> None:
    assert bc.latest_cbp_file(_CBP_PAGE) == {
        "url": _CBP_CSV_URL,
        "title": "FY23 - FY26 (FYTD) Nationwide Encounters by Area of Responsibility - August",
        "file_date": "2026-09-04",
        "size_bytes": 6730588,
        "newer_disallowed_url": None,  # the FY20-FY23 file under /assets/documents/ is older
    }
    assert bc.cbp_page_last_modified(_CBP_PAGE) == "2026-09-16"
    assert bc.latest_cbp_file("<html><body><p>No links today.</p></body></html>") is None


_INVENTED_DISALLOWED = "/sites/default/files/assets/documents/2026-10/nationwide-encounters-fy23-fy26-sep-aor.csv"


def _page_with_disallowed_september() -> str:
    # Invented link, not on the live page: the newest-looking file, but under
    # /sites/default/files/assets/documents/, which cbp.gov's robots.txt disallows.
    return _CBP_PAGE.replace(
        '<ul class="undecorated list">',
        f'<ul class="undecorated list"><li><a href="{_INVENTED_DISALLOWED}">FY23 - FY26 (FYTD) Nationwide '
        'Encounters by Area of Responsibility - September</a></li>',
    )


def test_cbp_never_picks_a_path_robots_txt_disallows() -> None:
    latest = bc.latest_cbp_file(_page_with_disallowed_september())
    assert latest["url"] == _CBP_CSV_URL
    assert latest["newer_disallowed_url"] == "https://www.cbp.gov" + _INVENTED_DISALLOWED
    # Only disallowed links: nothing is chosen.
    only = '<html><body><a href="%s">September</a></body></html>' % _INVENTED_DISALLOWED
    assert bc.latest_cbp_file(only) is None


async def test_cbp_newer_file_behind_robots_txt_is_named_not_read(fetcher: Fetcher) -> None:
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG, page=_page_with_disallowed_september()) as mock:
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
        urls = [str(call.request.url) for call in mock.calls]  # read inside: respx clears calls on exit
    assert urls == [bc.CBP_DATASET_PAGE, _CBP_CSV_URL]  # nothing under /assets/documents/ was requested
    assert result["latest"]["period"] == "2026-08"
    assert any("robots.txt disallows" in gap and _INVENTED_DISALLOWED in gap for gap in result["data_gaps"])


def test_cbp_fiscal_months_map_to_calendar_months() -> None:
    assert bc.cbp_period(2026, "OCT") == "2025-10"  # FY2026 starts 1 October 2025
    assert bc.cbp_period(2026, "DEC") == "2025-12"
    assert bc.cbp_period(2026, "JAN") == "2026-01"
    assert bc.cbp_period(2023, "SEP") == "2023-09"
    with pytest.raises(ValueError):
        bc.cbp_period(2026, "Sept")


def test_cbp_csv_without_the_expected_columns_is_refused() -> None:
    with pytest.raises(ValueError, match="Encounter Count"):
        bc.parse_cbp_csv("Fiscal Year,Month (abbv),Citizenship\n2026 (FYTD),AUG,MEXICO\n")
    with pytest.raises(ValueError, match="no data rows"):
        bc.parse_cbp_csv(_CBP_HEADER + "\n")


def test_cbp_csv_with_a_byte_order_mark_still_parses() -> None:
    # The live file starts with "Fiscal Year" (no BOM, checked 2026-10-17); a
    # re-saved export often gains one, and the header must still be found.
    parsed = bc.parse_cbp_csv("\ufeff" + _CBP_HEADER + "\n" + _CBP_NORTHERN_USBP_AUG)
    assert (parsed["rows"], parsed["unreadable_rows"]) == (167, 0)
    by_period = Counter()
    for cell in parsed["cells"]:
        by_period[parsed["periods"][cell[0]]] += cell[-1]
    assert by_period == {"2025-08": 586, "2026-08": 563}


# ---------------------------------------------------------------------------
# CBP: real figures
# ---------------------------------------------------------------------------

async def test_cbp_northern_border_patrol_matches_cbp_published_apprehensions(fetcher: Fetcher) -> None:
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG) as mock:
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
        requests = [call.request for call in mock.calls]  # read inside: respx clears calls on exit
    assert "degraded" not in result and "error" not in result
    assert result["stale"] is False
    assert result["selection"] == {"region": "Northern Land Border", "component": "U.S. Border Patrol",
                                   "citizenship": None}
    latest = result["latest"]
    assert latest["period"] == "2026-08"
    assert (latest["period_start"], latest["period_end"]) == ("2026-08-01", "2026-08-31")
    assert latest["fiscal_year"] == 2026
    assert latest["encounters"] == 563  # CBP FY2026 page, Northern Border Total Apprehensions, Aug-26
    assert latest["same_month_prior_year"] == 586  # CBP FY2025 page, Aug-25
    assert latest["change_vs_prior_year"] == -23
    assert latest["pct_change_vs_prior_year"] == -3.9
    month = result["months"][-1]
    assert month["by_encounter_type"] == {"Apprehensions": 563}
    assert month["by_demographic"] == {"Single Adults": 522, "FMUA": 34, "UAC": 7}
    top = result["top_citizenships_latest_month"]
    assert [(c["citizenship"], c["encounters"]) for c in top[:3]] == [
        ("MEXICO", 191), ("HONDURAS", 73), ("GUATEMALA", 70)]
    assert top[0]["share_pct"] == 33.9
    assert result["other_citizenships_latest_month"] == 54  # CBP's OTHER bucket, never ranked
    assert all(c["citizenship"] != "OTHER" for c in top)

    dataset = result["dataset"]
    assert dataset["url"] == _CBP_CSV_URL
    assert dataset["file_date"] == "2026-09-04"
    assert dataset["page_last_modified"] == "2026-09-16"
    assert dataset["coverage"] == {"first_period": "2025-08", "last_period": "2026-08"}
    assert result["measure_kind"] == "flow"
    assert result["unit"].startswith("encounters")
    assert any("not unique people" in n for n in result["notes"])
    assert any("Data is extracted from live CBP systems" in c for c in result["publisher_caveats"])
    # The slice holds two months a year apart: the eleven between are named, never filled with zeros.
    assert len(result["data_gaps"]) == 1
    assert result["data_gaps"][0].startswith(
        "11 month(s) between 2025-08 and 2026-08 have no rows in CBP's file and are not listed: 2025-09, ")
    assert [m["period"] for m in result["months"]] == ["2025-08", "2026-08"]
    # Two requests (the page, then the file), each with an honest, identifying User-Agent.
    assert [str(r.url) for r in requests] == [bc.CBP_DATASET_PAGE, _CBP_CSV_URL]
    assert all(r.headers["user-agent"] == bc.USER_AGENT for r in requests)
    assert "github.com/marc-shade/world-intel-mcp" in bc.USER_AGENT


async def test_cbp_regions_citizenships_and_the_other_bucket(fetcher: Fetcher) -> None:
    with _cbp_mock(_CBP_FEATURE_ROWS):
        nation = await bc.fetch_us_border_encounters(fetcher)
        south = await bc.fetch_us_border_encounters(fetcher, region="southwest")
        other = await bc.fetch_us_border_encounters(fetcher, region="other")
        mexico = await bc.fetch_us_border_encounters(fetcher, citizenship="mexico")
        china = await bc.fetch_us_border_encounters(fetcher, citizenship="China, Peoples Republic of")
        nowhere = await bc.fetch_us_border_encounters(fetcher, citizenship="Atlantis")

    latest = nation["latest"]
    assert (latest["period"], latest["encounters"]) == ("2026-08", 98)
    assert latest["same_month_prior_year"] == 123 and latest["change_vs_prior_year"] == -25
    month = nation["months"][-1]
    assert month["by_component"] == {"U.S. Border Patrol": 86, "Office of Field Operations": 12}
    assert month["by_encounter_type"] == {"Apprehensions": 86, "Inadmissibles": 12}
    assert month["by_demographic"] == {"Single Adults": 92, "Accompanied Minors": 5, "UAC": 1}
    assert [c["citizenship"] for c in nation["top_citizenships_latest_month"]] == [
        "MEXICO", "CHINA, PEOPLES REPUBLIC OF", "INDIA", "GUATEMALA"]
    assert nation["other_citizenships_latest_month"] == 3

    assert south["latest"]["encounters"] == 93
    assert other["latest"]["encounters"] == 5
    assert [(m["period"], m["encounters"]) for m in mexico["months"]] == [
        ("2022-10", 700), ("2023-09", 361), ("2025-08", 123), ("2025-10", 105), ("2026-08", 86)]
    assert mexico["months"][0]["by_encounter_type"] == {"Expulsions": 700}  # Title 42
    assert mexico["months"][3]["fiscal_year"] == 2026  # October 2025 is FY2026
    assert "top_citizenships_latest_month" not in mexico
    assert china["selection"]["citizenship"] == "CHINA, PEOPLES REPUBLIC OF"
    assert china["latest"]["encounters"] == 6
    # A citizenship CBP does not publish is a usage error that names OTHER, never a zero.
    assert "OTHER" in nowhere["error"] and "degraded" not in nowhere and "latest" not in nowhere

    years = {f["fiscal_year"]: f for f in nation["fiscal_years"]}
    assert years[2023] == {"fiscal_year": 2023, "period_start": "2022-10-01", "period_end": "2023-09-30",
                           "months_reported": 2, "complete": False, "fiscal_year_to_date": False,
                           "encounters": 1061}
    assert years[2026]["fiscal_year_to_date"] is True and years[2026]["encounters"] == 203
    # FY2025 has no October row here, so a same-months comparison would be partial: none is made.
    assert years[2026]["prior_fiscal_year_same_months"] is None


async def test_cbp_month_without_rows_inside_coverage_is_zero_and_after_it_absent(fetcher: Fetcher) -> None:
    """CBP's file lists only non-zero cells (no 0 among its 52,986 counts), so a
    covered month with no row for a selection is 0; months after the newest
    published month are never invented."""
    with _cbp_mock(_CBP_FEATURE_ROWS):
        india = await bc.fetch_us_border_encounters(fetcher, citizenship="INDIA")
    assert [(m["period"], m["encounters"]) for m in india["months"]] == [
        ("2022-10", 0), ("2023-09", 0), ("2025-08", 0), ("2025-10", 0), ("2026-08", 2)]
    assert india["latest"]["period"] == "2026-08"


# ---------------------------------------------------------------------------
# CBP: failure is never a zero
# ---------------------------------------------------------------------------

async def test_cbp_outage_is_degraded_never_zero(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with respx.mock:
        respx.get(bc.CBP_DATASET_PAGE).mock(return_value=httpx.Response(503))
        result = await bc.fetch_us_border_encounters(fetcher)
    assert result["degraded"] is True
    assert result["reason"] == "cbp_fetch_failed"
    assert "503" in result["error"]
    for key in ("latest", "months", "fiscal_years", "top_citizenships_latest_month"):
        assert key not in result
    assert _no_numbers(result)


async def test_cbp_outage_after_a_good_fetch_serves_that_copy_marked_stale(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        fresh = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    value, _created = fetcher.cache.get_stale_meta(bc.CBP_CACHE_KEY)
    fetcher.cache.set(bc.CBP_CACHE_KEY, value, ttl_seconds=-1)  # past its TTL, still on disk

    with respx.mock:
        respx.get(bc.CBP_DATASET_PAGE).mock(return_value=httpx.Response(503))
        stale = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert fresh["stale"] is False
    assert stale["stale"] is True and stale["stale_age_seconds"] >= 0
    assert stale["latest"]["encounters"] == 563  # the last good figure, not 0
    assert any("503" in gap for gap in stale["data_gaps"])


async def test_cbp_html_in_place_of_the_csv_is_degraded(fetcher: Fetcher) -> None:
    denied = httpx.Response(200, text="<html><body><h1>Access Denied</h1></body></html>",
                            headers={"content-type": "text/html"})
    with _cbp_mock("", csv_response=denied):
        result = await bc.fetch_us_border_encounters(fetcher)
    assert result["degraded"] is True
    assert result["reason"] == "cbp_unexpected_format"
    assert "latest" not in result and _no_numbers(result)


async def test_cbp_caveat_missing_from_the_live_page_is_flagged(fetcher: Fetcher) -> None:
    page = _CBP_PAGE.replace("Final statistics are available at the conclusion of each fiscal year.", "")
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG, page=page):
        result = await bc.fetch_us_border_encounters(fetcher, region="northern", component="usbp")
    assert result["latest"]["encounters"] == 563
    drift = [gap for gap in result["data_gaps"] if "no longer shows this caveat" in gap]
    assert len(drift) == 1 and "Data is extracted from live CBP systems" in drift[0]


async def test_cbp_bad_arguments_are_usage_errors_checked_before_any_fetch(fetcher: Fetcher) -> None:
    with respx.mock(assert_all_called=False):  # no routes: any request would fail the call
        region = await bc.fetch_us_border_encounters(fetcher, region="mars")
        component = await bc.fetch_us_border_encounters(fetcher, component="navy")
    assert "region" in region["error"] and "degraded" not in region
    assert "component" in component["error"] and "degraded" not in component


# ---------------------------------------------------------------------------
# UK: captured shapes
# ---------------------------------------------------------------------------

_UK_ODS_URL = ("https://assets.publishing.service.gov.uk/media/6ad1f5b949ba86d6c953cabd/"
               "16_October_2026_Small_boats_-_time_series.ods")

# GOV.UK Content API item, trimmed to the fields the module reads (values verbatim).
_UK_ITEM = {
    "base_path": "/government/publications/migrants-detected-crossing-the-english-channel-in-small-boats",
    "title": "Small boat activity in the English Channel",
    "document_type": "transparency",
    "schema_name": "publication",
    "first_published_at": "2023-02-01T00:00:00+00:00",
    "public_updated_at": "2026-10-16T13:13:46+01:00",
    "details": {
        "attachments": [
            {"attachment_type": "html", "id": "9561576", "title": "Small boat arrivals: last 7 days",
             "url": "/government/publications/migrants-detected-crossing-the-english-channel-in-small-boats/"
                    "migrants-detected-crossing-the-english-channel-in-small-boats-last-7-days"},
            {"attachment_type": "html", "id": "9561577",
             "title": "Weekly summary of small boat arrivals and preventions",
             "url": "/government/publications/migrants-detected-crossing-the-english-channel-in-small-boats/"
                    "weekly-summary-of-small-boat-arrivals-and-preventions"},
            {"attachment_type": "file", "id": "9561578",
             "content_type": "application/vnd.oasis.opendocument.spreadsheet", "file_size": 76787,
             "filename": "16_October_2026_Small_boats_-_time_series.ods",
             "title": "Migrants detected crossing the English Channel in small boats – time series",
             "url": _UK_ODS_URL},
        ],
        "change_history": [
            {"note": "Daily and weekly figures updated. ", "public_timestamp": "2026-10-16T12:13:46Z"},
            {"note": "Updated with latest statistics.", "public_timestamp": "2026-10-15T12:34:06Z"},
        ],
    },
}

# SB_01, every row from 1 January 2025 to 15 October 2026: date, migrants, boats, uncontrolled, note.
_UK_DAILY = """\
2025-01-01,0,0,0
2025-01-02,0,0,0
2025-01-03,0,0,0
2025-01-04,61,1,0
2025-01-05,0,0,0
2025-01-06,0,0,0
2025-01-07,0,0,0
2025-01-08,0,0,0
2025-01-09,0,0,0
2025-01-10,0,0,0
2025-01-11,67,1,0
2025-01-12,0,0,0
2025-01-13,260,5,0
2025-01-14,0,0,0
2025-01-15,215,4,0
2025-01-16,104,2,0
2025-01-17,183,3,0
2025-01-18,0,0,0
2025-01-19,0,0,0
2025-01-20,0,0,0
2025-01-21,129,2,0
2025-01-22,79,2,0
2025-01-23,0,0,0
2025-01-24,0,0,0
2025-01-25,0,0,0
2025-01-26,0,0,0
2025-01-27,0,0,0
2025-01-28,0,0,0
2025-01-29,0,0,0
2025-01-30,0,0,0
2025-01-31,0,0,0
2025-02-01,47,1,0
2025-02-02,147,3,0
2025-02-03,52,1,0
2025-02-04,0,0,0
2025-02-05,145,2,0
2025-02-06,0,0,0
2025-02-07,0,0,0
2025-02-08,0,0,0
2025-02-09,62,2,0
2025-02-10,3,1,0
2025-02-11,0,0,0
2025-02-12,99,2,0
2025-02-13,0,0,0
2025-02-14,0,0,0
2025-02-15,240,4,0
2025-02-16,0,0,0
2025-02-17,0,0,0
2025-02-18,0,0,0
2025-02-19,0,0,0
2025-02-20,0,0,0
2025-02-21,0,0,0
2025-02-22,0,0,0
2025-02-23,0,0,0
2025-02-24,0,0,0
2025-02-25,163,3,0
2025-02-26,0,0,0
2025-02-27,0,0,0
2025-02-28,0,0,0
2025-03-01,68,1,0
2025-03-02,592,11,0
2025-03-03,182,3,0
2025-03-04,326,6,0
2025-03-05,210,3,0
2025-03-06,235,4,0
2025-03-07,54,1,0
2025-03-08,174,3,0
2025-03-09,237,4,0
2025-03-10,261,5,0
2025-03-11,0,0,0
2025-03-12,0,0,0
2025-03-13,0,0,0
2025-03-14,0,0,0
2025-03-15,0,0,0
2025-03-16,0,0,0
2025-03-17,0,0,0
2025-03-18,0,0,0
2025-03-19,289,5,0
2025-03-20,341,6,0
2025-03-21,246,5,0
2025-03-22,241,4,0
2025-03-23,335,6,0
2025-03-24,73,1,0
2025-03-25,129,2,0
2025-03-26,357,6,0
2025-03-27,236,4,0
2025-03-28,0,0,0
2025-03-29,0,0,0
2025-03-30,0,0,0
2025-03-31,0,0,0
2025-04-01,0,0,0
2025-04-02,0,0,0
2025-04-03,0,0,0
2025-04-04,154,4,0
2025-04-05,0,0,0
2025-04-06,0,0,0
2025-04-07,0,0,0
2025-04-08,432,8,0
2025-04-09,0,0,0
2025-04-10,0,0,0
2025-04-11,180,3,0
2025-04-12,656,11,0
2025-04-13,0,0,0
2025-04-14,119,2,0
2025-04-15,705,12,0
2025-04-16,0,0,0
2025-04-17,211,3,0
2025-04-18,253,5,0
2025-04-19,24,1,0
2025-04-20,0,0,0
2025-04-21,184,3,0
2025-04-22,63,1,0
2025-04-23,15,1,0
2025-04-24,0,0,0
2025-04-25,0,0,0
2025-04-26,0,0,0
2025-04-27,247,5,0
2025-04-28,473,8,0
2025-04-29,422,8,0
2025-04-30,294,5,0
2025-05-01,226,6,0
2025-05-02,216,4,0
2025-05-03,0,0,0
2025-05-04,0,0,0
2025-05-05,0,0,0
2025-05-06,0,0,0
2025-05-07,0,0,0
2025-05-08,0,0,0
2025-05-09,0,0,0
2025-05-10,58,1,0
2025-05-11,232,4,0
2025-05-12,601,10,0
2025-05-13,292,5,0
2025-05-14,0,0,0
2025-05-15,0,0,0
2025-05-16,0,0,0
2025-05-17,0,0,0
2025-05-18,0,0,0
2025-05-19,0,0,0
2025-05-20,49,1,0
2025-05-21,825,13,0
2025-05-22,0,0,0
2025-05-23,44,1,0
2025-05-24,0,0,0
2025-05-25,0,0,0
2025-05-26,0,0,0
2025-05-27,0,0,0
2025-05-28,0,0,0
2025-05-29,0,0,0
2025-05-30,0,0,0
2025-05-31,1195,19,0
2025-06-01,0,0,0
2025-06-02,0,0,0
2025-06-03,0,0,0
2025-06-04,0,0,0
2025-06-05,0,0,0
2025-06-06,0,0,0
2025-06-07,0,0,0
2025-06-08,0,0,0
2025-06-09,0,0,0
2025-06-10,0,0,0
2025-06-11,400,6,0
2025-06-12,52,1,0
2025-06-13,919,14,0
2025-06-14,134,2,0
2025-06-15,0,0,0
2025-06-16,228,4,0
2025-06-17,489,8,0
2025-06-18,244,4,0
2025-06-19,102,2,0
2025-06-20,437,7,0
2025-06-21,583,9,0
2025-06-22,0,0,0
2025-06-23,0,0,0
2025-06-24,0,0,0
2025-06-25,118,2,0
2025-06-26,0,0,0
2025-06-27,0,0,0
2025-06-28,0,0,0
2025-06-29,585,8,0
2025-06-30,879,13,0
2025-07-01,440,8,0
2025-07-02,178,3,0
2025-07-03,0,0,0
2025-07-04,517,8,0
2025-07-05,0,0,0
2025-07-06,0,0,0
2025-07-07,0,0,0
2025-07-08,0,0,0
2025-07-09,0,0,0
2025-07-10,573,10,0
2025-07-11,353,5,0
2025-07-12,317,6,0
2025-07-13,132,2,0
2025-07-14,12,1,0
2025-07-15,0,0,0
2025-07-16,0,0,0
2025-07-17,398,6,0
2025-07-18,419,6,0
2025-07-19,153,3,0
2025-07-20,60,1,0
2025-07-21,0,0,0
2025-07-22,0,0,0
2025-07-23,132,2,0
2025-07-24,225,4,0
2025-07-25,68,1,0
2025-07-26,122,2,0
2025-07-27,96,2,0
2025-07-28,212,3,0
2025-07-29,149,2,0
2025-07-30,898,13,0
2025-07-31,0,0,0
2025-08-01,0,0,0
2025-08-02,0,0,0
2025-08-03,0,0,0
2025-08-04,0,0,0
2025-08-05,0,0,0
2025-08-06,155,2,0
2025-08-07,248,5,0
2025-08-08,0,0,0
2025-08-09,435,7,0
2025-08-10,281,5,0
2025-08-11,474,8,0
2025-08-12,445,6,0
2025-08-13,325,4,0
2025-08-14,82,1,0
2025-08-15,116,2,0
2025-08-16,0,0,0
2025-08-17,0,0,0
2025-08-18,0,0,0
2025-08-19,0,0,0
2025-08-20,0,0,0
2025-08-21,0,0,0
2025-08-22,79,1,0
2025-08-23,0,0,0
2025-08-24,212,4,0
2025-08-25,659,9,0
2025-08-26,56,2,0
2025-08-27,0,0,0
2025-08-28,0,0,0
2025-08-29,0,0,0
2025-08-30,0,0,0
2025-08-31,0,0,0
2025-09-01,0,0,0
2025-09-02,0,0,0
2025-09-03,0,0,0
2025-09-04,0,0,0
2025-09-05,0,0,0
2025-09-06,1101,18,1,Revised
2025-09-07,65,1,0
2025-09-08,0,0,0
2025-09-09,674,9,0
2025-09-10,188,3,0
2025-09-11,0,0,0
2025-09-12,0,0,0
2025-09-13,0,0,0
2025-09-14,0,0,0
2025-09-15,0,0,0
2025-09-16,0,0,0
2025-09-17,0,0,0
2025-09-18,0,0,0
2025-09-19,1072,13,0
2025-09-20,85,1,0
2025-09-21,0,0,0
2025-09-22,0,0,0
2025-09-23,0,0,0
2025-09-24,0,0,0
2025-09-25,0,0,0
2025-09-26,0,0,0
2025-09-27,895,12,0
2025-09-28,403,7,0
2025-09-29,70,1,0
2025-09-30,531,7,0
2025-10-01,138,3,0
2025-10-02,176,3,0
2025-10-03,0,0,0
2025-10-04,0,0,0
2025-10-05,0,0,0
2025-10-06,0,0,0
2025-10-07,0,0,0
2025-10-08,1075,15,0
2025-10-09,27,1,0
2025-10-10,557,7,0
2025-10-11,184,3,0
2025-10-12,89,1,0
2025-10-13,32,1,0
2025-10-14,0,0,0
2025-10-15,0,0,0
2025-10-16,0,0,0
2025-10-17,0,0,0
2025-10-18,369,7,0
2025-10-19,0,0,0
2025-10-20,0,0,0
2025-10-21,0,0,0
2025-10-22,220,3,0
2025-10-23,0,0,0
2025-10-24,0,0,0
2025-10-25,0,0,0
2025-10-26,0,0,0
2025-10-27,0,0,0
2025-10-28,0,0,0
2025-10-29,0,0,0
2025-10-30,0,0,0
2025-10-31,0,0,0
2025-11-01,0,0,0
2025-11-02,0,0,0
2025-11-03,0,0,0
2025-11-04,0,0,0
2025-11-05,0,0,0
2025-11-06,621,9,0
2025-11-07,648,9,0
2025-11-08,503,7,0
2025-11-09,349,5,0
2025-11-10,0,0,0
2025-11-11,0,0,0
2025-11-12,0,0,0
2025-11-13,0,0,0
2025-11-14,217,3,0
2025-11-15,0,0,0
2025-11-16,0,0,0
2025-11-17,0,0,0
2025-11-18,0,0,0
2025-11-19,0,0,0
2025-11-20,0,0,0
2025-11-21,0,0,0
2025-11-22,0,0,0
2025-11-23,0,0,0
2025-11-24,0,0,0
2025-11-25,0,0,0
2025-11-26,0,0,0
2025-11-27,0,0,0
2025-11-28,0,0,0
2025-11-29,0,0,0
2025-11-30,0,0,0
2025-12-01,0,0,0
2025-12-02,0,0,0
2025-12-03,0,0,0
2025-12-04,0,0,0
2025-12-05,0,0,0
2025-12-06,0,0,0
2025-12-07,0,0,0
2025-12-08,0,0,0
2025-12-09,0,0,0
2025-12-10,0,0,0
2025-12-11,0,0,0
2025-12-12,0,0,0
2025-12-13,737,11,0
2025-12-14,52,1,0
2025-12-15,0,0,0
2025-12-16,74,1,0
2025-12-17,497,9,0
2025-12-18,0,0,0
2025-12-19,0,0,0
2025-12-20,803,13,0
2025-12-21,0,0,0
2025-12-22,17,1,0
2025-12-23,0,0,0
2025-12-24,0,0,0
2025-12-25,0,0,0
2025-12-26,0,0,0
2025-12-27,0,0,0
2025-12-28,0,0,0
2025-12-29,0,0,0
2025-12-30,0,0,0
2025-12-31,0,0,0
2026-01-01,0,0,0
2026-01-02,0,0,0
2026-01-03,0,0,0
2026-01-04,0,0,0
2026-01-05,32,1,0
2026-01-06,0,0,0
2026-01-07,0,0,0
2026-01-08,0,0,0
2026-01-09,0,0,0
2026-01-10,0,0,0
2026-01-11,0,0,0
2026-01-12,0,0,0
2026-01-13,0,0,0
2026-01-14,171,3,0
2026-01-15,0,0,0
2026-01-16,0,0,0
2026-01-17,317,5,0
2026-01-18,138,2,0
2026-01-19,275,4,0
2026-01-20,0,0,0
2026-01-21,0,0,0
2026-01-22,0,0,0
2026-01-23,0,0,0
2026-01-24,0,0,0
2026-01-25,0,0,0
2026-01-26,0,0,0
2026-01-27,0,0,0
2026-01-28,0,0,0
2026-01-29,0,0,0
2026-01-30,0,0,0
2026-01-31,0,0,0
2026-02-01,0,0,0
2026-02-02,56,1,0
2026-02-03,0,0,0
2026-02-04,0,0,0
2026-02-05,0,0,0
2026-02-06,0,0,0
2026-02-07,0,0,0
2026-02-08,219,3,0
2026-02-09,322,5,0
2026-02-10,0,0,0
2026-02-11,0,0,0
2026-02-12,0,0,0
2026-02-13,0,0,0
2026-02-14,0,0,0
2026-02-15,0,0,0
2026-02-16,0,0,0
2026-02-17,0,0,0
2026-02-18,0,0,0
2026-02-19,0,0,0
2026-02-20,0,0,0
2026-02-21,0,0,0
2026-02-22,0,0,0
2026-02-23,0,0,0
2026-02-24,74,1,0
2026-02-25,605,10,0
2026-02-26,0,0,0
2026-02-27,0,0,0
2026-02-28,0,0,0
2026-03-01,0,0,0
2026-03-02,0,0,0
2026-03-03,204,3,0
2026-03-04,275,4,0
2026-03-05,66,1,0
2026-03-06,157,3,0
2026-03-07,114,2,0
2026-03-08,75,1,0
2026-03-09,309,4,0
2026-03-10,0,0,0
2026-03-11,0,0,0
2026-03-12,0,0,0
2026-03-13,0,0,0
2026-03-14,0,0,0
2026-03-15,48,1,0
2026-03-16,0,0,0
2026-03-17,0,0,0
2026-03-18,262,4,0
2026-03-19,144,2,0
2026-03-20,116,3,0
2026-03-21,0,0,0
2026-03-22,190,3,0
2026-03-23,272,4,0
2026-03-24,0,0,0
2026-03-25,0,0,0
2026-03-26,0,0,0
2026-03-27,0,0,0
2026-03-28,0,0,0
2026-03-29,0,0,0
2026-03-30,0,0,0
2026-03-31,0,0,0
2026-04-01,325,5,0
2026-04-02,0,0,0
2026-04-03,0,0,0
2026-04-04,0,0,0
2026-04-05,0,0,0
2026-04-06,0,0,0
2026-04-07,137,2,0
2026-04-08,159,2,0
2026-04-09,74,1,0
2026-04-10,0,0,0
2026-04-11,0,0,0
2026-04-12,0,0,0
2026-04-13,201,3,0
2026-04-14,75,1,0
2026-04-15,0,0,0
2026-04-16,0,0,0
2026-04-17,63,1,0
2026-04-18,602,9,0
2026-04-19,0,0,0
2026-04-20,0,0,0
2026-04-21,0,0,0
2026-04-22,0,0,0
2026-04-23,0,0,0
2026-04-24,0,0,0
2026-04-25,122,2,0
2026-04-26,127,2,0
2026-04-27,90,2,0
2026-04-28,0,0,0
2026-04-29,0,0,0
2026-04-30,0,0,0
2026-05-01,55,1,0
2026-05-02,325,6,0
2026-05-03,422,6,0
2026-05-04,92,2,0
2026-05-05,0,0,0
2026-05-06,0,0,0
2026-05-07,0,0,0
2026-05-08,70,1,0
2026-05-09,196,3,0
2026-05-10,0,0,0
2026-05-11,0,0,0
2026-05-12,0,0,0
2026-05-13,0,0,0
2026-05-14,0,0,0
2026-05-15,0,0,0
2026-05-16,0,0,0
2026-05-17,0,0,0
2026-05-18,0,0,0
2026-05-19,0,0,0
2026-05-20,0,0,0
2026-05-21,0,0,0
2026-05-22,394,6,0
2026-05-23,287,4,0
2026-05-24,231,3,0
2026-05-25,77,1,0
2026-05-26,139,2,0
2026-05-27,74,1,0
2026-05-28,0,0,0
2026-05-29,128,2,0
2026-05-30,223,3,0
2026-05-31,13,1,0
2026-06-01,0,0,0
2026-06-02,0,0,0
2026-06-03,0,0,0
2026-06-04,0,0,0
2026-06-05,0,0,0
2026-06-06,0,0,0
2026-06-07,0,0,0
2026-06-08,0,0,0
2026-06-09,0,0,0
2026-06-10,0,0,0
2026-06-11,0,0,0
2026-06-12,0,0,0
2026-06-13,0,0,0
2026-06-14,0,0,0
2026-06-15,710,11,0
2026-06-16,0,0,0
2026-06-17,0,0,0
2026-06-18,392,6,0
2026-06-19,406,5,0
2026-06-20,94,1,0
2026-06-21,310,5,0
2026-06-22,27,1,0
2026-06-23,186,3,0
2026-06-24,221,3,0
2026-06-25,150,2,0
2026-06-26,0,0,0
2026-06-27,105,1,0
2026-06-28,78,1,0
2026-06-29,0,0,0
2026-06-30,63,1,0
2026-07-01,0,0,0
2026-07-02,0,0,0
2026-07-03,0,0,0
2026-07-04,0,0,0
2026-07-05,0,0,0
2026-07-06,0,0,0
2026-07-07,0,0,0
2026-07-08,249,4,0
2026-07-09,81,1,0
2026-07-10,255,3,0
2026-07-11,0,0,0
2026-07-12,0,0,0
2026-07-13,0,0,0
2026-07-14,0,0,0
2026-07-15,0,0,0
2026-07-16,0,0,0
2026-07-17,0,0,0
2026-07-18,0,0,0
2026-07-19,0,0,0
2026-07-20,0,0,0
2026-07-21,0,0,0
2026-07-22,172,3,0
2026-07-23,366,3,0
2026-07-24,103,2,0
2026-07-25,240,5,0
2026-07-26,0,0,0
2026-07-27,0,0,0
2026-07-28,26,1,0
2026-07-29,752,9,0
2026-07-30,72,1,0
2026-07-31,0,0,0
2026-08-01,326,4,0
2026-08-02,0,0,0
2026-08-03,35,1,0
2026-08-04,0,0,0
2026-08-05,0,0,0
2026-08-06,0,0,0
2026-08-07,0,0,0
2026-08-08,258,4,0
2026-08-09,423,5,0
2026-08-10,230,1,0
2026-08-11,0,0,0
2026-08-12,0,0,0
2026-08-13,0,0,0
2026-08-14,93,1,0
2026-08-15,82,1,0
2026-08-16,0,0,0
2026-08-17,250,3,0
2026-08-18,0,0,0
2026-08-19,0,0,0
2026-08-20,0,0,0
2026-08-21,0,0,0
2026-08-22,0,0,0
2026-08-23,0,0,0
2026-08-24,0,0,0
2026-08-25,0,0,0
2026-08-26,0,0,0
2026-08-27,494,7,0
2026-08-28,122,3,0
2026-08-29,0,0,0
2026-08-30,0,0,0
2026-08-31,0,0,0
2026-09-01,0,0,0
2026-09-02,0,0,0
2026-09-03,0,0,0
2026-09-04,0,0,0
2026-09-05,0,0,0
2026-09-06,625,8,0
2026-09-07,143,2,0
2026-09-08,0,0,0
2026-09-09,0,0,0
2026-09-10,0,0,0
2026-09-11,0,0,0
2026-09-12,0,0,0
2026-09-13,0,0,0
2026-09-14,144,2,0
2026-09-15,55,1,0
2026-09-16,0,0,0
2026-09-17,0,0,0
2026-09-18,0,0,0
2026-09-19,0,0,0
2026-09-20,0,0,0
2026-09-21,150,3,1
2026-09-22,154,3,1
2026-09-23,781,10,0
2026-09-24,44,1,0
2026-09-25,0,0,0
2026-09-26,0,0,0
2026-09-27,526,7,0
2026-09-28,0,0,0
2026-09-29,61,1,0
2026-09-30,0,0,0
2026-10-01,0,0,0
2026-10-02,91,1,0
2026-10-03,164,2,0
2026-10-04,418,5,0
2026-10-05,609,8,0
2026-10-06,338,5,0
2026-10-07,119,2,0
2026-10-08,0,0,0
2026-10-09,0,0,0
2026-10-10,0,0,0
2026-10-11,0,0,0
2026-10-12,0,0,0
2026-10-13,0,0,0
2026-10-14,0,0,0
2026-10-15,0,0,0
"""

# SB_02 rows: week ending, migrants, boats, uncontrolled, migrants prevented, events prevented.
_UK_WEEKLY = """\
2024-04-28,902,17,0,-,-
2024-05-05,1409,28,0,849,30
2026-08-23,250,3,0,136,3
2026-08-30,616,10,0,339,13
2026-09-06,625,8,0,282,10
2026-09-13,143,2,0,100,2
2026-09-20,199,3,0,40,5
2026-09-27,1655,24,2,860,29
2026-10-04,734,9,0,475,17
2026-10-11,1066,15,0,245,12"""

_DAILY_HEADER = ["Date", "Migrants arrived", "Boats arrived",
                 "Boats arrived - involved in uncontrolled landings", "Notes"]
_WEEKLY_HEADER = ["Week ending", "Migrants arrived", "Boats arrived",
                  "Boats arrived - involved in uncontrolled landings", "Migrants prevented",
                  "Events prevented", "Notes"]

# Notes sheet: title row and notes 1, 6 and 7, row markup verbatim from the live content.xml.
_UK_NOTES_ROWS = (
    '<table:table-row table:style-name="ro8"><table:table-cell office:value-type="string" table:style-name="ce10"><text:p>Notes</text:p></table:table-cell><table:table-cell table:number-columns-repeated="15" table:style-name="ce11"/><table:table-cell table:number-columns-repeated="16368" table:style-name="ce4"/></table:table-row>'
    '<table:table-row table:style-name="ro9"><table:table-cell office:value-type="string" table:style-name="ce12"><text:p>1. These statistics are taken from provisional operational data and are subject to change. They may differ from figures published in the final official statistics on Illegal entry routes <text:s/>to the UK, which will have undergone a more thorough quality assurance process.<text:s/></text:p><text:p>The data may also differ from daily figures previously published by the Ministry of Defence, due to the point at which arrivals are counted and differences in the counting of ‘boats’ compared to ‘events’. This means that total arrivals over long time periods should be similar between the two sources, but Home Office figures for number of boats detected will be higher than the Ministry of Defence’s figures where boats arriving together would be counted as one. The quarterly Illegal entry routes <text:s/>to the UK statistical release presents final and authoritative statistics on small boat arrivals.</text:p></table:table-cell><table:table-cell table:number-columns-repeated="9" table:style-name="ce13"/><table:table-cell table:number-columns-repeated="5" table:style-name="ce14"/><table:table-cell table:number-columns-repeated="16369" table:style-name="ce13"/></table:table-row>'
    '<table:table-row table:style-name="ro11"><table:table-cell office:value-type="string" table:style-name="ce15"><text:p>6. The data in table SB_01 covers arrivals only, and relates to daily totals. The data in table SB_02 covers both arrivals and preventions, and relates to weekly totals - the data is shown on the last day of the week period, but the activity may have occurred on any day within that week period.<text:s/></text:p></table:table-cell><table:table-cell table:number-columns-repeated="9" table:style-name="ce16"/><table:table-cell table:number-columns-repeated="5" table:style-name="ce17"/><table:table-cell table:style-name="ce11"/><table:table-cell table:number-columns-repeated="16368" table:style-name="ce4"/></table:table-row>'
    '<table:table-row table:style-name="ro5"><table:table-cell office:value-type="string" table:style-name="ce15"><text:p>7. Data on preventions are operational estimates collected and provided to the UK by French authorities.<text:s/></text:p></table:table-cell><table:table-cell table:number-columns-repeated="9" table:style-name="ce16"/><table:table-cell table:number-columns-repeated="5" table:style-name="ce17"/><table:table-cell table:style-name="ce11"/><table:table-cell table:number-columns-repeated="16368" table:style-name="ce4"/></table:table-row>'
)

_ODS_NS = ('xmlns:table="urn:oasis:names:tc:opendocument:xmlns:table:1.0" '
           'xmlns:office="urn:oasis:names:tc:opendocument:xmlns:office:1.0" '
           'xmlns:text="urn:oasis:names:tc:opendocument:xmlns:text:1.0"')
_BLANK_TAIL = ('<table:table-row table:number-rows-repeated="1048553" table:style-name="ro15">'
               '<table:table-cell table:number-columns-repeated="16384"/></table:table-row>')


def _cell(value: str) -> str:
    """One cell in the live file's markup: dates, floats, and strings ("-", "Revised")."""
    if len(value) == 10 and value[4] == "-" and value[7] == "-":
        shown = date.fromisoformat(value).strftime("%d/%m/%Y")
        return (f'<table:table-cell office:value-type="date" office:date-value="{value}T00:00:00" '
                f'table:style-name="ce27"><text:p>{shown}</text:p></table:table-cell>')
    if value.isdigit():
        return (f'<table:table-cell office:value-type="float" office:value="{value}" '
                f'table:style-name="ce11"><text:p>{value}</text:p></table:table-cell>')
    return (f'<table:table-cell office:value-type="string" table:style-name="ce28">'
            f'<text:p>{value}</text:p></table:table-cell>')


def _sheet(name: str, header: list[str], lines: list[str]) -> str:
    """A data sheet as the live file writes it: each row padded to 16,384 columns
    by one repeated blank cell, the sheet padded to 1,048,576 rows by one
    repeated blank row (1,045,365 of them after SB_01's 3,211 live rows)."""
    rows = [header] + [line.split(",") for line in lines]
    xml = []
    for i, values in enumerate(rows):
        cells = "".join(_cell(v) for v in values)
        xml.append(f'<table:table-row table:style-name="{"ro10" if i == 0 else "ro16"}">{cells}'
                   f'<table:table-cell table:number-columns-repeated="{16384 - len(values)}" '
                   'table:style-name="ce11"/></table:table-row>')
    tail = (f'<table:table-row table:number-rows-repeated="{1048576 - len(rows)}" table:style-name="ro16">'
            '<table:table-cell table:number-columns-repeated="16384"/></table:table-row>')
    return (f'<table:table table:name="{name}" table:style-name="ta1">'
            '<table:table-column table:style-name="co6" table:default-cell-style-name="ce27"/>'
            + "".join(xml) + tail + "</table:table>")


def _ods_bytes(tables_xml: str) -> bytes:
    content = ('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
               f'<office:document-content {_ODS_NS} office:version="1.4"><office:body>'
               f'<office:spreadsheet>{tables_xml}</office:spreadsheet></office:body></office:document-content>')
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("mimetype", "application/vnd.oasis.opendocument.spreadsheet")  # stored, first
        zf.writestr("content.xml", content, compress_type=zipfile.ZIP_DEFLATED)
    return buf.getvalue()


def _ods(daily: list[str], weekly: list[str], notes: str = _UK_NOTES_ROWS) -> bytes:
    notes_sheet = f'<table:table table:name="Notes" table:style-name="ta1">{notes}{_BLANK_TAIL}</table:table>'
    return _ods_bytes(notes_sheet + _sheet("SB_01", _DAILY_HEADER, daily)
                      + _sheet("SB_02", _WEEKLY_HEADER, weekly))


def _main_ods() -> bytes:
    return _ods(_UK_DAILY.splitlines(), _UK_WEEKLY.splitlines())


@contextmanager
def _uk_mock(ods: bytes, item: dict | None = None):
    with respx.mock(assert_all_called=False) as mock:
        mock.get(bc.UK_CONTENT_API).mock(return_value=httpx.Response(200, json=item or _UK_ITEM))
        mock.get(_UK_ODS_URL).mock(return_value=httpx.Response(
            200, content=ods, headers={"content-type": "application/vnd.oasis.opendocument.spreadsheet"}))
        yield mock


# ---------------------------------------------------------------------------
# UK: the ODS reader
# ---------------------------------------------------------------------------

def test_read_ods_tables_keeps_positions_and_never_expands_blank_repeats() -> None:
    xml = ('<table:table table:name="T"><table:table-row>'
           '<table:table-cell table:number-columns-repeated="2"/>'
           '<table:table-cell office:value-type="float" office:value="7"><text:p>7</text:p></table:table-cell>'
           '<table:table-cell office:value-type="string" table:number-columns-repeated="2">'
           '<text:p>a<text:s text:c="3"/>b</text:p></table:table-cell>'
           '<table:table-cell table:number-columns-repeated="16379"/>'
           '</table:table-row>' + _BLANK_TAIL + '</table:table>')
    assert bc.read_ods_tables(_ods_bytes(xml)) == {"T": [[None, None, 7.0, "a   b", "a   b"]]}


def test_read_ods_tables_refuses_a_file_that_is_not_an_ods() -> None:
    with pytest.raises(ValueError):
        bc.read_ods_tables(b"<!DOCTYPE html><html><body>Page not found</body></html>")


# ---------------------------------------------------------------------------
# UK: real figures
# ---------------------------------------------------------------------------

async def test_uk_latest_week_and_last_7_days_match_the_home_office_pages(fetcher: Fetcher) -> None:
    with _uk_mock(_main_ods()) as mock:
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
        requests = [call.request for call in mock.calls]  # read inside: respx clears calls on exit
    assert "degraded" not in result and result["stale"] is False
    assert result["latest_week"] == {
        "week_ending": "2026-10-11", "period_start": "2026-10-05", "period_end": "2026-10-11",
        "migrants_arrived": 1066, "boats_arrived": 15, "boats_uncontrolled_landings": 0,
        "migrants_prevented": 245, "events_prevented": 12, "revised": False}
    assert result["last_7_days"] == {
        "period_start": "2026-10-09", "period_end": "2026-10-15", "days_reported": 7,
        "migrants_arrived": 0, "boats_arrived": 0, "boats_uncontrolled_landings": 0}
    assert result["latest_day"] == {"date": "2026-10-15", "migrants_arrived": 0, "boats_arrived": 0,
                                    "boats_uncontrolled_landings": 0, "revised": False}
    assert [d["date"] for d in result["daily"]][0] == "2026-10-02"
    assert [d["migrants_arrived"] for d in result["daily"]] == [91, 164, 418, 609, 338, 119] + [0] * 8
    assert [w["week_ending"] for w in result["weeks"]] == [
        "2026-08-23", "2026-08-30", "2026-09-06", "2026-09-13", "2026-09-20", "2026-09-27",
        "2026-10-04", "2026-10-11"]

    dataset = result["dataset"]
    assert dataset["url"] == _UK_ODS_URL
    assert dataset["public_updated_at"] == "2026-10-16T13:13:46+01:00"
    assert dataset["latest_change_note"] == "Daily and weekly figures updated."
    assert dataset["daily_data_through"] == "2026-10-15"
    assert dataset["weekly_data_through"] == "2026-10-11"
    assert dataset["file_size_bytes"] == 76787
    assert result["measure_kind"] == "flow"
    assert result["data_gaps"] == []
    assert [str(r.url) for r in requests] == [bc.UK_CONTENT_API, _UK_ODS_URL]
    assert all(r.headers["user-agent"] == bc.USER_AGENT for r in requests)


async def test_uk_months_years_and_year_to_date_are_sums_of_the_daily_table(fetcher: Fetcher) -> None:
    with _uk_mock(_main_ods()):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher, months=14)
    months = {m["period"]: m for m in result["months"]}
    assert [m["period"] for m in result["months"]][0] == "2025-09" and len(result["months"]) == 14
    assert months["2026-10"] == {
        "period": "2026-10", "period_start": "2026-10-01", "period_end": "2026-10-15", "complete": False,
        "days_reported": 15, "days_in_month": 31, "migrants_arrived": 1739, "boats_arrived": 23,
        "boats_uncontrolled_landings": 0}
    assert months["2026-09"]["complete"] is True
    assert (months["2026-09"]["migrants_arrived"], months["2026-09"]["boats_arrived"]) == (2683, 38)
    assert months["2026-09"]["boats_uncontrolled_landings"] == 2
    assert (months["2025-09"]["migrants_arrived"], months["2025-09"]["boats_arrived"]) == (5084, 72)
    assert result["revised_dates"] == ["2025-09-06"]

    years = {y["year"]: y for y in result["years"]}
    assert years[2025] == {"year": 2025, "period_start": "2025-01-01", "period_end": "2025-12-31",
                           "complete": True, "days_reported": 365, "migrants_arrived": 41472,
                           "boats_arrived": 672, "boats_uncontrolled_landings": 1}
    assert years[2026]["complete"] is False and years[2026]["period_end"] == "2026-10-15"
    assert years[2026]["migrants_arrived"] == 20935

    ytd = result["year_to_date"]
    assert (ytd["period_start"], ytd["period_end"]) == ("2026-01-01", "2026-10-15")
    assert (ytd["migrants_arrived"], ytd["boats_arrived"]) == (20935, 305)
    assert ytd["prior_year"] == {"period_start": "2025-01-01", "period_end": "2025-10-15",
                                 "migrants_arrived": 36365, "boats_arrived": 593}
    assert (ytd["change_migrants"], ytd["pct_change_migrants"]) == (-15430, -42.4)
    assert (ytd["change_boats"], ytd["pct_change_boats"]) == (-288, -48.6)


async def test_uk_dash_means_not_collected_and_is_never_zero(fetcher: Fetcher) -> None:
    # Real rows: the series' first days (no uncontrolled-landing figure yet) and
    # the weeks either side of the first French preventions figure.
    ods = _ods(["2018-01-01,0,0,-", "2018-01-02,0,0,-", "2018-01-03,0,0,-"],
               ["2018-01-07,0,0,-,-,-", "2024-04-28,902,17,0,-,-", "2024-05-05,1409,28,0,849,30"])
    with _uk_mock(ods):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["latest_day"] == {"date": "2018-01-03", "migrants_arrived": 0, "boats_arrived": 0,
                                    "boats_uncontrolled_landings": None, "revised": False}
    assert result["months"][-1]["boats_uncontrolled_landings"] is None
    weeks = {w["week_ending"]: w for w in result["weeks"]}
    assert weeks["2018-01-07"]["boats_uncontrolled_landings"] is None
    assert weeks["2024-04-28"]["migrants_prevented"] is None and weeks["2024-04-28"]["events_prevented"] is None
    assert weeks["2024-05-05"]["migrants_prevented"] == 849
    # 2017 is not in the file, so there is nothing to compare 2018 with: no zero baseline.
    assert result["year_to_date"]["prior_year"] is None
    assert result["year_to_date"]["change_migrants"] is None
    assert any("2017" in gap for gap in result["data_gaps"])


async def test_uk_missing_day_makes_sums_partial_and_drops_the_comparison(fetcher: Fetcher) -> None:
    # The live rows with one day (10 March 2025) taken out: a gap in the publisher's series.
    daily = [line for line in _UK_DAILY.splitlines() if not line.startswith("2025-03-10,")]
    with _uk_mock(_ods(daily, _UK_WEEKLY.splitlines())):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher, months=24)
    march = next(m for m in result["months"] if m["period"] == "2025-03")
    assert (march["days_reported"], march["complete"]) == (30, False)
    years = {y["year"]: y for y in result["years"]}
    assert (years[2025]["days_reported"], years[2025]["complete"]) == (364, False)
    # 2025-01-01 to 2025-10-15 is no longer whole, so no like-for-like comparison is made.
    assert result["year_to_date"]["prior_year"] is None
    assert result["year_to_date"]["pct_change_migrants"] is None
    assert result["year_to_date"]["migrants_arrived"] == 20935  # 2026 itself is untouched
    assert any(gap.startswith("1 day(s) between 2025-01-01 and 2026-10-15") for gap in result["data_gaps"])
    assert any("2025-01-01 to 2025-10-15" in gap for gap in result["data_gaps"])


async def test_uk_year_to_date_on_29_february_compares_with_28_february(fetcher: Fetcher) -> None:
    # Invented all-zero rows: only the calendar matters here (the live file ends in October).
    from datetime import timedelta

    start, end = date(2023, 1, 1), date(2024, 2, 29)
    daily = [f"{start + timedelta(days=k)},0,0,0" for k in range((end - start).days + 1)]
    with _uk_mock(_ods(daily, ["2024-02-25,0,0,0,0,0"])):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    ytd = result["year_to_date"]
    assert (ytd["period_start"], ytd["period_end"]) == ("2024-01-01", "2024-02-29")
    assert (ytd["prior_year"]["period_start"], ytd["prior_year"]["period_end"]) == ("2023-01-01", "2023-02-28")
    assert ytd["pct_change_migrants"] is None  # 0 against 0: no percentage, not 0%
    assert ytd["change_migrants"] == 0


async def test_uk_weekly_total_that_disagrees_with_its_days_is_flagged(fetcher: Fetcher) -> None:
    # Deliberately altered: the live SB_02 row says 1066 for the week ending 2026-10-11.
    weekly = _UK_WEEKLY.replace("2026-10-11,1066,", "2026-10-11,1065,").splitlines()
    with _uk_mock(_ods(_UK_DAILY.splitlines(), weekly)):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["latest_week"]["migrants_arrived"] == 1065  # the publisher's weekly figure, as published
    assert result["data_gaps"] == [
        "SB_02 week ending 2026-10-11 has 1065 migrants; the seven SB_01 days sum to 1066."]


async def test_uk_publisher_notes_are_read_from_the_file(fetcher: Fetcher) -> None:
    with _uk_mock(_main_ods()):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    notes = result["publisher_caveats"]
    assert len(notes) == 3
    assert notes[0].startswith("1. These statistics are taken from provisional operational data and are "
                               "subject to change.")
    assert "Illegal entry routes to the UK, which will have undergone" in notes[0]
    assert "\nThe data may also differ from daily figures previously published by the Ministry of Defence" in notes[0]
    assert notes[2] == ("7. Data on preventions are operational estimates collected and provided to the UK "
                        "by French authorities.")


# ---------------------------------------------------------------------------
# UK: failure is never a zero
# ---------------------------------------------------------------------------

_UK_FIGURE_KEYS = ("latest_day", "last_7_days", "daily", "latest_week", "weeks", "months", "years",
                   "year_to_date")


async def test_uk_outage_is_degraded_never_zero(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with respx.mock:
        respx.get(bc.UK_CONTENT_API).mock(side_effect=httpx.ConnectError("connection refused"))
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True
    assert result["reason"] == "uk_fetch_failed"
    assert "ConnectError" in result["error"]
    assert not any(key in result for key in _UK_FIGURE_KEYS)
    assert _no_numbers(result)


async def test_uk_time_series_file_outage_is_degraded(fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with respx.mock:
        respx.get(bc.UK_CONTENT_API).mock(return_value=httpx.Response(200, json=_UK_ITEM))
        respx.get(_UK_ODS_URL).mock(return_value=httpx.Response(404))
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True and result["reason"] == "uk_fetch_failed"
    assert "404" in result["error"]
    assert not any(key in result for key in _UK_FIGURE_KEYS)


async def test_uk_outage_after_a_good_fetch_serves_that_copy_marked_stale(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    _no_sleep(monkeypatch)
    with _uk_mock(_main_ods()):
        fresh = await bc.fetch_uk_small_boat_arrivals(fetcher)
    value, _created = fetcher.cache.get_stale_meta(bc.UK_CACHE_KEY)
    fetcher.cache.set(bc.UK_CACHE_KEY, value, ttl_seconds=-1)
    with respx.mock:
        respx.get(bc.UK_CONTENT_API).mock(return_value=httpx.Response(502))
        stale = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert fresh["stale"] is False
    assert stale["stale"] is True and stale["stale_age_seconds"] >= 0
    assert stale["latest_week"]["migrants_arrived"] == 1066
    assert any("502" in gap for gap in stale["data_gaps"])


async def test_uk_html_in_place_of_the_ods_is_degraded(fetcher: Fetcher) -> None:
    with _uk_mock(b"<!DOCTYPE html><html><body>Page not found</body></html>"):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert not any(key in result for key in _UK_FIGURE_KEYS)


async def test_uk_publication_without_a_time_series_attachment_is_degraded(fetcher: Fetcher) -> None:
    item = copy.deepcopy(_UK_ITEM)
    item["details"]["attachments"] = item["details"]["attachments"][:2]  # the two HTML pages only
    with _uk_mock(b"", item=item):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert "attachment" in result["error"]


async def test_uk_ods_without_the_daily_table_is_degraded(fetcher: Fetcher) -> None:
    ods = _ods_bytes(_sheet("SB_02", _WEEKLY_HEADER, _UK_WEEKLY.splitlines()))
    with _uk_mock(ods):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher)
    assert result["degraded"] is True and result["reason"] == "uk_unexpected_format"
    assert "SB_01" in result["error"]


async def test_uk_bad_arguments_are_usage_errors_checked_before_any_fetch(fetcher: Fetcher) -> None:
    with respx.mock(assert_all_called=False):
        result = await bc.fetch_uk_small_boat_arrivals(fetcher, days="many")
    assert "days" in result["error"] and "degraded" not in result


# ---------------------------------------------------------------------------
# Registry wiring (tools/society.py)
# ---------------------------------------------------------------------------

def _tools_registry():
    """Import world_intel_mcp.tools under the suite's temp cache path (same
    helper as test_daily_digest: importing it opens runtime's Cache)."""
    import importlib
    import os

    from .test_server_registry import _TMP_CACHE

    prior = os.environ.get("WORLD_INTEL_CACHE_DB")
    os.environ["WORLD_INTEL_CACHE_DB"] = str(_TMP_CACHE)
    try:
        return importlib.import_module("world_intel_mcp.tools")
    finally:
        if prior is None:
            os.environ.pop("WORLD_INTEL_CACHE_DB", None)
        else:
            os.environ["WORLD_INTEL_CACHE_DB"] = prior


async def test_border_tools_are_registered_and_dispatch_to_real_figures(
        fetcher: Fetcher, monkeypatch: pytest.MonkeyPatch) -> None:
    tools_pkg = _tools_registry()
    tools = {t.name: t for t in tools_pkg.ALL_TOOLS}
    us_tool, uk_tool = tools["intel_us_border_encounters"], tools["intel_uk_small_boat_arrivals"]
    props = us_tool.inputSchema["properties"]
    assert set(props["region"]["enum"]) == {"nationwide", *bc.CBP_REGIONS}
    assert set(props["component"]["enum"]) == {"all", *bc.CBP_COMPONENTS}
    assert set(uk_tool.inputSchema["properties"]) == {"days", "weeks", "months"}

    from world_intel_mcp import runtime

    monkeypatch.setattr(runtime, "fetcher", fetcher)
    with _cbp_mock(_CBP_NORTHERN_USBP_AUG):
        us = await tools_pkg.ALL_HANDLERS["intel_us_border_encounters"](
            {"region": "northern", "component": "usbp"})
    with _uk_mock(_main_ods()):
        uk = await tools_pkg.ALL_HANDLERS["intel_uk_small_boat_arrivals"]({"weeks": 1})
    assert us["latest"]["encounters"] == 563
    assert [w["week_ending"] for w in uk["weeks"]] == ["2026-10-11"]


# ---------------------------------------------------------------------------
# Live publishers (deselected by default; run with: pytest -m smoke -k live_)
# ---------------------------------------------------------------------------

@pytest.fixture
async def live_fetcher(tmp_path):
    from world_intel_mcp.cache import Cache
    from world_intel_mcp.circuit_breaker import CircuitBreaker

    cache = Cache(db_path=tmp_path / "live.db")
    fetcher = Fetcher(cache=cache, breaker=CircuitBreaker())
    yield fetcher
    await fetcher.close()
    cache.close()


@pytest.mark.smoke
async def test_live_cbp_encounters(live_fetcher: Fetcher) -> None:
    result = await bc.fetch_us_border_encounters(live_fetcher)
    assert "degraded" not in result and "error" not in result, result
    assert result["stale"] is False and result["measure_kind"] == "flow"
    assert result["dataset"]["url"].startswith("https://www.cbp.gov/")
    assert "/assets/documents/" not in result["dataset"]["url"]
    latest = result["latest"]
    assert latest["encounters"] > 0  # nationwide CBP encounters, never 0 in a published month
    assert latest["period"] == result["dataset"]["coverage"]["last_period"]
    assert result["top_citizenships_latest_month"]
    assert result["data_gaps"] == [], result["data_gaps"]


@pytest.mark.smoke
async def test_live_uk_small_boat_arrivals(live_fetcher: Fetcher) -> None:
    result = await bc.fetch_uk_small_boat_arrivals(live_fetcher)
    assert "degraded" not in result and "error" not in result, result
    assert result["stale"] is False and result["measure_kind"] == "flow"
    assert result["dataset"]["url"].startswith("https://assets.publishing.service.gov.uk/")
    week_ending = date.fromisoformat(result["latest_week"]["week_ending"])
    assert week_ending.weekday() == 6  # SB_02 weeks end on a Sunday
    assert result["dataset"]["daily_data_through"] >= result["dataset"]["weekly_data_through"]
    assert len(result["publisher_caveats"]) >= 5
    assert result["data_gaps"] == [], result["data_gaps"]
