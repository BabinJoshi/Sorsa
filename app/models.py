from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class TimeSlice:
    slice_id: int
    since: datetime
    until: datetime
    api_key_alias: str


@dataclass
class IngestionContext:
    run_id: str
    project_keyword: str
    slice_id: int
    endpoint: str
    api_key_alias: str


JsonDict = dict[str, Any]

