from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass
class NewsEvent:
    timestamp: datetime
    currency: str
    impact: str
    title: str
    forecast: Any = None
    previous: Any = None
    actual: Any = None
    source: str = ""
