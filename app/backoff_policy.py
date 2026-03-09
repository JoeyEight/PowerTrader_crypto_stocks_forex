from __future__ import annotations

import random
from dataclasses import dataclass


@dataclass
class BackoffPolicy:
    base_delay_s: float = 0.35
    max_delay_s: float = 8.0
    jitter_s: float = 0.35
    max_retry_after_s: float = 300.0

    def wait_seconds(self, attempt: int, retry_after_s: float = 0.0) -> float:
        idx = max(1, int(attempt))
        base = max(0.01, float(self.base_delay_s))
        cap = max(base, float(self.max_delay_s))
        jitter = max(0.0, float(self.jitter_s))
        exp = min(cap, base * (2 ** (idx - 1)))
        wait_s = min(cap, exp + random.uniform(0.0, jitter))
        ra = max(0.0, float(retry_after_s or 0.0))
        if ra > 0.0:
            ra_cap = max(0.01, float(self.max_retry_after_s))
            wait_s = max(wait_s, min(ra, ra_cap))
        return max(0.01, float(wait_s))
