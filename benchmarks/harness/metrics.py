"""Latency statistics and a Docker-based resource sampler (CPU% / peak memory).

Resource sampling shells out to ``docker stats --no-stream`` on a background thread,
which works identically on Linux and Docker Desktop for Windows/Mac. We track peak
memory and mean CPU per container over a sampling window.
"""

from __future__ import annotations

import subprocess
import threading
import time
from dataclasses import dataclass, field


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return float("nan")
    s = sorted(values)
    if len(s) == 1:
        return s[0]
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    frac = k - lo
    return s[lo] * (1 - frac) + s[hi] * frac


@dataclass
class LatencyStats:
    samples: list[float] = field(default_factory=list)

    def add(self, seconds: float) -> None:
        self.samples.append(seconds)

    def summary(self) -> dict:
        if not self.samples:
            return {"n": 0}
        return {
            "n": len(self.samples),
            "min_ms": round(min(self.samples) * 1000, 2),
            "mean_ms": round(sum(self.samples) / len(self.samples) * 1000, 2),
            "p50_ms": round(percentile(self.samples, 50) * 1000, 2),
            "p95_ms": round(percentile(self.samples, 95) * 1000, 2),
            "max_ms": round(max(self.samples) * 1000, 2),
        }


_UNIT = {"B": 1, "KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "KB": 1000, "MB": 1000**2, "GB": 1000**3}


def _parse_mem(token: str) -> float:
    """Parse a docker-stats memory token like '1.53GiB' into bytes."""
    token = token.strip()
    num = ""
    unit = ""
    for ch in token:
        if ch.isdigit() or ch == ".":
            num += ch
        else:
            unit += ch
    try:
        return float(num) * _UNIT.get(unit.strip().upper(), 1)
    except ValueError:
        return 0.0


class ResourceSampler:
    """Polls ``docker stats`` for the given container names on a background thread."""

    def __init__(self, container: str, interval: float = 0.5):
        self.container = container
        self.interval = interval
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.peak_mem_bytes = 0.0
        self.cpu_samples: list[float] = []

    def _sample_once(self) -> None:
        if not self.container:  # engine without a container to sample (every engine here has one)
            return
        try:
            out = subprocess.run(
                ["docker", "stats", "--no-stream", "--format",
                 "{{.CPUPerc}};{{.MemUsage}}", self.container],
                capture_output=True, text=True, timeout=10,
            )
            line = out.stdout.strip()
            if line and ";" in line:
                cpu_s, mem_s = line.split(";", 1)
                try:
                    self.cpu_samples.append(float(cpu_s.strip().rstrip("%")))
                except ValueError:
                    pass
                self.peak_mem_bytes = max(self.peak_mem_bytes, _parse_mem(mem_s.split("/")[0]))
        except Exception:
            pass

    def _poll(self) -> None:
        while not self._stop.is_set():
            self._sample_once()
            self._stop.wait(self.interval)

    def __enter__(self) -> "ResourceSampler":
        # One synchronous sample guarantees >=1 data point even for fast (sub-second)
        # query phases, where a single `docker stats --no-stream` call (~1.5s) would
        # otherwise outlast the whole phase.
        self._sample_once()
        self._thread = threading.Thread(target=self._poll, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *exc) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2)

    def summary(self) -> dict:
        return {
            "peak_mem_mb": round(self.peak_mem_bytes / (1024**2), 1),
            "mean_cpu_pct": round(sum(self.cpu_samples) / len(self.cpu_samples), 1) if self.cpu_samples else None,
            "cpu_n": len(self.cpu_samples),
        }
