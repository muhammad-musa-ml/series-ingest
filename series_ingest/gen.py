"""Synthetic sensor data. Four signal shapes, deterministic for a seed.

Nothing here comes from real hardware. The shapes are chosen because they
compress very differently: a slow drift, a noisy oscillation, a counter and a
two-level status flag.
"""
import math
import random

KINDS = ["temperature", "vibration", "counter", "status"]
T0 = 1_700_000_000_000_000_000
PERIOD_NS = 1_000_000  # 1 kHz


def value(kind, idx, rng):
    if kind == "temperature":
        return 20.0 + 5.0 * math.sin(idx / 5000.0) + rng.gauss(0, 0.02)
    if kind == "vibration":
        return math.sin(2 * math.pi * 50 * idx * 1e-3) + rng.gauss(0, 0.05)
    if kind == "counter":
        return float(idx)
    return 1.0 if (idx // 4000) % 2 else 0.0


def series_points(kind, start_idx, n, seed, jitter_ns=0):
    rng = random.Random(seed * 1_000_003 + start_idx)
    pts = []
    for idx in range(start_idx, start_idx + n):
        t = T0 + idx * PERIOD_NS + (rng.randint(-jitter_ns, jitter_ns) if jitter_ns else 0)
        pts.append((t, value(kind, idx, rng)))
    return pts


def crash_batch(seed, i):
    # small on purpose: a batch smaller than the file buffer is what a missing flush would lose
    return batch_for(seed, i, channels=4, per_channel=5)


def batch_for(seed, i, channels=4, per_channel=100):
    """Batch number i: the same call always returns the same points."""
    batch = {}
    for c in range(channels):
        kind = KINDS[c % len(KINDS)]
        name = "%s_%d" % (kind, c)
        batch[name] = series_points(kind, i * per_channel, per_channel, seed * 100 + c)
    return batch
