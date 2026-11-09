"""Kill the ingest worker at a random moment, reopen the store, check the points.

Rule being checked: every batch the worker acknowledged is present exactly
once, and nothing beyond the one batch that was in flight is present.
"""
import os
import random
import subprocess
import sys
import time

from .gen import crash_batch
from .store import Store

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def expected_points(seed, n_batches):
    out = set()
    for i in range(n_batches):
        for name, pts in crash_batch(seed, i).items():
            out.update((name, t, v) for t, v in pts)
    return out


def read_acks(path):
    if not os.path.exists(path):
        return -1
    with open(path) as f:
        done = [ln for ln in f.read().split("\n")[:-1] if ln]
    return int(done[-1]) if done else -1


def state_before_recovery(directory):
    names = os.listdir(directory)
    segs = sorted(n for n in names if n.endswith(".seg"))
    wals = sorted(n for n in names if n.endswith(".log"))
    tmp = [n for n in names if n.endswith(".tmp")]
    covered_wal = bool(segs) and any(w[4:10] <= segs[-1][4:10] for w in wals)
    return {"tmp_segment": bool(tmp), "covered_wal": covered_wal, "segments": len(segs)}


def kill_round(workdir, seed, delay):
    os.makedirs(workdir, exist_ok=True)
    data_dir = os.path.join(workdir, "data")
    ack_path = os.path.join(workdir, "acks.txt")
    proc = subprocess.Popen(
        [sys.executable, "-m", "series_ingest.crash_worker", data_dir, str(seed), ack_path],
        cwd=ROOT,
    )
    time.sleep(delay)
    proc.kill()
    proc.wait()
    last_acked = read_acks(ack_path)
    before = state_before_recovery(data_dir) if os.path.isdir(data_dir) else {}
    result = {"seed": seed, "delay_s": round(delay, 3), "last_acked": last_acked, **before}

    must = expected_points(seed, last_acked + 1)
    may = expected_points(seed, last_acked + 2)
    ok = True
    for reopen in range(2):
        store = Store(data_dir, memtable_limit=10**9, fsync=False)
        got = []
        for name in {n for n, _, _ in may}:
            got.extend((name, t, v) for t, v in store.query(name, 0, 2**62))
        store.close()
        got_set = set(got)
        no_dupes = len(got) == len(got_set)
        whole = got_set == must or got_set == may
        ok = ok and no_dupes and whole
        result["duplicates"] = len(got) - len(got_set)
        result["points"] = len(got)
        result["in_flight_batch_kept"] = got_set == may
    result["ok"] = ok
    return result


def run_rounds(base_dir, rounds, seed0=1000):
    rng = random.Random(seed0)
    rows = []
    for r in range(rounds):
        delay = rng.uniform(0.25, 1.4)
        rows.append(kill_round(os.path.join(base_dir, "round-%03d" % r), seed0 + r, delay))
    return rows
