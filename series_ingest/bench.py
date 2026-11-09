"""Measurements behind the README. Every measured figure in the README's Results
section is written to results/*.csv and results/summary.md by this file."""
import csv
import os
import shutil
import statistics
import tempfile
import time

from . import crashcheck
from .gen import KINDS, batch_for, series_points
from .segment import SegmentReader, write_segment
from .store import Store

RAW_BYTES_PER_POINT = 16  # int64 timestamp + float64 value
REPEATS = 5


def span(xs, digits=2):
    return "%s (%s to %s)" % (
        round(statistics.median(xs), digits), round(min(xs), digits), round(max(xs), digits))


def write_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)


def bench_compression(tmp, n=100_000):
    rows = []
    for kind in KINDS:
        for jitter in (0, 1000):
            for seed in range(1, REPEATS + 1):
                pts = series_points(kind, 0, n, seed, jitter_ns=jitter)
                path = os.path.join(tmp, "c.seg")
                t = time.perf_counter()
                write_segment(path, {kind: pts})
                write_s = time.perf_counter() - t
                size = os.path.getsize(path)
                rows.append({
                    "kind": kind, "timestamp_jitter_ns": jitter, "seed": seed, "points": n,
                    "segment_bytes": size,
                    "ratio_vs_raw": round(n * RAW_BYTES_PER_POINT / size, 2),
                    "write_seconds": round(write_s, 3),
                })
    return rows


def bench_ingest(tmp, total_points=200_000):
    rows = []
    per_batch = 400
    for fsync in (False, True):
        for rep in range(REPEATS):
            d = os.path.join(tmp, "i-%s-%d" % (fsync, rep))
            store = Store(d, memtable_limit=50_000, fsync=fsync)
            n_batches = total_points // per_batch
            batches = [batch_for(rep, i) for i in range(n_batches)]
            t = time.perf_counter()
            for b in batches:
                store.ingest(b)
            secs = time.perf_counter() - t
            store.close()
            rows.append({"fsync": fsync, "rep": rep, "points": n_batches * per_batch,
                         "points_per_batch": per_batch, "seconds": round(secs, 3),
                         "points_per_second": round(n_batches * per_batch / secs)})
    return rows


def bench_query(tmp, per_series=250_000):
    d = os.path.join(tmp, "q")
    store = Store(d, memtable_limit=10**9, fsync=False, block_size=1024)
    chunk = 5_000
    for start in range(0, per_series, chunk):
        store.ingest({"%s_%d" % (k, c): series_points(k, start, chunk, c)
                      for c, k in enumerate(KINDS)})
    store.flush()
    seg = store._segments[0]
    name = "vibration_1"
    first = series_points("vibration", 0, 1, 1)[0][0]
    last = first + (per_series - 1) * 1_000_000
    window = (last - first) // 100
    rows = []
    for rep in range(REPEATS):
        lo = first + (last - first) * (rep + 1) // 7
        before = seg.blocks_read
        t = time.perf_counter()
        got = seg.query(name, lo, lo + window)
        q_s = time.perf_counter() - t
        blocks = seg.blocks_read - before
        t = time.perf_counter()
        full = seg.query(name, first, last)
        full_s = time.perf_counter() - t
        rows.append({"rep": rep, "points_in_series": per_series, "window_points": len(got),
                     "blocks_read": blocks, "blocks_in_series": seg.block_count(name),
                     "window_seconds": round(q_s, 4), "full_scan_seconds": round(full_s, 4)})
    store.close()
    return rows


def bench_recovery(tmp, total_points=200_000):
    rows = []
    for rep in range(REPEATS):
        d = os.path.join(tmp, "r-%d" % rep)
        store = Store(d, memtable_limit=10**9, fsync=False)
        for i in range(total_points // 400):
            store.ingest(batch_for(rep, i))
        store.close()
        t = time.perf_counter()
        again = Store(d, memtable_limit=10**9, fsync=False)
        secs = time.perf_counter() - t
        assert again.stats()["points_in_memory"] == total_points
        again.close()
        rows.append({"rep": rep, "points_replayed": total_points, "seconds": round(secs, 3)})
    return rows


def run_all(out_dir, kill_rounds=30):
    os.makedirs(out_dir, exist_ok=True)
    tmp = tempfile.mkdtemp(prefix="series-ingest-")
    try:
        comp = bench_compression(tmp)
        ing = bench_ingest(tmp)
        qry = bench_query(tmp)
        rec = bench_recovery(tmp)
        kill = crashcheck.run_rounds(os.path.join(tmp, "kill"), kill_rounds)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
    for name, rows in (("compression", comp), ("ingest", ing), ("query", qry),
                       ("recovery", rec), ("crash", kill)):
        write_csv(os.path.join(out_dir, name + ".csv"), rows)
    write_summary(out_dir, comp, ing, qry, rec, kill)


def write_summary(out_dir, comp, ing, qry, rec, kill):
    L = ["# Results", "", "Generated by `python -m series_ingest bench`. Median with min to max over %d runs." % REPEATS, ""]
    L += ["## Compression (100,000 points in one series, raw = %d bytes per point)" % RAW_BYTES_PER_POINT, "",
          "| signal | timestamp jitter | compression ratio |", "|---|---|---|"]
    for kind in KINDS:
        for jitter in (0, 1000):
            xs = [r["ratio_vs_raw"] for r in comp if r["kind"] == kind and r["timestamp_jitter_ns"] == jitter]
            L.append("| %s | %s | %s |" % (kind, "none" if not jitter else "+/-%d ns" % jitter, span(xs)))
    L += ["", "## Ingest (200,000 points in batches of 400, memtable limit 50,000)", "",
          "| fsync on every batch | points per second |", "|---|---|"]
    for fs in (False, True):
        L.append("| %s | %s |" % ("yes" if fs else "no",
                                  span([r["points_per_second"] for r in ing if r["fsync"] == fs], 0)))
    L += ["", "## Range query (250,000 points in the series, window = 1% of the time span)", "",
          "| blocks read | blocks in series | window query (s) | full scan (s) |", "|---|---|---|---|",
          "| %s | %s | %s | %s |" % (span([r["blocks_read"] for r in qry], 0), qry[0]["blocks_in_series"],
                                     span([r["window_seconds"] for r in qry], 4),
                                     span([r["full_scan_seconds"] for r in qry], 4))]
    L += ["", "## Recovery (reopen with 200,000 points only in the log)", "",
          "Reopen time in seconds: %s" % span([r["seconds"] for r in rec], 3), ""]
    mid = sum(1 for r in kill if r.get("tmp_segment") or r.get("covered_wal"))
    L += ["## Process kill", "",
          "- rounds: %d, each kills the writer once at a random moment and reopens the store twice" % len(kill),
          "- rounds that passed (every acknowledged batch present once, nothing beyond the in-flight batch): %d of %d"
          % (sum(1 for r in kill if r["ok"]), len(kill)),
          "- duplicate points found: %d" % sum(r["duplicates"] for r in kill),
          "- batches acknowledged per round: %s" % span([r["last_acked"] + 1 for r in kill], 0),
          "- rounds where the kill left a half-finished flush on disk (a temporary segment, or a log already covered by a segment): %d" % mid,
          "- rounds where the in-flight batch had reached the log and was kept: %d" % sum(1 for r in kill if r["in_flight_batch_kept"]),
          ""]
    with open(os.path.join(out_dir, "summary.md"), "w", newline="\n") as f:
        f.write("\n".join(L))
