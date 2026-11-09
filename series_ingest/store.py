"""Ingest path: log first, then memory, then a sorted segment.

ingest(batch) returns only after the batch is in the log. When the buffer
reaches memtable_limit points the log is rotated, the buffer is written to
seg-N.seg (temporary file, fsync, rename) and only then are the logs up to N
deleted. Recovery trusts that order: a log covered by a segment is dropped,
any other log is replayed.
"""
import os
import re
import threading

from .memtable import Memtable
from .segment import SegmentReader, write_segment
from .wal import Wal, read_wal


class SimulatedCrash(Exception):
    pass


def _gens(path, prefix, suffix):
    pat = re.compile(r"^%s-(\d{6})%s$" % (prefix, re.escape(suffix)))
    return sorted(int(m.group(1)) for m in map(pat.match, os.listdir(path)) if m)


class Store:
    def __init__(self, path, memtable_limit=100_000, fsync=True, block_size=1024, crash_hook=None):
        self.path = path
        self.limit = memtable_limit
        self.fsync = fsync
        self.block_size = block_size
        self._hook = crash_hook or (lambda name: None)
        self._lock = threading.RLock()
        self._mem = Memtable()
        self._segments = []
        os.makedirs(path, exist_ok=True)
        self._recover()

    def _wal_path(self, gen):
        return os.path.join(self.path, "wal-%06d.log" % gen)

    def _seg_path(self, gen):
        return os.path.join(self.path, "seg-%06d.seg" % gen)

    def _recover(self):
        for name in os.listdir(self.path):
            if name.endswith(".tmp"):
                os.remove(os.path.join(self.path, name))
        segs = _gens(self.path, "seg", ".seg")
        wals = _gens(self.path, "wal", ".log")
        covered = segs[-1] if segs else 0
        for gen in wals:
            if gen <= covered:
                os.remove(self._wal_path(gen))
        for gen in wals:
            if gen > covered:
                for batch in read_wal(self._wal_path(gen)):
                    self._mem.add(batch)
        self._segments = [SegmentReader(self._seg_path(g)) for g in segs]
        self._gen = max(segs + wals + [0]) + 1
        self._wal = Wal(self._wal_path(self._gen), fsync=self.fsync)

    def ingest(self, batch):
        batch = {name: [(int(t), float(v)) for t, v in pts] for name, pts in batch.items()}
        with self._lock:
            self._wal.append(batch)
            self._hook("wal_appended")
            self._mem.add(batch)
            if self._mem.count() >= self.limit:
                self.flush()
        return sum(len(p) for p in batch.values())

    def flush(self):
        with self._lock:
            if self._mem.count() == 0:
                return
            gen = self._gen
            self._wal.close()
            self._gen += 1
            self._wal = Wal(self._wal_path(self._gen), fsync=self.fsync)
            _, points = self._mem.take()
            tmp = self._seg_path(gen) + ".tmp"
            write_segment(tmp, points, self.block_size)
            self._hook("segment_tmp_written")
            os.replace(tmp, self._seg_path(gen))
            self._hook("segment_renamed")
            for g in _gens(self.path, "wal", ".log"):
                if g <= gen:
                    os.remove(self._wal_path(g))
            self._hook("wals_removed")
            self._segments.append(SegmentReader(self._seg_path(gen)))

    def query(self, name, t0, t1):
        with self._lock:
            out = []
            for seg in self._segments:
                out.extend(seg.query(name, t0, t1))
            out.extend(self._mem.query(name, t0, t1))
        return sorted(out, key=lambda p: p[0])

    def stats(self):
        with self._lock:
            return {
                "segments": len(self._segments),
                "points_in_segments": sum(s.count() for s in self._segments),
                "points_in_memory": self._mem.count(),
            }

    def close(self):
        with self._lock:
            self._wal.close()
