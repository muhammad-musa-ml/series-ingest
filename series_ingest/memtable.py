"""In-memory buffer of points not yet written to a segment.

The points and the count they are measured by live under one lock, so a
reader can never see a count that disagrees with what is stored. Taking the
buffer hands back both together and leaves it empty.
"""
import threading


class Memtable:
    def __init__(self):
        self._lock = threading.Lock()
        self._points = {}
        self._count = 0

    def add(self, batch):
        with self._lock:
            for name, pts in batch.items():
                self._points.setdefault(name, []).extend(pts)
                self._count += len(pts)

    def count(self):
        with self._lock:
            return self._count

    def is_consistent(self):
        with self._lock:
            return self._count == sum(len(v) for v in self._points.values())

    def take(self):
        with self._lock:
            points, count = self._points, self._count
            self._points, self._count = {}, 0
        return count, points

    def query(self, name, t0, t1):
        with self._lock:
            pts = list(self._points.get(name, ()))
        return [p for p in pts if t0 <= p[0] <= t1]
