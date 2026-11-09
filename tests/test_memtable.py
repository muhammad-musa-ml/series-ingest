import sys
import threading

import pytest

from series_ingest.memtable import Memtable


@pytest.fixture
def frequent_thread_switches():
    old = sys.getswitchinterval()
    sys.setswitchinterval(1e-6)
    yield
    sys.setswitchinterval(old)


def test_count_and_points_agree_under_concurrent_add_and_take(frequent_thread_switches):
    mt = Memtable()
    n_writers, batches, per_batch = 4, 300, 7
    added_total = n_writers * batches * per_batch
    taken = []
    bad = []
    stop = threading.Event()

    def writer(w):
        for i in range(batches):
            mt.add({"s%d" % (i % 3): [(w * 10**6 + i * 10 + k, 1.0) for k in range(per_batch)]})

    def watcher():
        while not stop.is_set():
            if not mt.is_consistent():
                bad.append(1)
            count, points = mt.take()
            if count != sum(len(v) for v in points.values()):
                bad.append(2)
            taken.append(count)

    threads = [threading.Thread(target=writer, args=(w,)) for w in range(n_writers)]
    watch = threading.Thread(target=watcher)
    watch.start()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    stop.set()
    watch.join()
    taken.append(mt.take()[0])
    assert bad == []
    assert sum(taken) == added_total


def test_take_empties_the_buffer():
    mt = Memtable()
    mt.add({"a": [(1, 1.0), (2, 2.0)]})
    assert mt.count() == 2
    count, points = mt.take()
    assert count == 2 and points == {"a": [(1, 1.0), (2, 2.0)]}
    assert mt.count() == 0 and mt.query("a", 0, 10) == []


def test_query_filters_by_time_range():
    mt = Memtable()
    mt.add({"a": [(1, 1.0), (5, 5.0), (9, 9.0)]})
    assert mt.query("a", 2, 9) == [(5, 5.0), (9, 9.0)]
    assert mt.query("missing", 0, 10) == []
