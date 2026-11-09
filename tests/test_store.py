import os

import pytest

from series_ingest import SimulatedCrash, Store


def batch_for(i):
    return {
        "temp": [(i * 100 + k, 20.0 + i + k / 10) for k in range(5)],
        "rpm": [(i * 100 + k, float(1000 + i)) for k in range(5)],
    }


def expected(n):
    out = {"temp": [], "rpm": []}
    for i in range(n):
        for name, pts in batch_for(i).items():
            out[name].extend(pts)
    return out


def contents(store, n_batches):
    hi = n_batches * 100 + 100
    return {name: store.query(name, 0, hi) for name in ("temp", "rpm")}


def test_query_spans_segments_and_memory(tmp_path):
    s = Store(str(tmp_path), memtable_limit=40, fsync=False)
    for i in range(10):
        s.ingest(batch_for(i))
    st = s.stats()
    assert st["segments"] >= 2 and st["points_in_memory"] > 0
    assert contents(s, 10) == expected(10)
    s.close()


def test_reopen_without_flush_recovers_from_the_log(tmp_path):
    s = Store(str(tmp_path), memtable_limit=10**9, fsync=False)
    for i in range(6):
        s.ingest(batch_for(i))
    s.close()
    again = Store(str(tmp_path), memtable_limit=10**9, fsync=False)
    assert contents(again, 6) == expected(6)
    again.close()


@pytest.mark.parametrize("hook", ["wal_appended", "segment_tmp_written", "segment_renamed", "wals_removed"])
def test_crash_at_every_step_of_a_flush_loses_and_duplicates_nothing(tmp_path, hook):
    state = {"armed": False}

    def crash(name):
        if state["armed"] and name == hook:
            raise SimulatedCrash(name)

    s = Store(str(tmp_path), memtable_limit=40, fsync=False, crash_hook=crash)
    acked = 0
    # a batch is 10 points: three batches leave 30 in memory, the fourth reaches 40 and flushes
    for i in range(3):
        s.ingest(batch_for(i))
        acked += 1
    state["armed"] = True
    with pytest.raises(SimulatedCrash):
        s.ingest(batch_for(acked))
    s.close()
    for _ in range(2):  # reopening twice must not change anything
        r = Store(str(tmp_path), memtable_limit=10**9, fsync=False)
        # the crashing batch was logged before every hook, so it survives whole
        assert contents(r, acked + 1) == expected(acked + 1)
        r.close()


def test_torn_log_tail_drops_only_the_unfinished_batch(tmp_path):
    s = Store(str(tmp_path), memtable_limit=10**9, fsync=False)
    for i in range(5):
        s.ingest(batch_for(i))
    s.close()
    log = [f for f in os.listdir(tmp_path) if f.endswith(".log")]
    newest = os.path.join(tmp_path, max(log))
    size = os.path.getsize(newest)
    with open(newest, "r+b") as f:
        f.truncate(size - 7)
    r = Store(str(tmp_path), memtable_limit=10**9, fsync=False)
    assert contents(r, 5) == expected(4)
    r.close()


def test_leftover_temporary_segment_is_ignored_and_removed(tmp_path):
    s = Store(str(tmp_path), memtable_limit=10**9, fsync=False)
    s.ingest(batch_for(0))
    s.close()
    stray = tmp_path / "seg-000009.seg.tmp"
    stray.write_bytes(b"half a segment")
    r = Store(str(tmp_path), memtable_limit=10**9, fsync=False)
    assert not stray.exists()
    assert contents(r, 1) == expected(1)
    r.close()
