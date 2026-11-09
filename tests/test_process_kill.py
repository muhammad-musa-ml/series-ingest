from series_ingest import crashcheck


def test_killing_the_writer_never_loses_or_duplicates_an_acknowledged_batch(tmp_path):
    rows = crashcheck.run_rounds(str(tmp_path), rounds=8, seed0=500)
    assert len(rows) == 8
    assert all(r["ok"] for r in rows), rows
    assert all(r["duplicates"] == 0 for r in rows)
    # the writer was running long enough to acknowledge something in most rounds
    assert sum(1 for r in rows if r["last_acked"] >= 0) >= 6
