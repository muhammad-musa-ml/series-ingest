import pytest

from series_ingest.segment import SegmentReader, write_segment


def points(n, start=0, step=1000):
    return [(start + i * step, float(i)) for i in range(n)]


def test_round_trip_with_several_series_and_blocks(tmp_path):
    data = {"a": points(2500), "b": points(10, step=7), "c": []}
    p = str(tmp_path / "s.seg")
    write_segment(p, data, block_size=1000)
    r = SegmentReader(p)
    assert r.series() == ["a", "b"]
    assert r.count() == 2510
    assert r.query("a", 0, 10**9) == data["a"]
    assert r.query("b", 0, 10**9) == data["b"]
    assert r.block_count("a") == 3


def test_out_of_order_input_is_stored_sorted(tmp_path):
    p = str(tmp_path / "s.seg")
    write_segment(p, {"a": [(5, 5.0), (1, 1.0), (3, 3.0)]})
    assert SegmentReader(p).query("a", 0, 10) == [(1, 1.0), (3, 3.0), (5, 5.0)]


def test_range_query_reads_only_the_blocks_it_overlaps(tmp_path):
    p = str(tmp_path / "s.seg")
    write_segment(p, {"a": points(10_000)}, block_size=1000)
    r = SegmentReader(p)
    got = r.query("a", 4_500_000, 5_200_000)
    assert [t for t, _ in got] == list(range(4_500_000, 5_200_001, 1000))
    assert r.blocks_read == 2
    assert r.block_count("a") == 10
    assert r.query("a", 10**12, 10**13) == []
    assert r.blocks_read == 2


def test_truncated_file_is_rejected(tmp_path):
    p = tmp_path / "s.seg"
    write_segment(str(p), {"a": points(50)})
    data = p.read_bytes()
    p.write_bytes(data[:-3])
    with pytest.raises(ValueError):
        SegmentReader(str(p))
