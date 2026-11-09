from series_ingest.wal import Wal, read_wal


def make_batch(i):
    return {
        "temp": [(i * 10 + k, 20.0 + i + k / 10) for k in range(3)],
        "rpm": [(i * 10, float(1000 + i))],
    }


def write_log(path, n):
    wal = Wal(str(path), fsync=False)
    for i in range(n):
        wal.append(make_batch(i))
    wal.close()


def test_replay_returns_every_batch_in_order(tmp_path):
    p = tmp_path / "a.log"
    write_log(p, 5)
    assert list(read_wal(str(p))) == [make_batch(i) for i in range(5)]


def test_torn_tail_at_every_byte_returns_a_whole_prefix(tmp_path):
    p = tmp_path / "a.log"
    write_log(p, 4)
    data = p.read_bytes()
    full = [make_batch(i) for i in range(4)]
    seen_lengths = set()
    for cut in range(len(data) + 1):
        q = tmp_path / "cut.log"
        q.write_bytes(data[:cut])
        got = list(read_wal(str(q)))
        assert got == full[:len(got)]
        seen_lengths.add(len(got))
    assert seen_lengths == {0, 1, 2, 3, 4}


def test_corrupt_record_ends_the_log(tmp_path):
    p = tmp_path / "a.log"
    write_log(p, 4)
    data = bytearray(p.read_bytes())
    first_len = int.from_bytes(data[0:4], "little")
    second_payload_start = 8 + first_len + 8
    data[second_payload_start + 3] ^= 0xFF
    p.write_bytes(bytes(data))
    assert list(read_wal(str(p))) == [make_batch(0)]
