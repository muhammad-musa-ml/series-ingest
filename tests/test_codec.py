import math
import random
import struct

from series_ingest import codec


def test_timestamps_round_trip_regular_and_jittered():
    rng = random.Random(1)
    regular = [1_000_000_000 + i * 1_000_000 for i in range(500)]
    jittered = [t + rng.randint(-300, 300) for t in regular]
    for ts in (regular, jittered, [], [42]):
        assert codec.decode_timestamps(codec.encode_timestamps(ts)) == ts


def test_timestamps_handle_out_of_order_and_big_steps():
    ts = [10, 5, 5, 2**40, -7, 2**62]
    assert codec.decode_timestamps(codec.encode_timestamps(ts)) == ts


def test_regular_timestamps_cost_about_one_byte_each():
    ts = [i * 1_000_000 for i in range(10_000)]
    assert len(codec.encode_timestamps(ts)) < 10_000 + 20


def test_values_round_trip_bit_exact_including_nan_and_inf():
    vals = [0.0, -0.0, 1.5, math.inf, -math.inf, math.nan, 1e-310, 3.14159]
    out = codec.decode_values(codec.encode_values(vals))
    assert [struct.pack("<d", v) for v in out] == [struct.pack("<d", v) for v in vals]


def test_zigzag_is_its_own_inverse():
    for n in (0, 1, -1, 2, -2, 2**40, -(2**40)):
        assert codec.unzigzag(codec.zigzag(n)) == n
