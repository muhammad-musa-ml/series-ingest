"""Column encodings for one block of one series.

Timestamps: first value, then the delta-of-delta of the rest as zigzag varints.
Values: float64 bytes, shuffled so byte k of every value sits together, which
gives zlib long runs when the high bytes barely change.
"""
import struct


def zigzag(n):
    return (n << 1) if n >= 0 else ((-n) << 1) - 1


def unzigzag(z):
    return (z >> 1) if z & 1 == 0 else -((z + 1) >> 1)


def put_varint(out, n):
    while n >= 0x80:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)


def get_varint(buf, pos):
    shift = 0
    n = 0
    while True:
        b = buf[pos]
        pos += 1
        n |= (b & 0x7F) << shift
        if b < 0x80:
            return n, pos
        shift += 7


def encode_timestamps(ts):
    out = bytearray(struct.pack("<I", len(ts)))
    if not ts:
        return bytes(out)
    out += struct.pack("<q", ts[0])
    prev_t, prev_delta = ts[0], 0
    for t in ts[1:]:
        delta = t - prev_t
        put_varint(out, zigzag(delta - prev_delta))
        prev_t, prev_delta = t, delta
    return bytes(out)


def decode_timestamps(buf):
    (n,) = struct.unpack_from("<I", buf, 0)
    if n == 0:
        return []
    (first,) = struct.unpack_from("<q", buf, 4)
    ts = [first]
    pos = 12
    prev_t, prev_delta = first, 0
    for _ in range(n - 1):
        z, pos = get_varint(buf, pos)
        delta = prev_delta + unzigzag(z)
        prev_t += delta
        prev_delta = delta
        ts.append(prev_t)
    return ts


def encode_values(vals):
    n = len(vals)
    raw = struct.pack("<%dd" % n, *vals)
    return struct.pack("<I", n) + b"".join(raw[k::8] for k in range(8))


def decode_values(buf):
    (n,) = struct.unpack_from("<I", buf, 0)
    shuffled = buf[4:]
    raw = bytearray(8 * n)
    for k in range(8):
        raw[k::8] = shuffled[k * n:(k + 1) * n]
    return list(struct.unpack("<%dd" % n, bytes(raw)))
