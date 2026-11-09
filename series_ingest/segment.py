"""Immutable segment file: points sorted by series, then time, in compressed blocks.

Layout: magic, blocks, footer (one index entry per block), footer offset, magic.
Each index entry holds the block's series, first and last timestamp, point
count and byte range, so a time-range query reads only the blocks it overlaps.
"""
import os
import struct
import zlib

from . import codec

MAGIC = b"SGS1"
ENTRY_TAIL = struct.Struct("<qqIQI")  # min_ts, max_ts, n, offset, length


def write_segment(path, points_by_series, block_size=1024):
    index = []
    with open(path, "wb") as f:
        f.write(MAGIC)
        for name in sorted(points_by_series):
            pts = sorted(points_by_series[name], key=lambda p: p[0])
            for i in range(0, len(pts), block_size):
                blk = pts[i:i + block_size]
                ts_b = zlib.compress(codec.encode_timestamps([p[0] for p in blk]))
                v_b = zlib.compress(codec.encode_values([p[1] for p in blk]))
                body = struct.pack("<II", len(ts_b), len(v_b)) + ts_b + v_b
                offset = f.tell()
                f.write(body)
                index.append((name, blk[0][0], blk[-1][0], len(blk), offset, len(body)))
        footer_offset = f.tell()
        f.write(struct.pack("<I", len(index)))
        for name, lo, hi, n, offset, length in index:
            raw = name.encode("utf-8")
            f.write(struct.pack("<H", len(raw)) + raw + ENTRY_TAIL.pack(lo, hi, n, offset, length))
        f.write(struct.pack("<Q", footer_offset) + MAGIC)
        f.flush()
        os.fsync(f.fileno())


class SegmentReader:
    def __init__(self, path):
        self.path = path
        self.blocks_read = 0
        with open(path, "rb") as f:
            data = f.read()
        if data[:4] != MAGIC or data[-4:] != MAGIC:
            raise ValueError("not a complete segment: %s" % path)
        (footer_offset,) = struct.unpack_from("<Q", data, len(data) - 12)
        (count,) = struct.unpack_from("<I", data, footer_offset)
        pos = footer_offset + 4
        self.index = []
        for _ in range(count):
            (name_len,) = struct.unpack_from("<H", data, pos)
            pos += 2
            name = data[pos:pos + name_len].decode("utf-8")
            pos += name_len
            self.index.append((name,) + ENTRY_TAIL.unpack_from(data, pos))
            pos += ENTRY_TAIL.size

    def series(self):
        return sorted({e[0] for e in self.index})

    def count(self):
        return sum(e[3] for e in self.index)

    def block_count(self, name):
        return sum(1 for e in self.index if e[0] == name)

    def query(self, name, t0, t1):
        out = []
        with open(self.path, "rb") as f:
            for ename, lo, hi, n, offset, length in self.index:
                if ename != name or hi < t0 or lo > t1:
                    continue
                f.seek(offset)
                body = f.read(length)
                self.blocks_read += 1
                ts_len, v_len = struct.unpack_from("<II", body, 0)
                ts = codec.decode_timestamps(zlib.decompress(body[8:8 + ts_len]))
                vals = codec.decode_values(zlib.decompress(body[8 + ts_len:8 + ts_len + v_len]))
                out.extend((t, v) for t, v in zip(ts, vals) if t0 <= t <= t1)
        return out
