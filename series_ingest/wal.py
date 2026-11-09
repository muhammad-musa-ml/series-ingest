"""Append-only log. One record per ingested batch: length, crc32, payload.

A batch is acknowledged only after its record is written and flushed (and
fsynced, if asked). On replay a record that is cut short or fails its crc ends
the log, so a batch is replayed whole or not at all.
"""
import os
import struct
import zlib

HEADER = struct.Struct("<II")


def encode_batch(batch):
    out = bytearray(struct.pack("<I", len(batch)))
    for name, pts in batch.items():
        raw = name.encode("utf-8")
        n = len(pts)
        out += struct.pack("<H", len(raw)) + raw + struct.pack("<I", n)
        out += struct.pack("<%dq" % n, *[p[0] for p in pts])
        out += struct.pack("<%dd" % n, *[p[1] for p in pts])
    return bytes(out)


def decode_batch(payload):
    (n_series,) = struct.unpack_from("<I", payload, 0)
    pos = 4
    batch = {}
    for _ in range(n_series):
        (name_len,) = struct.unpack_from("<H", payload, pos)
        pos += 2
        name = payload[pos:pos + name_len].decode("utf-8")
        pos += name_len
        (n,) = struct.unpack_from("<I", payload, pos)
        pos += 4
        ts = struct.unpack_from("<%dq" % n, payload, pos)
        pos += 8 * n
        vals = struct.unpack_from("<%dd" % n, payload, pos)
        pos += 8 * n
        batch[name] = list(zip(ts, vals))
    return batch


class Wal:
    def __init__(self, path, fsync=True):
        self.path = path
        self.fsync = fsync
        self._f = open(path, "ab")

    def append(self, batch):
        payload = encode_batch(batch)
        self._f.write(HEADER.pack(len(payload), zlib.crc32(payload)) + payload)
        self._f.flush()
        if self.fsync:
            os.fsync(self._f.fileno())

    def close(self):
        if not self._f.closed:
            self._f.close()


def read_wal(path):
    """Yield the batches of a log, stopping at the first short or corrupt record."""
    with open(path, "rb") as f:
        data = f.read()
    pos = 0
    while pos + HEADER.size <= len(data):
        length, crc = HEADER.unpack_from(data, pos)
        start = pos + HEADER.size
        payload = data[start:start + length]
        if len(payload) < length or zlib.crc32(payload) != crc:
            return
        yield decode_batch(payload)
        pos = start + length
