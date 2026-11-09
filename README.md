# series-ingest

A small ingest path for timestamped sensor data, in Python's standard library only. Points go to a write-ahead log first, then to an in-memory buffer, then into sorted, compressed segment files with a block index. If the writer process is killed, reopening the store gives back every point that was acknowledged, once. The kill test below checks this in 30 random kills.

## Why I wrote it

Nominal's post "Architecture is the Advantage" (nominal.io/blog/architecture-is-the-advantage) describes live data being written first to a message bus that holds it when a downstream component is slow or unresponsive, and stored by column, sorted by series and then time. I wanted to see how small a version of that path could be while still keeping one promise: after the writer process is killed, every point that was acknowledged is still there, once. I wanted to test that promise by killing the writer process, not by reading the code. This repo is independent of Nominal and has not been reviewed by them.

## How it works

- `wal.py`: one record per batch (length, crc32, payload). `ingest()` returns after the record is written and flushed. Replay stops at the first short or corrupt record, so a batch comes back whole or not at all.
- `memtable.py`: the points and their count change under one lock, so a reader never sees a count that disagrees with what is stored.
- `segment.py`: points are sorted by series then time and cut into blocks of 1,024; each block's timestamps and values are encoded by `codec.py` and compressed with zlib. The footer holds each block's first and last timestamp, so a time-range query reads only the blocks it overlaps. The file is fsynced when it is written.
- `codec.py`: timestamps as first value plus delta-of-delta varints; values as float64 bytes shuffled so byte k of every value sits together.
- `store.py`: a flush rotates the log, writes the segment to a temporary file, renames it to `seg-N.seg`, and only then deletes the logs it covers. Recovery drops any log covered by a segment, replays the rest, and removes leftover temporary files.

## Run it

```
python -m pytest              # 25 tests, about 14 s
python -m series_ingest bench 30   # about a minute; writes results/ (csv and summary.md)
```

The package uses only the standard library; the tests need pytest. They include a simulated crash at each of four points (after the batch reaches the log, after the temporary segment is written, after the rename, after the covered logs are deleted), with the store reopened twice each time; a log cut at every byte; and a worker process killed at a random moment, eight times.

## Results

Copied from `results/summary.md`, which `bench` generates. Median with min to max over 5 runs, one Windows 11 machine, Python 3.13.5. The compression ratio is 16 raw bytes per point (an int64 timestamp and a float64 value) divided by the size of the segment file.

| signal (100,000 points) | compression ratio, regular timestamps | with +/-1000 ns timestamp jitter |
|---|---|---|
| temperature (slow drift plus noise) | 2.58 | 1.99 |
| vibration (50 Hz sine plus noise) | 2.25 | 1.78 |
| counter | 44.21 | 7.23 |
| status (two levels) | 141.12 | 8.15 |

- Ingest, 200,000 points in batches of 400: 718,306 points per second (695,559 to 795,586) without fsync, 355,324 (336,043 to 360,357) with an fsync on every batch. Batches are built before the timer starts.
- Range query over a series of 250,000 points, window of 1% of the time span: 4 of 245 blocks read (3 to 4), 0.0031 s (0.0025 to 0.004) against 0.2052 s (0.1857 to 0.2766) for a full scan.
- Reopening with 200,000 points only in the log: 0.117 s (0.103 to 0.135).
- Process kill, 30 rounds: 30 passed, 0 duplicate points, 92 to 822 batches acknowledged per round (median 398).

## What this does not show

- The throughput numbers differ between runs. The same command on a fresh clone, run a few minutes later, gave 570,547 points per second (537,447 to 665,028) without fsync and 135,489 (132,451 to 140,647) with it.
- The data is synthetic. No real hardware produced it, and the ratios depend on the four shapes I chose: the two signals with noise in the values (temperature, vibration) are the two near 2x; the two without noise compress 44x and 141x with regular timestamps, and 7x and 8x with jitter.
- The kill test ends the process. The operating system's cache survives it, so it does not test power loss or what `fsync` buys.
- A random kill left a half-finished flush on disk (a temporary segment, or a log already covered by a segment) in only 1 of the 30 rounds. The four crash points listed under "Run it" are tested one at a time by raising at each, not by luck of timing.
- The random kill alone is a weak detector for a log that is never flushed (with that bug added to a copy, three runs of 20 kill rounds failed in 3, 5 and 2 rounds); the test that reads the log file while the writer is still open is what catches it.
- One writer: ingest and flush share a lock, and a flush runs inside `ingest()` while the lock is held. There is no message bus and no backpressure. No compaction, no retention, no de-duplication of repeated timestamps, no network, no second node.
- zlib, not ZSTD, because Python 3.13's standard library has no ZSTD. The throughput is pure Python and says nothing about any production engine.
