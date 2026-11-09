"""Ingest batches forever and write each batch number to an ack file after
ingest() returns. Run by crashcheck, which kills it at a random moment."""
import sys

from .gen import crash_batch
from .store import Store


def main(directory, seed, ack_path):
    store = Store(directory, memtable_limit=500, fsync=True)
    with open(ack_path, "a") as ack:
        i = 0
        while True:
            store.ingest(crash_batch(seed, i))
            ack.write("%d\n" % i)
            ack.flush()
            i += 1


if __name__ == "__main__":
    main(sys.argv[1], int(sys.argv[2]), sys.argv[3])
