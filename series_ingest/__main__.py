import os
import sys

from . import bench


def main(argv):
    if len(argv) >= 2 and argv[1] == "bench":
        rounds = int(argv[2]) if len(argv) > 2 else 30
        out = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results")
        bench.run_all(out, kill_rounds=rounds)
        print(open(os.path.join(out, "summary.md")).read())
        return 0
    print("usage: python -m series_ingest bench [kill_rounds]")
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
