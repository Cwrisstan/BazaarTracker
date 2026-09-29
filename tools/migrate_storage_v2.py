import argparse
import fcntl
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.ingestion.bazaar_collector import Storage, GIB
from src.storage.migration import migrate


def main(argv=None):
    from src.ingestion.run_collector import positive
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, default=Path(__file__).resolve().parents[1] / 'data')
    parser.add_argument('--budget-gib', type=positive, default=1)
    parser.add_argument('--min-free-gib', type=positive, default=5)
    args = parser.parse_args(argv)
    storage = Storage(args.data_dir, budget=int(args.budget_gib * GIB), min_free=int(args.min_free_gib * GIB))
    with (storage.root / '.collector.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        print(json.dumps(migrate(storage), indent=2))


if __name__ == '__main__':
    main()
