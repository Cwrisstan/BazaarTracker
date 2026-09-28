"""CLI scheduler. One collector per data root; waits are interruptible."""
import argparse
from datetime import datetime, timezone
import fcntl
import logging
from pathlib import Path
import signal
import sqlite3
import threading
import time

import requests

try:
    from .bazaar_collector import (Storage, StorageFull, InvalidPayload, FetchError,
                                   open_database, persist, fetch, backoff, GIB)
except ImportError:
    from bazaar_collector import (Storage, StorageFull, InvalidPayload, FetchError,
                                 open_database, persist, fetch, backoff, GIB)

LOG = logging.getLogger(__name__)


def positive(value):
    import math
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise argparse.ArgumentTypeError("must be finite and positive")
    return result


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--data-dir", type=Path, default=Path(__file__).resolve().parents[2] / "data")
    result.add_argument("--raw-dir", type=Path, help="child of data-dir; defaults to data-dir/raw")
    result.add_argument("--poll-interval", type=positive, default=60)
    result.add_argument("--budget-gib", type=positive, default=1)
    result.add_argument("--min-free-gib", type=positive, default=5)
    result.add_argument("--raw-retention-hours", type=positive, default=48)
    result.add_argument("--headroom-mib", type=positive, default=16)
    result.add_argument("--timeout", type=positive, default=10)
    mode = result.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true")
    mode.add_argument("--duration", type=positive, help="run for this many seconds")
    return result


def run(args, stop, session, monotonic=time.monotonic):
    deadline = monotonic() + args.duration if args.duration else float("inf")
    storage = Storage(args.data_dir, args.raw_dir, int(args.budget_gib * GIB),
                      int(args.min_free_gib * GIB), args.raw_retention_hours * 3600,
                      int(args.headroom_mib * 1024 ** 2))
    with (storage.root / ".collector.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            LOG.error("stopping: another collector owns this data directory")
            return 1
        storage.cleanup()
        conn = open_database(storage)
        failures = 0
        try:
            while not stop.is_set() and monotonic() < deadline:
                storage.cleanup()
                storage.check()
                delay = args.poll_interval
                try:
                    payload = fetch(session, min(args.timeout, max(0.001, deadline - monotonic())))
                    persist(conn, storage, payload, datetime.now(timezone.utc))
                    failures = 0
                except FetchError as exc:
                    LOG.warning("request failed: %s", exc)
                    if args.once or not exc.retryable:
                        return 1
                    failures += 1
                    delay = max(args.poll_interval, backoff(failures, exc.retry_after))
                    LOG.info("retry in %.1f seconds", delay)
                except InvalidPayload as exc:
                    LOG.error("rejected entire snapshot: %s", exc)
                    if args.once:
                        return 1
                if args.once:
                    LOG.info("stopping: one-shot complete")
                    return 0
                stop.wait(min(delay, max(0, deadline - monotonic())))
            LOG.info("stopping: %s", "shutdown signal" if stop.is_set() else "duration reached")
            return 0
        finally:
            conn.close()


def main(argv=None):
    args = parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = threading.Event()
    previous = {sig: signal.signal(sig, lambda *_: stop.set()) for sig in (signal.SIGINT, signal.SIGTERM)}
    try:
        with requests.Session() as session:
            return run(args, stop, session)
    except (StorageFull, OSError, sqlite3.Error, ValueError) as exc:
        LOG.error("stopping safely: %s", exc)
        return 1
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)


if __name__ == "__main__":
    raise SystemExit(main())
