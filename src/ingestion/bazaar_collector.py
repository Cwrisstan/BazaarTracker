"""Validated snapshot persistence, conservative storage checks, and HTTP fetching."""
import gzip
import json
import logging
import math
import os
from pathlib import Path
import re
import shutil
import sqlite3
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import requests
from src.storage import archive, schema, universe

LOG = logging.getLogger(__name__)
HYPIXEL_URL = "https://api.hypixel.net/v2/skyblock/bazaar"
GIB = 1024 ** 3
QUICK_FIELDS = (
    "sellPrice", "sellVolume", "sellOrders", "buyPrice", "buyVolume",
    "buyOrders", "sellMovingWeek", "buyMovingWeek",
)
OWNED = re.compile(r"bazaar-v1-[1-9][0-9]*\.json\.gz(?:\.tmp)?\Z")


class StorageFull(Exception):
    pass


class InvalidPayload(ValueError):
    pass


class FetchError(Exception):
    def __init__(self, message, retryable=True, retry_after=0):
        super().__init__(message)
        self.retryable = retryable
        self.retry_after = retry_after


def number(value, label, integer=False):
    try:
        finite = math.isfinite(value)
    except (TypeError, OverflowError):
        finite = False
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not finite or value < 0
            or (integer and (not isinstance(value, int) or value > 2**63 - 1))):
        raise InvalidPayload(f"invalid {label}: expected nonnegative {'integer' if integer else 'number'}")
    return value


def validate(payload):
    """Reject the entire snapshot on any malformed product; empty books are valid."""
    if not isinstance(payload, dict) or payload.get("success") is not True:
        raise InvalidPayload("response must be an object with success=true")
    source = number(payload.get("lastUpdated"), "lastUpdated", integer=True)
    if source == 0:
        raise InvalidPayload("lastUpdated must be positive")
    products = payload.get("products")
    if not isinstance(products, dict):
        raise InvalidPayload("products must be an object")
    quick, levels = [], []
    for pid, product in products.items():
        if not isinstance(pid, str) or not pid or not isinstance(product, dict):
            raise InvalidPayload("invalid product")
        qs = product.get("quick_status")
        if (product.get("product_id") != pid or not isinstance(qs, dict)
                or qs.get("productId") != pid):
            raise InvalidPayload(f"{pid}: missing or inconsistent product identity")
        values = [number(qs.get(key), f"{pid}.{key}", integer=not key.endswith("Price"))
                  for key in QUICK_FIELDS]
        quick.append((source, pid, *values))
        for side in ("buy_summary", "sell_summary"):
            book = product.get(side)
            if not isinstance(book, list):
                raise InvalidPayload(f"{pid}.{side}: expected array")
            for index, entry in enumerate(book):
                if not isinstance(entry, dict):
                    raise InvalidPayload(f"{pid}.{side}[{index}]: expected object")
                levels.append((source, pid, side, index,
                               number(entry.get("pricePerUnit"), f"{pid}.{side}.pricePerUnit"),
                               number(entry.get("amount"), f"{pid}.{side}.amount", integer=True),
                               number(entry.get("orders"), f"{pid}.{side}.orders", integer=True)))
    return source, quick, levels


class Storage:
    def __init__(self, root, raw_dir=None, budget=GIB, min_free=5 * GIB,
                 retention=48 * 3600, headroom=16 * 1024 ** 2, research_universe=None):
        self.root = Path(root).resolve()
        self.raw = Path(raw_dir).resolve() if raw_dir else self.root / "raw"
        if self.raw == self.root or self.root not in self.raw.parents:
            raise ValueError("raw directory must be a child of data directory")
        self.db = self.root / "bazaar.sqlite3"
        if self.raw == self.db or self.raw in self.db.parents:
            raise ValueError("database must be outside raw directory")
        self.budget, self.min_free = budget, min_free
        self.retention, self.headroom = retention, headroom
        self.research_universe = universe.normalize(universe.DEFAULT if research_universe is None else research_universe)
        self.root.mkdir(parents=True, exist_ok=True)
        self.raw.mkdir(parents=True, exist_ok=True)

    def usage(self):
        # Count all project data, including legacy files and SQLite sidecars.
        return sum(p.stat().st_size for p in self.root.rglob("*") if p.is_file())

    def check(self, incoming=0):
        used = self.usage()
        reserve = incoming + self.headroom
        free = shutil.disk_usage(self.root).free
        LOG.info("storage used=%d budget=%d free=%d reserve=%d", used, self.budget, free, reserve)
        if used + reserve > self.budget:
            raise StorageFull(f"data budget: {used} used + {reserve} reserved > {self.budget}")
        # Raw directory may be a separately mounted filesystem.
        raw_free = shutil.disk_usage(self.raw).free
        if min(free, raw_free) - reserve < self.min_free:
            raise StorageFull(f"free disk reserve: free={min(free, raw_free)} required={self.min_free + reserve}")

    def cleanup(self, now=None):
        now = datetime.now(timezone.utc).timestamp() if now is None else now
        removed = 0
        for path in self.raw.iterdir():
            if (OWNED.fullmatch(path.name) and not path.is_symlink() and path.is_file()
                    and path.stat().st_mtime < now - self.retention):
                path.unlink()
                removed += 1
        LOG.info("retention removed=%d expired raw files", removed)
        return removed

    def write_raw(self, source, compressed):
        final = self.raw / f"bazaar-v1-{source}.json.gz"
        temporary = final.with_name(final.name + ".tmp")
        # O_NOFOLLOW prevents a stale/malicious symlink from redirecting writes.
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(compressed)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, final)
        directory = os.open(self.raw, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


def open_database(storage):
    storage.check()
    conn = sqlite3.connect(storage.db)
    try:
        if conn.execute("SELECT 1 FROM sqlite_master WHERE name='snapshots'").fetchone():
            schema.require_ready(conn)
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=DELETE")
        conn.execute("PRAGMA synchronous=FULL")
        conn.executescript("""BEGIN IMMEDIATE;
            CREATE TABLE IF NOT EXISTS snapshots (
                source_updated_ms INTEGER PRIMARY KEY,
                collected_at_utc TEXT NOT NULL,
                product_count INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS quick_status (
                source_updated_ms INTEGER NOT NULL REFERENCES snapshots(source_updated_ms),
                product_id TEXT NOT NULL,
                sell_price REAL NOT NULL, sell_volume INTEGER NOT NULL, sell_orders INTEGER NOT NULL,
                buy_price REAL NOT NULL, buy_volume INTEGER NOT NULL, buy_orders INTEGER NOT NULL,
                sell_moving_week INTEGER NOT NULL, buy_moving_week INTEGER NOT NULL,
                PRIMARY KEY (source_updated_ms, product_id)
            );
            CREATE TABLE IF NOT EXISTS order_book_levels (
                source_updated_ms INTEGER NOT NULL, product_id TEXT NOT NULL,
                api_side TEXT NOT NULL CHECK(api_side IN ('buy_summary', 'sell_summary')),
                level_index INTEGER NOT NULL, price_per_unit REAL NOT NULL,
                amount INTEGER NOT NULL, orders INTEGER NOT NULL,
                PRIMARY KEY (source_updated_ms, product_id, api_side, level_index),
                FOREIGN KEY (source_updated_ms, product_id)
                    REFERENCES quick_status(source_updated_ms, product_id)
            );
        """)
        if not schema.exists(conn):
            schema.create(conn, datetime.now(timezone.utc).isoformat())
        conn.commit()
        return conn
    except BaseException:
        conn.close()
        raise


def persist(conn, storage, payload, collected_at=None):
    schema.require_ready(conn)
    source, quick, levels = validate(payload)
    collected_at = collected_at or datetime.now(timezone.utc)
    if collected_at.tzinfo is None or collected_at.utcoffset() is None:
        raise InvalidPayload("collection time must be timezone-aware")
    if conn.execute("SELECT 1 FROM snapshots WHERE source_updated_ms=?", (source,)).fetchone():
        LOG.info("duplicate snapshot source_updated_ms=%d products=%d", source, len(quick))
        return False
    try:
        compact = json.dumps(payload, separators=(",", ":"), allow_nan=False).encode()
    except (ValueError, TypeError) as exc:
        raise InvalidPayload("response contains invalid JSON values") from exc
    compressed = gzip.compress(compact, mtime=0)
    # Allow for raw staging plus indexed DB rows and rollback journal growth.
    storage.check(2 * len(compressed) + max(4 * len(compact), 1024 * (len(quick) + len(levels))))
    with conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("INSERT INTO snapshots VALUES (?, ?, ?)",
                     (source, collected_at.astimezone(timezone.utc).isoformat(), len(quick)))
        conn.executemany("INSERT INTO quick_status VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", quick)
        conn.executemany("INSERT INTO order_book_levels VALUES (?, ?, ?, ?, ?, ?, ?)", levels)
        archive.write_snapshot(conn, source, quick, payload['products'], storage.research_universe)
        # Publish raw before commit: a crash can leave an orphan, never a success marker.
        storage.write_raw(source, compressed)
    LOG.info("new snapshot source_updated_ms=%d products=%d levels=%d", source, len(quick), len(levels))
    storage.check(0)
    return True


def retry_after_seconds(value, now=None):
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        try:
            seconds = parsedate_to_datetime(value).timestamp() - (now or datetime.now(timezone.utc)).timestamp()
        except (TypeError, ValueError, OverflowError):
            return 0
    return max(0, seconds) if math.isfinite(seconds) else 0


def backoff(failures, retry_after=0, maximum=300):
    # Server-directed waits may exceed the exponential cap; never retry early.
    return max(min(maximum, 2 ** min(failures, 20)), retry_after)


def fetch(session, timeout=10):
    try:
        response = session.get(HYPIXEL_URL, timeout=timeout)
        with response:
            if response.status_code == 429 or 500 <= response.status_code < 600:
                raise FetchError(f"HTTP {response.status_code}", retry_after=retry_after_seconds(response.headers.get("Retry-After")))
            if response.status_code != 200:
                raise FetchError(f"HTTP {response.status_code}", retryable=False)
            try:
                return response.json()
            except ValueError as exc:
                raise InvalidPayload("response is not valid JSON") from exc
    except requests.RequestException as exc:
        raise FetchError(f"{type(exc).__name__}: {exc}") from exc
