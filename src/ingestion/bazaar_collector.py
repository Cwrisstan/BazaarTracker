import json
import os
import sqlite3
import requests
from datetime import datetime, timezone

# 1. Directory setups
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, "..", ".."))

# Save the database inside data/raw to ensure parent directory creation
RAW_DATA_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DB_PATH = os.path.join(RAW_DATA_DIR, "hypixel_bazaar.db")

HYPIXEL_URL = "https://api.hypixel.net/v2/skyblock/bazaar"

def init_db():
    """Ensures parent folder exists and initializes SQLite table."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS product_quick_status (
                timestamp DATETIME,
                product_id TEXT,
                sell_price REAL,
                sell_volume INTEGER,
                sell_orders INTEGER,
                buy_price REAL,
                buy_volume INTEGER,
                buy_orders INTEGER,
                sell_moving_week INTEGER,
                buy_moving_week INTEGER
            )
        """)
        conn.commit()


def save_raw_json(payload, now):
    """Saves the un-truncated API response to data/raw/YYYY-MM-DD/HH-MM-SS.json."""
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H-%M-%S")

    dir_path = os.path.join(RAW_DATA_DIR, date_str)
    os.makedirs(dir_path, exist_ok=True)

    file_path = os.path.join(dir_path, f"{time_str}.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def collect_bazaar():
    """Fetches, validates, stores raw JSON, and inserts quick status into SQLite."""
    init_db()

    api_key = os.getenv("HYPIXEL_API_KEY")
    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    try:
        response = requests.get(HYPIXEL_URL, headers=headers, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except requests.exceptions.Timeout:
        print("[ERROR] Request timed out while connecting to Hypixel API.")
        return
    except requests.exceptions.RequestException as err:
        print(f"[ERROR] Failed to fetch Bazaar data: {err}")
        return
    except json.JSONDecodeError:
        print("[ERROR] Failed to parse JSON from response.")
        return

    if not payload.get("success", False):
        print("[ERROR] API returned success=False")
        return

    now = datetime.now(timezone.utc)

    # Preserve RAW JSON
    save_raw_json(payload, now)

    # Extract quick_status and persist to SQLite
    products = payload.get("products", {})
    records = []

    for product_id, product_data in products.items():
        qs = product_data.get("quick_status")
        if not qs:
            continue

        records.append((
            now,
            qs.get("productId"),
            qs.get("sellPrice"),
            qs.get("sellVolume"),
            qs.get("sellOrders"),
            qs.get("buyPrice"),
            qs.get("buyVolume"),
            qs.get("buyOrders"),
            qs.get("sellMovingWeek"),
            qs.get("buyMovingWeek"),
        ))

    if records:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.executemany("""
                INSERT INTO product_quick_status (
                    timestamp, product_id, sell_price, sell_volume, sell_orders,
                    buy_price, buy_volume, buy_orders, sell_moving_week, buy_moving_week
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, records)
            conn.commit()
        print(
            f"[{now.strftime('%H:%M:%S UTC')}] Saved RAW JSON and inserted {len(records)} DB records."
        )


if __name__ == "__main__":
    collect_bazaar()