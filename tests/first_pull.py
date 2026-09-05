import os
import sqlite3
import requests
from datetime import datetime, timezone

# 1. Define an absolute path so the DB is always saved next to this script
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "hypixel_bazaar.db")

# 2. Database Initialization
def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_quick_status (
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
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
    conn.close()

# Always run init_db first to guarantee the table exists
init_db()

# 3. API Request setup
api_key = os.getenv("HYPIXEL_API_KEY")
headers = {
    "Authorization": f"Bearer {api_key}",
    "Content-Type": "application/json"
}

url = "https://api.hypixel.net/v2/skyblock/bazaar"
response = requests.get(url, headers=headers)

# 4. Parse & Store Response Data
if response.status_code == 200:
    data = response.json().get("products", {})
    now = datetime.now(timezone.utc)
    quick_status_records = []

    for product_id, product_data in data.items():
        qs = product_data.get("quick_status", {})
        if qs and qs.get("productId"):
            quick_status_records.append((
                now,
                qs.get("productId"),
                qs.get("sellPrice"),
                qs.get("sellVolume"),
                qs.get("sellOrders"),
                qs.get("buyPrice"),
                qs.get("buyVolume"),
                qs.get("buyOrders"),
                qs.get("sellMovingWeek"),
                qs.get("buyMovingWeek")
            ))

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.executemany("""
        INSERT INTO product_quick_status (
            timestamp, product_id, sell_price, sell_volume, sell_orders,
            buy_price, buy_volume, buy_orders, sell_moving_week, buy_moving_week
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, quick_status_records)
    
    conn.commit()
    conn.close()

    print(f"Successfully inserted {len(quick_status_records)} items into SQLite at: {DB_PATH}")
elif response.status_code == 401:
    print("Authentication failed: Invalid API Key")
else:
    print(f"Request failed with status code: {response.status_code}")