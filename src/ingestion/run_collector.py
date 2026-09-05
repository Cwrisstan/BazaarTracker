import time
import signal
import sys
from datetime import datetime, timezone
from bazaar_collector import collect_bazaar

# Configuration
POLL_INTERVAL = 60  # seconds between snapshot collections

running = True

def handle_exit(signum, frame):
    """Gracefully handles CTRL+C / terminate signals."""
    global running
    print("\n[Scheduler] Shutdown signal received. Finishing last run and exiting...")
    running = False

# Register signal listeners for clean shutdown
signal.signal(signal.SIGINT, handle_exit)
signal.signal(signal.SIGTERM, handle_exit)

def run_scheduler():
    print(f"[Scheduler] Started Bazaar Collector Service (Polling every {POLL_INTERVAL}s)")
    
    while running:
        start_time = time.time()
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        
        print(f"[{now_str}] Running Bazaar snapshot...")
        try:
            collect_bazaar()
        except Exception as err:
            print(f"[{now_str}] Unexpected error during collection: {err}")

        # Account for execution drift so loop stays closely aligned to 60s steps
        elapsed = time.time() - start_time
        sleep_duration = max(0, POLL_INTERVAL - elapsed)

        # Sleep in small 1-second chunks so CTRL+C stays responsive immediately
        slept = 0
        while slept < sleep_duration and running:
            time.sleep(1)
            slept += 1

    print("[Scheduler] Stopped safely.")

if __name__ == "__main__":
    run_scheduler()