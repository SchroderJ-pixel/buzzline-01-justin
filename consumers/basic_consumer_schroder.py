"""
basic_consumer_schroder.py
Monitor streaming Taylor Swift engagement comments in real time.
- Tails the project log file
- Alerts on SCHRODER_ALERT lines
- Tracks keyword mentions
- Prints a quick summary every 10 comments
"""

from __future__ import annotations
import time
import re
from utils.utils_logger import logger, get_log_file_path

ALERT_TOKEN = re.compile(r"\bSCHRODER_ALERT\b", re.IGNORECASE)
KEYWORDS = {"engaged", "engagement", "fiancé", "wedding", "album", "love", "eras"}
SUMMARY_EVERY = 10

def process_stream(log_path: str) -> None:
    """Follow the log file and process new lines as they arrive."""
    with open(log_path, "r", encoding="utf-8") as f:
        f.seek(0, 2)  # go to end (like tail -f)
        print("Consumer ready: monitoring comments...")

        total = 0
        keyword_hits = 0

        while True:
            line = f.readline()
            if not line:
                time.sleep(0.25)
                continue

            msg = line.strip()
            if not msg:
                continue

            total += 1
            print(f"💬 {msg}")

            if ALERT_TOKEN.search(msg):
                print("🚨 VIRAL SPIKE ALERT! 🚨")
                logger.warning(f"[ALERT] Viral spike detected -> {msg}")

            lower = msg.lower()
            if any(k in lower for k in KEYWORDS):
                keyword_hits += 1
                logger.info(f"[KEYWORD] {msg}")

            if total % SUMMARY_EVERY == 0:
                print("\n--- SUMMARY ---")
                print(f"Total comments processed: {total}")
                print(f"Keyword mentions:         {keyword_hits}")
                print("----------------\n")
                logger.info(f"[SUMMARY] total={total} keyword_hits={keyword_hits}")

def main() -> None:
    logger.info("START consumer (schroder)...")
    log_path = get_log_file_path()
    logger.info(f"Monitoring file: {log_path}")
    try:
        process_stream(log_path)
    except KeyboardInterrupt:
        print("User stopped the consumer.")
    logger.info("END consumer (schroder).")

if __name__ == "__main__":
    main()
