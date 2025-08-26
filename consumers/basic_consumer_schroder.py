"""
basic_producer_schroder.py
Emit fake social media comments about Taylor Swift getting engaged.
"""

import os
import time
import itertools
import random
from typing import Iterator
from utils.utils_logger import logger, get_log_file_path


def get_message_interval() -> float:
    """Interval between comments (default 1.5s)."""
    return float(os.getenv("MESSAGE_INTERVAL_SECONDS", 1.5))


def schroder_comment_stream() -> Iterator[str]:
    """
    Generator yielding fake Taylor Swift engagement comments.
    Every ~8th message includes a SCHRODER_ALERT keyword for demo alerts.
    """
    positive = [
        "Omg I can’t believe Taylor is engaged!! 🥹💍",
        "So happy for Taylor and her fiancé 💖",
        "This is iconic, Taylor deserves the world 💕",
        "Engagement era is here!!! ✨",
    ]
    skeptical = [
        "I didn’t see this coming 👀",
        "Hope the media doesn’t ruin this moment",
        "This feels too sudden… anyone else?",
        "Is this really true?",
    ]
    funny = [
        "Time to start planning the Eras Wedding Tour 😂",
        "Wonder if she’ll write ‘Love Story (Engaged Version)’",
        "Bet this inspires 3 new albums 📀",
    ]

    all_comments = positive + skeptical + funny

    for i in itertools.count(1):
        # About every 8th comment is “special”
        if i % 8 == 0:
            yield f"SCHRODER_ALERT: Viral comment spike detected at seq={i}"
        else:
            yield random.choice(all_comments)


def main() -> None:
    interval = get_message_interval()
    log_path = get_log_file_path()
    logger.info(f"[PRODUCER] Social comments every {interval}s -> {log_path}")

    for comment in schroder_comment_stream():
        logger.info(comment)
        time.sleep(interval)


if __name__ == "__main__":
    main()
