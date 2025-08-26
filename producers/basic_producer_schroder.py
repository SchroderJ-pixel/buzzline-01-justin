"""
basic_producer_schroder.py

Emit streaming social comments about Taylor Swift getting engaged.
Based on the original basic_generator_case.py template.
"""

#####################################
# Import Modules
#####################################

import os
import random
import time
from itertools import count

from dotenv import load_dotenv
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter for interval
#####################################

def get_message_interval() -> int:
    """
    Fetch message interval (seconds) from env (default 2).
    """
    return int(os.getenv("MESSAGE_INTERVAL_SECONDS", 2))

#####################################
# Comment phrases
#####################################

POSITIVE: list[str] = [
    "Omg I can’t believe Taylor is engaged!! 🥹💍",
    "So happy for Taylor and her fiancé 💖",
    "This is iconic—Taylor deserves the world 💕",
    "Engagement era is here!!! ✨",
]
SKEPTICAL: list[str] = [
    "I didn’t see this coming 👀",
    "Hope the media doesn’t ruin this moment",
    "This feels too sudden… anyone else?",
    "Is this really true?",
]
FUNNY: list[str] = [
    "Time to plan the Eras Wedding Tour 😂",
    "Will we get ‘Love Story (Engaged Version)’?",
    "Bet this inspires 3 new albums 📀",
]
ALL_COMMENTS: list[str] = POSITIVE + SKEPTICAL + FUNNY

#####################################
# Generator
#####################################

def generate_comments():
    """
    Yield one comment at a time forever.
    Every 8th message emits an alert token to demo consumer alerts.
    """
    for i in count(1):
        if i % 8 == 0:
            yield f"SCHRODER_ALERT: Viral comment spike detected at seq={i}"
        else:
            yield random.choice(ALL_COMMENTS)

#####################################
# main()
#####################################

def main() -> None:
    logger.info("START producer (schroder)...")
    logger.info("Hit CTRL+C to stop.")

    interval_secs: int = get_message_interval()
    logger.info(f"Messages will be sent every {interval_secs} seconds.")

    for message in generate_comments():
        logger.info(message)
        time.sleep(interval_secs)

    logger.info("END producer (schroder).")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
