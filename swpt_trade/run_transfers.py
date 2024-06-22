from typing import TypeVar, Callable
from flask import current_app
from swpt_trade.extensions import db
from swpt_trade.procedures import process_rescheduled_transfers_batch

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


def process_rescheduled_transfers() -> int:
    count = 0
    batch_size = current_app.config["APP_RESCHEDULED_TRANSFERS_BURST_COUNT"]

    while True:
        n = process_rescheduled_transfers_batch(batch_size)
        count += n
        if n < batch_size:
            break

    return count
