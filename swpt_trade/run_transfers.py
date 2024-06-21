from typing import TypeVar, Callable
from datetime import datetime, timezone
from flask import current_app
from sqlalchemy.sql.expression import null
from sqlalchemy.orm import load_only
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from swpt_trade.models import TransferAttempt, TriggerTransferSignal

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


def process_rescheduled_transfers() -> int:
    count = 0
    batch_size = current_app.config["APP_RESCHEDULED_TRANSFERS_BURST_COUNT"]

    while True:
        n = _process_rescheduled_transfers_batch(batch_size)
        count += n
        if n < batch_size:
            break

    return count


@atomic
def _process_rescheduled_transfers_batch(batch_size: int) -> int:
    assert batch_size > 0
    current_ts = datetime.now(tz=timezone.utc)

    transfer_attempts = (
        db.session.query(TransferAttempt)
        .filter(TransferAttempt.rescheduled_for != null())
        .filter(TransferAttempt.rescheduled_for <= current_ts)
        .options(load_only(TransferAttempt.rescheduled_for))
        .with_for_update(skip_locked=True)
        .limit(batch_size)
        .all()
    )

    for attempt in transfer_attempts:
        db.session.add(
            TriggerTransferSignal(
                collector_id=attempt.collector_id,
                turn_id=attempt.turn_id,
                debtor_id=attempt.debtor_id,
                creditor_id=attempt.creditor_id,
                is_dispatching=attempt.is_dispatching,
            )
        )
        attempt.rescheduled_for = None

    return len(transfer_attempts)
