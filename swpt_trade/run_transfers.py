from typing import TypeVar, Callable
from flask import current_app
from datetime import datetime, timezone
from sqlalchemy import select, insert, update, delete
from sqlalchemy.sql.expression import tuple_, and_, true, false, func
from swpt_trade.extensions import db
from swpt_trade.procedures import process_rescheduled_transfers_batch
from swpt_trade.models import (
    DispatchingStatus,
    WorkerCollecting,
    WorkerSending,
    WorkerReceiving,
    WorkerDispatching,
    StartSendingSignal,
    StartDispatchingSignal,
)
from swpt_trade.utils import batched

DISPATCHING_STATUS_PK = tuple_(
    DispatchingStatus.collector_id,
    DispatchingStatus.turn_id,
    DispatchingStatus.debtor_id,
)
INSERT_BATCH_SIZE = 6000
SELECT_BATCH_SIZE = 50000

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


def kick_collectors_ready_to_send() -> None:
    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .outerjoin(DispatchingStatus.pending_collectings)
                .where(
                    and_(
                        DispatchingStatus.started_sending == false(),
                        DispatchingStatus.awaiting_signal_flag == false(),
                    )
                )
                .group_by(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .having(func.count(WorkerCollecting.collector_id) == 0)
        ) as result:
            for rows in batched(result, INSERT_BATCH_SIZE):
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.turn_id,
                            DispatchingStatus.debtor_id,
                        )
                        .where(DISPATCHING_STATUS_PK.in_(rows))
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    current_ts = datetime.now(tz=timezone.utc)

                    db.session.execute(
                        update(DispatchingStatus)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                        .values(awaiting_signal_flag=True)
                    )
                    db.session.execute(
                        insert(StartSendingSignal).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE
                        ),
                        [
                            {
                                "collector_id": row.collector_id,
                                "turn_id": row.turn_id,
                                "debtor_id": row.debtor_id,
                                "inserted_at": current_ts,
                            }
                            for row in locked_rows
                        ],
                    )

                db.session.commit()


def update_collectors_with_everything_sent() -> None:
    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .outerjoin(DispatchingStatus.sendings)
                .where(
                    DispatchingStatus.started_sending == true(),
                    DispatchingStatus.all_sent == false(),
                )
                .group_by(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .having(func.count(WorkerSending.from_collector_id) == 0)
        ) as result:
            for rows in batched(result, INSERT_BATCH_SIZE):
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.turn_id,
                            DispatchingStatus.debtor_id,
                        )
                        .where(DISPATCHING_STATUS_PK.in_(rows))
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    db.session.execute(
                        update(DispatchingStatus)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                        .values(all_sent=True)
                    )

                db.session.commit()


def kick_collectors_ready_to_dispatch() -> None:
    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .outerjoin(DispatchingStatus.pending_receivings)
                .where(
                    and_(
                        DispatchingStatus.all_sent == true(),
                        DispatchingStatus.started_dispatching == false(),
                        DispatchingStatus.awaiting_signal_flag == false(),
                    )
                )
                .group_by(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .having(func.count(WorkerReceiving.to_collector_id) == 0)
        ) as result:
            for rows in batched(result, INSERT_BATCH_SIZE):
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.turn_id,
                            DispatchingStatus.debtor_id,
                        )
                        .where(DISPATCHING_STATUS_PK.in_(rows))
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    current_ts = datetime.now(tz=timezone.utc)

                    db.session.execute(
                        update(DispatchingStatus)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                        .values(awaiting_signal_flag=True)
                    )
                    db.session.execute(
                        insert(StartDispatchingSignal).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE
                        ),
                        [
                            {
                                "collector_id": row.collector_id,
                                "turn_id": row.turn_id,
                                "debtor_id": row.debtor_id,
                                "inserted_at": current_ts,
                            }
                            for row in locked_rows
                        ],
                    )

                db.session.commit()


def delete_collectors_ready_to_be_deleted() -> None:
    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .outerjoin(DispatchingStatus.dispatchings)
                .where(DispatchingStatus.started_dispatching == true())
                .group_by(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .having(func.count(WorkerDispatching.collector_id) == 0)
        ) as result:
            for rows in batched(result, INSERT_BATCH_SIZE):
                locked_rows = (
                    db.session.execute(
                        select(
                            DispatchingStatus.collector_id,
                            DispatchingStatus.turn_id,
                            DispatchingStatus.debtor_id,
                        )
                        .where(DISPATCHING_STATUS_PK.in_(rows))
                        .with_for_update(skip_locked=True)
                    )
                    .all()
                )
                if locked_rows:
                    db.session.execute(
                        delete(DispatchingStatus)
                        .where(DISPATCHING_STATUS_PK.in_(locked_rows))
                    )

                db.session.commit()
