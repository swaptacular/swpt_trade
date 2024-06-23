from typing import TypeVar, Callable
from flask import current_app
from datetime import datetime, timezone
from sqlalchemy import select, insert, update, delete
from sqlalchemy.sql.expression import tuple_, and_, true, false, text
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


def kick_dispatching_statuses_ready_to_send() -> None:
    pending_collectings_subquery = (
        select(1)
        .select_from(WorkerCollecting)
        .where(
            WorkerCollecting.collector_id == DispatchingStatus.collector_id,
            WorkerCollecting.turn_id == DispatchingStatus.turn_id,
            WorkerCollecting.debtor_id == DispatchingStatus.debtor_id,
            WorkerCollecting.collected == false(),
        )
    ).exists()

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .where(
                    and_(
                        DispatchingStatus.started_sending == false(),
                        DispatchingStatus.awaiting_signal_flag == false(),
                        ~pending_collectings_subquery,
                    )
                )
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


def update_dispatching_statuses_with_everything_sent() -> None:
    pending_sendings_subquery = (
        select(1)
        .select_from(WorkerSending)
        .where(
            WorkerSending.from_collector_id == DispatchingStatus.collector_id,
            WorkerSending.turn_id == DispatchingStatus.turn_id,
            WorkerSending.debtor_id == DispatchingStatus.debtor_id,
        )
    ).exists()

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .where(
                    DispatchingStatus.started_sending == true(),
                    DispatchingStatus.all_sent == false(),
                    ~pending_sendings_subquery,
                )
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


def kick_dispatching_statuses_ready_to_dispatch() -> None:
    pending_receivings_subquery = (
        select(1)
        .select_from(WorkerReceiving)
        .where(
            WorkerReceiving.to_collector_id == DispatchingStatus.collector_id,
            WorkerReceiving.turn_id == DispatchingStatus.turn_id,
            WorkerReceiving.debtor_id == DispatchingStatus.debtor_id,
            WorkerReceiving.received_amount == text("0"),
        )
    ).exists()

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .where(
                    and_(
                        DispatchingStatus.all_sent == true(),
                        DispatchingStatus.started_dispatching == false(),
                        DispatchingStatus.awaiting_signal_flag == false(),
                        ~pending_receivings_subquery,
                    )
                )
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


def delete_dispatching_statuses_with_everything_dispatched() -> None:
    pending_dispatchings_subquery = (
        select(1)
        .select_from(WorkerDispatching)
        .where(
            WorkerDispatching.collector_id == DispatchingStatus.collector_id,
            WorkerDispatching.turn_id == DispatchingStatus.turn_id,
            WorkerDispatching.debtor_id == DispatchingStatus.debtor_id,
        )
    ).exists()

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    DispatchingStatus.collector_id,
                    DispatchingStatus.turn_id,
                    DispatchingStatus.debtor_id,
                )
                .where(
                    and_(
                        DispatchingStatus.started_dispatching == true(),
                        ~pending_dispatchings_subquery,
                    )
                )
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
