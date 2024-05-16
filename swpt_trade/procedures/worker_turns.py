import random
import math
from typing import TypeVar, Callable, List, Optional
from datetime import datetime, timezone, date
from sqlalchemy import select
from sqlalchemy.orm import load_only
from swpt_trade.utils import calc_k, contain_principal_overflow
from swpt_trade.extensions import db
from swpt_trade.models import (
    MAX_INT32,
    T_INFINITY,
    cr_seq,
    Turn,
    WorkerTurn,
    RecentlyNeededCollector,
    ActiveCollector,
    AccountLock,
    PrepareTransferSignal,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def update_or_create_worker_turn(turn: Turn) -> None:
    phase = turn.phase
    phase_deadline = turn.phase_deadline

    if phase == 4:
        # From the worker's point of view, solver's turn phase 4 is no
        # different than solver's turn phase 3.
        phase = 3
        phase_deadline = None

    worker_turn = (
        WorkerTurn.query
        .filter_by(turn_id=turn.turn_id)
        .one_or_none()
    )
    if worker_turn is None:
        with db.retry_on_integrity_error():
            db.session.add(
                WorkerTurn(
                    turn_id=turn.turn_id,
                    started_at=turn.started_at,
                    base_debtor_info_locator=turn.base_debtor_info_locator,
                    base_debtor_id=turn.base_debtor_id,
                    max_distance_to_base=turn.max_distance_to_base,
                    min_trade_amount=turn.min_trade_amount,
                    phase=phase,
                    phase_deadline=phase_deadline,
                    collection_started_at=turn.collection_started_at,
                    collection_deadline=turn.collection_deadline,
                )
            )
    elif worker_turn.phase < phase:
        worker_turn.phase = phase
        worker_turn.phase_deadline = phase_deadline
        worker_turn.collection_started_at = turn.collection_started_at
        worker_turn.collection_deadline = turn.collection_deadline
        worker_turn.worker_turn_subphase = 0


@atomic
def get_unfinished_worker_turn_ids() -> List[int]:
    return (
        db.session.execute(
            select(WorkerTurn.turn_id)
            .filter(WorkerTurn.phase < 3)
        )
        .scalars()
        .all()
    )


@atomic
def get_pending_worker_turns() -> List[WorkerTurn]:
    return (
        WorkerTurn.query
        .filter(WorkerTurn.worker_turn_subphase < 10)
        .all()
    )


@atomic
def is_recently_needed_collector(debtor_id: int) -> bool:
    return (
        db.session.query(
            RecentlyNeededCollector.query
            .filter_by(debtor_id=debtor_id)
            .exists()
        )
        .scalar()
    )


@atomic
def mark_as_recently_needed_collector(
        debtor_id: int,
        needed_at: Optional[datetime] = None,
) -> None:
    if needed_at is None:
        needed_at = datetime.now(tz=timezone.utc)

    if not is_recently_needed_collector(debtor_id):
        with db.retry_on_integrity_error():
            db.session.add(
                RecentlyNeededCollector(
                    debtor_id=debtor_id,
                    needed_at=needed_at,
                )
            )


@atomic
def process_candidate_offer_signal(
        *,
        demurrage_rate: float,
        min_trade_amount: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        amount: int,
        account_creation_date: date,
        last_transfer_number: int,
):
    current_ts = datetime.now(tz=timezone.utc)

    worker_turn = (
        WorkerTurn.query
        .filter_by(turn_id=turn_id, phase=2, worker_turn_subphase=5)
        .options(load_only(WorkerTurn.collection_deadline))
        .with_for_update(read=True, skip_locked=True)
        .one_or_none()
    )
    if not worker_turn:
        return

    account_lock = (
        AccountLock.query
        .filter(creditor_id=creditor_id, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if account_lock and account_lock.is_in_force(
            account_creation_date, last_transfer_number
    ):
        return

    active_collectors = (
        ActiveCollector.query
        .filter_by(debtor_id=debtor_id)
        .all()
    )
    try:
        collector = random.choice(active_collectors)
    except IndexError:
        return

    max_locked_amount = contain_principal_overflow(int(
        amount * math.exp(
            calc_k(demurrage_rate)
            * (worker_turn.collection_deadline - current_ts).total_seconds()
        )
    ))
    if max_locked_amount < min_trade_amount:
        return

    coordinator_request_id = db.session.scalar(cr_seq)

    if account_lock:
        account_lock.turn_id = turn_id
        account_lock.coordinator_request_id = coordinator_request_id
        account_lock.collector_id = collector.collector_id
        account_lock.initiated_at = current_ts
        account_lock.has_been_released = False
        account_lock.transfer_id = None
        account_lock.amount = None
        account_lock.finalized_at = None
        account_lock.status_code = None
        account_lock.account_creation_date = None
        account_lock.account_last_transfer_number = None
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                AccountLock(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    turn_id=turn_id,
                    coordinator_request_id=coordinator_request_id,
                    collector_id=collector.collector_id,
                )
            )

    db.session.add(
        PrepareTransferSignal(
            creditor_id=creditor_id,
            coordinator_request_id=coordinator_request_id,
            debtor_id=debtor_id,
            recipient=collector.account_id,
            min_locked_amount=min_trade_amount,
            max_locked_amount=max_locked_amount,
            final_interest_rate_ts=T_INFINITY,
            max_commit_delay=MAX_INT32,
            inserted_at=current_ts,
        )
    )
