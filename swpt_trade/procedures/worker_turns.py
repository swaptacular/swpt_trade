from typing import TypeVar, Callable, List, Optional
from datetime import datetime, timezone
from sqlalchemy import select
from swpt_trade.extensions import db
from swpt_trade.models import (
    Turn,
    WorkerTurn,
    RecentlyNeededCollector,
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
