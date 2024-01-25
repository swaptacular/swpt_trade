from typing import TypeVar, Callable
from swpt_trade.extensions import db
from swpt_trade.models import Turn, TS0


T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def try_to_advance_turn_to_phase3(turn_id: int) -> None:
    # TODO: Add a real implementation.
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 2:
        turn.phase = 3
        turn.phase_deadline = None
        turn.collection_started_at = TS0
