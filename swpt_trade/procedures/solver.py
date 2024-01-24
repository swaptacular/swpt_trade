from typing import TypeVar, Callable, List
from datetime import datetime, timezone, timedelta
from sqlalchemy import select, insert, text
from sqlalchemy.sql.expression import null, and_
from swpt_trade.utils import can_start_new_turn
from swpt_trade.extensions import db
from swpt_trade.models import (
    Turn,
    DebtorInfo,
    ConfirmedDebtor,
    CurrencyInfo,
    CreditorGiving,
    CollectorReceiving,
    CollectorSending,
    CollectorCollecting,
    CreditorTaking,
    TS0,
)


T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def start_new_turn_if_possible(
        *,
        turn_period: timedelta,
        turn_period_offset: timedelta,
        phase1_duration: timedelta,
) -> List[Turn]:
    current_ts = datetime.now(tz=timezone.utc)
    db.session.execute(
        text("LOCK TABLE turn IN SHARE ROW EXCLUSIVE MODE"),
        bind_arguments={"bind": db.engines["solver"]},
    )
    unfinished_turns = Turn.query.filter(Turn.phase < 4).all()
    if not unfinished_turns:
        latest_turn = (
            Turn.query
            .order_by(Turn.started_at.desc())
            .limit(1)
            .one_or_none()
        )
        if can_start_new_turn(
                turn_period=turn_period,
                turn_period_offset=turn_period_offset,
                latest_turn_started_at=(
                    latest_turn.started_at if latest_turn else TS0
                ),
                current_ts=current_ts,
        ):
            new_turn = Turn(
                started_at=current_ts,
                phase_deadline=current_ts + phase1_duration,
            )
            db.session.add(new_turn)
            return [new_turn]

    return unfinished_turns


@atomic
def try_to_advence_turn_to_phase2(
        *,
        turn_id: int,
        phase2_duration: timedelta,
        max_commit_period: timedelta,
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 1:
        db.session.execute(
            insert(CurrencyInfo).from_select(
                [
                    "turn_id",
                    "debtor_info_locator",
                    "debtor_id",
                    "peg_debtor_info_locator",
                    "peg_debtor_id",
                    "peg_exchange_rate",
                    "is_confirmed",
                ],
                select(
                    DebtorInfo.turn_id,
                    DebtorInfo.debtor_info_locator,
                    DebtorInfo.debtor_id,
                    DebtorInfo.peg_debtor_info_locator,
                    DebtorInfo.peg_debtor_id,
                    DebtorInfo.peg_exchange_rate,
                    ConfirmedDebtor.turn_id != null(),
                )
                .select_from(DebtorInfo)
                .join(
                    ConfirmedDebtor,
                    and_(
                        ConfirmedDebtor.turn_id == DebtorInfo.turn_id,
                        ConfirmedDebtor.debtor_id == DebtorInfo.debtor_id,
                        ConfirmedDebtor.debtor_info_locator
                        == DebtorInfo.debtor_info_locator,
                    ),
                    isouter=True,
                )
                .where(DebtorInfo.turn_id == turn_id),
            )
        )
        turn.phase = 2
        turn.phase_deadline = current_ts + phase2_duration
        turn.collection_deadline = current_ts + max_commit_period


@atomic
def try_to_advence_turn_to_phase4(turn_id: int) -> None:
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 3:
        for table in [
                CreditorGiving,
                CollectorReceiving,
                CollectorSending,
                CollectorCollecting,
                CreditorTaking,
        ]:
            has_pending_rows = bool(
                db.session.execute(
                    select(1)
                    .select_from(table)
                    .filter_by(turn_id=turn_id)
                    .limit(1)
                ).one_or_none()
            )
            if has_pending_rows:
                break
        else:
            # There are no pending rows.
            turn.phase = 4
            turn.phase_deadline = None
