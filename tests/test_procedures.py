import pytest
from datetime import timedelta, datetime, timezone
from swpt_trade import procedures as p
from swpt_trade.models import Turn


def test_start_new_turn_if_possible(db_session):
    current_ts = datetime.now(tz=timezone.utc)
    midnight = current_ts.replace(hour=0, minute=0, second=0, microsecond=0)
    turns = p.start_new_turn_if_possible(
        turn_period=timedelta(days=1),
        turn_period_offset=current_ts - midnight,
        phase1_duration=timedelta(hours=1),
    )
    assert len(turns) == 0

    turns = Turn.query.all()
    assert len(turns) == 1
    assert turns[0].phase == 1

    turns = p.start_new_turn_if_possible(
        turn_period=timedelta(days=1),
        turn_period_offset=current_ts - midnight,
        phase1_duration=timedelta(hours=1),
    )
    assert len(turns) == 1
    assert turns[0].phase == 1
