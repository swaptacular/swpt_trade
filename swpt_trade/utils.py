import re
from datetime import datetime, timedelta, timezone

RE_PERIOD = re.compile(r"^(\d+)([shdw])\s*$")
DATETIME0 = datetime(2024, 1, 1, tzinfo=timezone.utc)  # 2024-01-01 is Monday.


def parse_timedelta(s: str) -> timedelta:
    """Parse a string to a timedelta object.

    The string must be in the format: "<int><unit>". <unit> can be `s`
    (seconds), `h` (hours), `d` (days), or `w` (weeks). For example:

    >>> parse_timedelta("20d")
    datetime.timedelta(days=20)
    """
    m = RE_PERIOD.match(s)
    if m:
        n = int(m[1])
        if n > 0:
            unit = m[2]
            if unit == "s":
                return timedelta(seconds=n)
            elif unit == "h":
                return timedelta(hours=n)
            elif unit == "d":
                return timedelta(days=n)
            else:
                assert unit == 'w'
                return timedelta(weeks=n)

    raise ValueError(f"invalid time interval: {s}")


def can_start_new_turn(
    *,
    turn_period: timedelta,
    turn_period_offset: timedelta,
    latest_turn_started_at: datetime,
    current_ts: datetime,
) -> bool:
    """Decide whether a new turn can be started.

    Turns should be started after a "turn starting point", but and as
    close as possible to it. Turn starting points are separated by an
    exact time interval (`turn_period`), and the first starting point
    is calculated to be at `DATETIME0 + turn_period_offset`.
    """
    start_of_first_turn = DATETIME0 + turn_period_offset
    time_since_latest_turn = current_ts - latest_turn_started_at
    overdue = (current_ts - start_of_first_turn) % turn_period
    return overdue < 0.5 * turn_period < time_since_latest_turn
