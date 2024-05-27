import time
from typing import TypeVar, Callable
from swpt_pythonlib.scan_table import TableScanner
from flask import current_app
from swpt_trade.extensions import db

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


class ParentRecordsCleaner(TableScanner):
    """A scanner which only task is to delete parent shard records."""

    def run(self, engine, completion_goal, quit_early):
        if current_app.config["DELETE_PARENT_SHARD_RECORDS"]:
            return super().run(engine, completion_goal, quit_early)

        elif not quit_early:  # pragma: no cover
            while True:
                time.sleep(5)
