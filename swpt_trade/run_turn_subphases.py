import logging
from typing import TypeVar, Callable
from datetime import datetime, timezone
from sqlalchemy import select, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import null
from flask import current_app
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorInfoDocument,
    DebtorLocatorClaim,
    DebtorInfo,
    ConfirmedDebtor,
    WorkerTurn,
)
from swpt_trade.utils import batched


INSERT_BATCH_SIZE = 50000
SELECT_BATCH_SIZE = 50000


T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def run_phase1_subphase0(turn_id: int) -> None:
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=1,
            worker_turn_subphase=0,
        )
        .with_for_update()
        .one_or_none()
    )
    if worker_turn:
        if worker_turn.phase_deadline > datetime.now(tz=timezone.utc):
            with (
                    db.engine.connect() as w_conn,
                    db.engines["solver"].connect() as s_conn,
            ):
                _populate_debtor_infos(w_conn, s_conn, turn_id)
                _populate_confirmed_debtors(w_conn, s_conn, turn_id)

        worker_turn.worker_turn_subphase = 10


def _populate_debtor_infos(w_conn, s_conn, turn_id):
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                DebtorInfoDocument.debtor_info_locator,
                DebtorInfoDocument.debtor_id,
                DebtorInfoDocument.peg_debtor_info_locator,
                DebtorInfoDocument.peg_debtor_id,
                DebtorInfoDocument.peg_exchange_rate,
            )
    ) as result:
        for rows in batched(result, INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "debtor_info_locator": row.debtor_info_locator,
                    "debtor_id": row.debtor_id,
                    "peg_debtor_info_locator": row.peg_debtor_info_locator,
                    "peg_debtor_id": row.peg_debtor_id,
                    "peg_exchange_rate": row.peg_exchange_rate,
                }
                for row in rows
                if sharding_realm.match_str(row.debtor_info_locator)
            ]
            if dicts_to_insert:
                try:
                    s_conn.execute(
                        insert(DebtorInfo).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE
                        ),
                        dicts_to_insert,
                    )
                except IntegrityError:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "An attempt has been made to insert an already"
                        " existing debtor info row for turn %d.",
                        turn_id,
                    )
                    s_conn.rollback()
                    break
        else:
            s_conn.commit()


def _populate_confirmed_debtors(w_conn, s_conn, turn_id):
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]

    with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
            select(
                DebtorLocatorClaim.debtor_id,
                DebtorLocatorClaim.debtor_info_locator,
            )
            .where(DebtorLocatorClaim.debtor_info_locator != null())
    ) as result:
        for rows in batched(result, INSERT_BATCH_SIZE):
            dicts_to_insert = [
                {
                    "turn_id": turn_id,
                    "debtor_info_locator": row.debtor_info_locator,
                    "debtor_id": row.debtor_id,
                }
                for row in rows
                if sharding_realm.match(row.debtor_id)
            ]
            if dicts_to_insert:
                try:
                    s_conn.execute(
                        insert(ConfirmedDebtor).execution_options(
                            insertmanyvalues_page_size=INSERT_BATCH_SIZE
                        ),
                        dicts_to_insert,
                    )
                except IntegrityError:
                    logger = logging.getLogger(__name__)
                    logger.warning(
                        "An attempt has been made to insert an already"
                        " existing confirmed debtor row for turn %d.",
                        turn_id,
                    )
                    s_conn.rollback()
                    break
        else:
            s_conn.commit()


def run_phase2_subphase0(worker_turn: WorkerTurn) -> None:
    # TODO: implement
    pass


def run_phase2_subphase5(worker_turn: WorkerTurn) -> None:
    # TODO: implement
    pass


def run_phase3_subphase0(worker_turn: WorkerTurn) -> None:
    # TODO: implement
    pass
