import logging
import math
from typing import TypeVar, Callable
from datetime import datetime, timezone
from itertools import groupby
from sqlalchemy import select, insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.sql.expression import null, and_
from flask import current_app
from swpt_pythonlib.utils import ShardingRealm
from swpt_trade.extensions import db
from swpt_trade.solver import CandidateOfferAuxData, BidProcessor
from swpt_trade.models import (
    DebtorInfoDocument,
    DebtorLocatorClaim,
    DebtorInfo,
    ConfirmedDebtor,
    WorkerTurn,
    CurrencyInfo,
    TradingPolicy,
    WorkerAccount,
    CandidateOfferSignal,
)
from swpt_trade.utils import batched, contain_principal_overflow


INSERT_BATCH_SIZE = 50000
SELECT_BATCH_SIZE = 50000
BID_COUNTER_THRESHOLD = 100000
DELETION_FLAG = WorkerAccount.CONFIG_SCHEDULED_FOR_DELETION_FLAG


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


@atomic
def run_phase2_subphase0(turn_id: int) -> None:
    worker_turn = (
        WorkerTurn.query
        .filter_by(
            turn_id=turn_id,
            phase=2,
            worker_turn_subphase=0,
        )
        .with_for_update()
        .one_or_none()
    )
    if worker_turn:
        if worker_turn.phase_deadline > datetime.now(tz=timezone.utc):
            bp = BidProcessor(
                worker_turn.base_debtor_info_locator,
                worker_turn.base_debtor_id,
                worker_turn.max_distance_to_base,
                worker_turn.min_trade_amount,
            )
            _load_currencies(bp, turn_id)
            _generate_candidate_offers(bp, turn_id)
            _schedule_currencies_to_be_confirmed(bp)

        worker_turn.worker_turn_subphase = 5


def _load_currencies(bp: BidProcessor, turn_id: int) -> None:
    with db.engines["solver"].connect() as s_conn:
        with s_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    CurrencyInfo.is_confirmed,
                    CurrencyInfo.debtor_info_locator,
                    CurrencyInfo.debtor_id,
                    CurrencyInfo.peg_debtor_info_locator,
                    CurrencyInfo.peg_debtor_id,
                    CurrencyInfo.peg_exchange_rate,
                )
                .where(CurrencyInfo.turn_id == turn_id)
        ) as result:
            for row in result:
                if row[3] is None or row[4] is None or row[5] is None:
                    bp.register_currency(row[0], row[1], row[2])
                else:
                    bp.register_currency(*row)


def _generate_candidate_offers(bp, turn_id):
    current_ts = datetime.now(tz=timezone.utc)
    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    bid_counter = 0

    with db.engine.connect() as w_conn:
        with w_conn.execution_options(yield_per=SELECT_BATCH_SIZE).execute(
                select(
                    TradingPolicy.creditor_id,
                    TradingPolicy.debtor_id,
                    TradingPolicy.creation_date,
                    TradingPolicy.principal,
                    TradingPolicy.last_transfer_number,
                    TradingPolicy.min_principal,
                    TradingPolicy.max_principal,
                    TradingPolicy.peg_debtor_id,
                    TradingPolicy.peg_exchange_rate,
                )
                .where(
                    and_(
                        TradingPolicy.account_id != "",
                        TradingPolicy.config_flags.op("&")(DELETION_FLAG) == 0,
                        TradingPolicy.policy_name != null(),
                    )
                )
                .order_by(TradingPolicy.creditor_id)
        ) as result:
            for creditor_id, rows in groupby(result, lambda r: r.creditor_id):
                if sharding_realm.match(creditor_id):
                    for row in rows:
                        assert row.creditor_id == creditor_id
                        rate = row.peg_exchange_rate

                        bp.register_bid(
                            creditor_id,
                            row.debtor_id,
                            _calc_bid_amount(row),
                            row.peg_debtor_id or 0,
                            math.nan if rate is None else rate,
                            CandidateOfferAuxData(
                                creation_date=row.creation_date,
                                last_transfer_number=row.last_transfer_number,
                            ),
                        )
                        bid_counter += 1

                    # Process the registered bids when they become too
                    # many, so that they can not use up the available
                    # memory.
                    if bid_counter >= BID_COUNTER_THRESHOLD:
                        _process_bids(bp, turn_id, current_ts)
                        bid_counter = 0

            _process_bids(bp, turn_id, current_ts)


def _calc_bid_amount(row) -> int:
    if row.principal < row.min_principal:  # buy
        return contain_principal_overflow(row.min_principal - row.principal)
    elif row.principal > row.max_principal:  # sell
        return contain_principal_overflow(row.max_principal - row.principal)
    else:  # do nothing
        return 0


def _process_bids(bp: BidProcessor, turn_id: int, ts: datetime) -> None:
    for candidate_offers in batched(bp.analyze_bids(), INSERT_BATCH_SIZE):
        db.session.execute(
            insert(CandidateOfferSignal).execution_options(
                insertmanyvalues_page_size=INSERT_BATCH_SIZE
            ),
            [
                {
                    "turn_id": turn_id,
                    "amount": o.amount,
                    "debtor_id": o.debtor_id,
                    "creditor_id": o.creditor_id,
                    "account_creation_date": o.aux_data.creation_date,
                    "last_transfer_number": o.aux_data.last_transfer_number,
                    "inserted_at": ts,
                } for o in candidate_offers
            ],
        )


def _schedule_currencies_to_be_confirmed(bp: BidProcessor) -> None:
    # TODO: implement
    pass


def run_phase2_subphase5(worker_turn: WorkerTurn) -> None:
    # TODO: implement
    pass


def run_phase3_subphase0(worker_turn: WorkerTurn) -> None:
    # TODO: implement
    pass
