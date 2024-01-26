from typing import TypeVar, Callable
from sqlalchemy import select
from swpt_trade.extensions import db
from swpt_trade.models import (
    CollectorAccount,
    Turn,
    CurrencyInfo,
    SellOffer,
    BuyOffer,
    TS0,
)
from swpt_trade.aggregation import Solver


T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


def try_to_advance_turn_to_phase3(turn_id: int) -> None:
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 2:
        solver = Solver(
            turn.base_debtor_info_locator,
            turn.base_debtor_id,
            turn.max_distance_to_base,
            turn.min_trade_amount,
        )
        _register_currencies(solver, turn_id)
        _register_collector_accounts(solver, turn_id)
        solver.analyze_currencies()

        _register_sell_offers(solver, turn_id)
        _register_buy_offers(solver, turn_id)
        solver.analyze_offers()

        _try_to_write_solver_results(solver, turn_id)


def _register_currencies(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        with conn.execution_options(yield_per=50_000).execute(
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
                    solver.register_currency(row[0], row[1], row[2])
                else:
                    solver.register_currency(*row)


def _register_collector_accounts(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        with conn.execution_options(yield_per=50_000).execute(
                select(
                    CollectorAccount.collector_id,
                    CollectorAccount.debtor_id,
                )
        ) as result:
            for row in result:
                solver.register_collector_account(*row)


def _register_sell_offers(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        with conn.execution_options(yield_per=50_000).execute(
                select(
                    SellOffer.creditor_id,
                    SellOffer.debtor_id,
                    SellOffer.amount,
                    SellOffer.collector_id,
                )
                .where(SellOffer.turn_id == turn_id)
        ) as result:
            for row in result:
                solver.register_sell_offer(*row)


def _register_buy_offers(solver: Solver, turn_id: int) -> None:
    with db.engines['solver'].connect() as conn:
        with conn.execution_options(yield_per=50_000).execute(
                select(
                    BuyOffer.creditor_id,
                    BuyOffer.debtor_id,
                    BuyOffer.amount,
                )
                .where(BuyOffer.turn_id == turn_id)
        ) as result:
            for row in result:
                solver.register_buy_offer(*row)


@atomic
def _try_to_write_solver_results(solver: Solver, turn_id: int) -> None:
    turn = (
        Turn.query.filter_by(turn_id=turn_id)
        .with_for_update()
        .one_or_none()
    )
    if turn and turn.phase == 2:
        # for solver.takings_iter():
        #     db.session.execute(
        #         insert()
        #     )
        # _write_takings(solver, turn_id)
        # _write_collector_transfers(solver, turn_id)
        # _write_givings(solver, turn_id)

        CurrencyInfo.query.filter_by(turn_id=turn_id).delete()
        SellOffer.query.filter_by(turn_id=turn_id).delete()
        BuyOffer.query.filter_by(turn_id=turn_id).delete()

        turn.phase = 3
        turn.phase_deadline = None
        turn.collection_started_at = TS0
