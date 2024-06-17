import random
import math
from typing import TypeVar, Callable, Tuple, Optional
from dataclasses import dataclass
from datetime import datetime, date, timezone, timedelta
from sqlalchemy import select, delete, update
from sqlalchemy.sql.expression import and_, null, false
from sqlalchemy.orm import exc, load_only, Load
from swpt_trade.utils import (
    TransferNote,
    calc_demurrage,
    contain_principal_overflow,
)
from swpt_trade.extensions import db
from swpt_trade.models import (
    DATE0,
    MAX_INT16,
    MAX_INT32,
    MIN_INT64,
    MAX_INT64,
    T_INFINITY,
    AGENT_TRANSFER_NOTE_FORMAT,
    cr_seq,
    WorkerTurn,
    AccountLock,
    ActiveCollector,
    PrepareTransferSignal,
    FinalizeTransferSignal,
    CreditorParticipation,
    WorkerCollecting,
    WorkerSending,
    WorkerReceiving,
    WorkerDispatching,
    TransferAttempt,
    TradingPolicy,
    WorkerAccount,
    InterestRateChange,
    AccountIdRequestSignal,
    AccountIdResponseSignal,
)

TD_POSSIBLE_ROUNDING_ERROR = timedelta(seconds=2)
T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@dataclass
class DemurrageInfo:
    """Information about the demurrage on a collector account.

    The `rate` field represents the lowest interest rate on the
    collector account for the duration of the trading turn, but not
    exceeding zero.

    The `final_interest_rate_ts` field represents the onset moment of
    the last known interest rate on the collector account. A `None`
    indicates that we know that we are missing some interest rate
    changes, that could be important.
    """
    rate: float
    final_interest_rate_ts: Optional[datetime]


@dataclass
class TransferParams:
    amount: int
    final_interest_rate_ts: Optional[datetime]
    max_commit_delay: int


@atomic
def process_candidate_offer_signal(
        *,
        demurrage_rate: float,
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
        .filter_by(creditor_id=creditor_id, debtor_id=debtor_id)
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

    if amount > 0:
        # a buy offer
        min_locked_amount = 0
        max_locked_amount = 0
    else:
        # a sell offer
        assert amount < 0
        assert demurrage_rate > -100.0
        assert worker_turn.min_trade_amount > 0
        worst_possible_demurrage = calc_demurrage(
            demurrage_rate, worker_turn.collection_deadline - current_ts
        )
        min_locked_amount = contain_principal_overflow(
            math.ceil(worker_turn.min_trade_amount / worst_possible_demurrage)
        )
        max_locked_amount = contain_principal_overflow(
            math.ceil((-amount) / worst_possible_demurrage)
        )

    if max_locked_amount < min_locked_amount:
        return  # pragma: no cover

    coordinator_request_id = db.session.scalar(cr_seq)

    if account_lock:
        account_lock.turn_id = turn_id
        account_lock.coordinator_request_id = coordinator_request_id
        account_lock.collector_id = collector.collector_id
        account_lock.initiated_at = current_ts
        account_lock.amount = amount
        account_lock.transfer_id = None
        account_lock.finalized_at = None
        account_lock.released_at = None
        account_lock.account_creation_date = None
        account_lock.account_last_transfer_number = None
    else:
        with db.retry_on_integrity_error():
            account_lock = AccountLock(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                turn_id=turn_id,
                coordinator_request_id=coordinator_request_id,
                collector_id=collector.collector_id,
                initiated_at=current_ts,
                amount=amount,
            )
            db.session.add(account_lock)

    if account_lock.is_self_lock:
        # NOTE: In this case, a collector account is both the sender
        # and the recipient. This can happen if we want to trade
        # surpluses accumulated on collector accounts. Obviously, it
        # does not make sense to attempt to prepare such a transfer.
        # Instead, we pretend that the transfer has been successfully
        # prepared.
        account_lock.transfer_id = 0  # a made-up transfer ID
        account_lock.amount = amount  # successfully locked the whole amount
    else:
        db.session.add(
            PrepareTransferSignal(
                creditor_id=creditor_id,
                coordinator_request_id=coordinator_request_id,
                debtor_id=debtor_id,
                recipient=collector.account_id,
                min_locked_amount=min_locked_amount,
                max_locked_amount=max_locked_amount,
                final_interest_rate_ts=T_INFINITY,
                max_commit_delay=MAX_INT32,
                inserted_at=current_ts,
            )
        )


@atomic
def dismiss_prepared_transfer(
        *,
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        current_ts: Optional[datetime] = None,
):
    if current_ts is None:
        current_ts = datetime.now(tz=timezone.utc)

    db.session.add(
        FinalizeTransferSignal(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_id=transfer_id,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            committed_amount=0,
            transfer_note_format="",
            transfer_note="",
            inserted_at=current_ts,
        )
    )


@atomic
def put_rejected_transfer_through_account_locks(
        *,
        coordinator_id: int,
        coordinator_request_id: int,
        status_code: str,
        debtor_id: int,
        creditor_id: int,
) -> bool:
    """Return `True` if a corresponding account lock has been found.
    """
    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
        )
        .one_or_none()
    )
    if lock is None:
        return False

    if (
            lock.released_at is None
            and lock.transfer_id is None
            and lock.debtor_id == debtor_id
            and lock.creditor_id == creditor_id
    ):
        # The current status is "initiated". Delete the account lock.
        db.session.delete(lock)

    return True


@atomic
def put_prepared_transfer_through_account_locks(
        *,
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_id: int,
        coordinator_request_id: int,
        locked_amount: int,
        demurrage_rate: float,
        deadline: datetime,
        min_demurrage_rate: float,
) -> bool:
    """Return `True` if a corresponding account lock has been found.
    """
    def dismiss():
        dismiss_prepared_transfer(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_id=transfer_id,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
        )

    query = (
        db.session.query(AccountLock, WorkerTurn)
        .join(AccountLock.worker_turn)
        .filter(
            AccountLock.creditor_id == coordinator_id,
            AccountLock.coordinator_request_id == coordinator_request_id,
        )
        .options(Load(WorkerTurn).load_only(WorkerTurn.collection_deadline))
    )
    try:
        lock, worker_turn = query.one()
    except exc.NoResultFound:
        return False

    if lock.debtor_id == debtor_id and lock.creditor_id == creditor_id:
        if lock.released_at is None and lock.transfer_id is None:
            # The current status is "initiated".
            assert lock.finalized_at is None

            min_deadline = worker_turn.collection_deadline or T_INFINITY

            if deadline < min_deadline or demurrage_rate < min_demurrage_rate:
                # Dismiss the transfer directly when either the
                # deadline or the demurrage rate is not appropriate.
                db.session.delete(lock)

            else:
                # Change the current status to "prepared".
                lock.transfer_id = transfer_id

                if lock.amount <= 0:
                    # When selling, re-calculate the amount so that it
                    # is equal to the locked amount reduced in
                    # accordance with the effective demurrage rate,
                    # but not exceeding the original amount which is
                    # for sale.
                    worst_possible_demurrage = calc_demurrage(
                        demurrage_rate, min_deadline - lock.initiated_at
                    )
                    lock.amount = max(
                        lock.amount,
                        - math.floor(locked_amount * worst_possible_demurrage),
                    )

                return True

        elif lock.released_at is None and lock.finalized_at is None:
            # The current status is "prepared".
            assert lock.transfer_id is not None

            if lock.transfer_id == transfer_id:
                return True

        else:
            # The current status is "settled".
            assert lock.transfer_id is not None
            assert lock.finalized_at is not None

            if lock.transfer_id == transfer_id and lock.amount < 0:
                db.session.add(
                    FinalizeTransferSignal(
                        creditor_id=creditor_id,
                        debtor_id=debtor_id,
                        transfer_id=transfer_id,
                        coordinator_id=coordinator_id,
                        coordinator_request_id=coordinator_request_id,
                        committed_amount=(-lock.amount),
                        transfer_note_format=AGENT_TRANSFER_NOTE_FORMAT,
                        transfer_note=str(
                            TransferNote(
                                lock.turn_id,
                                TransferNote.Kind.COLLECTING,
                                lock.collector_id,
                                creditor_id,
                            )
                        ),
                    )
                )
                return True

    dismiss()
    return True


@atomic
def process_revise_account_lock_signal(
        *,
        creditor_id: int,
        debtor_id: int,
        turn_id: int,
):
    current_ts = datetime.now(tz=timezone.utc)

    participation = (
        CreditorParticipation.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
        )
        .with_for_update()
        .one_or_none()
    )
    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
            finalized_at=null(),
            has_been_revised=False,
        )
        .with_for_update()
        .one_or_none()
    )
    if lock:
        def dismiss():
            if not (lock.transfer_id is None or lock.is_self_lock):
                dismiss_prepared_transfer(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    transfer_id=lock.transfer_id,
                    coordinator_id=creditor_id,
                    coordinator_request_id=lock.coordinator_request_id,
                    current_ts=current_ts,
                )

        def commit(committed_amount: int):
            assert participation
            assert lock.transfer_id is not None
            assert lock.collector_id != creditor_id
            assert lock.collector_id == participation.collector_id
            db.session.add(
                FinalizeTransferSignal(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    transfer_id=lock.transfer_id,
                    coordinator_id=creditor_id,
                    coordinator_request_id=lock.coordinator_request_id,
                    committed_amount=committed_amount,
                    transfer_note_format=AGENT_TRANSFER_NOTE_FORMAT,
                    transfer_note=str(
                        TransferNote(
                            turn_id,
                            TransferNote.Kind.COLLECTING,
                            lock.collector_id,
                            creditor_id,
                        )
                    ),
                    inserted_at=current_ts,
                )
            )

        if participation:
            amount = participation.amount
            assert lock.transfer_id is not None
            assert (
                0 < abs(amount) <= abs(lock.amount) < abs(amount + lock.amount)
            )

            if creditor_id == participation.collector_id:
                # The sender and the recipient is the same collector
                # account. In this case, transferring the amount is
                # neither possible nor needed. Instead, we simply
                # release the account lock.
                acd, altn = _register_collector_trade(creditor_id, amount)
                lock.released_at = lock.finalized_at = current_ts
                lock.account_creation_date = acd
                lock.account_last_transfer_number = altn
                dismiss()

            elif amount < 0:
                # The account is the sender. The amount must be taken
                # from the account, and transferred to the collector.
                lock.finalized_at = current_ts
                commit(-amount)

            else:
                # The account is the receiver. We expect the amount to
                # be transferred to the account at a later stage. In
                # order to guarantee that the account will not be
                # deleted in the meantime, we should not dismiss the
                # prepared transfer yet.
                assert amount > 1

            lock.amount = amount
            lock.has_been_revised = True  # ensure idempotency

        else:
            dismiss()
            db.session.delete(lock)

    if participation:
        db.session.delete(participation)


@atomic
def release_seller_account_lock(
        creditor_id: int,
        debtor_id: int,
        turn_id: int,
        acquired_amount: int,
        account_creation_date: date,
        account_transfer_number: int,
        is_collector_trade: bool,
) -> None:
    assert acquired_amount < 0
    current_ts = datetime.now(tz=timezone.utc)

    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
        )
        .filter(AccountLock.amount == acquired_amount)
        .filter(AccountLock.finalized_at != null())
        .filter(AccountLock.released_at == null())
        .with_for_update()
        .one_or_none()
    )
    if lock:
        if is_collector_trade:
            account_creation_date, account_transfer_number = (
                _register_collector_trade(creditor_id, acquired_amount)
            )
        lock.released_at = current_ts
        lock.account_creation_date = account_creation_date
        lock.account_last_transfer_number = account_transfer_number


@atomic
def update_worker_collecting_record(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        acquired_amount: int,
) -> None:
    assert acquired_amount > 0

    db.session.execute(
        update(WorkerCollecting)
        .where(
            and_(
                WorkerCollecting.collector_id == collector_id,
                WorkerCollecting.turn_id == turn_id,
                WorkerCollecting.debtor_id == debtor_id,
                WorkerCollecting.creditor_id == creditor_id,
                WorkerCollecting.amount == acquired_amount,
                WorkerCollecting.collected == false(),
            )
        )
        .values(collected=True)
    )


@atomic
def delete_worker_sending_record(
        from_collector_id: int,
        turn_id: int,
        debtor_id: int,
        to_collector_id: int,
) -> None:
    db.session.execute(
        delete(WorkerSending)
        .where(
            and_(
                WorkerSending.from_collector_id == from_collector_id,
                WorkerSending.turn_id == turn_id,
                WorkerSending.debtor_id == debtor_id,
                WorkerSending.to_collector_id == to_collector_id,
            )
        )
    )


@atomic
def update_worker_receiving_record(
        to_collector_id: int,
        turn_id: int,
        debtor_id: int,
        from_collector_id: int,
        acquired_amount: int,
) -> None:
    assert acquired_amount > 0

    db.session.execute(
        update(WorkerReceiving)
        .where(
            and_(
                WorkerReceiving.to_collector_id == to_collector_id,
                WorkerReceiving.turn_id == turn_id,
                WorkerReceiving.debtor_id == debtor_id,
                WorkerReceiving.from_collector_id == from_collector_id,
                WorkerReceiving.received_amount == 0,
            )
        )
        .values(received_amount=acquired_amount)
    )


@atomic
def delete_worker_dispatching_record(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
) -> None:
    db.session.execute(
        delete(WorkerDispatching)
        .where(
            and_(
                WorkerDispatching.collector_id == collector_id,
                WorkerDispatching.turn_id == turn_id,
                WorkerDispatching.debtor_id == debtor_id,
                WorkerDispatching.creditor_id == creditor_id,
            )
        )
    )


@atomic
def release_buyer_account_lock(
        creditor_id: int,
        debtor_id: int,
        turn_id: int,
        acquired_amount: int,
        account_creation_date: date,
        account_transfer_number: int,
        is_collector_trade: bool,
) -> None:
    assert acquired_amount > 0
    current_ts = datetime.now(tz=timezone.utc)

    lock = (
        AccountLock.query
        .filter_by(
            creditor_id=creditor_id,
            debtor_id=debtor_id,
            turn_id=turn_id,
        )
        .filter(AccountLock.amount > 0)
        .filter(AccountLock.transfer_id != null())
        .filter(AccountLock.finalized_at == null())
        .filter(AccountLock.released_at == null())
        .with_for_update()
        .one_or_none()
    )
    if lock:
        if is_collector_trade:
            account_creation_date, account_transfer_number = (
                _register_collector_trade(creditor_id, acquired_amount)
            )
        lock.released_at = lock.finalized_at = current_ts
        lock.account_creation_date = account_creation_date
        lock.account_last_transfer_number = account_transfer_number

        if not lock.is_self_lock:
            dismiss_prepared_transfer(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                transfer_id=lock.transfer_id,
                coordinator_id=creditor_id,
                coordinator_request_id=lock.coordinator_request_id,
                current_ts=current_ts,
            )


def _register_collector_trade(
        collector_id: int,
        amount: int,
) -> Tuple[date, int]:
    # TODO: Consider implementing this function.
    #
    # This function will be called when a collector account
    # participates in a trading turn. The `amount` will be negative
    # when the collector account is the seller, and will be positive
    # when the collector account is the buyer.
    #
    # It can be very useful to make a collector account participate in
    # circular trades like any "normal" creditor account, because this
    # allows exchanging accumulated (collected) useless surpluses for
    # useful currencies.
    #
    # The implementation of this function should update the amount
    # that is available on the collector account.

    account_creation_date = DATE0  # currently, a made-up date
    account_last_transfer_number = 0  # currently, a made-up number

    return account_creation_date, account_last_transfer_number


@atomic
def process_account_id_request_signal(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        is_dispatching: bool,
) -> None:
    row = (
        db.session.execute(
            select(
                TradingPolicy.account_id,
                TradingPolicy.latest_ledger_update_id,
            )
            .where(
                and_(
                    TradingPolicy.creditor_id == creditor_id,
                    TradingPolicy.debtor_id == debtor_id,
                )
            )
        ).one_or_none()

        or db.session.execute(
            select(
                WorkerAccount.account_id,
                WorkerAccount.creation_date - DATE0,
            )
            .where(
                and_(
                    WorkerAccount.creditor_id == creditor_id,
                    WorkerAccount.debtor_id == debtor_id,
                    WorkerAccount.account_id != "",
                )
            )
        ).one_or_none()

        or ("", MIN_INT64)
    )

    db.session.add(
        AccountIdResponseSignal(
            collector_id=collector_id,
            turn_id=turn_id,
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            is_dispatching=is_dispatching,
            account_id=row[0],
            account_id_version=row[1],
        )
    )


@atomic
def process_account_id_response_signal(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        is_dispatching: bool,
        account_id: str,
        account_id_version: int,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
) -> None:
    attempt = (
        TransferAttempt.query.
        filter_by(
            collector_id=collector_id,
            turn_id=turn_id,
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            is_dispatching=is_dispatching,
        )
        .with_for_update()
        .one_or_none()
    )
    if (
            attempt
            and attempt.unknown_recipient
            and attempt.recipient_version < account_id_version
    ):
        attempt.recipient_version = account_id_version

        if attempt.recipient != account_id:
            attempt.recipient = account_id

            if attempt.failure_code is not None:
                # When the recipient changes, the failure code becomes
                # misleading.
                attempt.failure_code = attempt.UNSPECIFIED_FAILURE

            _trigger_transfer_if_possible(
                attempt,
                transfers_healthy_max_commit_delay,
                transfers_amount_cut,
            )


@atomic
def process_trigger_transfer_signal(
        collector_id: int,
        turn_id: int,
        debtor_id: int,
        creditor_id: int,
        is_dispatching: bool,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
) -> None:
    attempt = (
        TransferAttempt.query.
        filter_by(
            collector_id=collector_id,
            turn_id=turn_id,
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            is_dispatching=is_dispatching,
        )
        .with_for_update()
        .one_or_none()
    )
    if attempt:
        _trigger_transfer_if_possible(
            attempt,
            transfers_healthy_max_commit_delay,
            transfers_amount_cut,
        )


def _trigger_transfer_if_possible(
        attempt: TransferAttempt,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
) -> None:
    if not attempt.can_be_triggered:
        return

    assert attempt.rescheduled_for is None
    assert attempt.fatal_error is None
    assert attempt.recipient != ""

    current_ts = datetime.now(tz=timezone.utc)
    old_failure_code = attempt.failure_code
    old_interest_rate_ts = attempt.final_interest_rate_ts

    params = _calc_transfer_params(
        attempt,
        transfers_healthy_max_commit_delay,
        transfers_amount_cut,
        current_ts,
    )
    new_coordinator_request_id = db.session.scalar(cr_seq)
    new_interest_rate_ts = params.final_interest_rate_ts
    new_amount = params.amount

    if new_amount == 0:
        # Attempting to make a transfer will never make sense.
        # Rescheduling more attempts is futile. Normally, this should
        # not happen.
        return  # pragma: no cover

    if (
        new_interest_rate_ts is None
        or (
            old_failure_code == attempt.NEWER_INTEREST_RATE
            and old_interest_rate_ts is not None
            and old_interest_rate_ts >= new_interest_rate_ts
        )
    ):
        # Attempting to make a transfer right now does not make sense.
        # Instead, we reschedule the attempt for a later time.
        attempt.rescheduled_for = (
            current_ts + transfers_healthy_max_commit_delay
        )
        return

    attempt.attempted_at = current_ts
    attempt.coordinator_request_id = new_coordinator_request_id
    attempt.final_interest_rate_ts = new_interest_rate_ts
    attempt.amount = new_amount
    attempt.transfer_id = None
    attempt.finalized_at = None

    if old_failure_code == attempt.RECIPIENT_IS_UNREACHABLE:
        # In this case, we know beforehand that the transfer
        # will fail with a "RECIPIENT_IS_UNREACHABLE" error.
        # Therefore, we make a request to obtain the new
        # account ID of the creditor, and directly reschedule
        # the transfer for a later time.
        db.session.add(
            AccountIdRequestSignal(
                collector_id=attempt.collector_id,
                turn_id=attempt.turn_id,
                debtor_id=attempt.debtor_id,
                creditor_id=attempt.creditor_id,
                is_dispatching=attempt.is_dispatching,
            )
        )
        n = min(attempt.backoff_counter, 31)
        attempt.rescheduled_for = (
            current_ts + transfers_healthy_max_commit_delay * (2 ** n)
        )
        if attempt.backoff_counter < MAX_INT16:
            attempt.backoff_counter += 1
        return

    db.session.add(
        PrepareTransferSignal(
            creditor_id=attempt.collector_id,
            coordinator_request_id=new_coordinator_request_id,
            debtor_id=attempt.debtor_id,
            recipient=attempt.recipient,
            min_locked_amount=0,
            max_locked_amount=0,
            final_interest_rate_ts=new_interest_rate_ts,
            max_commit_delay=params.max_commit_delay,
            inserted_at=current_ts,
        )
    )
    attempt.failure_code = None


def _calc_transfer_params(
        attempt: TransferAttempt,
        transfers_healthy_max_commit_delay: timedelta,
        transfers_amount_cut: float,
        current_ts: datetime,
) -> TransferParams:
    n = min(attempt.backoff_counter, 31)
    max_commit_delay = min(
        transfers_healthy_max_commit_delay.total_seconds() * (2 ** n),
        MAX_INT32,
    )

    assert 0 <= max_commit_delay <= MAX_INT32
    past_demmurage_period = current_ts - attempt.collection_started_at
    future_demmurage_period = timedelta(seconds=max_commit_delay)
    demurrage_period = past_demmurage_period + future_demmurage_period
    demurrage_info = _get_demurrage_info(attempt)
    demurrage = calc_demurrage(demurrage_info.rate, demurrage_period)
    nominal_amount = attempt.nominal_amount

    assert 2.0 <= nominal_amount
    assert 0.0 <= demurrage <= 1.0
    assert 1e-10 <= transfers_amount_cut <= 0.1
    try:
        amount = contain_principal_overflow(
            math.floor(
                nominal_amount
                * demurrage
                * (1.0 - transfers_amount_cut)
            )
        )
    except OverflowError:
        # This can happen if `nominal_amount` is +Infinity
        amount = MAX_INT64  # pragma: no cover

    assert 0 <= amount <= MAX_INT64
    assert amount <= nominal_amount

    return TransferParams(
        amount,
        demurrage_info.final_interest_rate_ts,
        max_commit_delay,
    )


def _get_demurrage_info(attempt: TransferAttempt) -> DemurrageInfo:
    collector_id = attempt.collector_id
    debtor_id = attempt.debtor_id
    collection_started_at = attempt.collection_started_at

    try:
        collector_info = (
            db.session.execute(
                select(
                    WorkerAccount.interest_rate,
                    WorkerAccount.last_interest_rate_change_ts,
                )
                .where(
                    and_(
                        WorkerAccount.creditor_id == collector_id,
                        WorkerAccount.debtor_id == debtor_id,
                    )
                )
            )
            .one()
        )
    except exc.NoResultFound:  # pragma: no cover
        # Normally, this should never happen. But if it did happen,
        # returning an incorrect demurrage rate here is the least of
        # our problems.
        return DemurrageInfo(0.0, None)

    interest_rate_changes = (
        db.session.execute(
            select(
                InterestRateChange.interest_rate,
                InterestRateChange.change_ts,
            )
            .where(
                and_(
                    InterestRateChange.creditor_id == collector_id,
                    InterestRateChange.debtor_id == debtor_id,
                )
            )
            .order_by(InterestRateChange.change_ts.desc())
        )
        .all()
    )

    min_interest_rate = collector_info.interest_rate
    max_change_ts = collector_info.last_interest_rate_change_ts

    for interest_rate, change_ts in interest_rate_changes:
        if interest_rate < min_interest_rate:
            min_interest_rate = interest_rate

        if change_ts > max_change_ts:
            max_change_ts = change_ts

        if change_ts < collection_started_at:
            break

    return DemurrageInfo(
        min(min_interest_rate, 0.0),
        max_change_ts + TD_POSSIBLE_ROUNDING_ERROR,
    )
