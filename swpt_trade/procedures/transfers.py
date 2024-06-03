import random
import math
from typing import TypeVar, Callable, Tuple
from datetime import datetime, date, timezone
from sqlalchemy.sql.expression import null
from sqlalchemy.orm import exc, load_only, Load
from swpt_trade.utils import (
    TT_BUYER,
    calc_demurrage,
    generate_transfer_note,
    contain_principal_overflow,
)
from swpt_trade.extensions import db
from swpt_trade.models import (
    DATE0,
    ROOT_CREDITOR_ID,
    MAX_INT32,
    T_INFINITY,
    AGENT_TRANSFER_NOTE_FORMAT,
    cr_seq,
    WorkerTurn,
    AccountLock,
    ActiveCollector,
    PrepareTransferSignal,
    FinalizeTransferSignal,
    CreditorParticipation,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


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
):
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
                        transfer_note=generate_transfer_note(
                            lock.turn_id, TT_BUYER, lock.collector_id
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

    creditor_participation = (
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
        )
        .filter(AccountLock.finalized_at == null())
        .filter(AccountLock.collector_id != ROOT_CREDITOR_ID)
        .with_for_update()
        .one_or_none()
    )
    if lock:
        if creditor_participation is None:
            db.session.delete(lock)
            return

        assert lock.transfer_id is not None
        assert lock.finalized_at is None
        assert lock.collector_id != ROOT_CREDITOR_ID
        amount = lock.amount = creditor_participation.amount

        if creditor_id == creditor_participation.collector_id:
            acd, altn = _register_collector_participation(creditor_id, amount)

            # When the sender and the recipient accounts are the same,
            # transferring the amount is neither possible nor needed.
            # Instead, we simply release the account lock.
            lock.released_at = lock.finalized_at = current_ts
            lock.account_creation_date = acd
            lock.account_last_transfer_number = altn

        elif amount < 0:
            assert lock.collector_id == creditor_participation.collector_id

            # Send the amount to the collector.
            lock.finalized_at = current_ts
            db.session.add(
                FinalizeTransferSignal(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    transfer_id=lock.transfer_id,
                    coordinator_id=creditor_id,
                    coordinator_request_id=lock.coordinator_request_id,
                    committed_amount=(-amount),
                    transfer_note_format=AGENT_TRANSFER_NOTE_FORMAT,
                    transfer_note=generate_transfer_note(
                        turn_id, TT_BUYER, lock.collector_id
                    ),
                    inserted_at=current_ts,
                )
            )

        else:
            assert amount > 0

            # The creditor's account is the recipient. In this case
            # the value of the `collector_id` field is irrelevant.
            # Here we change it to `ROOT_CREDITOR_ID`, just to ensure
            # that this procedure is idempotent.
            lock.collector_id = ROOT_CREDITOR_ID

    if creditor_participation:
        db.session.delete(creditor_participation)


def _register_collector_participation(
        collector_id: int,
        amount: int,
) -> Tuple[date, int]:
    # TODO: Implement.
    return DATE0, 0
