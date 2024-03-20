from typing import TypeVar, Callable, Optional
from datetime import date, datetime, timezone
from swpt_pythonlib.utils import Seqnum
from sqlalchemy.orm import load_only
from swpt_trade.extensions import db
from swpt_trade.models import (
    NeededWorkerAccount,
    WorkerAccount,
    ConfigureAccountSignal,
    ActivateCollectorSignal,
    HUGE_NEGLIGIBLE_AMOUNT,
    DEFAULT_CONFIG_FLAGS,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic

EPS = 1e-5


@atomic
def process_account_update_signal(
        *,
        debtor_id: int,
        creditor_id: int,
        creation_date: date,
        last_change_ts: datetime,
        last_change_seqnum: int,
        principal: int,
        interest: float,
        interest_rate: float,
        demurrage_rate: float,
        commit_period: int,
        last_interest_rate_change_ts: datetime,
        transfer_note_max_bytes: int,
        last_config_ts: datetime,
        last_config_seqnum: int,
        negligible_amount: float,
        config_flags: int,
        config_data: str,
        account_id: str,
        debtor_info_iri: Optional[str],
        debtor_info_content_type: Optional[str],
        debtor_info_sha256: Optional[bytes],
        last_transfer_number: int,
        last_transfer_committed_at: datetime,
        ts: datetime,
        ttl: int
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    if (current_ts - ts).total_seconds() > ttl:
        return

    needed_worker_account = (
        NeededWorkerAccount.query.filter_by(
            creditor_id=creditor_id, debtor_id=debtor_id
        )
        .with_for_update(read=True)
        .one_or_none()
    )
    if needed_worker_account is None:
        _discard_orphaned_account(
            creditor_id, debtor_id, config_flags, negligible_amount
        )
        return

    data = (
        WorkerAccount.query.filter_by(
            creditor_id=creditor_id, debtor_id=debtor_id
        )
        .with_for_update()
        .one_or_none()
    )
    if data is None:
        with db.retry_on_integrity_error():
            db.session.add(
                WorkerAccount(
                    creditor_id=creditor_id,
                    debtor_id=debtor_id,
                    creation_date=creation_date,
                    last_change_ts=last_change_ts,
                    last_change_seqnum=last_change_seqnum,
                    principal=principal,
                    interest=interest,
                    interest_rate=interest_rate,
                    last_interest_rate_change_ts=last_interest_rate_change_ts,
                    last_config_ts=last_config_ts,
                    last_config_seqnum=last_config_seqnum,
                    negligible_amount=negligible_amount,
                    config_flags=config_flags,
                    config_data=config_data,
                    account_id=account_id,
                    debtor_info_iri=debtor_info_iri,
                    debtor_info_content_type=debtor_info_content_type,
                    debtor_info_sha256=debtor_info_sha256,
                    last_transfer_number=last_transfer_number,
                    last_transfer_committed_at=last_transfer_committed_at,
                    demurrage_rate=demurrage_rate,
                    commit_period=commit_period,
                    transfer_note_max_bytes=transfer_note_max_bytes,
                    last_heartbeat_ts=min(ts, current_ts),
                )
            )
        if account_id != "":
            db.session.add(
                ActivateCollectorSignal(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    account_id=account_id,
                )
            )
    else:
        if ts > data.last_heartbeat_ts:
            data.last_heartbeat_ts = min(ts, current_ts)

        prev_event = (
            data.creation_date,
            data.last_change_ts,
            Seqnum(data.last_change_seqnum),
        )
        this_event = (
            creation_date, last_change_ts, Seqnum(last_change_seqnum)
        )
        if this_event <= prev_event:
            return

        assert creation_date >= data.creation_date
        data.creation_date = creation_date
        data.last_change_ts = last_change_ts
        data.last_change_seqnum = last_change_seqnum
        data.principal = principal
        data.interest = interest
        data.interest_rate = interest_rate
        data.demurrage_rate = demurrage_rate
        data.commit_period = commit_period
        data.last_interest_rate_change_ts = last_interest_rate_change_ts
        data.last_config_ts = last_config_ts
        data.last_config_seqnum = last_config_seqnum
        data.negligible_amount = negligible_amount
        data.config_flags = config_flags
        data.config_data = config_data
        data.transfer_note_max_bytes = transfer_note_max_bytes
        data.debtor_info_iri = debtor_info_iri
        data.debtor_info_content_type = debtor_info_content_type
        data.debtor_info_sha256 = debtor_info_sha256
        data.last_transfer_number = last_transfer_number
        data.last_transfer_committed_at = last_transfer_committed_at

        if account_id != "" and data.account_id == "":
            db.session.add(
                ActivateCollectorSignal(
                    debtor_id=debtor_id,
                    creditor_id=creditor_id,
                    account_id=account_id,
                )
            )
            data.account_id = account_id


@atomic
def process_account_purge_signal(
        *,
        debtor_id: int,
        creditor_id: int,
        creation_date: date,
) -> None:
    worker_account = (
        WorkerAccount.query.filter_by(
            creditor_id=creditor_id, debtor_id=debtor_id
        )
        .filter(WorkerAccount.creation_date <= creation_date)
        .with_for_update()
        .options(load_only())
        .one_or_none()
    )
    if worker_account:
        db.session.delete(worker_account)


def _discard_orphaned_account(
    creditor_id: int,
    debtor_id: int,
    config_flags: int,
    negligible_amount: float,
) -> None:
    scheduled_for_deletion_flag = (
        WorkerAccount.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    )
    safely_huge_amount = (1 - EPS) * HUGE_NEGLIGIBLE_AMOUNT
    is_already_discarded = (
        config_flags & scheduled_for_deletion_flag
        and negligible_amount >= safely_huge_amount
    )

    if not is_already_discarded:
        db.session.add(
            ConfigureAccountSignal(
                creditor_id=creditor_id,
                debtor_id=debtor_id,
                ts=datetime.now(tz=timezone.utc),
                seqnum=0,
                negligible_amount=HUGE_NEGLIGIBLE_AMOUNT,
                config_flags=DEFAULT_CONFIG_FLAGS
                | scheduled_for_deletion_flag,
            )
        )
