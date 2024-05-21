import logging
import json
from typing import Optional
from datetime import datetime, date, timedelta
from marshmallow import ValidationError
from flask import current_app
import swpt_pythonlib.protocol_schemas as ps
from swpt_pythonlib import rabbitmq
from swpt_trade import procedures, schemas
from swpt_trade.models import CT_AGENT, message_belongs_to_this_shard


def _on_rejected_config_signal(
    debtor_id: int,
    creditor_id: int,
    config_ts: datetime,
    config_seqnum: int,
    negligible_amount: float,
    config_data: str,
    config_flags: int,
    rejection_code: str,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    logger = logging.getLogger(__name__)
    logger.warning(
        "Received RejectedConfig message"
        " for WorkerAccount(creditor_id=%d, debtor_id=%d).",
        creditor_id,
        debtor_id,
    )


def _on_account_update_signal(
    debtor_id: int,
    creditor_id: int,
    last_change_ts: datetime,
    last_change_seqnum: int,
    principal: int,
    interest: float,
    interest_rate: float,
    demurrage_rate: float,
    commit_period: int,
    transfer_note_max_bytes: int,
    last_interest_rate_change_ts: datetime,
    last_transfer_number: int,
    last_transfer_committed_at: datetime,
    creation_date: date,
    negligible_amount: float,
    config_flags: int,
    ts: datetime,
    ttl: int,
    account_id: str,
    debtor_info_iri: str,
    *args,
    **kwargs
) -> None:
    cfg = current_app.config
    is_legible_for_trade = (
        demurrage_rate >= cfg["APP_MIN_DEMURRAGE_RATE"]
        and commit_period >= cfg["APP_TURN_MAX_COMMIT_PERIOD"].total_seconds()
        and transfer_note_max_bytes >= cfg["APP_MIN_TRANSFER_NOTE_MAX_BYTES"]
    )
    procedures.process_account_update_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
        last_change_ts=last_change_ts,
        last_change_seqnum=last_change_seqnum,
        principal=principal,
        interest=interest,
        interest_rate=interest_rate,
        demurrage_rate=demurrage_rate,
        commit_period=commit_period,
        last_interest_rate_change_ts=last_interest_rate_change_ts,
        transfer_note_max_bytes=transfer_note_max_bytes,
        negligible_amount=negligible_amount,
        config_flags=config_flags,
        account_id=account_id,
        debtor_info_iri=debtor_info_iri or None,
        last_transfer_number=last_transfer_number,
        last_transfer_committed_at=last_transfer_committed_at,
        ts=ts,
        ttl=ttl,
        is_legible_for_trade=is_legible_for_trade,
        interest_rate_history_period=cfg["APP_INTEREST_RATE_HISTORY_PERIOD"],
    )


def _on_account_purge_signal(
    debtor_id: int,
    creditor_id: int,
    creation_date: date,
    ts: str,
    *args,
    **kwargs
) -> None:
    is_needed_account = procedures.process_account_purge_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
    )
    if is_needed_account:  # pragma: no cover
        logger = logging.getLogger(__name__)
        logger.warning(
            "Received AccountPurge message"
            " for NeededWorkerAccount(creditor_id=%d, debtor_id=%d).",
            creditor_id,
            debtor_id,
        )


def _on_account_transfer_signal(
    debtor_id: int,
    creditor_id: int,
    transfer_number: int,
    creation_date: date,
    coordinator_type: str,
    sender: str,
    recipient: str,
    acquired_amount: int,
    transfer_note_format: str,
    transfer_note: str,
    committed_at: datetime,
    principal: int,
    ts: datetime,
    previous_transfer_number: int,
    *args,
    **kwargs
) -> None:
    # TODO: implement!

    procedures.process_account_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=creation_date,
        transfer_number=transfer_number,
        coordinator_type=coordinator_type,
        sender=sender,
        recipient=recipient,
        acquired_amount=acquired_amount,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        committed_at=committed_at,
        principal=principal,
        ts=ts,
        previous_transfer_number=previous_transfer_number,
        retention_interval=timedelta(
            days=current_app.config["APP_LOG_RETENTION_DAYS"]
        ),
    )


def _on_rejected_agent_transfer_signal(
    debtor_id: int,
    creditor_id: int,
    coordinator_type: str,
    coordinator_id: int,
    coordinator_request_id: int,
    status_code: str,
    total_locked_amount: int,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    if coordinator_type != CT_AGENT:  # pragma: no cover
        raise RuntimeError(
            f'Unexpected coordinator type: "{coordinator_type}"'
        )

    procedures.process_account_lock_rejected_transfer(
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        status_code=status_code,
        debtor_id=debtor_id,
        creditor_id=creditor_id,
    )


def _on_prepared_agent_transfer_signal(
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_type: str,
    coordinator_id: int,
    coordinator_request_id: int,
    locked_amount: int,
    recipient: str,
    prepared_at: datetime,
    demurrage_rate: float,
    deadline: datetime,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    if coordinator_type != CT_AGENT:  # pragma: no cover
        raise RuntimeError(
            f'Unexpected coordinator type: "{coordinator_type}"'
        )

    has_been_processed = procedures.process_account_lock_prepared_transfer(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        locked_amount=locked_amount,
        demurrage_rate=demurrage_rate,
        deadline=deadline,
        min_demurrage_rate=current_app.config["APP_MIN_DEMURRAGE_RATE"],
    )
    if not has_been_processed:
        procedures.dismiss_prepared_transfer(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            transfer_id=transfer_id,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
        )


def _on_finalized_agent_transfer_signal(
    debtor_id: int,
    creditor_id: int,
    transfer_id: int,
    coordinator_type: str,
    coordinator_id: int,
    coordinator_request_id: int,
    committed_amount: int,
    status_code: str,
    total_locked_amount: int,
    prepared_at: datetime,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    # TODO: implement!

    if coordinator_type != CT_AGENT:  # pragma: no cover
        raise RuntimeError(
            f'Unexpected coordinator type: "{coordinator_type}"'
        )

    procedures.process_finalized_agent_transfer_signal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=committed_amount,
        status_code=status_code,
        total_locked_amount=total_locked_amount,
    )


def _on_updated_ledger_signal(
    creditor_id: int,
    debtor_id: int,
    update_id: int,
    account_id: str,
    creation_date: date,
    principal: int,
    last_transfer_number: int,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.process_updated_ledger_signal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=update_id,
        account_id=account_id,
        creation_date=creation_date,
        principal=principal,
        last_transfer_number=last_transfer_number,
        ts=ts,
    )


def _on_updated_policy_signal(
    creditor_id: int,
    debtor_id: int,
    update_id: int,
    policy_name: Optional[str],
    min_principal: int,
    max_principal: int,
    peg_exchange_rate: Optional[float],
    peg_debtor_id: Optional[int],
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.process_updated_policy_signal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=update_id,
        policy_name=policy_name,
        min_principal=min_principal,
        max_principal=max_principal,
        peg_exchange_rate=peg_exchange_rate,
        peg_debtor_id=peg_debtor_id,
        ts=ts,
    )


def _on_updated_flags_signal(
    creditor_id: int,
    debtor_id: int,
    update_id: int,
    config_flags: int,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.process_updated_flags_signal(
        creditor_id=creditor_id,
        debtor_id=debtor_id,
        update_id=update_id,
        config_flags=config_flags,
        ts=ts,
    )


def _on_fetch_debtor_info_signal(
    iri: str,
    debtor_id: int,
    is_locator_fetch: bool,
    is_discovery_fetch: bool,
    ignore_cache: bool,
    recursion_level: int,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.schedule_debtor_info_fetch(
        iri=iri,
        debtor_id=debtor_id,
        is_locator_fetch=is_locator_fetch,
        is_discovery_fetch=is_discovery_fetch,
        ignore_cache=ignore_cache,
        recursion_level=recursion_level,
        ts=ts,
    )


def _on_store_document_signal(
    debtor_info_locator: str,
    debtor_id: int,
    peg_debtor_info_locator: Optional[str],
    peg_debtor_id: Optional[int],
    peg_exchange_rate: Optional[float],
    will_not_change_until: Optional[datetime],
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.store_document(
        debtor_info_locator=debtor_info_locator,
        debtor_id=debtor_id,
        peg_debtor_info_locator=peg_debtor_info_locator,
        peg_debtor_id=peg_debtor_id,
        peg_exchange_rate=peg_exchange_rate,
        will_not_change_until=will_not_change_until,
        ts=ts,
    )


def _on_discover_debtor_signal(
    debtor_id: int,
    iri: str,
    force_locator_refetch: bool,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.discover_debtor(
        debtor_id=debtor_id,
        iri=iri,
        force_locator_refetch=force_locator_refetch,
        ts=ts,
        debtor_info_expiry_period=timedelta(
            days=current_app.config["APP_DEBTOR_INFO_EXPIRY_DAYS"]
        ),
        locator_claim_expiry_period=timedelta(
            days=current_app.config["APP_LOCATOR_CLAIM_EXPIRY_DAYS"]
        ),
    )


def _on_confirm_debtor_signal(
    debtor_id: int,
    debtor_info_locator: str,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.confirm_debtor(
        debtor_id=debtor_id,
        debtor_info_locator=debtor_info_locator,
        ts=ts,
        max_message_delay=timedelta(
            days=current_app.config["APP_EXTREME_MESSAGE_DELAY_DAYS"]
        ),
    )


def _on_activate_collector_signal(
    debtor_id: int,
    creditor_id: int,
    account_id: str,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.activate_collector(
        debtor_id=debtor_id,
        collector_id=creditor_id,
        account_id=account_id,
    )


def _on_candidate_offer_signal(
    turn_id: int,
    debtor_id: int,
    creditor_id: int,
    amount: int,
    account_creation_date: date,
    last_transfer_number: int,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    procedures.process_candidate_offer_signal(
        demurrage_rate=current_app.config["APP_MIN_DEMURRAGE_RATE"],
        turn_id=turn_id,
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        amount=amount,
        account_creation_date=account_creation_date,
        last_transfer_number=last_transfer_number,
    )


def _on_needed_collector_signal(
    debtor_id: int,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    # NOTE: When there are more than one "worker" servers, it is quite
    # likely that more than one signal will be received for a given
    # debtor ID because every "worker" server may send such a signal.
    # Here we try to detect such duplicated signals, and avoid making
    # repetitive queries to the central database.
    if not procedures.is_recently_needed_collector(debtor_id):
        procedures.ensure_collector_accounts(
            debtor_id=debtor_id,
            min_collector_id=current_app.config["MIN_COLLECTOR_ID"],
            max_collector_id=current_app.config["MAX_COLLECTOR_ID"],
        )
        procedures.mark_as_recently_needed_collector(debtor_id, ts)


_MESSAGE_TYPES = {
    "RejectedConfig": (
        ps.RejectedConfigMessageSchema(),
        _on_rejected_config_signal,
    ),
    "AccountUpdate": (
        ps.AccountUpdateMessageSchema(),
        _on_account_update_signal,
    ),
    "AccountPurge": (
        ps.AccountPurgeMessageSchema(),
        _on_account_purge_signal,
    ),
    "AccountTransfer": (
        ps.AccountTransferMessageSchema(),
        _on_account_transfer_signal,
    ),
    "RejectedTransfer": (
        ps.RejectedTransferMessageSchema(),
        _on_rejected_agent_transfer_signal,
    ),
    "PreparedTransfer": (
        ps.PreparedTransferMessageSchema(),
        _on_prepared_agent_transfer_signal,
    ),
    "FinalizedTransfer": (
        ps.FinalizedTransferMessageSchema(),
        _on_finalized_agent_transfer_signal,
    ),
    "FetchDebtorInfo": (
        schemas.FetchDebtorInfoMessageSchema(),
        _on_fetch_debtor_info_signal,
    ),
    "StoreDocument": (
        schemas.StoreDocumentMessageSchema(),
        _on_store_document_signal,
    ),
    "DiscoverDebtor": (
        schemas.DiscoverDebtorMessageSchema(),
        _on_discover_debtor_signal,
    ),
    "ConfirmDebtor": (
        schemas.ConfirmDebtorMessageSchema(),
        _on_confirm_debtor_signal,
    ),
    "ActivateCollector": (
        schemas.ActivateCollectorMessageSchema(),
        _on_activate_collector_signal,
    ),
    "CandidateOffer": (
        schemas.CandidateOfferMessageSchema(),
        _on_candidate_offer_signal,
    ),
    "NeededCollector": (
        schemas.NeededCollectorMessageSchema(),
        _on_needed_collector_signal,
    ),
    "UpdatedLedger": (
        schemas.UpdatedLedgerMessageSchema(),
        _on_updated_ledger_signal,
    ),
    "UpdatedPolicy": (
        schemas.UpdatedPolicyMessageSchema(),
        _on_updated_policy_signal,
    ),
    "UpdatedFlags": (
        schemas.UpdatedFlagsMessageSchema(),
        _on_updated_flags_signal,
    ),
}

_LOGGER = logging.getLogger(__name__)


TerminatedConsumtion = rabbitmq.TerminatedConsumtion


class SmpConsumer(rabbitmq.Consumer):
    """Passes messages to proper handlers (actors)."""

    def process_message(self, body, properties):
        content_type = getattr(properties, "content_type", None)
        if content_type != "application/json":
            _LOGGER.error('Unknown message content type: "%s"', content_type)
            return False

        massage_type = getattr(properties, "type", None)
        try:
            schema, actor = _MESSAGE_TYPES[massage_type]
        except KeyError:
            _LOGGER.error('Unknown message type: "%s"', massage_type)
            return False

        try:
            obj = json.loads(body.decode("utf8"))
        except (UnicodeError, json.JSONDecodeError):
            _LOGGER.error(
                "The message does not contain a valid JSON document."
            )
            return False

        try:
            message_content = schema.load(obj)
        except ValidationError as e:
            _LOGGER.error("Message validation error: %s", str(e))
            return False

        if not message_belongs_to_this_shard(message_content):
            raise RuntimeError("The server is not responsible for this shard.")

        actor(**message_content)
        return True
