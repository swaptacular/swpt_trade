from typing import TypeVar, Callable, Optional
from datetime import datetime, timezone, timedelta
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorInfoDocument,
    DebtorLocatorClaim,
    FetchDebtorInfoSignal,
    DebtorInfoFetch,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@atomic
def schedule_debtor_info_fetch(
        *,
        iri: str,
        debtor_id: int,
        is_locator_fetch: bool,
        is_discovery_fetch: bool,
        recursion_level: int,
        ts: datetime,
) -> None:
    debtor_info_fetch = (
        DebtorInfoFetch.query
        .filter_by(iri=iri, debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if debtor_info_fetch:
        debtor_info_fetch.is_locator_fetch |= is_locator_fetch
        debtor_info_fetch.is_discovery_fetch |= is_discovery_fetch
        debtor_info_fetch.recursion_level = min(
            recursion_level, debtor_info_fetch.recursion_level
        )
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                DebtorInfoFetch(
                    iri=iri,
                    debtor_id=debtor_id,
                    is_locator_fetch=is_locator_fetch,
                    is_discovery_fetch=is_discovery_fetch,
                    recursion_level=recursion_level,
                )
            )


@atomic
def discover_debtor(
        *,
        debtor_id: int,
        iri: str,
        ts: datetime,
        locator_claim_expiration_period: timedelta,
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    locator_claim = (
        DebtorLocatorClaim.query
        .filter_by(debtor_id=debtor_id)
        .one_or_none()
    )
    if locator_claim:
        age = current_ts - locator_claim.latest_discovery_fetch_at
        if age < locator_claim_expiration_period:
            # We should ignore this message because the existing
            # locator claim is still valid.
            return
        locator_claim.latest_discovery_fetch_at = current_ts
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                DebtorLocatorClaim(
                    debtor_id=debtor_id,
                    debtor_info_locator=None,
                    latest_discovery_fetch_at=current_ts,
                )
            )

    db.session.add(
        FetchDebtorInfoSignal(
            iri=iri,
            debtor_id=debtor_id,
            is_locator_fetch=False,
            is_discovery_fetch=True,
            recursion_level=0,
        )
    )


@atomic
def confirm_debtor(
        *,
        debtor_id: int,
        debtor_info_locator: str,
        ts: datetime,
        max_message_delay: timedelta,
) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    message_delay = current_ts - ts

    if message_delay > max_message_delay:
        # We should ignore this message because it is too old.
        return

    locator_claim = (
        DebtorLocatorClaim.query
        .filter_by(debtor_id=debtor_id)
        .with_for_update()
        .one_or_none()
    )
    if locator_claim:
        locator_claim.debtor_info_locator = debtor_info_locator
        locator_claim.latest_locator_fetch_at = ts
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                DebtorLocatorClaim(
                    debtor_id=debtor_id,
                    debtor_info_locator=debtor_info_locator,
                    latest_locator_fetch_at=ts,
                )
            )


@atomic
def store_document(
        *,
        debtor_info_locator: str,
        debtor_id: int,
        peg_debtor_info_locator: Optional[str],
        peg_debtor_id: Optional[int],
        peg_exchange_rate: Optional[float],
        will_not_change_until: Optional[datetime],
        ts: datetime,
) -> None:
    document = (
        DebtorInfoDocument.query
        .filter_by(debtor_info_locator=debtor_info_locator)
        .with_for_update()
        .one_or_none()
    )
    if document:
        if document.fetched_at < ts:
            document.debtor_id = debtor_id
            document.peg_debtor_info_locator = peg_debtor_info_locator
            document.peg_debtor_id = peg_debtor_id
            document.peg_exchange_rate = peg_exchange_rate
            document.will_not_change_until = will_not_change_until
            document.fetched_at = ts
    else:
        with db.retry_on_integrity_error():
            db.session.add(
                DebtorInfoDocument(
                    debtor_info_locator=debtor_info_locator,
                    debtor_id=debtor_id,
                    peg_debtor_info_locator=peg_debtor_info_locator,
                    peg_debtor_id=peg_debtor_id,
                    peg_exchange_rate=peg_exchange_rate,
                    will_not_change_until=will_not_change_until,
                    fetched_at=ts,
                )
            )
