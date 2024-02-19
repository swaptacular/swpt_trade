from typing import TypeVar, Callable
from datetime import datetime, timezone, timedelta
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorLocatorClaim,
    FetchDebtorInfoSignal,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


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
