from typing import TypeVar, Callable, List, Iterable, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
from flask import current_app
from sqlalchemy import select, insert
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorInfoFetch,
    DebtorInfoDocument,
    ConfirmDebtorSignal,
    FetchDebtorInfoSignal,
)

T = TypeVar("T")
atomic: Callable[[T], T] = db.atomic


@dataclass
class FetchResult:
    fetch: DebtorInfoFetch
    status_code: str
    retry: bool
    document: Optional[DebtorInfoDocument]


def perform_debtor_info_fetches(connections: int, timeout: float) -> int:
    count = 0
    burst_count = current_app.config["APP_DEBTOR_INFO_FETCH_BURST_COUNT"]
    assert burst_count > 0
    assert connections > 0
    assert timeout > 0.0

    while True:
        n = _perform_debtor_info_fetches(burst_count, connections, timeout)
        count += n
        if n < burst_count:
            break

    return count


@atomic
def _perform_debtor_info_fetches(
        burst_count: int,
        connections: int,
        timeout: float,
) -> int:
    max_distance_to_base = current_app.config["MAX_DISTANCE_TO_BASE"]
    fetches = _query_debtor_info_fetches(burst_count)
    fetch_results = _perform_fetches(fetches)
    documents: List[DebtorInfoDocument] = []

    for r in fetch_results:
        fetch = r.fetch
        retry = r.retry
        document = r.document

        if document:
            assert not retry
            debtor_id = document.debtor_id
            debtor_info_locator = document.debtor_info_locator

            if fetch.is_discovery_fetch and fetch.debtor_id == debtor_id:
                db.session.add(
                    ConfirmDebtorSignal(
                        debtor_id=debtor_id,
                        debtor_info_locator=debtor_info_locator,
                    )
                )
                db.session.add(
                    FetchDebtorInfoSignal(
                        iri=debtor_info_locator,
                        debtor_id=debtor_id,
                        is_locator_fetch=True,
                        is_discovery_fetch=False,
                        recursion_level=0,
                    )
                )

            if fetch.is_locator_fetch and fetch.iri == debtor_info_locator:
                peg_debtor_info_locator = document.peg_debtor_info_locator
                peg_debtor_id = document.peg_debtor_id
                recursion_level = fetch.recursion_level

                if (
                    peg_debtor_info_locator is not None
                    and peg_debtor_id is not None
                    and recursion_level < max_distance_to_base
                ):
                    db.session.add(
                        FetchDebtorInfoSignal(
                            iri=peg_debtor_info_locator,
                            debtor_id=peg_debtor_id,
                            is_locator_fetch=True,
                            is_discovery_fetch=False,
                            recursion_level=recursion_level + 1,
                        )
                    )
                documents.append(document)

        if retry:
            _retry_fetch(fetch)
        else:
            db.session.delete(fetch)

    _save_debtor_info_documents(documents)
    return len(fetches)


def _query_debtor_info_fetches(max_count: int) -> List[DebtorInfoFetch]:
    current_ts = datetime.now(tz=timezone.utc)
    return (
        DebtorInfoFetch.query
        .filter(DebtorInfoFetch.next_attempt_at <= current_ts)
        .with_for_update(skip_locked=True)
        .limit(max_count)
        .all()
    )


def _perform_fetches(it: Iterable[DebtorInfoFetch]) -> List[FetchResult]:
    # TODO: Add a real implementation.
    return []


def _save_debtor_info_documents(documents: List[DebtorInfoDocument]) -> None:
    # TODO: Add a real implementation.
    pass


def _retry_fetch(fetch: DebtorInfoFetch) -> None:
    # TODO: Add a real implementation.
    pass
