from typing import TypeVar, Callable, Tuple, List, Optional
from dataclasses import dataclass
from datetime import datetime, timezone
from flask import current_app
from swpt_trade.extensions import db
from swpt_trade.models import (
    DebtorInfoFetch,
    DebtorInfoDocument,
    ConfirmDebtorSignal,
    FetchDebtorInfoSignal,
    StoreDocumentSignal,
)

T = TypeVar("T")
FetchTuple = Tuple[DebtorInfoFetch, Optional[DebtorInfoDocument]]
Classifcation = Tuple[List[FetchTuple], List[FetchTuple], List[FetchTuple]]
atomic: Callable[[T], T] = db.atomic


@dataclass
class FetchResult:
    fetch: DebtorInfoFetch
    errorcode: Optional[int] = None
    retry: bool = False
    document: Optional[DebtorInfoDocument] = None
    store_document: bool = False


def perform_debtor_info_fetches(connections: int, timeout: float) -> int:
    count = 0
    burst_count = current_app.config["APP_DEBTOR_INFO_FETCH_BURST_COUNT"]
    max_distance_to_base = current_app.config["MAX_DISTANCE_TO_BASE"]
    assert burst_count > 0
    assert max_distance_to_base > 1
    assert connections > 0
    assert timeout > 0.0

    while True:
        n = _perform_debtor_info_fetches_burst(
            burst_count, max_distance_to_base, connections, timeout
        )
        count += n
        if n < burst_count:
            break

    return count


@atomic
def _perform_debtor_info_fetches_burst(
        burst_count: int,
        max_distance_to_base: int,
        connections: int,
        timeout: float,
) -> int:
    fetch_results = _resolve_debtor_info_fetches(burst_count)

    for r in fetch_results:
        fetch = r.fetch
        errorcode = r.errorcode
        retry = r.retry
        document = r.document
        store_document = r.store_document

        if document:
            assert not retry
            debtor_info_locator = document.debtor_info_locator
            debtor_id = document.debtor_id

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
                peg_exchange_rate = document.peg_exchange_rate
                will_not_change_until = document.will_not_change_until
                recursion_level = fetch.recursion_level

                if (
                    peg_debtor_info_locator is not None
                    and peg_debtor_id is not None
                    and peg_exchange_rate is not None
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
                if store_document:
                    db.session.add(
                        StoreDocumentSignal(
                            debtor_info_locator=debtor_info_locator,
                            debtor_id=debtor_id,
                            peg_debtor_info_locator=peg_debtor_info_locator,
                            peg_debtor_id=peg_debtor_id,
                            peg_exchange_rate=peg_exchange_rate,
                            will_not_change_until=will_not_change_until,
                        )
                    )

        if retry:
            _retry_fetch(fetch, errorcode)
        else:
            db.session.delete(fetch)

    return len(fetch_results)


def _resolve_debtor_info_fetches(max_count: int) -> List[FetchResult]:
    current_ts = datetime.now(tz=timezone.utc)

    fetch_tuples: List[FetchTuple] = (
        db.session.query(DebtorInfoFetch, DebtorInfoDocument)
        .outerjoin(
            DebtorInfoDocument,
            DebtorInfoDocument.debtor_info_locator == DebtorInfoFetch.iri,
        )
        .filter(DebtorInfoFetch.next_attempt_at <= current_ts)
        .with_for_update(of=DebtorInfoFetch, skip_locked=True)
        .limit(max_count)
        .all()
    )
    wrong_shard, cached, new = _classify_fetch_tuples(fetch_tuples)

    wrong_shard_results = [FetchResult(fetch=f) for f, _ in wrong_shard]
    cached_results = [FetchResult(fetch=f, document=d) for f, d in cached]
    new_results = _perform_fetches([f for f, _ in new])

    all_results = wrong_shard_results + cached_results + new_results
    assert len(all_results) == len(fetch_tuples)
    return all_results


def _classify_fetch_tuples(fetch_tuples: List[FetchTuple]) -> Classifcation:
    current_ts = datetime.now(tz=timezone.utc)
    sharding_realm = current_app.config["SHARDING_REALM"]
    exp_period = current_app.config["APP_DEBTOR_INFO_EXPIRATION_DAYS"]

    wrong_shard: List[FetchTuple] = []
    cached: List[FetchTuple] = []
    new: List[FetchTuple] = []

    for t in fetch_tuples:
        fetch, document = t
        if not sharding_realm.match_str(fetch.iri):
            wrong_shard.append(t)
        elif document and not document.has_expired(current_ts, exp_period):
            cached.append(t)
        else:
            new.append(t)

    return wrong_shard, cached, new


def _perform_fetches(fetches: List[DebtorInfoFetch]) -> List[FetchResult]:
    # TODO: Add a real implementation.
    return [FetchResult(fetch=f, errorcode=500, retry=True) for f in fetches]


def _retry_fetch(fetch: DebtorInfoFetch, errorcode: Optional[int]) -> None:
    # TODO: Add a real implementation.
    pass
