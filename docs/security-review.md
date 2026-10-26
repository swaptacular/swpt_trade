# Security and bug review: swpt_trade

| | |
|---|---|
| Reviewed commit | `1cb4cb6` (2026-04-01) |
| Review date | 2026-10-26 |
| Scope | `swpt_trade/` (message actors, debtor info fetching, transfer processing), with focus on input from other nodes and from remote HTTPS servers |
| Method | Manual code review; findings reproduced with unit tests against PostgreSQL 16 (separate solver and worker databases), with the Cython extensions built |

This document covers the issues **fixed in this pull request**, plus
review notes. Additional findings that are not fixed here have been
reported to the maintainers privately.

## Summary

| ID | Severity | Finding | Status |
|---|---|---|---|
| SEC-1 | Medium | Debtor info fetcher reads unbounded response bodies into memory | Fixed |
| SEC-2 | Low | Messages with an unexpected coordinator type or a zero amount stop the consumer | Fixed |

## SEC-1: Unbounded response bodies from debtor info servers (Medium, fixed)

**Where:** `swpt_trade/fetch_debtor_infos.py`, `_make_https_request`.

**Problem.** Debtor info documents are fetched from HTTPS URLs that
debtors control (and from URLs inside those documents, following peg
chains). For every `200` response, the code called
`await response.read()`, which reads the **whole body** into memory.
Only afterwards did `_parse_debtor_info_document` check the content
type and the `APP_COIN_INFO_DOCUMENT_MAX_LENGTH` limit (50000 bytes).

A malicious debtor can point its info IRI at a server that sends a
huge (or endless) body. The fetcher keeps reading until the request
timeout. Since many fetches run concurrently (`max_connections`), the
process can run out of memory.

**Fix.** The body is read with
`response.content.read(APP_COIN_INFO_DOCUMENT_MAX_LENGTH + 1)`. One
extra byte is enough for the existing length check to reject oversized
documents, and the fetcher never reads more than that.

**Test:** `test_make_https_request_reads_bounded_body` uses a fake
response that fails if the whole body is read. It also checks that
exactly `max_length + 1` bytes are requested.

## SEC-2: Invalid messages stop the consumer (Low, fixed)

**Where:** `swpt_trade/actors.py`.

**Problem.**

- `_on_rejected_agent_transfer_signal`,
  `_on_prepared_agent_transfer_signal` and
  `_on_finalized_agent_transfer_signal` **raised `RuntimeError`** when
  the coordinator type was not `agent`.
- `_on_account_transfer_signal` (for agent transfers) called
  `TransferNote.validate`, which does `assert acquired_amount != 0`.
  The protocol schema allows `acquired_amount == 0`, so such a message
  raised `AssertionError`, which the `except ValueError` around it does
  not catch.

In `swpt_pythonlib.rabbitmq.Consumer`, an exception from a handler
stops consumption, and the unacknowledged message is redelivered, so
the consumer stops again on every restart.

**Reachability.** `swpt_stomp` rejects both cases at the network edge
(`acquired_amount == 0` and wrong coordinator types), so remote peers
cannot normally deliver such messages. This is a defence-in-depth fix.

**Fix.** These messages are now logged and ignored.

**Tests:** `test_unexpected_coordinator_type`,
`test_on_account_transfer_signal_zero_amount`.

## Review notes

- **Fetch URL scheme.** Only `https://` URLs are fetched at first
  (`aiohttp.InvalidURL` otherwise), at most 2 redirects are followed,
  and every request has a total timeout.
- **CoinInfo parsing** validates the document type, length, JSON
  schema, and the debtor URIs.
- **Peg chains** are followed only up to `MAX_DISTANCE_TO_BASE`, so a
  cycle of documents cannot cause endless fetching.

## Tests

- `test_make_https_request_reads_bounded_body`
- `test_unexpected_coordinator_type`
- `test_on_account_transfer_signal_zero_amount`

Each fails without its fix. Full suite (Cython extensions built,
`test_make_https_requests` deselected because it makes real Internet
requests): 734 passed, 1 skipped, 1 failed. The one failure,
`tests/test_cli.py::test_roll_turns`, fails identically without this
change. The 4 existing `flake8` warnings in `tests/` predate this
change.
