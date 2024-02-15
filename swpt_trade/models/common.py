from __future__ import annotations
import json
from datetime import datetime, timezone
from flask import current_app
from swpt_trade.extensions import db, publisher
from swpt_pythonlib import rabbitmq

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
MAX_UINT64 = (1 << 64) - 1
ROOT_CREDITOR_ID = 0
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
TS0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
DATE0 = TS0.date()
TRANSFER_NOTE_MAX_BYTES = 500
TRANSFER_NOTE_FORMAT_REGEX = r"^[0-9A-Za-z.-]{0,8}$"
CONFIG_DATA_MAX_BYTES = 2000
ACCOUNT_ID_MAX_BYTES = 100

CT_AGENT = "agent"


def get_now_utc():
    return datetime.now(tz=timezone.utc)


def is_valid_creditor_id(creditor_id: int, match_parent=False) -> bool:
    min_creditor_id = current_app.config["MIN_CREDITOR_ID"]
    max_creditor_id = current_app.config["MAX_CREDITOR_ID"]
    return min_creditor_id <= creditor_id <= max_creditor_id


class Signal(db.Model):
    __abstract__ = True

    @classmethod
    def send_signalbus_messages(cls, objects):  # pragma: no cover
        assert all(isinstance(obj, cls) for obj in objects)
        messages = (obj._create_message() for obj in objects)
        publisher.publish_messages([m for m in messages if m is not None])

    def send_signalbus_message(self):  # pragma: no cover
        self.send_signalbus_messages([self])

    def _create_message(self):
        data = self.__marshmallow_schema__.dump(self)
        message_type = data["type"]
        headers = {"message-type": message_type}
        is_smp_message = "creditor_id" in data

        if is_smp_message:
            creditor_id = data["creditor_id"]
            if not is_valid_creditor_id(creditor_id):
                raise RuntimeError(
                    "The agent is not responsible for this creditor."
                )
            headers["creditor-id"] = creditor_id
            headers["debtor-id"] = data["debtor_id"]

            if "coordinator_id" in data:
                headers["coordinator-id"] = data["coordinator_id"]
                headers["coordinator-type"] = data["coordinator_type"]

        properties = rabbitmq.MessageProperties(
            delivery_mode=2,
            app_id="swpt_trade",
            content_type="application/json",
            type=message_type,
            headers=headers,
        )
        body = json.dumps(
            data,
            ensure_ascii=False,
            check_circular=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf8")

        return rabbitmq.Message(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=body,
            properties=properties,
            mandatory=message_type == "FinalizeTransfer" or not is_smp_message,
        )

    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
