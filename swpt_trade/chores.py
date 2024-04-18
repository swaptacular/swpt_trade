import logging
import json
from marshmallow import ValidationError
from flask import current_app
from swpt_pythonlib import rabbitmq
from swpt_trade import procedures
from swpt_trade import schemas


def _on_do_something(
    debtor_id: int, creditor_id: int, *args, **kwargs
) -> None:
    """Do something.

    This is en example.

    """

    procedures.do_something(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
    )


_LOGGER = logging.getLogger(__name__)

_MESSAGE_TYPES = {
    # NOTE: This is just an example. `FetchDebtorInfo` is not a chore
    # message and must not be processed here.
    "FetchDebtorInfo": (
        schemas.FetchDebtorInfoMessageSchema(),
        _on_do_something,
    ),
}


TerminatedConsumtion = rabbitmq.TerminatedConsumtion


class ChoresConsumer(rabbitmq.Consumer):
    """Passes messages to proper handlers."""

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

        actor(**message_content)
        return True


def create_chore_message(data):
    message_type = data["type"]
    properties = rabbitmq.MessageProperties(
        delivery_mode=2,
        app_id="swpt_trade",
        content_type="application/json",
        type=message_type,
    )
    schema, actor = _MESSAGE_TYPES[message_type]
    body = schema.dumps(data).encode("utf8")

    return rabbitmq.Message(
        exchange="",
        routing_key=current_app.config["CHORES_BROKER_QUEUE"],
        body=body,
        properties=properties,
        mandatory=True,
    )
