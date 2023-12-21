from marshmallow import (
    Schema,
    fields,
    validate,
    validates,
    ValidationError,
    EXCLUDE,
)
from swpt_trade.models import (
    MIN_INT64,
    MAX_INT64,
)


class ValidateTypeMixin:
    @validates("type")
    def validate_type(self, value):
        if f"{value}Schema" != type(self).__name__:
            raise ValidationError("Invalid type.")


class DoDoSomethingMessageSchema(ValidateTypeMixin, Schema):
    """``DoDoSomething`` message schema."""

    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
