from marshmallow import (
    Schema,
    fields,
    validate,
    validates,
    ValidationError,
)
from swpt_trade.models import MAX_INT64


TYPE_DESCRIPTION = (
    "The type of this object. Will always be present in the responses from the"
    " server."
)


class ValidateTypeMixin:
    @validates("type")
    def validate_type(self, value):
        if f"{value}Schema" != type(self).__name__:
            raise ValidationError("Invalid type.")


class EnsureAliveCollectorsRequestSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="EnsureAliveCollectorsRequest",
        load_only=True,
        metadata=dict(
            description=TYPE_DESCRIPTION,
            example="EnsureAliveCollectorsRequest",
        ),
    )
    number_of_accounts = fields.Integer(
        load_default=1,
        validate=validate.Range(min=1, max=MAX_INT64),
        data_key="numberOfAccounts",
        metadata=dict(
            format="int64",
            description="The number of needed alive collector accounts.",
            example=3,
        ),
    )
