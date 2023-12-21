"""JSON snippets to be included in the OpenAPI specification file."""

# TODO: This is copied from swpt_creditors, and must be rewritten.

DID = {
    "in": "path",
    "name": "debtorId",
    "required": True,
    "description": "The debtor's ID",
    "schema": {
        "type": "string",
        "pattern": "^[0-9A-Za-z_=-]{1,64}$",
    },
}

CID = {
    "in": "path",
    "name": "creditorId",
    "required": True,
    "description": "The creditor's ID",
    "schema": {
        "type": "string",
        "pattern": "^[0-9A-Za-z_=-]{1,64}$",
    },
}

TID = {
    "in": "path",
    "name": "transferId",
    "required": True,
    "description": "The transfer's ID",
    "schema": {
        "type": "string",
        "pattern": "^[0-9A-Za-z_=-]{1,64}$",
    },
}

TRANSFER_UUID = {
    "in": "path",
    "name": "transferUuid",
    "required": True,
    "description": "The transfer's UUID",
    "schema": {
        "type": "string",
        "format": "uuid",
    },
}

LOCATION_HEADER = {
    "Location": {
        "description": "The URI of the entry.",
        "schema": {
            "type": "string",
            "format": "uri",
        },
    },
}

ERROR_CONTENT = {
    "application/json": {
        "schema": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "integer",
                    "format": "int32",
                    "description": "Error code",
                },
                "errors": {
                    "type": "object",
                    "description": "Errors",
                },
                "status": {
                    "type": "string",
                    "description": "Error name",
                },
                "message": {
                    "type": "string",
                    "description": "Error message",
                },
            },
        }
    }
}

CONFLICTING_CREDITOR = {
    "description": "A creditor with the same ID already exists.",
    "content": ERROR_CONTENT,
}

TRANSFER_CONFLICT = {
    "description": (
        "A different transfer entry with the same UUID already exists."
    ),
    "content": ERROR_CONTENT,
}

TRANSFER_CANCELLATION_FAILURE = {
    "description": "The transfer can not be canceled.",
    "content": ERROR_CONTENT,
}

TRANSFER_EXISTS = {
    "description": "The same transfer entry already exists.",
    "headers": LOCATION_HEADER,
}

FORBIDDEN_OPERATION = {
    "description": "Forbidden operation.",
    "content": ERROR_CONTENT,
}

UPDATE_CONFLICT = {
    "description": "Conflicting update attempts.",
    "content": ERROR_CONTENT,
}

ACCOUNT_EXISTS = {
    "description": "Account exists.",
    "headers": LOCATION_HEADER,
}

FORBIDDEN_ACCOUNT_DELETION = {
    "description": "Forbidden account deletion.",
    "content": ERROR_CONTENT,
}

PEG_ACCOUNT_DELETION = {
    "description": "The account acts as a currency peg.",
    "content": ERROR_CONTENT,
}

NO_ACCOUNT_WITH_THIS_DEBTOR = {
    "description": "Account does not exist.",
}

WALLET_DOES_NOT_EXIST = {
    "description": "The wallet has not been found.",
}

WALLET_EXISTS = {
    "description": "The wallet has been found.",
    "headers": LOCATION_HEADER,
}

SCOPE_ACCESS_READONLY = [
    {"oauth2": ["access.readonly"]},
]

SCOPE_ACCESS_MODIFY = [
    {"oauth2": ["access"]},
]

SCOPE_DISABLE_PIN = [
    {"oauth2": ["disable_pin"]},
]

SCOPE_ACTIVATE = [
    {"oauth2": ["activate"]},
]

SCOPE_DEACTIVATE = [
    {"oauth2": ["deactivate"]},
]

API_DESCRIPTION = """In order to allow currency holders to use client
applications of their choice, Swaptacular recommends this `Payments Web API`.
The API allows for efficient client-side caching, as well as efficient cache
and data synchronization between two or more clients.

Note that every potentially dangerous operation, that the creditor is
allowed to perform via this API, can optionally be protected by a PIN
(Personal Identification Number). This allows users to stay logged in
for a long time, without compromising the security of their wallets.

This API is organized in four separate sections: **admin**,
**creditors**, **accounts**, **transfers**."""

API_SPEC_OPTIONS = {
    "info": {
        "description": API_DESCRIPTION,
    },
    "servers": [
        {"url": "$API_ROOT"},
        {"url": "/"},
    ],
    "components": {
        "securitySchemes": {
            "oauth2": {
                "type": "oauth2",
                "description": (
                    "This API uses OAuth 2. [More info](https://oauth.net/2/)."
                ),
                "flows": {
                    "authorizationCode": {
                        "authorizationUrl": "$OAUTH2_AUTHORIZATION_URL",
                        "tokenUrl": "$OAUTH2_TOKEN_URL",
                        "refreshUrl": "$OAUTH2_REFRESH_URL",
                        "scopes": {
                            "access.readonly": "read-only access",
                            "access": "read-write access",
                            "disable_pin": (
                                "disable the Personal Identification Number"
                            ),
                        },
                    },
                    "clientCredentials": {
                        "tokenUrl": "$OAUTH2_TOKEN_URL",
                        "refreshUrl": "$OAUTH2_REFRESH_URL",
                        "scopes": {
                            "access.readonly": "read-only access",
                            "access": "read-write access",
                            "disable_pin": (
                                "disable the Personal Identification Number"
                            ),
                            "activate": "activate new creditors",
                            "deactivate": "deactivate existing creditors",
                        },
                    },
                },
            },
        },
    },
}
