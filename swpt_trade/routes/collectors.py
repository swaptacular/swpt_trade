from flask import current_app
from flask.views import MethodView
from flask_smorest import abort
from swpt_trade import procedures
from .common import ensure_admin, Blueprint
from .specs import DID
from . import specs
from . import schemas


collectors_api = Blueprint(
    "collectors",
    __name__,
    url_prefix="/trade",
    description="""**Manage collector accounts.**""",
)
collectors_api.before_request(ensure_admin)


# TODO: Consider implementing endpoints that give information about
# the already existing collector accounts.


@collectors_api.route(
    "collectors/<i64:debtorId>/activate", parameters=[DID]
)
class ActivateCollectorsEndpoint(MethodView):
    @collectors_api.arguments(schemas.ActivateCollectorsRequestSchema)
    @collectors_api.response(204)
    @collectors_api.doc(
        operationId="activateCollectors", security=specs.SCOPE_ACTIVATE
    )
    def post(self, activate_collectors_request, debtorId):
        """Ensure a number of alive collector accounts.
        """
        if debtorId == 0:
            abort(500)

        try:
            procedures.ensure_collector_accounts(
                debtor_id=debtorId,
                min_collector_id=current_app.config["MIN_COLLECTOR_ID"],
                max_collector_id=current_app.config["MAX_COLLECTOR_ID"],
                number_of_accounts=(
                    activate_collectors_request["number_of_accounts"]
                ),
            )
        except RuntimeError:
            abort(500)
