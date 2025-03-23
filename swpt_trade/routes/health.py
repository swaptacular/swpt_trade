from flask import make_response
from flask.views import MethodView
from .common import Blueprint


health_api = Blueprint(
    "health",
    __name__,
    url_prefix="/trade/health",
    description="""**Check health.** These are public endpoints
    for checking server's health status.
    """,
)


@health_api.route("/check/public")
class HealthCheckEndpoint(MethodView):
    @health_api.response(200)
    @health_api.doc(operationId="checkHealth")
    def get(self):
        """Return HTTP status code 200 if the server is healthy.

        On success, the content type of the returned document will be
        `text/plain`.

        """

        message = "I am healthy."
        headers = {
            "Content-Type": "text/plain",
        }

        return make_response(message, headers)
