from .common import ensure_admin, Blueprint


admin_api = Blueprint(
    "admin",
    __name__,
    url_prefix="/trade",
    description="""**Admin API.** TODO.""",
)
admin_api.before_request(ensure_admin)
