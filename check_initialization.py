from flask_app import app
from flask import request, Request
from werkzeug.exceptions import BadRequest
from modules.view_tracker import getViewManager


# overwrite Request.on_json_loading_failed to return required response on bad JSON
# https://stackoverflow.com/a/39451091
class JSONBadRequest(BadRequest):
    def get_body(self, environ=None, scope=None):
        return '{\n"error": "bad request"\n}\n'

    def get_headers(self, environ=None, scope=None) -> list[tuple[str, str]]:
        return [("Content-Type", "application/json")]


def on_json_loading_failed(self, e):
    raise JSONBadRequest("Failed to decode JSON object")


Request.on_json_loading_failed = on_json_loading_failed


@app.before_request
def checkInitialization():
    # PUTs and GETs to /kvs/admin/view are the only endpoints that are allowed
    # to be accessed whether the node is initialized or not
    if str(request.url_rule) == "/kvs/admin/view" and (request.method == "PUT" or request.method == "GET"):
        return

    view_manager = getViewManager()
    view_manager.connect()

    if not view_manager.get().isInitialized():
        return {"error": "uninitialized"}, 418
