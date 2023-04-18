from flask import request

from flask_app import app, our_address
from modules.view_tracker import getViewManager
from modules.local_database import getKVSManager
from supporting_libs.requests_handler import KVSRequest, executeRequestsFork


@app.route("/kvs/admin/view", methods=["GET"])
def getView():
    # Perhaps handling the ViewManager should be done as a g variable?
    # But I don't think flask will ever end up using it in our case
    view_manager = getViewManager()
    view_manager.connect()
    view = view_manager.get().getView()
    return {"view": view}, 200


@app.route("/kvs/admin/view", methods=["PUT"])
def putView():
    view_manager = getViewManager()
    view_manager.connect()

    # As per the spec, we can always assume we are to initialize (or stay
    # initialized) when we receive a message on this endpoint
    view_manager.get().setInitialized(True)

    old_view = view_manager.get().getView()
    new_view = request.get_json().get("view")
    view_manager.get().setView(new_view)

    # Look through the new_view and find if the sender of the view change
    # was another node in the view. If so, we won't bother broadcasting
    for host in new_view:
        if request.remote_addr == host.split(":")[0]:
            return "", 200

    removed_nodes = list(set(old_view) - set(new_view))
    added_nodes = list(set(new_view) - set(old_view))
    requests = []
    requests.extend([KVSRequest(removed_node, "/kvs/admin/view", "DELETE", {}) for removed_node in removed_nodes])
    # Don't bother generating a request to send to ourselves nor node who sent it to us if exists.
    requests.extend([KVSRequest(added_node, "/kvs/admin/view", "PUT", {"view": new_view})
                     for added_node in added_nodes if not added_node == our_address])
    executeRequestsFork(requests)
    return "", 200


@app.route("/kvs/admin/view", methods=["DELETE"])
def deleteView():
    view_manager = getViewManager()
    view_manager.connect()
    view_manager.get().setInitialized(False)
    view_manager.get().setView([])
    kvs_manager = getKVSManager()
    kvs_manager.connect()
    kvs_manager.get().setDict({})
    app.logger.info("Uninitializing self!")

    return "", 200
