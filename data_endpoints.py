from time import sleep, time_ns

from flask import request
from flask_app import app, our_address
from modules.local_database import getKVSManager
from modules.view_tracker import getViewManager
from supporting_libs.requests_handler import KVSRequest, executeRequestsFork


def broadcastToOtherNodes(timestamp) -> None:
    # Don't broadcast if on messages that have already been broadcast by another node
    endpoint = request.path
    request_method = request.method
    json_body = request.json
    if json_body.get("dont_broadcast"):
        return
    requests = []
    json_body["dont_broadcast"] = True
    json_body["timestamp"] = timestamp
    view_manager = getViewManager()
    view_manager.connect()
    view = view_manager.get().getView()

    # Don't bother generating a request to send to ourselves
    requests.extend([KVSRequest(node, endpoint, request_method, json_body) for node in view if node != our_address])
    executeRequestsFork(requests)


def getConflictingKeys(client_dependencies: dict[str, int], key_dependencies: dict[str, int]) -> set[str]:
    conflicting_keys = set()
    shared_keys = set(key_dependencies.keys()).intersection(client_dependencies.keys())
    for shared_key in shared_keys:
        if key_dependencies[shared_key] > client_dependencies[shared_key]:
            conflicting_keys.add(shared_key)
    return conflicting_keys


@app.route("/kvs/data/<key>", methods=["GET"])
def getKey(key: str):
    if not request.is_json or "causal-metadata" not in request.json.keys():
        return {"error": "bad request"}, 400
    prev_metadata = request.json.get("causal-metadata")
    if prev_metadata is None:
        prev_metadata = {}

    kvs_manager = getKVSManager()
    kvs_manager.connect()

    val_tuple = kvs_manager.get().getDictValue(key)
    if val_tuple is None:
        return {"causal-metadata": prev_metadata}, 404
    val, ver, dependencies = val_tuple

    conflicting_keys = getConflictingKeys(prev_metadata, dependencies)
    if key in prev_metadata or len(conflicting_keys) != 0:
        i = 0
        # Wait for update to save us
        while i < 20 and (ver < prev_metadata[key] or len(conflicting_keys) != 0):
            sleep(1)
            val, ver, dependencies = kvs_manager.get().getDictValue(key)
            conflicting_keys = getConflictingKeys(prev_metadata, dependencies)
            i += 1

        if ver < prev_metadata[key] or len(conflicting_keys) != 0:
            return {"error": "timed out while waiting for depended updates", "causal-metadata": prev_metadata}, 500

    # add returned key to client's dependencies
    prev_metadata[key] = ver
    # also add the key's dependencies to the client's, overriding older values when necessary
    for d_key, d_ver in dependencies.items():
        if (client_ver := prev_metadata.get(d_key)) is not None:
            prev_metadata[d_key] = max(client_ver, d_ver)
        else:
            prev_metadata[d_key] = d_ver
    # I believe we are supposed to return 404 on deleted keys?
    if val is None:
        return {"causal-metadata": prev_metadata}, 404
    return {"causal-metadata": prev_metadata, "val": val}, 200


@app.route("/kvs/data/<key>", methods=["PUT"])
def putKey(key: str):
    if not request.is_json or not (val := request.json.get("val")) or \
            "causal-metadata" not in request.json.keys():
        return {"error": "bad request"}, 400
    prev_metadata = request.json.get("causal-metadata")
    if prev_metadata is None:
        prev_metadata = {}

    app.logger.debug(f"putKey: key {key} with prev_metadata {prev_metadata}")

    if len(val) > 8000:
        return {"error": "val too large"}, 400

    if (timestamp := request.json.get("timestamp")) is None:
        timestamp = time_ns()
        broadcastToOtherNodes(timestamp)

    kvs_manager = getKVSManager()
    kvs_manager.connect()

    app.logger.debug(f"putKey current dict {kvs_manager.get().getDict()}")
    replaced = kvs_manager.get().setDictValue(key, val, timestamp, prev_metadata)
    app.logger.debug(f"putKey new dict {kvs_manager.get().getDict()}")
    prev_metadata[key] = timestamp
    app.logger.debug(f"putKey returning metadata {prev_metadata}")
    return {"causal-metadata": prev_metadata}, 200 if replaced else 201


@app.route("/kvs/data/<key>", methods=["DELETE"])
def deleteKey(key: str):
    if not request.is_json or "causal-metadata" not in request.json.keys():
        return {"error": "bad request"}, 400
    prev_metadata = request.json.get("causal-metadata")

    if (timestamp := request.json.get("timestamp")) is None:
        timestamp = time_ns()
        broadcastToOtherNodes(timestamp)

    kvs_manager = getKVSManager()
    kvs_manager.connect()

    replaced = kvs_manager.get().removeDictValue(key, timestamp, prev_metadata)
    return {"causal-metadata": {key: timestamp}}, 200 if replaced else 201


def existsMissingDependencies(current_kvs: dict[str, tuple[str, int, dict[str, int]]]) -> bool:
    for key, (val, ver, dependencies) in current_kvs.items():
        for d_key, d_ver in dependencies.items():
            if d_key not in current_kvs:
                return True
            elif d_ver > current_kvs[d_key][1]:
                return True
    return False


@app.route("/kvs/data", methods=["GET"])
def getData():
    if not request.is_json or "causal-metadata" not in request.json.keys():
        return {"error": "bad request"}, 400

    prev_metadata = request.json.get("causal-metadata")
    kvs_manager = getKVSManager()
    kvs_manager.connect()
    data = kvs_manager.get().getDict()
    keys = list(data.keys())

    # This is dumb, but I can't think of a better way to do this rn
    # (where are C style for-loops when you need them...)
    has_conflict = True
    missing_dependencies = existsMissingDependencies(data)
    i = 0
    while i < 20 and (has_conflict or missing_dependencies):
        app.logger.debug(f"Waiting because (has_conflict = {has_conflict} or missing_dependencies "
                         f"= {missing_dependencies})")
        has_conflict = False
        for key, client_ver in prev_metadata.items():
            if key not in data or client_ver > data[key][1]:
                has_conflict = True
        if not has_conflict:
            break
        sleep(1)
        data = kvs_manager.get().getDict()
        missing_dependencies = existsMissingDependencies(data)
        i += 1
    if has_conflict or missing_dependencies:
        return {"error": "timed out while waiting for depended updates", "causal-metadata": prev_metadata}, 500

    return_metadata = {}
    for key, (val, ver, dependencies) in data.items():
        return_metadata[key] = ver
        # Don't return deleted vals
        for d_key, d_ver in dependencies.items():
            if return_metadata.get(key) is not None:
                return_metadata[key] = max(d_ver, return_metadata[key])
            else:
                return_metadata[key] = d_ver
        if val is None:
            keys.pop(key)

    return {"count": len(keys), "keys": keys, "causal-metadata": "TODO"}
