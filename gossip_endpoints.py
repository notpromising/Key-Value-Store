from flask_app import app
from flask import request
from modules.local_database import getKVSManager

VALUE = 0
TIMESTAMP = 1
DEPENDENCY_LIST = 2


@app.route("/gossip", methods=["PUT"])
def putGossip():
    response_body = {}
    response_status_code = 200

    request_body = request.json
    gossiped_node = request_body["origin"]
    gossiped_node_kvs = request_body["kvs"]

    kvs_manager = getKVSManager()
    kvs_manager.connect()

    my_kvs = kvs_manager.get().getDict()

    missing_keys_in_my_kvs = gossiped_node_kvs.keys() - my_kvs.keys()
    missing_keys_in_gossiped_node = my_kvs.keys() - gossiped_node_kvs.keys()
    intersection = my_kvs.keys() & gossiped_node_kvs.keys()

    if missing_keys_in_my_kvs:
        response_status_code = 201

    for key in missing_keys_in_my_kvs:
        # format: Tuple(value, timestamp, dependency list)
        value_tuple = gossiped_node_kvs[key]
        app.logger.info(f"gossip: Adding missing key {key} with val {value_tuple}")
        kvs_manager.get().setDictValue(
            key,
            value_tuple[VALUE],
            value_tuple[TIMESTAMP],
            value_tuple[DEPENDENCY_LIST],
        )

    for key in intersection:
        my_value_tuple = my_kvs[key]
        gossiped_node_value_tuple = gossiped_node_kvs[key]

        if my_value_tuple[TIMESTAMP] < gossiped_node_value_tuple[TIMESTAMP]:
            app.logger.info(f"gossip: Overriding old key {key} with val {gossiped_node_value_tuple}")
            kvs_manager.get().setDictValue(
                key,
                gossiped_node_value_tuple[VALUE],
                gossiped_node_value_tuple[TIMESTAMP],
                gossiped_node_value_tuple[DEPENDENCY_LIST],
            )

            response_status_code = 201
        elif my_value_tuple[TIMESTAMP] > gossiped_node_value_tuple[TIMESTAMP]:
            response_body[key] = my_value_tuple

    for key in missing_keys_in_gossiped_node:
        response_body[key] = my_kvs[key]
    
    return response_body, response_status_code
