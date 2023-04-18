import asyncio
import json
import os
import time
import logging

from random import randint

import aiohttp

from local_database import getKVSManager, KVSManager
from view_tracker import getViewManager

from supporting_libs.requests_handler import KVSRequest, asyncExecuteRequests

GOSSIP_ENDPOINT = "/gossip"
GOSSIP_REQUEST_METHOD = "PUT"
MY_ADDRESS = os.environ.get("ADDRESS")

logger = logging.getLogger(__name__)
logging.basicConfig(format="[%(asctime)s] %(levelname)s in %(funcName)s: %(message)s")
logger.setLevel(logging.DEBUG)


async def sendGossip(
        current_view: list[str],
        current_kvs: dict[str, tuple[str, int, dict[str, int]]],
        kvs_manager: KVSManager
) -> float:
    random_node = current_view[randint(0, len(current_view) - 1)]

    while random_node == MY_ADDRESS:
        random_node = current_view[randint(0, len(current_view) - 1)]

    json_body = {}
    json_body["kvs"] = current_kvs
    json_body["origin"] = MY_ADDRESS

    requests = [KVSRequest(random_node, GOSSIP_ENDPOINT, GOSSIP_REQUEST_METHOD, json_body)]
    start_time = time.time_ns()
    response_list: list[tuple[int, dict]] = await asyncExecuteRequests(requests, timeout=GOSSIP_INTERVAL,
                                                                       process_requests=True)
    if len(response_list) != 0:
        resp_status, resp_json = response_list[0]
        if resp_status == 418:
            # They haven't seen the light! (they lack critical info (send them view))
            logger.info(f"Telling node {random_node} the our current view ({current_view}")
            await asyncExecuteRequests([KVSRequest(random_node, "/kvs/admin/view", "PUT", {"view": current_view})])
        elif resp_status == 200 or resp_status == 201:
            for key, (val, ver, timestamp) in resp_json.items():
                logger.info(f"Gossip returned new value {key}: {(val, ver, timestamp)}")
                kvs_manager.get().setDictValue(key, val, ver, timestamp)
        else:
            logger.error(f"Gossip endpoint returned weird status! ({resp_status}")

    end_time = time.time_ns()
    execute_time = (end_time - start_time) * pow(10, -9)
    time_to_sleep = GOSSIP_INTERVAL - execute_time
    if time_to_sleep < 0:
        time_to_sleep = 0

    logger.debug(f"Gossip finished, returning time_to_sleep = {time_to_sleep}")
    return time_to_sleep


GOSSIP_INTERVAL = 0.5


async def send_gossip():
    """
    For every GOSSIP_INTERVAL seconds, send the node's KVS contents and vector clock to a random node in the current view.

    Return: return_description
    """
    view_manager = getViewManager()
    view_manager.connect()
    kvs_manager = getKVSManager()
    kvs_manager.connect()

    while True:
        if view_manager.get().isInitialized():
            view = view_manager.get().getView()

            kvs = kvs_manager.get().getDict()

            if view and len(kvs) != 0:
                time_to_sleep = await sendGossip(view, kvs, kvs_manager)
                time.sleep(time_to_sleep)
            else:
                time.sleep(GOSSIP_INTERVAL)


def main():
    asyncio.run(send_gossip())


if __name__ == "__main__":
    main()
