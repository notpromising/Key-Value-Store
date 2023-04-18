import asyncio
import aiohttp
import os

ALLOWED_REQUEST_TYPES = {"GET", "PUT", "DELETE", "POST"}


class KVSRequest:
    def __init__(self, hostname: str, endpoint: str, request_type: str, json_content: dict):
        """
        Small class to aggregate data required to do a request.
        :param hostname: hostname including port number to contact e.g. 192.168.1.5:1337
        :param endpoint: endpoint to contact, including leading slash e.g. /kvs/data/124214
        :param request_type: Request type *must* be one of: GET, PUT, DELETE, or POST
        :param json_content:
        """
        self.hostname = hostname
        if len(endpoint) < 1 or endpoint[0] != "/":
            raise ValueError(f"endpoint must not be empty and must begin with a slash")
        self.endpoint = endpoint
        if request_type.upper() not in ALLOWED_REQUEST_TYPES:
            raise ValueError(f"request_type must be one of {str(ALLOWED_REQUEST_TYPES)}!")
        self.request_type = request_type
        self.json_content = json_content

    async def executeRequest(self, session: aiohttp.ClientSession) -> aiohttp.ClientResponse:
        url = f"http://{self.hostname}{self.endpoint}"
        try:
            return await session.request(self.request_type, url, json=self.json_content)
        except aiohttp.ClientError as e:
            print(f"Request to {url} failed with exception: {e}")
        except asyncio.TimeoutError:
            print(f"Request to {url} timed out!")


def executeRequestsFork(request_list: list[KVSRequest], timeout=300):
    """
    Spawn a new process, immediately returning on parent process, but new
    process calls asyncExecuteRequests() with the given request_list.
    :param request_list: Requests to execute
    :param timeout: time in seconds before a request fails; default: 300
    :return:
    """
    if len(request_list) == 0:
        return
    if os.fork() != 0:
        return
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(asyncExecuteRequests(request_list, timeout))
    exit(0)


async def asyncExecuteRequests(request_list: list[KVSRequest], timeout=300, process_requests=False):
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=client_timeout) as session:
        client_responses = await asyncio.gather(*[request.executeRequest(session) for request in request_list])
        processed_responses: list[tuple[int, dict]] = []
        if process_requests:
            for client_response in client_responses:
                if client_response is None:
                    return processed_responses
                json = await client_response.json()
                status = client_response.status
                processed_responses.append((status, json))
            return processed_responses
        else:
            return None
