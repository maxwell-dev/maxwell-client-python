import random
import aiohttp
import time
from maxwell.utils.logger import get_logger


logger = get_logger(__name__)


class Master(object):
    # ===========================================
    # apis
    # ===========================================
    def __init__(self, endpoints, options):
        self.__endpoints = endpoints
        self.__options = options
        self.__init_endpoint_index()
        self.__cached_frontend_endponit = None
        self.__last_cached_at = None

    async def pick_frontend(self, force=False):
        if (
            not force
            and self.__cached_frontend_endponit is not None
            and self.__last_cached_at + self.__options["endpoint_cache_ttl"]
            > time.time()
        ):
            return self.__cached_frontend_endponit
        rep = await self.__request("$pick-frontend")
        if rep["code"] != 0:
            raise Exception(f"Failed to pick frontend: rep: {rep}")
        self.__cached_frontend_endponit = rep["endpoint"]
        self.__last_cached_at = time.time()
        return rep["endpoint"]

    # ===========================================
    # internal functions
    # ===========================================
    async def __request(self, path):
        async with aiohttp.ClientSession() as session:
            url = self.__next_url(path)
            logger.info("Requesting master: url: %s", url)
            async with session.get(url) as response:
                if response.status != 200:
                    raise Exception(
                        f"Failed to pick frontend: status: {response.status}"
                    )
                rep = await response.json()
                logger.info("Sucessfully requested: rep: %s", rep)
                return rep

    def __init_endpoint_index(self):
        if self.__endpoints.__len__() == 0:
            raise Exception("No endpoint provided")
        self.__endpoint_index = random.randint(0, self.__endpoints.__len__() - 1)

    def __next_url(self, path):
        return "http://" + self.__next_endpoint() + "/" + path

    def __next_endpoint(self):
        self.__endpoint_index += 1
        if self.__endpoint_index >= len(self.__endpoints):
            self.__endpoint_index = 0
        return self.__endpoints[self.__endpoint_index]
