import asyncio

from .master import Master
from .frontend import Frontend


class Client(object):
    # ===========================================
    # APIs
    # ===========================================
    def __init__(self, endpoints, options=None, loop=None):
        self.__endpoints = endpoints
        self.__options = self.__build_options(options)
        self.__loop = self.__loop = loop if loop else asyncio.get_event_loop()

        self.__master = Master(self.__endpoints, self.__options)
        self.__frontend = Frontend(self.__master, self.__options, self.__loop)

    async def close(self):
        await self.__frontend.close()

    def subscribe(self, topic, offset, callback):
        self.__frontend.subscribe(topic, offset, callback)

    def unsubscribe(self, topic):
        self.__frontend.unsubscribe(topic)

    def receive(self, topic, limit):
        return self.__frontend.receive(topic, limit)

    async def request(
        self,
        path,
        payload=None,
        header={},
        wait_open_timeout=None,
        round_timeout=None,
    ):
        return await self.__frontend.request(
            path, payload, header, wait_open_timeout, round_timeout
        )

    # ===========================================
    #  internal functions
    # ===========================================
    def __build_options(self, options):
        options = options if options else {}
        if options.get("wait_open_timeout") == None:
            options["wait_open_timeout"] = 3
        if options.get("round_timeout") == None:
            options["round_timeout"] = 5
        if options.get("wait_consuming_timeout") == None:
            options["wait_consuming_timeout"] = 10
        if options.get("queue_capacity") == None:
            options["queue_capacity"] = 512
        if options.get("get_limit") == None:
            options["get_limit"] = 128
        if options.get("endpoint_cache_ttl") == None:
            options["endpoint_cache_ttl"] = 10
        return options
