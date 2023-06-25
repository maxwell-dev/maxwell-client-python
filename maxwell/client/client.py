import asyncio
from .logger import get_instance
from .connection_mgr import ConnectionMgr
from .master import Master
from .frontend import Frontend


logger = get_instance(__name__)


class Client(object):
    # ===========================================
    # APIs
    # ===========================================
    def __init__(self, endpoints, options=None, loop=None):
        self.__endpoints = endpoints
        self.__init_options(options)
        self.__init_loop(loop)

        self.__init_connection_mgr()
        self.__init_master()
        self.__init_frontend()

    def close(self):
        self.__connection_mgr.clear()
        self.__master.close()
        self.__frontend.close()

    def get_master(self):
        return self.__master

    def get_frontend(self):
        return self.__frontend

    # ===========================================
    #  Init functions
    # ===========================================
    def __init_options(self, options):
        options = options if options else {}
        # if options.get('check_interval') == None:
        #     options['check_interval'] = 1
        if options.get("reconnect_delay") == None:
            options["reconnect_delay"] = 1
        if options.get("ping_interval") == None:
            options["ping_interval"] = 10
        # if options.get('max_idle_period') == None:
        #     options['max_idle_period'] = 15
        if options.get("default_round_timeout") == None:
            options["default_round_timeout"] = 5
        if options.get("default_offset") == None:
            options["default_offset"] = -600
        if options.get("get_limit") == None:
            options["get_limit"] = 64
        if options.get("queue_capacity") == None:
            options["queue_capacity"] = 512
        self.__options = options

    def __init_loop(self, loop):
        self.__loop = loop if loop else asyncio.get_event_loop()

    def __init_connection_mgr(self):
        self.__connection_mgr = ConnectionMgr(self.__options, self.__loop)

    def __init_master(self):
        self.__master = Master(self.__endpoints, self.__options, self.__loop)

    def __init_frontend(self):
        self.__frontend = Frontend(
            self.__master, self.__connection_mgr, self.__options, self.__loop
        )

    def subscribe(self, topic, offset, callback):
        self.__frontend.subscribe(topic, offset, callback)

    def unsubscribe(self, topic):
        self.__frontend.unsubscribe(topic)

    def receive(self, topic, limit):
        return self.__frontend.receive(topic, limit)

    async def request(self, path, payload=None, header={}):
        return await self.__frontend.request(path, payload, header)
