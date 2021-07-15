import asyncio
import pycommons.logger
from maxwell.connection_mgr import ConnectionMgr
from maxwell.master import Master
from maxwell.frontend import Frontend
from maxwell.subscriber import Subscriber
from maxwell.publisher import Publisher
from maxwell.doer import Doer
from maxwell.watcher import Watcher

logger = pycommons.logger.get_instance(__name__)


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
        self.__publisher = None
        self.__frontend = None
        self.__subscriber = None
        self.__doer = None
        self.__watcher = None

    def close(self):
        self.__connection_mgr.clear()
        self.__master.close()
        if self.__frontend != None:
            self.__frontend.close()
        if self.__publisher != None:
            self.__publisher.close()

    def get_master(self):
        return self.__master

    def get_publisher(self):
        self.__ensure_publisher_inited()
        return self.__publisher

    def get_frontend(self):
        self.__ensure_frontend_inited()
        return self.__frontend

    def get_subscriber(self):
        self.__ensure_subscriber_inited()
        return self.__subscriber

    def get_doer(self):
        self.__ensure_doer_inited()
        return self.__doer

    def get_watcher(self):
        self.__ensure_watcher_inited()
        return self.__watcher

    # ===========================================
    #  Init functions
    # ===========================================
    def __init_options(self, options):
        options = options if options else {}
        # if options.get('check_interval') == None:
        #     options['check_interval'] = 1
        if options.get('reconnect_delay') == None:
            options['reconnect_delay'] = 1
        if options.get('ping_interval') == None:
            options['ping_interval'] = 10
        # if options.get('max_idle_period') == None:
        #     options['max_idle_period'] = 15
        if options.get('default_round_timeout') == None:
            options['default_round_timeout'] = 5
        if options.get('default_offset') == None:
            options['default_offset'] = -600
        if options.get('get_limit') == None:
            options['get_limit'] = 64
        if options.get('queue_capacity') == None:
            options['queue_capacity'] = 512
        self.__options = options

    def __init_loop(self, loop):
        self.__loop = loop if loop else asyncio.get_event_loop()

    def __init_connection_mgr(self):
        self.__connection_mgr = ConnectionMgr(self.__options, self.__loop)

    def __init_master(self):
        self.__master = Master(
            self.__endpoints, self.__connection_mgr, self.__options, self.__loop
        )

    def __ensure_publisher_inited(self):
        if self.__publisher != None:
            return
        self.__publisher = Publisher(
            self.__master, self.__connection_mgr, self.__options, self.__loop
        )

    def __ensure_frontend_inited(self):
        if self.__frontend != None:
            return
        self.__frontend = Frontend(
            self.__master, self.__connection_mgr, self.__options, self.__loop
        )

    def __ensure_subscriber_inited(self):
        if self.__subscriber != None:
            return
        self.__ensure_frontend_inited()
        self.__subscriber = Subscriber(self.__frontend)

    def __ensure_doer_inited(self):
        if self.__doer != None:
            return
        self.__ensure_frontend_inited()
        self.__doer = Doer(self.__frontend)

    def __ensure_watcher_inited(self):
        if self.__watcher != None:
            return
        self.__ensure_frontend_inited()
        self.__watcher = Watcher(self.__frontend)
