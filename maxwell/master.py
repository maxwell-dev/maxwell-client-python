import pycommons.logger
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types
from maxwell.connection import Code
from maxwell.connection import Event

logger = pycommons.logger.get_instance(__name__)


class Master(object):

    # ===========================================
    # apis
    # ===========================================
    def __init__(self, endpoints, connection_mgr, options, loop):
        self.__endpoints = endpoints
        self.__connection_mgr = connection_mgr
        self.__options = options
        self.__loop = loop

        self.__endpoint_index = -1
        self.__frontend_endpoint = None
        self.__backend_endpoints = {}
        self.__connect_to_master()

    def close(self):
        self.__disconnect_from_master()

    async def resolve_frontend(self, cache=True):
        if cache:
            if self.__frontend_endpoint != None:
                return self.__frontend_endpoint
        return await self.__resolve_frontend()

    async def resolve_backend(self, topic, cache=True):
        if cache:
            endpoint = self.__backend_endpoints.get(topic)
            if endpoint != None:
                return endpoint
        return await self.__resolve_backend(topic)

    # ===========================================
    # connector
    # ===========================================
    def __connect_to_master(self):
        self.__connection = self.__connection_mgr.fetch(
            self.__next_endpoint()
        )
        self.__connection.add_listener(
            Event.ON_CONNECTED,
            self.__on_connect_to_master_done
        )
        self.__connection.add_listener(
            (Event.ON_ERROR, Code.FAILED_TO_CONNECT),
            self.__on_connect_to_master_failed
        )

    def __disconnect_from_master(self):
        self.__connection.delete_listener(
            Event.ON_CONNECTED,
            self.__on_connect_to_master_done
        )
        self.__connection.delete_listener(
            (Event.ON_ERROR, Code.FAILED_TO_CONNECT),
            self.__on_connect_to_master_failed
        )
        self.__connection_mgr.release(self.__connection)
        self.__connection = None

    def __on_connect_to_master_done(self):
        pass

    def __on_connect_to_master_failed(self, _code):
        self.__disconnect_from_master()
        self.__loop.call_later(1, self.__connect_to_master)

    # ===========================================
    # internal coroutines
    # ===========================================
    async def __resolve_frontend(self):
        resolve_frontend_rep = await self.__request(
            self.__build_resolve_frontend_req()
        )
        self.__frontend_endpoint = resolve_frontend_rep.endpoint
        return self.__frontend_endpoint

    async def __resolve_backend(self, topic):
        resolve_backend_rep = await self.__request(
            self.__build_resolve_backend_req(topic)
        )
        self.__backend_endpoints[topic] = resolve_backend_rep.endpoint
        return resolve_backend_rep.endpoint

    async def __request(self, action):
        await self.__connection.wait_until_open()
        return await self.__connection.request(action)

    # ===========================================
    # req builders
    # ===========================================
    def __build_resolve_frontend_req(self):
        resolve_frontend_req = protocol_types.resolve_frontend_req_t()
        return resolve_frontend_req

    def __build_resolve_backend_req(self, topic):
        resolve_backend_req = protocol_types.resolve_backend_req_t()
        resolve_backend_req.topic = topic
        return resolve_backend_req

    # ===========================================
    # urls
    # ===========================================
    def __next_endpoint(self):
        self.__endpoint_index += 1
        if self.__endpoint_index >= len(self.__endpoints):
            self.__endpoint_index = 0
        return self.__endpoints[self.__endpoint_index]
