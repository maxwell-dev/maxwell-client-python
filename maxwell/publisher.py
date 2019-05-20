import pycommons.logger
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types

logger = pycommons.logger.get_instance(__name__)


class Publisher(object):

    # ===========================================
    # apis
    # ===========================================
    def __init__(self, master, connection_mgr, options, loop):
        self.__master = master
        self.__connection_mgr = connection_mgr
        self.__options = options
        self.__loop = loop
        self.__connections = {}

    def close(self):
        for connection in self.__connections.values():
            self.__disconnect_from_backend(connection)

    async def publish(self, topic, value):
        endpoint = await self.__master.resolve_backend(topic)
        connection = self.__connect_to_backend(endpoint)
        await self.__publish(connection, topic, value)

    # ===========================================
    # connector
    # ===========================================
    def __connect_to_backend(self, endpoint):
        connection = self.__connections.get(endpoint)
        if connection == None:
            connection = self.__connection_mgr.fetch(endpoint)
            self.__connections[endpoint] = connection
        return connection

    def __disconnect_from_backend(self, endpoint):
        connection = self.__connections.pop(endpoint, None)
        if connection != None:
            self.__connection_mgr.release(connection)

    # ===========================================
    # internal coros
    # ===========================================
    async def __publish(self, connection, topic, value):
        await connection.wait_until_open()
        await connection.request(
            self.__build_publish_req(topic, value)
        )

    # ===========================================
    # req builders
    # ===========================================
    def __build_publish_req(self, topic, value):
        push_req = protocol_types.push_req_t()
        push_req.topic = topic
        push_req.value = value
        return push_req
