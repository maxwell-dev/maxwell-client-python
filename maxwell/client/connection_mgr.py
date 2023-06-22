from .connection import Connection


class ConnectionMgr(object):
    def __init__(self, options, loop):
        self.__options = options
        self.__loop = loop
        self.__connections = {}
        self.__ref_counts = {}

    def fetch(self, endpoint):
        connection = self.__connections.get(endpoint)
        if connection == None:
            connection = Connection(endpoint, self.__options, self.__loop)
            self.__connections[endpoint] = connection

        ref_count = self.__ref_counts.get(endpoint, 0)
        self.__ref_counts[endpoint] = ref_count + 1

        return connection

    def release(self, connection):
        endpoint = connection.get_endpoint()
        ref_count = self.__ref_counts.get(endpoint, 0)
        if ref_count - 1 <= 0:
            connection.close()
            self.__connections.pop(endpoint, None)
            self.__ref_counts.pop(endpoint, None)
        else:
            self.__ref_counts[endpoint] = ref_count - 1

    def clear(self):
        for connection in self.__connections.values():
            connection.close()
        self.__connections.clear()
