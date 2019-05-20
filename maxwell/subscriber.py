class Subscriber(object):

    def __init__(self, frontend):
        self.__frontend = frontend

    def subscribe(self, topic, offset, callback):
        self.__frontend.subscribe(topic, offset, callback)

    def unsubscribe(self, topic):
        self.__frontend.unsubscribe(topic)

    def recv(self, topic, limit):
        return self.__frontend.recv(topic, limit)