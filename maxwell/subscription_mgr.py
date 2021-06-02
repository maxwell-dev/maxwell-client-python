class SubscriptionMgr(object):

    def __init__(self):
        self.__pendings = dict()
        self.__doings = dict()

    def add_subscription(self, topic, offset):
        self.__pendings[topic] = offset
        self.__doings.pop(topic, None)

    def to_doing(self, topic, offset = None):
        if offset == None:
            offset = self.__pendings.get(topic)
        self.__doings[topic] = offset
        self.__pendings.pop(topic, None)

    def to_pendings(self):
        for topic, offset in self.__doings.items():
            self.to_pending(topic, offset)

    def to_pending(self, topic, offset):
        self.add_subscription(topic, offset)

    def delete_subscription(self, topic):
        self.__pendings.pop(topic, None)
        self.__doings.pop(topic, None)

    def clear(self):
        self.__pendings.clear()
        self.__doings.clear()

    def has_subscribed(self, topic):
        return topic in self.__pendings or topic in self.__doings

    def get_all_pendings(self):
        return set(self.__pendings.items())

    def get_all_doings(self):
        return set(self.__doings.items())

    def get_doing(self, topic):
        return self.__doings.get(topic, -1)