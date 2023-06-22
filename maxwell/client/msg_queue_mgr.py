from .msg_queue import MsgQueue


class MsgQueueMgr(object):
    def __init__(self, queue_capacity):
        self.__queue_capacity = queue_capacity
        self.__dict = dict()

    def clear(self):
        for queue in self.__dict.values():
            queue.clear()
        self.__dict.clear()

    def get(self, topic):
        queue = self.__dict.get(topic)
        if not queue:
            queue = MsgQueue(self.__queue_capacity)
            self.__dict[topic] = queue
        return queue

    def delete(self, topic):
        queue = self.__dict.get(topic)
        if not queue:
            return
        queue.clear()
        self.__dict.pop(topic, None)
