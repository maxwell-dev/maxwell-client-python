class MsgQueue(object):

    def __init__(self, capacity):
        self.__capacity = capacity
        self.__list = []

    def put(self, msgs):
        if len(msgs) <= 0:
            return
        min_index = self.min_index_from(msgs[0].offset)
        if min_index > -1:
            del self.__list[min_index:]
        for msg in msgs:
            if self.is_full():
                break
            self.__list.append(msg)

    def delete_first(self):
        if len(self.__list) <= 0:
            return
        del self.__list[0]

    def delete_to(self, offset):
        if offset < 0:
            return
        max_index = self.max_index_to(offset)
        if max_index == -1:
            return
        del self.__list[0:max_index + 1]

    def clear(self):
        self.__list.clear()

    def get(self, limit):
        return self.get_from(self.first_offset(), limit)

    def get_from(self, offset, limit):
        result = []
        if limit <= 0 or len(self.__list) <= 0:
            return result

        start_index = self.min_index_from(offset)
        if start_index == -1:
            return result

        end_index = start_index + limit - 1
        if end_index >= len(self.__list):
            end_index = len(self.__list) - 1

        return self.__list[start_index:end_index + 1]

    def size(self):
        return len(self.__list)

    def is_full(self):
        return len(self.__list) >= self.__capacity

    def first_offset(self):
        if len(self.__list) <= 0:
            return -1
        return self.__list[0].offset

    def last_offset(self):
        if len(self.__list) <= 0:
            return -1
        return self.__list[-1].offset

    def min_index_from(self, offset):
        index = -1
        for msg in self.__list:
            if msg.offset >= offset:
                index += 1
                break
        return index

    def max_index_to(self, offset):
        index = -1
        for msg in self.__list:
            if msg.offset > offset:
                break
            index += 1
        return index
