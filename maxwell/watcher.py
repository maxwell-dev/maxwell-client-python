class Watcher(object):
    def __init__(self, frontend):
        self.__frontend = frontend

    def watch(self, action_type, callback):
        self.__frontend.watch(action_type, callback)

    def unwatch(self, action_type):
        self.__frontend.unwatch(action_type)