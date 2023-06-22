import collections
import traceback
from .logger import get_instance

logger = get_instance(__name__)


class Listenable(object):
    def __init__(self):
        self.__listeners = collections.OrderedDict()

    def add_listener(self, event, callback):
        callbacks = self.__listeners.get(event)
        if callbacks == None:
            callbacks = []
            self.__listeners[event] = callbacks
        callbacks.append(callback)

    def delete_listener(self, event, callback):
        callbacks = self.__listeners.get(event)
        if callbacks == None:
            return
        try:
            callbacks.remove(callback)
        except Exception:
            pass

    def notify(self, event, result=None):
        callbacks = self.__listeners.get(event, [])
        for callback in callbacks:
            try:
                if result != None:
                    callback(result)
                else:
                    callback()
            except Exception:
                logger.error("Failed to notify: %s", traceback.format_exc())
