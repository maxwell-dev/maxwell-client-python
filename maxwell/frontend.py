import asyncio
import traceback
import json
import pycommons.logger
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types
from maxwell.action import Action
from maxwell.connection import Code
from maxwell.connection import Event
from maxwell.listenable import Listenable
from maxwell.subscription_mgr import SubscriptionMgr
from maxwell.msg_queue_mgr import MsgQueueMgr

logger = pycommons.logger.get_instance(__name__)


class Frontend(Listenable):

    # ===========================================
    # apis
    # ===========================================
    def __init__(self, master, connection_mgr, options, loop):
        super().__init__()

        self.__master = master
        self.__connection_mgr = connection_mgr
        self.__options = options
        self.__loop = loop

        self.__msg_queue_mgr = MsgQueueMgr(
            self.__options.get('queue_capacity')
        )
        self.__subscribe_callbacks = {}
        self.__events = {}  # {topic: not_full_event}
        self.__subscription_mgr = SubscriptionMgr()

        self.__loop.create_task(self.__connect_to_frontend())
        self.__pull_tasks = {}

        self.__watch_callbacks = {}
        self.__watched_actions = set()

        self.__connection = None

    def close(self):
        self.__do_disconnect_from_frontend()

    def subscribe(self, topic, offset, callback):
        if self.__subscription_mgr.has_subscribed(topic):
            raise Exception(f"Already subscribed: topic: {topic}")

        self.__subscribe_callbacks[topic] = callback
        event = asyncio.Event(loop=self.__loop)
        event.set()
        self.__events[topic] = event
        self.__subscription_mgr.add_subscription(topic, offset)
        if self.__connection != None and self.__connection.is_open():
            self.__new_pull_task(topic)

    def unsubscribe(self, topic):
        self.__delete_pull_task(topic)
        self.__subscription_mgr.delete_subscription(topic)
        self.__events.pop(topic)
        self.__subscribe_callbacks.pop(topic)
        self.__msg_queue_mgr.delete(topic)

    def recv(self, topic, limit):
        if not self.__subscription_mgr.has_subscribed(topic):
            raise Exception(f"Not subscribed yet: topic: {topic}")

        event = self.__events.get(topic)
        msg_queue = self.__msg_queue_mgr.get(topic)
        msgs = msg_queue.get(limit)
        if len(msgs) > 0:
            msg_queue.delete_to(msgs[-1].offset)
            event.set()  # trigger not_full_event
            return msgs
        else:
            event.set()  # trigger not_full_event
            return []

    async def do(self, action):
        result = await self.__request(self.__build_do_req(action))
        return json.loads(result.value)

    def watch(self, action_type, callback):
        self.__watched_actions.add(action_type)
        self.__watch_callbacks[action_type] = callback
        if self.__connection != None and self.__connection.is_open():
            self.__loop.create_task(self.__ensure_watched(action_type))

    def unwatch(self, action_type):
        self.__watched_actions.discard(action_type)
        self.__watch_callbacks.pop(action_type)
        if self.__connection != None and self.__connection.is_open():
            self.__loop.create_task(self.__unwatch(action_type))

    # ===========================================
    # connection management
    # ===========================================
    async def __connect_to_frontend(self):
        self.__do_connect_to_frontend(
            await self.__ensure_frontend_resolved()
        )

    def __do_connect_to_frontend(self, endpoint):
        self.__connection \
            = self.__connection_mgr.fetch(endpoint)
        self.__connection.add_listener(
            Event.ON_CONNECTED,
            self.__on_connect_to_frontend_done
        )
        self.__connection.add_listener(
            (Event.ON_ERROR, Code.FAILED_TO_CONNECT),
            self.__on_connect_to_frontend_failed
        )
        self.__connection.add_listener(
            Event.ON_DISCONNECTED,
            self.__on_disconnect_from_frontend_done
        )
        self.__connection.add_listener(
            Event.ON_MESSAGE, self.__on_action
        )

    def __do_disconnect_from_frontend(self):
        self.__connection.delete_listener(
            Event.ON_CONNECTED,
            self.__on_connect_to_frontend_done
        )
        self.__connection.delete_listener(
            (Event.ON_ERROR, Code.FAILED_TO_CONNECT),
            self.__on_connect_to_frontend_failed
        )
        self.__connection.delete_listener(
            Event.ON_DISCONNECTED,
            self.__on_disconnect_from_frontend_done
        )
        self.__connection.delete_listener(
            Event.ON_MESSAGE, self.__on_action
        )
        self.__connection_mgr.release(self.__connection)
        self.__connection = None

    def __on_connect_to_frontend_done(self):
        self.__loop.create_task(self.__pull_and_watch())
        self.notify(Event.ON_CONNECTED)

    def __on_connect_to_frontend_failed(self, _code):
        self.__do_disconnect_from_frontend()
        self.__loop.call_later(1, self.__reconnect_to_frontend)

    def __on_disconnect_from_frontend_done(self):
        self.__delete_all_pull_tasks()
        self.notify(Event.ON_DISCONNECTED)

    def __reconnect_to_frontend(self):
        self.__loop.create_task(self.__connect_to_frontend())

    async def __ensure_frontend_resolved(self):
        while True:
            try:
                return await self.__master.resolve_frontend(False)
            except Exception:
                logger.error(
                    "Failed to resolve frontend: %s", traceback.format_exc()
                )
                await asyncio.sleep(1)
                continue

    # ===========================================
    # core functions
    # ===========================================
    async def __pull_and_watch(self):
        self.__renew_all_pull_tasks()
        await self.__rewatch_all()

    def __renew_all_pull_tasks(self):
        self.__subscription_mgr.to_pendings()
        for topic, offset in self.__subscription_mgr.get_all_pendings():
            self.__new_pull_task(topic)

    def __new_pull_task(self, topic):
        self.__delete_pull_task(topic)
        self.__pull_tasks[topic] = self.__loop.create_task(
            self.__repeat_pull(topic)
        )

    def __delete_all_pull_tasks(self):
        for topic, task in self.__pull_tasks.items():
            task.cancel()
        self.__pull_tasks.clear()

    def __delete_pull_task(self, topic):
        task = self.__pull_tasks.get(topic)
        if task != None:
            task.cancel()
        self.__pull_tasks.pop(topic, None)

    async def __repeat_pull(self, topic):
        while True:
            try:
                await self.__pull(topic)
            except asyncio.TimeoutError:
                logger.debug("Timeout triggered, pull again...")
            except Exception:
                logger.warning("Error occured: %s", traceback.format_exc())
                await asyncio.sleep(1)

    async def __pull(self, topic):
        msg_queue = self.__msg_queue_mgr.get(topic)
        event = self.__events.get(topic)
        callback = self.__subscribe_callbacks.get(topic)
        offset = self.__subscription_mgr.get_doing(topic)
        pull_rep = await self.__request(
            self.__build_pull_req(topic, offset)
        )
        msg_queue.put(pull_rep.msgs)
        self.__subscription_mgr.to_doing(topic, msg_queue.last_offset() + 1)

        try:
            callback(topic)
        except Exception:
            logger.error("Failed to notify: %s", traceback.format_exc())

        if msg_queue.is_full():
            logger.info(
                "Msg queue is full, waiting for consuming: "
                "topic: %s, last offset: %s", topic, msg_queue.last_offset()
            )
            event.clear()
            await event.wait()

    async def __rewatch_all(self):
        for action_type in self.__watched_actions:
            await self.__ensure_watched(action_type)

    async def __ensure_watched(self, action_type):
        while True:
            try:
                await self.__request(self.__build_watch_req(action_type))
                logger.info("Watched action_type: %s", action_type)
                break
            except Exception:
                logger.error("Failed to watch: %s", traceback.format_exc())
                await asyncio.sleep(1)

    async def __unwatch(self, action_type):
        await self.__request(self.__build_unwatch_req(action_type))

    def __on_action(self, action):
        if action.__class__ != protocol_types.do_req_t:
            logger.error("Ignored action: %s", action)
            return
        callback = self.__watch_callbacks.get(action.type)
        if callback != None:
            try:
                callback(
                    Action(action, self.__connection, self.__loop)
                )
            except Exception:
                logger.error("Failed to notify: %s", traceback.format_exc())

    async def __request(self, msg):
        if self.__connection == None:
            raise Exception("Connection isn't open!")
        await self.__connection.wait_until_open()
        return await self.__connection.request(msg)

    # ===========================================
    # req builders
    # ===========================================
    def __build_auth_req(self):
        auth_req = protocol_types.auth_req_t()
        auth_req.token = "ignore"
        return auth_req

    def __build_pull_req(self, topic, offset):
        pull_req = protocol_types.pull_req_t()
        pull_req.topic = topic
        pull_req.offset = offset
        pull_req.limit = self.__options.get("get_limit")
        return pull_req

    def __build_do_req(self, action):
        do_req = protocol_types.do_req_t()
        do_req.type = action.get("type")
        do_req.value = json.dumps(action.get("value"))
        do_req.traces.add()
        return do_req

    def __build_watch_req(self, action_type):
        watch_req = protocol_types.watch_req_t()
        watch_req.type = action_type
        return watch_req

    def __build_unwatch_req(self, action_type):
        unwatch_req = protocol_types.unwatch_req_t()
        unwatch_req.type = action_type
        return unwatch_req
