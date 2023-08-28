import asyncio
import traceback
import json
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types
from .logger import get_instance
from .connection import Code
from .connection import Event
from .listenable import Listenable
from .subscription_mgr import SubscriptionMgr
from .msg_queue_mgr import MsgQueueMgr

logger = get_instance(__name__)


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

        self.__msg_queue_mgr = MsgQueueMgr(self.__options.get("queue_capacity"))
        self.__subscribe_callbacks = {}
        self.__events = {}  # {topic: not_full_event}
        self.__subscription_mgr = SubscriptionMgr()

        self.__loop.create_task(self.__connect_to_frontend())
        self.__pull_tasks = {}

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

    def receive(self, topic, limit):
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

    async def request(self, path, payload=None, header=None):
        result = await self.__request(self.__build_req_req(path, payload, header))
        return json.loads(result.payload)

    # ===========================================
    # connection management
    # ===========================================
    async def __connect_to_frontend(self):
        self.__do_connect_to_frontend(await self.__ensure_frontend_resolved())

    def __do_connect_to_frontend(self, endpoint):
        self.__connection = self.__connection_mgr.fetch(endpoint)
        self.__connection.add_listener(
            Event.ON_CONNECTED, self.__on_connect_to_frontend_done
        )
        self.__connection.add_listener(
            (Event.ON_ERROR, Code.FAILED_TO_CONNECT),
            self.__on_connect_to_frontend_failed,
        )
        self.__connection.add_listener(
            Event.ON_DISCONNECTED, self.__on_disconnect_from_frontend_done
        )

    def __do_disconnect_from_frontend(self):
        self.__connection.delete_listener(
            Event.ON_CONNECTED, self.__on_connect_to_frontend_done
        )
        self.__connection.delete_listener(
            (Event.ON_ERROR, Code.FAILED_TO_CONNECT),
            self.__on_connect_to_frontend_failed,
        )
        self.__connection.delete_listener(
            Event.ON_DISCONNECTED, self.__on_disconnect_from_frontend_done
        )
        self.__connection_mgr.release(self.__connection)
        self.__connection = None

    def __on_connect_to_frontend_done(self):
        self.__renew_all_pull_tasks()
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
                pick_frontend_rep = await self.__master.request(
                    self.__build_pick_frontend_req()
                )
                return pick_frontend_rep.endpoint
            except Exception:
                logger.error("Failed to pick frontend: %s", traceback.format_exc())
                await asyncio.sleep(1)
                continue

    # ===========================================
    # core functions
    # ===========================================
    def __renew_all_pull_tasks(self):
        self.__subscription_mgr.to_pendings()
        for topic, offset in self.__subscription_mgr.get_all_pendings():
            self.__new_pull_task(topic)

    def __new_pull_task(self, topic):
        self.__delete_pull_task(topic)
        self.__pull_tasks[topic] = self.__loop.create_task(self.__repeat_pull(topic))

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
        pull_rep = await self.__request(self.__build_pull_req(topic, offset))
        msg_queue.put(pull_rep.msgs)
        self.__subscription_mgr.to_doing(topic, msg_queue.last_offset() + 1)

        try:
            callback(topic)
        except Exception:
            logger.error("Failed to notify: %s", traceback.format_exc())

        if msg_queue.is_full():
            logger.info(
                "Msg queue is full, waiting for consuming: "
                "topic: %s, last offset: %s",
                topic,
                msg_queue.last_offset(),
            )
            event.clear()
            await event.wait()

    async def __request(self, msg):
        if self.__connection == None:
            raise Exception("Connection isn't open!")
        await self.__connection.wait_open()
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

    def __build_req_req(self, path, payload=None, header={}):
        req_req = protocol_types.req_req_t()

        req_req.path = path
        req_req.payload = json.dumps(payload if payload else {})
        header2 = protocol_types.header_t()
        header2.token = header.get("token", "")
        req_req.header.CopyFrom(header2)

        return req_req

    def __build_pick_frontend_req(self):
        return protocol_types.pick_frontend_req_t()
