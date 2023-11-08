import asyncio
import json
import traceback
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types
from maxwell.utils.listenable import Listenable
from maxwell.utils.logger import get_logger
from maxwell.utils.connection import (
    MultiAltEndpointsConnection,
    Event,
    EventHandler,
    ErrorCode,
)

from .subscription_mgr import SubscriptionMgr
from .msg_queue_mgr import MsgQueueMgr


logger = get_logger(__name__)


class Frontend(Listenable, EventHandler):
    # ===========================================
    # apis
    # ===========================================
    def __init__(self, master, options, loop):
        super().__init__()

        self.__master = master
        self.__options = options
        self.__loop = loop

        self.__should_run = True

        self.__subscription_mgr = SubscriptionMgr()
        self.__subscribe_callbacks = {}
        self.__msg_queue_mgr = MsgQueueMgr(self.__options.get("queue_capacity"))
        self.__not_full_events = {}  # {topic0: event0, ...}
        self.__pull_tasks = {}

        self.__connection = MultiAltEndpointsConnection(
            pick_endpoint=self.__pick_frontend,
            options=self.__options,
            event_handler=self,
            loop=self.__loop,
        )
        self.__failed_to_connect = False

    async def close(self):
        self.__should_run = False
        self.__delete_all_pull_tasks()
        self.__subscription_mgr.clear()
        self.__subscribe_callbacks.clear()
        self.__msg_queue_mgr.clear()
        self.__not_full_events.clear()
        await self.__connection.close()

    def subscribe(self, topic, offset, callback):
        if self.__subscription_mgr.has_subscribed(topic):
            logger.info(f"Already subscribed: topic: {topic}")
            return

        self.__subscription_mgr.add_subscription(topic, offset)
        self.__subscribe_callbacks[topic] = callback
        event = asyncio.Event()
        event.set()
        self.__msg_queue_mgr.get_or_set(topic)
        self.__not_full_events[topic] = event
        self.__new_pull_task(topic)

    def unsubscribe(self, topic):
        self.__delete_pull_task(topic)
        self.__subscription_mgr.delete_subscription(topic)
        self.__subscribe_callbacks.pop(topic, None)
        self.__not_full_events.pop(topic, None)
        self.__msg_queue_mgr.delete(topic)

    def receive(self, topic, limit):
        event = self.__not_full_events.get(topic)
        msg_queue = self.__msg_queue_mgr.get_or_set(topic)
        msgs = msg_queue.get(limit)
        if len(msgs) > 0:
            msg_queue.delete_to(msgs[-1].offset)
            event.set()  # trigger not_full_event
            return msgs
        else:
            event.set()  # trigger not_full_event
            return []

    async def request(
        self,
        path,
        payload=None,
        header={},
        wait_open_timeout=None,
        round_timeout=None,
    ):
        result = await self.__request(
            self.__build_req_req(path, payload, header),
            wait_open_timeout,
            round_timeout,
        )
        return json.loads(result.payload)

    # ===========================================
    # EventHandler implementation
    # ===========================================
    def on_connecting(self, connection: MultiAltEndpointsConnection, _):
        self.notify(Event.ON_CONNECTING, connection)

    def on_connected(self, connection: MultiAltEndpointsConnection, _):
        self.__failed_to_connect = False
        self.__renew_all_pull_tasks()
        self.notify(Event.ON_CONNECTED, connection)

    def on_disconnecting(self, connection: MultiAltEndpointsConnection, _):
        self.notify(Event.ON_DISCONNECTING, connection)

    def on_disconnected(self, connection: MultiAltEndpointsConnection, _):
        self.__delete_all_pull_tasks()
        self.notify(Event.ON_DISCONNECTED, connection)

    def on_error(self, error_code, connection: MultiAltEndpointsConnection, _):
        if error_code == ErrorCode.FAILED_TO_CONNECT:
            self.__failed_to_connect = True
        self.notify(Event.ON_ERROR, error_code, self, connection)

    async def __pick_frontend(self):
        return await self.__master.pick_frontend(self.__failed_to_connect)

    # ===========================================
    # task functions
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
        while self.__should_run:
            try:
                await self.__pull(topic)
            except asyncio.TimeoutError:
                logger.debug("Timeout triggered, pull again...")
            except Exception as e:
                logger.warning("Error occured: %s", e)
                await asyncio.sleep(1)

    async def __pull(self, topic):
        if not self.__subscription_mgr.has_subscribed(topic):
            logger.warning(f"Already unsubscribed: topic: {topic}")
            return

        offset = self.__subscription_mgr.get_doing(topic)
        callback = self.__subscribe_callbacks.get(topic)
        msg_queue = self.__msg_queue_mgr.get_or_set(topic)
        event = self.__not_full_events.get(topic)

        pull_rep = await self.__request(self.__build_pull_req(topic, offset))
        if pull_rep.msgs == None or len(pull_rep.msgs) == 0:
            logger.info("No msgs pulled: topic: %s, offset: %s", topic, offset)
            return
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
            await event.wait(self.__options["wait_consuming_timeout"])

    async def __request(
        self,
        msg,
        wait_open_timeout=None,
        round_timeout=None,
    ):
        if wait_open_timeout == None:
            wait_open_timeout = self.__options["wait_open_timeout"]
        if round_timeout == None:
            round_timeout = self.__options["round_timeout"]

        async with asyncio.timeout(wait_open_timeout) as cm:
            await self.__connection.wait_open()
            next_delay = self.__loop.time() + round_timeout
            cm.reschedule(next_delay)
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
