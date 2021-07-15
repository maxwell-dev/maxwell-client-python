import enum
import asyncio
import websockets
import traceback
import pycommons.logger
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types
import maxwell.protocol.maxwell_protocol as protocol
from maxwell.listenable import Listenable

logger = pycommons.logger.get_instance(__name__)


class Code(enum.Enum):
    OK = 0
    FAILED_TO_ENCODE = 1
    FAILED_TO_SEND = 2
    FAILED_TO_DECODE = 3
    FAILED_TO_RECEIVE = 4
    FAILED_TO_CONNECT = 5
    FAILED_TO_DISCONNECT = 6
    UNKNOWN_ERROR = 99


class Event(enum.Enum):
    ON_CONNECTING = 100
    ON_CONNECTED = 101
    ON_DISCONNECTING = 102
    ON_DISCONNECTED = 103
    ON_MESSAGE = 104
    ON_ERROR = 1


class Connection(Listenable):

    # ===========================================
    # apis
    # ===========================================
    def __init__(self, endpoint, options, loop):
        super().__init__()

        self.__endpoint = endpoint
        self.__options = options
        self.__loop = loop

        self.__should_run = True
        self.__repeat_reconnect_task = None
        self.__repeat_ping_task = None
        self.__repeat_receive_task = None

        self.__last_ref = 0
        self.__open_event = asyncio.Event(loop=self.__loop)
        self.__closed_event = asyncio.Event(loop=self.__loop)
        self.__arrived_events = {}
        self.__msgs = {}  # exclude ping & pull msgs
        self.__active_time = 0
        self.__websocket = None

        self.__closed_event.set()
        self.__add_repeat_reconnect_task()
        self.__add_repeat_ping_task()
        self.__add_repeat_receive_task()

    def close(self):
        self.__should_run = False
        self.__delete_repeat_receive_task()
        self.__delete_repeat_ping_task()
        self.__delete_repeat_reconnect_task()

    async def send(self, msg):
        await self.__send(msg)

    async def request(self, msg, timeout=10):
        ref = self.__next_ref()
        if msg.__class__ == protocol_types.do_req_t:
            msg.traces[0].ref = ref
        else:
            msg.ref = ref

        self.__arrived_events[ref] = asyncio.Event(loop=self.__loop)
        await self.__send(msg)

        event = self.__arrived_events.get(ref)
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except Exception as e:
            raise e
        finally:
            self.__arrived_events.pop(ref)

        code, msg = self.__msgs.pop(ref)
        if code != Code.OK:
            logger.warning("Unexpected: code: %s; msg: %s", code, msg)
            raise Exception(code)

        return msg

    def is_open(self):
        return self.__websocket != None \
               and self.__websocket.open == True

    async def wait_until_open(self):
        return await self.__open_event.wait()

    def get_endpoint(self):
        return self.__endpoint

    # ===========================================
    # tasks
    # ===========================================
    def __add_repeat_reconnect_task(self):
        if self.__repeat_reconnect_task is None:
            self.__repeat_reconnect_task = \
                self.__loop.create_task(self.__repeat_reconnect())

    def __add_repeat_ping_task(self):
        if self.__repeat_ping_task is None:
            self.__repeat_ping_task \
                = self.__loop.create_task(self.__repeat_ping())

    def __add_repeat_receive_task(self):
        if self.__repeat_receive_task is None:
            self.__repeat_receive_task \
                = self.__loop.create_task(self.__repeat_receive())

    def __delete_repeat_reconnect_task(self):
        if self.__repeat_reconnect_task is not None:
            self.__repeat_reconnect_task.cancel()
            self.__repeat_reconnect_task = None

    def __delete_repeat_ping_task(self):
        if self.__repeat_ping_task is not None:
            self.__repeat_ping_task.cancel()
            self.__repeat_ping_task = None

    def __delete_repeat_receive_task(self):
        if self.__repeat_receive_task is not None:
            self.__repeat_receive_task.cancel()
            self.__repeat_receive_task = None

    # ===========================================
    # internal coroutines
    # ===========================================

    async def __repeat_reconnect(self):
        while self.__should_run:
            await self.__closed_event.wait()
            await self.__disconnect()
            await self.__connect()

    async def __repeat_ping(self):
        while self.__should_run:
            await self.__open_event.wait()
            await self.__ping()
            await asyncio.sleep(self.__options.get('ping_interval'))

    async def __repeat_receive(self):
        while self.__should_run:
            await self.__open_event.wait()
            await self.__recv()

    async def __ping(self):
        if self.__websocket != None:
            await self.__send(protocol_types.ping_req_t())

    async def __connect(self):
        try:
            self.__on_connecting()
            self.__websocket = await websockets.connect(
                uri=self.__build_url(self.__endpoint),
                ping_interval=None,
                max_size=None
            )
            self.__open_event.set()
            self.__closed_event.clear()
            self.__on_connected()
        except Exception:
            logger.error("Failed to connect: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_CONNECT)
            if self.__should_run:
                await asyncio.sleep(self.__options.get("reconnect_delay"))
                await self.__connect()

    async def __disconnect(self):
        try:
            if self.__websocket is not None:
                self.__on_disconnecting()
                await self.__websocket.close()
                self.__on_disconnected()
        except Exception:
            logger.error("Failed to disconnect: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_DISCONNECT)
        finally:
            self.__open_event.clear()
            self.__closed_event.set()

    async def __send(self, msg):
        encoded_msg = None
        try:
            encoded_msg = protocol.encode_msg(msg)
        except Exception:
            logger.error("Failed to encode: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_ENCODE)
        try:
            if encoded_msg:
                await self.__websocket.send(encoded_msg)
        except Exception:
            logger.error("Failed to send: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_SEND)

    async def __recv(self):
        try:
            encoded_msg = await self.__websocket.recv()
        except websockets.ConnectionClosed:
            logger.warning("Connection closed: endpoint: %s", self.__endpoint)
            self.__closed_event.set()
            return
        except Exception as e:
            logger.error("Failed to recv: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_RECEIVE)
            return

        try:
            msg = protocol.decode_msg(encoded_msg)
            if msg.__class__ != protocol_types.ping_rep_t:
                self.__on_msg(msg)
        except Exception:
            logger.error("Failed to decode: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_DECODE)

    # ===========================================
    # listeners
    # ===========================================
    def __on_connecting(self):
        logger.info("Connecting to endpoint: %s", self.__endpoint)
        self.notify(Event.ON_CONNECTING)

    def __on_connected(self):
        logger.info("Connected to endpoint: %s", self.__endpoint)
        self.notify(Event.ON_CONNECTED)

    def __on_disconnecting(self):
        logger.debug("Disconnecting from endpoint: %s", self.__endpoint)
        self.notify(Event.ON_DISCONNECTING)

    def __on_disconnected(self):
        logger.debug("Disconnected from endpoint: %s", self.__endpoint)
        self.notify(Event.ON_DISCONNECTED)

    def __on_error(self, code):
        logger.debug("Error occured: %s", code)
        self.notify(Event.ON_ERROR, code)
        self.notify((Event.ON_ERROR, code), code)

    def __on_msg(self, msg):
        msg_type = msg.__class__

        if msg_type == protocol_types.do_req_t:
            self.notify(Event.ON_MESSAGE, msg)
            return

        if msg_type == protocol_types.do_rep_t \
                or msg_type == protocol_types.ok2_rep_t \
                or msg_type == protocol_types.error2_rep_t:
            ref = msg.traces[0].ref
        else:
            ref = msg.ref
        event = self.__arrived_events.get(ref)
        if event == None:
            logger.error("Failed to find event: msg: %s", msg)
            return
        if msg_type == protocol_types.error_rep_t \
                or msg_type == protocol_types.error2_rep_t:
            self.__msgs[ref] = (msg.code, msg.desc)
        else:
            self.__msgs[ref] = (Code.OK, msg)
        event.set()

    # ===========================================
    # utils
    # ===========================================
    def __next_ref(self):
        new_ref = self.__last_ref + 1
        if new_ref > 600000:
            new_ref = 1
        self.__last_ref = new_ref
        return new_ref

    def __build_url(self, endpoint):
        return "ws://" + endpoint
