import enum
import asyncio
import collections
import traceback
import logging
from typing import Callable
import websockets
import maxwell.protocol.maxwell_protocol_pb2 as protocol_types
import maxwell.protocol.maxwell_protocol as protocol

logger = logging.getLogger(__name__)


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
    ON_ERROR = 1


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


class Connection(Listenable):
    # ===========================================
    # apis
    # ===========================================
    def __init__(self, endpoint: str | Callable[[], str], options, loop=None):
        super().__init__()

        self.__endpoint = endpoint
        self.__options = options
        self.__loop = loop if loop else asyncio.get_event_loop()

        self.__resolved_endpoint = None

        self.__should_run = True
        self.__repeat_reconnect_task = None
        self.__repeat_receive_task = None

        self.__websocket = None
        self.__open_event = asyncio.Event()
        self.__closed_event = asyncio.Event()
        self.__last_ref = 0
        self.__request_futures = {}

        self.__toggle_to_close()
        self.__add_repeat_reconnect_task()
        self.__add_repeat_receive_task()

    def __del__(self):
        self.close()

    async def close(self):
        self.__should_run = False
        self.__delete_repeat_receive_task()
        self.__delete_repeat_reconnect_task()
        await self.__disconnect()

    async def request(self, msg):
        ref = self.__next_ref()
        msg.ref = ref

        request_future = self.__loop.create_future()
        self.__request_futures[ref] = request_future
        try:
            await self.__send(msg)
            return await request_future
        except Exception as e:
            raise e
        finally:
            self.__request_futures.pop(ref)

    async def wait_open(self):
        return await self.__open_event.wait()

    def is_open(self):
        return self.__websocket != None and self.__websocket.open == True

    # ===========================================
    # tasks
    # ===========================================
    def __add_repeat_reconnect_task(self):
        if self.__repeat_reconnect_task is None:
            self.__repeat_reconnect_task = self.__loop.create_task(
                self.__repeat_reconnect()
            )

    def __add_repeat_receive_task(self):
        if self.__repeat_receive_task is None:
            self.__repeat_receive_task = self.__loop.create_task(
                self.__repeat_receive()
            )

    def __delete_repeat_reconnect_task(self):
        if self.__repeat_reconnect_task is not None:
            self.__repeat_reconnect_task.cancel()
            self.__repeat_reconnect_task = None

    def __delete_repeat_receive_task(self):
        if self.__repeat_receive_task is not None:
            self.__repeat_receive_task.cancel()
            self.__repeat_receive_task = None

    # ===========================================
    # connection management
    # ===========================================
    async def __repeat_reconnect(self):
        while self.__should_run:
            await self.__closed_event.wait()
            await self.__disconnect()
            await self.__connect()

    async def __connect(self):
        try:
            url = self.__resolve_url()
            self.__on_connecting()
            self.__websocket = await websockets.connect(
                uri=url,
                ping_interval=self.__options.get("ping_interval"),
                max_size=None,
            )
            self.__toggle_to_open()
            self.__on_connected()
        except Exception:
            logger.error("Failed to connect: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_CONNECT)
            if self.__should_run:
                await asyncio.sleep(self.__options.get("reconnect_delay"))

    async def __disconnect(self):
        try:
            if self.__websocket is not None:
                self.__on_disconnecting()
                await self.__websocket.close()
                self.__websocket = None
                self.__on_disconnected()
        except Exception:
            logger.error("Failed to disconnect: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_DISCONNECT)
        finally:
            self.__toggle_to_close()

    def __toggle_to_open(self):
        self.__open_event.set()
        self.__closed_event.clear()

    def __toggle_to_close(self):
        self.__open_event.clear()
        self.__closed_event.set()

    # ===========================================
    # msg communication
    # ===========================================
    async def __send(self, msg):
        encoded_msg = None
        try:
            encoded_msg = protocol.encode_msg(msg)
        except Exception:
            logger.error("Failed to encode: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_ENCODE)
            raise Exception(Code.FAILED_TO_ENCODE)

        try:
            await self.__websocket.send(encoded_msg)
        except websockets.ConnectionClosed:
            logger.warning("Connection closed: endpoint: %s", self.__resolved_endpoint)
            self.__toggle_to_close()
            self.__on_error(Code.FAILED_TO_SEND)
            raise Exception(Code.FAILED_TO_SEND)
        except Exception:
            logger.error(
                "Failed to send: length: %s, error: %s",
                len(encoded_msg),
                traceback.format_exc(),
            )
            self.__on_error(Code.FAILED_TO_SEND)
            raise Exception(Code.FAILED_TO_SEND)

    async def __repeat_receive(self):
        while self.__should_run:
            await self.__open_event.wait()
            await self.__receive()

    async def __receive(self):
        try:
            encoded_msg = await self.__websocket.recv()
        except websockets.ConnectionClosed:
            logger.warning("Connection closed: endpoint: %s", self.__resolved_endpoint)
            self.__toggle_to_close()
            self.__on_error(Code.FAILED_TO_RECEIVE)
            return
        except Exception:
            logger.error("Failed to receive: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_RECEIVE)
            return

        try:
            msg = protocol.decode_msg(encoded_msg)
            request_future = self.__request_futures.get(msg.ref)
            if request_future is None:
                logger.error("Failed to find future: msg: %s", msg)
                return
            if (
                msg.__class__ == protocol_types.error_rep_t
                or msg.__class__ == protocol_types.error2_rep_t
            ):
                request_future.set_exception(Exception(msg.code, msg.desc))
            else:
                request_future.set_result(msg)
        except Exception:
            logger.error("Failed to decode: %s", traceback.format_exc())
            self.__on_error(Code.FAILED_TO_DECODE)

    # ===========================================
    # listeners
    # ===========================================
    def __on_connecting(self):
        logger.info("Connecting to endpoint: %s", self.__resolved_endpoint)
        self.notify(Event.ON_CONNECTING)

    def __on_connected(self):
        logger.info("Connected to endpoint: %s", self.__resolved_endpoint)
        self.notify(Event.ON_CONNECTED)

    def __on_disconnecting(self):
        logger.info("Disconnecting from endpoint: %s", self.__resolved_endpoint)
        self.notify(Event.ON_DISCONNECTING)

    def __on_disconnected(self):
        logger.info("Disconnected from endpoint: %s", self.__resolved_endpoint)
        self.notify(Event.ON_DISCONNECTED)

    def __on_error(self, code):
        logger.error("Error occured: %s", code)
        self.notify(Event.ON_ERROR, code)

    # ===========================================
    # utils
    # ===========================================
    def __next_ref(self):
        new_ref = self.__last_ref + 1
        if new_ref > 2100000000:
            new_ref = 1
        self.__last_ref = new_ref
        return new_ref

    def __resolve_url(self):
        return "ws://" + self.__resolve_endpoint() + "/$ws"

    def __resolve_endpoint(self):
        self.__resolved_endpoint = (
            self.__endpoint() if callable(self.__endpoint) else self.__endpoint
        )
        return self.__resolved_endpoint
