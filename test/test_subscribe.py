import asyncio
import pytest
from maxwell.utils.logger import get_logger
from maxwell.client.client import Client

logger = get_logger(__name__)


def build_options(options=None):
    options = options if options else {}
    if options.get("queue_capacity") == None:
        options["queue_capacity"] = 512
    if options.get("get_limit") == None:
        options["get_limit"] = 128
    if options.get("endpoint_cache_ttl") == None:
        options["endpoint_cache_ttl"] = 10
    if options.get("wait_consuming_timeout") == None:
        options["wait_consuming_timeout"] = 10
    return options


class TestSubscribe:
    @pytest.mark.asyncio
    async def test_normal(self):
        client = Client(["localhost:8081"], build_options())

        def on_message(topic):
            while True:
                msgs = client.receive("topic_1", 8)
                values = [
                    (msg.timestamp, int.from_bytes(msg.value, byteorder="little"))
                    for msg in msgs
                ]
                logger.debug("Recevied: topic: %s, msgs: %s", topic, values)
                if len(msgs) < 8:
                    break

        client.subscribe("topic_1", 0, on_message)
        await asyncio.sleep(10)
        await client.close()
