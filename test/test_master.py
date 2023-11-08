from aiohttp import ClientConnectorError
import pytest
from maxwell.utils.logger import get_logger
from maxwell.client.master import Master

logger = get_logger(__name__)


def build_options(options=None):
    options = options if options else {}
    if options.get("wait_open_timeout") == None:
        options["wait_open_timeout"] = 3
    if options.get("round_timeout") == None:
        options["round_timeout"] = 5
    if options.get("wait_consuming_timeout") == None:
        options["wait_consuming_timeout"] = 10
    if options.get("pull_limit") == None:
        options["pull_limit"] = 128
    if options.get("queue_capacity") == None:
        options["queue_capacity"] = 512
    if options.get("endpoint_cache_ttl") == None:
        options["endpoint_cache_ttl"] = 10
    return options


class TestMaster:
    @pytest.mark.asyncio
    async def test_normal(self):
        master = Master(["localhost:8081"], build_options())
        frontend = await master.pick_frontend()
        assert frontend == "127.0.0.1:10000"
        frontend = await master.pick_frontend(force=False)
        assert frontend == "127.0.0.1:10000"
        frontend = await master.pick_frontend(force=True)
        assert frontend == "127.0.0.1:10000"

    @pytest.mark.asyncio
    async def test_wrong_endpoint(self):
        master = Master(["localhost:8082"], build_options())
        try:
            frontend = await master.pick_frontend()
            assert frontend == "127.0.0.1:10000"
        except Exception as e:
            assert e.__class__ == ClientConnectorError
