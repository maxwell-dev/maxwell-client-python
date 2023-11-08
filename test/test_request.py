import pytest
from maxwell.utils.connection import Error
from maxwell.utils.logger import get_logger
from maxwell.client.client import Client

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


class TestRequest:
    @pytest.mark.asyncio
    async def test_normal(self):
        client = Client(["localhost:8081"], build_options())
        result = await client.request(
            "/hello",
            {},
        )
        assert result == "world"
        await client.close()

    @pytest.mark.asyncio
    async def test_nonexist_path(self):
        client = Client(["localhost:8081"], build_options())
        try:
            await client.request(
                "/hello2",
                {},
            )
        except Exception as e:
            assert e.__class__ == Error
        finally:
            await client.close()
