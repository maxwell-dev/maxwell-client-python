import asyncio
from maxwell.client.logger import get_instance
from maxwell.client.client import Client

logger = get_instance(__name__)

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    client = Client(["localhost:8081", "localhost:8082"], loop=loop)
    master = client.get_master()
    endpoint = loop.run_until_complete(asyncio.ensure_future(master.resolve_frontend()))
    logger.info("endpoint: %s", endpoint)

    loop.run_forever()
