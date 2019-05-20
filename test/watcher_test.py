import asyncio
import pycommons.logger
from maxwell.client import Client

logger = pycommons.logger.get_instance(__name__)

async def repeat_recv():
    loop = asyncio.get_event_loop()
    client = Client(["localhost:8081"], loop=loop)
    watcher = client.get_watcher()
    def receive(action):
        logger.debug("Received action: %s", action)
        action.end("nothing")
    watcher.watch("get_candles", receive)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(repeat_recv())
    loop.run_forever()