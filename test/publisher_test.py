import asyncio
import time
import traceback
import pycommons.logger
from maxwell.client import Client

logger = pycommons.logger.get_instance(__name__)


async def repeat_publish():
    client = Client(["localhost:8081", "localhost:8082"], loop=loop)
    publisher = client.get_publisher()
    while True:
        value = int(time.time())
        logger.debug("************Publish msg: %s", value)

        try:
            await publisher.publish(
                "topic_3",
                value.to_bytes(8, byteorder='little')
            )
        except Exception:
            logger.error("Failed to encode: %s", traceback.format_exc())

        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(repeat_publish())
    loop.run_forever()
