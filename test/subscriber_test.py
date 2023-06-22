import asyncio
from maxwell.client.logger import get_instance
from maxwell.client.client import Client

logger = get_instance(__name__)
client = None


async def repeat_recv():
    loop = asyncio.get_event_loop()
    client = Client(["localhost:8081", "localhost:8082"], loop=loop)

    def on_message(topic):
        logger.debug("Received topic: %s", topic)
        while True:
            msgs = client.recv("topic_0", 8)
            values = [int.from_bytes(msg.value, byteorder="little") for msg in msgs]
            logger.debug("************Recevied msgs: %s", values)
            if len(msgs) < 8:
                break

    client.subscribe("topic_0", 0, on_message)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(repeat_recv())
    loop.run_forever()
