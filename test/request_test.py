import asyncio
import traceback
from maxwell.client.logger import get_instance
from maxwell.client.client import Client

logger = get_instance(__name__)
client = None


async def request():
    loop = asyncio.get_event_loop()
    client = Client(["localhost:8081"], loop=loop)

    while True:
        logger.debug("requesting.... ")
        try:
            result = await client.request(
                "/hello",
                {
                    "signatures": {
                        "default": "candle",
                    },
                    "topic": {
                        "exchange": "huobi",
                        "quote_currency": "usdt",
                        "base_currency": "btc",
                        "timeframe": "1m",
                    },
                    "start_ts": 1545293880,
                    "end_ts": 1545295020,
                },
            )
            logger.debug("result: %s", result)
        except Exception:
            logger.error("Failed to check: %s", traceback.format_exc())
        await asyncio.sleep(5)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.create_task(request())
    loop.run_forever()
