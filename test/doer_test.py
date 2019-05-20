import asyncio
import traceback
import pycommons.logger
from maxwell.client import Client

logger = pycommons.logger.get_instance(__name__)
client = None

async def do():
    loop = asyncio.get_event_loop()
    client = Client(["localhost:8081"], loop=loop)
    doer = client.get_doer()

    while True:
        logger.debug("doing.... ")
        try:
            result = await doer.do({
                "type": 'get_candles',
                "value": {
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
                    "end_ts": 1545295020
                }
            })
            logger.debug("result: %s", result)
        except Exception:
            logger.error("Failed to check: %s", traceback.format_exc())
        await asyncio.sleep(5)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(do())
    loop.run_forever()