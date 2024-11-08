import re
import aiohttp
import logging
import requests
from datetime import datetime


async def ding_auto_api(msg):
    ding_url = "https://oapi.dingtalk.com/robot/send?access_token="
    body = {"msgtype": "text", "text": {"content": f"{msg}"}}
    async with aiohttp.ClientSession() as session:
        async with session.post(ding_url, json=body) as response:
            await response.json()
            logging.info(f"Diting数据已发送")


def ding(msg):
    url = "https://oapi.dingtalk.com/robot/send?access_token="
    body = {"msgtype": "text", "text": {"content": f"{msg}"}}
    requests.get(url, json=body)
    logging.info(f"Diting数据已发送")

