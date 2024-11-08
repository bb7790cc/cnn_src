import asyncio
import aiohttp
import requests


def get_coingeckos():
    get_coinlist_url = "https://api.coingecko.com/api/v3/coins/markets"
    headers = {
        "accept": "application/json",
        "x-cg-demo-api-key": "" # api
    }
    map_list = {}
    a = []
    for i in range(5):

        params = {
            "vs_currency": "usd",  # 市场数据的货币单位，usd为美元
            "order": "market_cap_desc",  # 按市值降序排列
            "per_page": 200,  # 每页显示10个币种
            "page": i + 1  # 获取第1页的数据
        }
        data = requests.get(get_coinlist_url, headers=headers, params=params)

        for item in data.json():
            if item['symbol'].upper() in map_list.keys():
                continue
            map_list[item['symbol'].upper()] = [item['market_cap'], item['last_updated'], item['market_cap_rank']]
    return map_list

