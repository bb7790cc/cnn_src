import requests
import logging


def binance_get_all_tickers(coin):
    i = 1  # 将 i 放在循环外部
    while True:
        url = "https://api.coingecko.com/api/v3/exchanges/binance/tickers?include_exchange_logo=false&page={}&depth=false&order=volume_desc".format(
            i)

        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": "" # api
        }

        response = requests.get(url, headers=headers)
        data = response.json()

        # 检查是否有 tickers，若没有则结束循环
        if data['tickers']:
            for item in data['tickers']:
                # 添加 base 和 target 到集合中去重
                coin.add(item['base'])

            i += 1  # 正确增加页码
        else:
            break  # 没有更多 tickers，退出循环

    return coin  # 返回 coin 列表


def upbit_get_all_tickers(coin):
    i = 1  # 将 i 放在循环外部
    while True:
        url = "https://api.coingecko.com/api/v3/exchanges/upbit/tickers?include_exchange_logo=false&page={}&depth=false&order=volume_desc".format(
            i)

        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": "" # api
        }

        response = requests.get(url, headers=headers)
        data = response.json()

        # 检查是否有 tickers，若没有则结束循环
        if data['tickers']:
            for item in data['tickers']:
                # 添加 base 和 target 到集合中去重
                coin.add(item['base'])

            i += 1  # 正确增加页码
        else:
            break  # 没有更多 tickers，退出循环

    return coin  # 返回 coin 列表

