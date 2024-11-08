import gate_api

import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor


class CoinGATE:
    def __init__(self, api_key, api_secret, symbols, amount, slippage):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = symbols
        self.amount = amount
        self.slippage = slippage

        self.configuration = gate_api.Configuration(
            host="https://api.gateio.ws/api/v4",
            key=self.api_key,
            secret=self.api_secret
        )
        self.api_client = gate_api.ApiClient(self.configuration)
        self.api_instance = gate_api.SpotApi(self.api_client)
        self.executor = ThreadPoolExecutor()

    async def get_price(self):
        """异步获取最新市场价格，优化同步API调用"""
        time_ago = int((datetime.now() - timedelta(minutes=15)).timestamp())
        price_info = {}

        def fetch_price_sync(coin):
            try:
                ticker_info = self.api_instance.list_candlesticks(f"{coin}_USDT", limit=1, to=time_ago, interval='1m')
                if ticker_info and ticker_info[0][2]:
                    logging.info(f"GATE ticker_info_data: {ticker_info}")
                    close_price_str = ticker_info[0][2]
                    close_price_decimal = Decimal(close_price_str)
                    formatted_close_price = format(close_price_decimal, 'f')
                    price_info[coin] = formatted_close_price.rstrip('0').rstrip('.')
                else:
                    logging.info(f"GATE 解析 {coin} 的价格失败")
            except Exception as e:
                logging.info(f"GATE 获取 {coin} 的价格失败，错误: {e}")

        # 将同步任务交给线程池并发运行

        tasks = [
            asyncio.get_event_loop().run_in_executor(self.executor, fetch_price_sync, coin)
            for coin in self.symbols
        ]

        # 等待所有任务完成
        await asyncio.gather(*tasks)

        logging.info(f"GATE 价格数据: {price_info}")

        return price_info

    async def create_order(self, symbol, num, price):
        """将同步的下单请求移交给线程池执行以实现异步下单"""
        # 创建线程池执行器
        loop = asyncio.get_event_loop()

        def place_order_sync():
            """同步下单函数"""
            try:
                # 使用 GATE API 下单
                order = gate_api.Order(amount=str(num),
                                       price=price,
                                       side='buy',
                                       currency_pair=f"{symbol}_USDT", )
                return self.api_instance.create_order(order)
            except Exception as e:
                logging.info(f"GATE 订单创建失败: {symbol}, 错误: {e}")
                return None

        # 异步执行同步函数
        order_response = await loop.run_in_executor(self.executor, place_order_sync)
        # 处理订单响应结果
        msg = ""
        if order_response:
            # 使用属性访问，而不是 get 方法
            status = order_response.status if hasattr(order_response, 'status') else 'unknown'

            if status:  # 如果订单状态为 'open'，订单已创建但未成交
                msg += f"GATE 订单创建成功: {order_response.currency_pair}, 数量: {order_response.amount}, 价格: {order_response.price}, 状态: {status}"
                logging.info(f"GATE 订单响应: {order_response}")
        else:
            # 如果 order_response 为空
            msg += f"GATE 订单创建失败: {symbol}, 数量: {num}, 价格: {price}，响应为空"
            logging.info(f"GATE 订单响应为空")
        return msg

    async def spot(self):
        """并行处理现货下单"""
        try:
            # 获取市场价格
            price_info = await self.get_price()
            # 创建任务列表
            tasks = []
            if price_info:
                for coin, price in price_info.items():
                    # 计算滑点后的购买数量
                    num = int(self.amount / (float(price) * self.slippage))

                    # 计算价格的小数点位数
                    if '.' in str(price):
                        num_decimal_places = len(str(price).split('.')[1])
                    else:
                        num_decimal_places = 0  # 如果没有小数部分，设置为0

                    # 计算滑点后的购买价格
                    slippage_price_decimal = Decimal(price) * Decimal(self.slippage)
                    slippage_price_quantized = slippage_price_decimal.quantize(Decimal(f'1.{"0" * num_decimal_places}'))
                    slippage_price = format(slippage_price_quantized, f'.{num_decimal_places}f')

                    # 为每个代币创建一个异步任务，并将其添加到任务列表中
                    tasks.append(self.create_order(coin, num, slippage_price))

                # 并发执行所有下单任务
                data = await asyncio.gather(*tasks)
                return data
            else:
                logging.info("GATE error: un_coin")
        except Exception as e:
            logging.info(f"GATE 现货下单失败，错误: {e}")
            return "GATE 订单创建: 现货主逻辑下单错误"

