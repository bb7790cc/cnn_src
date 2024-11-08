import asyncio
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from pybit.unified_trading import HTTP


class CoinBYBIT:
    def __init__(self, api_key, api_secret, symbols, amount, slippage):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbols = symbols
        self.amount = amount
        self.slippage = slippage
        self.session = HTTP(api_key=self.api_key, api_secret=self.api_secret)
        self.executor = ThreadPoolExecutor()

    async def get_price(self):
        time_ago = int((datetime.now() - timedelta(minutes=15)).timestamp() * 1000)
        price_info = {}

        def fetch_price_sync(coin):
            try:

                ticker_info = self.session.get_kline(
                    category="spot",
                    symbol=f"{coin}USDT",
                    interval=1,
                    end=time_ago,
                    limit=1,
                )
                if ticker_info and ticker_info['result']['list'][0][4]:
                    logging.info(f"BYBIT ticker_info_data: {ticker_info}")
                    close_price_str = ticker_info['result']['list'][0][4]
                    close_price_decimal = Decimal(close_price_str)
                    formatted_close_price = format(close_price_decimal, 'f')
                    price_info[coin] = formatted_close_price.rstrip('0').rstrip('.')
                else:
                    logging.info(f"BYBIT 解析 {coin} 的价格失败")
            except Exception as e:
                logging.info(f"BYBIT 获取 {coin} 的价格失败，错误: {e}")

        tasks = [
            asyncio.get_event_loop().run_in_executor(self.executor, fetch_price_sync, coin)
            for coin in self.symbols
        ]

        # 等待所有任务完成
        await asyncio.gather(*tasks)

        logging.info(f"BYBIT 价格数据: {price_info}")
        return price_info

    async def create_order(self, symbol, num, price):
        """将同步的下单请求移交给线程池执行以实现异步下单"""
        # 创建线程池执行器
        loop = asyncio.get_event_loop()

        def place_order_sync():
            """同步下单函数"""
            try:
                return self.session.place_order(
                    category="spot",
                    symbol=f"{symbol}USDT",
                    side="Buy",
                    orderType="Limit",
                    qty=str(num),
                    price=str(price),
                    isLeverage=0,
                    orderFilter="Order",
                )
            except Exception as e:
                logging.info(f"BYBIT 订单创建失败: {symbol}, 错误: {e}")
                return None

        order_response = await loop.run_in_executor(self.executor, place_order_sync)
        # 处理订单响应结果
        msg = ""
        if order_response:
            if order_response['retCode'] == 0:
                msg += f"BYBIT 订单创建成功: {symbol}, 数量: {num}, 价格: {price}"
                logging.info(f"BYBIT 订单响应: {order_response}")
            else:
                msg += f"BYBIT 订单创建失败: {symbol}, 数量: {num}, 价格: {price}"
                logging.info(f"BYBIT 订单响应: {order_response}")
        else:
            msg += f"BYBIT 订单创建失败: {symbol}, 数量: {num}, 价格: {price}"
            logging.info(f"BYBIT 订单响应: {order_response}")
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
                logging.info("BYBIT error: un_coin")
                return "BYBIT 订单创建失败"
        except Exception as e:
            logging.info(f"BYBIT 现货下单失败，错误: {e}")
            return "BYBIT 订单创建: 现货主逻辑下单错误"


