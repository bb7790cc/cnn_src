import asyncio
import logging
from decimal import Decimal
from binance.spot import Spot
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
from logging import handlers


class CoinBinance:
    def __init__(self, key, secret, symbols, amount, slippage):
        self.key = key
        self.secret = secret
        self.symbols = symbols
        self.amount = amount
        self.slippage = slippage
        self.client = Spot(api_key=self.key, api_secret=self.secret)
        self.executor = ThreadPoolExecutor()

    async def price(self):
        """异步获取最新市场价格，优化同步API调用"""
        time_ago = int((datetime.now() - timedelta(minutes=15)).timestamp() * 1000)
        price_info = {}

        # 将同步任务交给线程池并发运行
        def fetch_price_sync(coin):
            try:
                # 使用 self.client 获取价格
                ticker_info = self.client.klines(symbol=coin + "USDT", interval='1m', startTime=time_ago, limit=1)
                if ticker_info and ticker_info[0][4]:
                    logging.info(f"BINANCE ticker_info_data: {ticker_info}")
                    close_price_str = ticker_info[0][4]
                    close_price_decimal = Decimal(close_price_str)
                    formatted_close_price = format(close_price_decimal, 'f')
                    price_info[coin] = formatted_close_price.rstrip('0').rstrip('.')
                else:
                    logging.info(f"BINANCE 解析 {coin} 的价格失败")
            except Exception as e:
                logging.info(f"BINANCE 获取 {coin} 的价格失败，错误: {e}")

        # 将同步任务交给线程池并发运行
        tasks = [
            asyncio.get_event_loop().run_in_executor(self.executor, fetch_price_sync, coin)
            for coin in self.symbols
        ]
        # 等待所有任务完成
        await asyncio.gather(*tasks)

        logging.info(f"BINANCE 价格数据: {price_info}")
        return price_info

    async def create_order(self, symbol, num, price):
        """执行单个下单请求的异步函数"""
        # 创建线程池执行器
        loop = asyncio.get_event_loop()

        def place_order_sync():
            """同步下单函数"""
            try:
                # 使用 BINANCE API 下单
                params = {
                    'symbol': symbol + "USDT",
                    'side': 'BUY',
                    'type': 'LIMIT',
                    'timeInForce': 'GTC',
                    'quantity': num,
                    'price': price,
                }
                return self.client.new_order(**params)
            except Exception as e:
                logging.info(f"BINANCE 订单创建失败: {symbol}, 错误: {e}")
                return None

        # 异步执行同步函数
        order_response = await loop.run_in_executor(self.executor, place_order_sync)
        # 处理订单响应结果
        msg = ""
        if order_response:
            status = order_response.get("status", "UNKNOWN")
            if status != "UNKNOWN":
                msg += f"BINANCE 订单创建成功: {symbol}, 数量: {num}, 价格: {price}, 状态: {status}"
                logging.info(f"BINANCE 订单响应: {order_response}")
        else:
            msg += f"BINANCE 订单创建失败: {symbol}, 数量: {num}, 价格: {price}"
            logging.info(f"BINANCE 订单响应为空: {order_response}")
        return msg

    async def spot(self):
        try:
            prices = await self.price()
            # 创建任务列表
            tasks = []
            if prices:
                for symbol, price in prices.items():
                    # 计算滑点后的购买数量
                    num = int(self.amount / (float(prices[symbol]) * self.slippage))
                    # 计算价格的小数点位数
                    if '.' in str(price):
                        num_decimal_places = len(str(price).split('.')[1])
                    else:
                        num_decimal_places = 0  # 如果没有小数部分，设置为0
                    # 计算滑点后的购买价格
                    slippage_price_decimal = Decimal(prices[symbol]) * Decimal(self.slippage)
                    slippage_price_quantized = slippage_price_decimal.quantize(Decimal(f'1.{"0" * num_decimal_places}'))
                    slippage_price = format(slippage_price_quantized, f'.{num_decimal_places}f')

                    # 为每个代币创建一个异步任务，并将其添加到任务列表中
                    tasks.append(self.create_order(symbol, num, slippage_price))

                # 并行运行所有订单任务
                data = await asyncio.gather(*tasks)
                return data
            else:
                logging.info("BINANCE error: un_coin")

        except Exception as e:
            logging.info(f"BINANCE 下单失败: {e}")
            return "BINANCE 订单创建: 现货主逻辑下单错误"
