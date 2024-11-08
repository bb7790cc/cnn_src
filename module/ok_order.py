import asyncio
import logging
import okx.Trade as Trade
import okx.MarketData as MarketData
from datetime import datetime, timedelta
from decimal import Decimal
from concurrent.futures import ThreadPoolExecutor
from logging import handlers


class CoinOKX:
    def __init__(self, api_key, api_secret, passphrase, symbols, amount, slippage):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.symbols = symbols
        self.amount = amount
        self.slippage = slippage
        self.executor = ThreadPoolExecutor(max_workers=50)

        # 初始化 OKX 现货交易 API
        self.tradeApi = Trade.TradeAPI(self.api_key, self.api_secret, self.passphrase, use_server_time=False, flag='0')
        self.MarketApi = MarketData.MarketAPI(self.api_key, self.api_secret, self.passphrase, use_server_time=False,
                                              flag='0')

    async def get_price(self):
        """异步获取最新市场价格，优化同步API调用"""
        time_ago = int((datetime.now() - timedelta(minutes=15)).timestamp() * 1000)
        price_info = {}

        # 包装同步的 MarketApi.get_candlesticks 方法
        def fetch_price_sync(coin):
            try:
                ticker_info = self.MarketApi.get_candlesticks(
                    instId=f"{coin}-USDT", bar="1m", after=time_ago, limit=1
                )
                if ticker_info and ticker_info['data'][0][4]:
                    logging.info(f"OK ticker_info_data: {ticker_info}")
                    close_price_str = ticker_info['data'][0][4]
                    close_price_decimal = Decimal(close_price_str)
                    formatted_close_price = format(close_price_decimal, 'f')
                    price_info[coin] = formatted_close_price.rstrip('0').rstrip('.')
                else:
                    logging.info(f"OK 解析 {coin} 的价格失败")
            except Exception as e:
                logging.info(f"OK 获取 {coin} 的价格失败，错误: {e}")

        # 将同步任务交给线程池并发运行
        tasks = [
            asyncio.get_event_loop().run_in_executor(self.executor, fetch_price_sync, coin)
            for coin in self.symbols
        ]

        # 等待所有任务完成
        await asyncio.gather(*tasks)

        logging.info(f"OK 价格数据: {price_info}")
        return price_info

    async def create_order(self, symbol, num, price):
        """将同步的下单请求移交给线程池执行以实现异步下单"""

        def place_order_sync():
            """同步下单函数"""
            try:
                # 使用 OKX TradeAPI 下单
                return self.tradeApi.place_order(
                    instId=f"{symbol}-USDT",
                    tdMode='cash',  # 现金账户，即现货交易
                    side='buy',
                    ordType='limit',
                    sz=str(num),
                    px=str(price)
                )
            except Exception as e:
                if "Resource temporarily unavailable" in str(e):
                    logging.info(f"OK {symbol}: {e}")
                    return "Resource temporarily unavailable"
                else:
                    logging.info(f"OK 订单创建失败 {symbol}: {e}")
                    return None
        # 异步执行同步函数
        order_response = await asyncio.get_event_loop().run_in_executor(self.executor, place_order_sync)
        # 处理订单响应结果
        msg = ""
        if order_response == "Resource temporarily unavailable":
            msg += f"OK 订单创建资源分配不均 自行查看是否下单成功"
        elif order_response:
            # 使用 get 获取响应中的 code，防止 KeyError
            code = order_response.get('code', None)

            if code != '0':  # 如果 code 不等于 '0'，则订单创建失败
                msg += f"OK 订单创建失败: {symbol}, 数量: {num}, 价格: {price}, 错误代码: {code}"
                logging.info(f"OK 订单响应: {order_response}")
            else:
                # 使用 get 获取 data 中的 sCode，确保不会因缺少字段而抛出异常
                s_code = order_response.get('data', [{}])[0].get('sCode', None)
                if s_code == '0':
                    msg += f"OK 订单创建成功: {symbol}, 数量: {num}, 价格: {price}"
                    logging.info(f"OK 订单响应: {order_response}")
                else:
                    error_msg = order_response.get('data', [{}])[0].get('sMsg', '未知错误')
                    msg += f"OK 订单创建失败: {symbol}, 数量: {num}, 价格: {price}, 错误信息: {error_msg}"
                    logging.info(f"OK 订单响应: {order_response}")
        else:
            msg += f"OK 订单创建失败: {symbol}, 代币数量(没有换算成张): {num}, 价格: {price}，响应为空"

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
                logging.info("OK error: un_coin")
                return "OK 订单创建: un_coin"
        except Exception as e:
            logging.info(f"OK 现货下单失败，错误: {e}")
            return "OK 订单创建: 现货主逻辑下单错误"
