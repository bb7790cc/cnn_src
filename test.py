import argparse
from module.ok_order import CoinOKX
from module.get_diting import ding_auto_api
from module.binance_order import CoinBinance
from module.gate_order import CoinGATE
from module.bybit_order import CoinBYBIT
from module.bybit_contract_order import CoinBYBITCONTRACT
from module.ok_contract_order import CoinOKXCONTRACT
from module.get_coingos import binance_get_all_tickers, upbit_get_all_tickers
import asyncio
from module.other import token_symbol_upper, token_symbol_contract_upper
from module.get_coingecko import get_coingeckos
from telethon import TelegramClient
import logging
from logging import handlers
from datetime import datetime, timedelta
import dotenv
import os
import yaml

'''客户端配置'''
clients_config = [
    {'api_id': 1, 'api_hash': '', 'channel_username': 'das8dw15saa',
     'session_name': 'session_1'},
    {'api_id': 2, 'api_hash': '', 'channel_username': 'das8dw15saa',
         'session_name': 'session_2'},
]

# 1. 加载 YAML 配置文件
with open("config.yaml", "r", encoding="utf-8") as file:
    config_data = yaml.safe_load(file)

# 2. 定义命令行参数
parser = argparse.ArgumentParser(description="脚本配置参数")

# 将 YAML 配置文件中的每个键作为命令行参数
for key, value in config_data.items():
    parser.add_argument(f'--{key}', type=type(value), default=value, help=f'配置: {key}')

# 3. 解析命令行参数
args = parser.parse_args()

# 4. 使用字典保存参数
config_dict = vars(args)

# loading the environment variables
dotenv.load_dotenv()
# 从环境变量获取敏感信息
# 币安api
binancekey = os.getenv("BINANCEAPI_KEY")
binancesecretKey = os.getenv("BINANCEAPI_SECRET_KEY")
# okapi
okkey = os.getenv("OKAPI_KEY")
oksecretKey = os.getenv("OKAPI_SECRET_KEY")
okpass = os.getenv("OKAPI_PASS")
# bybitapi
bybitkey = os.getenv("BYBITAPI_KEY")
bybitsecretKey = os.getenv("BYBITAPI_SECRET_KEY")
# GATEapi
gatekey = os.getenv("GATEAPI_KEY")
gatesecretKey = os.getenv("GATEAPI_SECRET_KEY")

# 识别逻辑关键词
binance_spot_keyword = "with Seed Tag Applied"
binance_contract_keyword = "Futures Will Launch USDⓈ-Margined"
upbit_keyword = "신규 거래지원 안내"

# 清除的关键字
coin_u = ["USDT", "ETH", "BTC", "USDC", "SOL", "BNB", "TRON", "WBTC", "BTC, USDT 마켓", "KRW, BTC, USDT 마켓", "BINANCE",
          "UPBIT"]

# 不买入关键字
keyword = ["Coinone", "NFT", "연기"]

# 记录符号上次购买的时间
last_purchase_time = {}

# 设置72小时的时间间隔
purchase_interval = timedelta(hours=72)

# 币安交易对库 99%-100% 可能会缺失
binance_coin = set()
binance_get_all_tickers(binance_coin)

# upbit交易对库 99%-100% 可能会缺失
upbit_coin = set()
upbit_get_all_tickers(upbit_coin)

# 初始化coingecko 市值数据
coingecko_map = get_coingeckos()
# 初始化上次更新的时间
last_update_time = datetime.now()
UPDATE_INTERVAL = timedelta(hours=72)  # 设置72小时的时间间隔

# 已处理的消息ID集合
processed_message_ids = set()


# 定义检查并更新 coingecko_map 的函数
def check_and_update_coingecko_map():
    global last_update_time, coingecko_map
    now = datetime.now()

    # 判断是否已超过72小时
    if now - last_update_time >= UPDATE_INTERVAL:
        # 如果超过了72小时，更新 coingecko_map 并重置上次更新的时间
        coingecko_map = get_coingeckos()
        last_update_time = now
        logging.info("coingecko_map 已根据72小时间隔进行更新。")
    else:
        logging.info("coingecko_map 数据暂未超过72小时 等待")


def log_symbols_and_filter(text, coin_list, key_word, group=""):
    logging.info(f"符合 {group} 判断条件: {key_word}")
    if key_word == binance_contract_keyword:
        symbols = token_symbol_contract_upper(text)
    else:
        symbols = token_symbol_upper(text)
    logging.info(f"获取到的全部符号: {symbols}")

    supper = []
    for item in symbols:
        if item not in coin_list:
            now = datetime.now()
            last_purchase = last_purchase_time.get(item)

            if last_purchase is None or now - last_purchase >= purchase_interval:
                # 如果符号未在24小时内购买，则添加到待处理列表
                supper.append(item)
                # 更新最后购买时间
                last_purchase_time[item] = now
            else:
                logging.info(f"{item} 已在过去72小时内购买，跳过")

    return supper


async def execute_orders_uncoin_binance(supper, platform,
                                        number_okx, number_bybit, number_gate,
                                        number_okx_contract, number_bybit_contract,
                                        slip_okx, slip_bybit, slip_gate,
                                        slip_okx_contract, slip_bybit_contract,
                                        ):
    if supper:
        start_okx = CoinOKX(okkey, oksecretKey, okpass, supper, number_okx, slip_okx)
        start_okx_contract = CoinOKXCONTRACT(okkey, oksecretKey, okpass, supper, number_okx_contract, slip_okx_contract)
        start_bybit = CoinBYBIT(bybitkey, bybitsecretKey, supper, number_bybit, slip_bybit)
        start_gate = CoinGATE(gatekey, gatesecretKey, supper, number_gate, slip_gate)
        start_bybit_contract = CoinBYBITCONTRACT(bybitkey, bybitsecretKey, supper, number_bybit_contract,
                                                 slip_bybit_contract)
        # 并行执行下单操作
        data_ = await asyncio.gather(start_okx.spot(),
                                     start_okx_contract.spot(),
                                     start_bybit.spot(),
                                     start_gate.spot(),
                                     start_bybit_contract.spot()
                                     )

        logging.info(f"使用代币{supper} {platform} ,"
                     f"OK 数量:{number_okx},滑点:{slip_okx},"
                     f"BYBIT 数量:{number_bybit},滑点:{slip_bybit},"
                     f"GATE 数量:{number_gate},滑点:{slip_gate},"
                     f"OKX 永续合约 数量:{number_okx_contract},滑点:{slip_okx_contract},"
                     f"BYBIT 永续合约 数量:{number_bybit_contract},滑点:{slip_bybit_contract},"
                     )

        return data_

    else:
        logging.info(f"{platform} 无符合条件的代币")


async def execute_orders_uncoin_binance_ok(supper, platform,
                                           number_okx_1, number_okx_contract_1,
                                           slip_okx_1, slip_okx_contract_1):
    if supper:

        start_okx_1 = CoinOKX(okkey, oksecretKey, okpass, supper, number_okx_1, slip_okx_1)

        start_okx_contract_1 = CoinOKXCONTRACT(okkey, oksecretKey, okpass, supper, number_okx_contract_1,
                                               slip_okx_contract_1)

        # 并行执行下单操作
        data_ = await asyncio.gather(
            start_okx_1.spot(),
            start_okx_contract_1.spot()
        )

        logging.info(f"使用代币{supper} {platform} ,"
                     f"OK(备) 数量:{number_okx_1},滑点:{slip_okx_1},"
                     f"OK 永续合约(备) 数量:{number_okx_contract_1} 滑点:{slip_okx_contract_1}"
                     )


        return data_
    else:
        logging.info(f"{platform} 无符合条件的代币")


async def execute_orders_binance(supper, platform, number_binance, slip_binance):
    if supper:
        start_binance = CoinBinance(binancekey, binancesecretKey, supper, number_binance, slip_binance)
        # 并行执行下单操作
        data_ = await asyncio.gather(start_binance.spot())
        logging.info(
            f"使用代币{supper} {platform} ,"
            f"数量: Binance: {number_binance}, 滑点: Binance: {slip_binance}"
        )

        return data_
    else:
        logging.info(f"{platform} 无符合条件的代币")


def map_list(supper, map_high, map_medium, map_low):
    coin_high = []
    coin_medium = []
    coin_low = []
    coin_other = []
    for coin in supper:
        if coin in coingecko_map.keys():
            market_cap = coingecko_map[coin][0]

            if market_cap >= map_high:
                coin_high.append(coin)

            elif map_high > market_cap >= map_medium:
                coin_medium.append(coin)

            elif map_medium > market_cap >= map_low:
                coin_low.append(coin)

            else:
                coin_other.append(coin)

        else:
            coin_other.append(coin)

    return coin_high, coin_medium, coin_low, coin_other


async def execute_order_uncoin_binance_map(supper, platform,
                                           map_high, map_medium, map_low,
                                           number_okx_high, number_okx_medium, number_okx_low,
                                           slip_okx_high, slip_okx_medium, slip_okx_low,
                                           number_bybit_high, number_bybit_medium, number_bybit_low,
                                           slip_bybit_high, slip_bybit_medium, slip_bybit_low,
                                           number_gate_high, number_gate_medium, number_gate_low,
                                           slip_gate_high, slip_gate_medium, slip_gate_low,
                                           number_okx_contract_high, number_okx_contract_medium,
                                           number_okx_contract_low,
                                           slip_okx_contract_high, slip_okx_contract_medium, slip_okx_contract_low,
                                           number_bybit_contract_high, number_bybit_contract_medium,
                                           number_bybit_contract_low,
                                           slip_bybit_contract_high, slip_bybit_contract_medium,
                                           slip_bybit_contract_low,
                                           number_okx_1_high, number_okx_1_medium, number_okx_1_low,
                                           slip_okx_1_high, slip_okx_1_medium, slip_okx_1_low,
                                           number_okx_contract_1_high, number_okx_contract_1_medium,
                                           number_okx_contract_1_low,
                                           slip_okx_contract_1_high, slip_okx_contract_1_medium,
                                           slip_okx_contract_1_low,
                                           ):
    tasks = []
    msg = f"{platform} 逻辑处理\n"
    coin_high, coin_medium, coin_low, coin_other = map_list(supper, map_high, map_medium, map_low)

    if coin_high:
        task = execute_orders_uncoin_binance(coin_high, platform,
                                             number_okx_high, number_bybit_high, number_gate_high,
                                             number_okx_contract_high, number_bybit_contract_high,
                                             slip_okx_high, slip_bybit_high, slip_gate_high,
                                             slip_okx_contract_high, slip_bybit_contract_high)
        task_ok = execute_orders_uncoin_binance_ok(coin_high, platform, number_okx_1_high,
                                                   number_okx_contract_1_high, slip_okx_1_high,
                                                   slip_okx_contract_1_high)
        tasks.append(task)
        tasks.append(task_ok)
        msg += f"\n高市值代币:{', '.join([str(coin) for coin in coin_high])} 市值数据:{[coingecko_map[coin][0] for coin in coin_high if coin in coingecko_map]}"

    if coin_medium:
        task = execute_orders_uncoin_binance(coin_medium, platform,
                                             number_okx_medium, number_bybit_medium, number_gate_medium,
                                             number_okx_contract_medium, number_bybit_contract_medium,
                                             slip_okx_medium, slip_bybit_medium, slip_gate_medium,
                                             slip_okx_contract_medium, slip_bybit_contract_medium,
                                             )
        task_ok = execute_orders_uncoin_binance_ok(coin_medium, platform, number_okx_1_medium,
                                                   number_okx_contract_1_medium, slip_okx_1_medium,
                                                   slip_okx_contract_1_medium)
        tasks.append(task)
        tasks.append(task_ok)
        msg += f"\n中市值代币:{', '.join([str(coin) for coin in coin_medium])} 市值数据:{[coingecko_map[coin][0] for coin in coin_medium if coin in coingecko_map]}"

    if coin_low:
        task = execute_orders_uncoin_binance(coin_low, platform,
                                             number_okx_low, number_bybit_low, number_gate_low,
                                             number_okx_contract_low, number_bybit_contract_low,
                                             slip_okx_low, slip_bybit_low, slip_gate_low,
                                             slip_okx_contract_low, slip_bybit_contract_low)
        task_ok = execute_orders_uncoin_binance_ok(coin_low, platform, number_okx_1_low,
                                                   number_okx_contract_1_low, slip_okx_1_low,
                                                   slip_okx_contract_1_low)
        tasks.append(task)
        tasks.append(task_ok)
        msg += f"\n低市值代币:{', '.join([str(coin) for coin in coin_low])} 市值数据:{[coingecko_map[coin][0] for coin in coin_low if coin in coingecko_map]}"

    if coin_other:
        task = execute_orders_uncoin_binance(coin_other, platform,
                                             number_okx_medium, number_bybit_medium, number_gate_high,
                                             number_okx_contract_medium, number_bybit_contract_medium,
                                             slip_okx_medium, slip_bybit_medium, slip_gate_high,
                                             slip_okx_contract_medium, slip_bybit_contract_medium,
                                             )
        task_ok = execute_orders_uncoin_binance_ok(coin_other, platform, number_okx_1_medium,
                                                   number_okx_contract_1_medium, slip_okx_1_medium,
                                                   slip_okx_contract_1_medium)
        tasks.append(task)
        tasks.append(task_ok)
        msg += f"\n代币:{coin_other} 未检测到市值 (使用中市值参数 gate高市值参数)\n"

    if tasks:
        data_info = await asyncio.gather(*tasks)
        body = (f"{platform} 逻辑处理\n\n"
               f"处理代币:{supper}\n\n"
               f"{platform} 逻辑处理成交响应\n")
        for data_ in data_info:
            for item in data_:
                if item is None:
                    continue  # 跳过 None 值
                elif isinstance(item, (list, tuple)):  # 检查 item 是否为可迭代对象
                    body += f"🟢🟢🟢\n" + "\n".join(item) + "\n"
                else:
                    body += f"🟢🟢🟢\n{item}\n"
        await ding_auto_api(body + f"\n⏱️{datetime.now()}\n")
    else:
        logging.info(f"execute_order_uncoin_binance_map 函数 线程池中无任务")
    logging.info(f"{platform}使用的市值配置参数: 高市值:{map_high},中市值:{map_medium},低市值:{map_low}")

    await ding_auto_api(msg + f"\n⏱️{datetime.now()}\n")


async def execute_order_binance_map(supper, platform,
                                    map_high, map_medium, map_low,
                                    number_binance_high, number_binance_medium, number_binance_low,
                                    slip_binance_high, slip_binance_medium, slip_binance_low):
    tasks = []
    msg = f"{platform} 逻辑处理\n"
    coin_high, coin_medium, coin_low, coin_other = map_list(supper, map_high, map_medium, map_low)

    if coin_high:
        task = execute_orders_binance(coin_high, platform, number_binance_high, slip_binance_high)
        tasks.append(task)
        msg += f"\n高市值代币:{', '.join([str(coin) for coin in coin_high])} 市值数据:{[coingecko_map[coin][0] for coin in coin_high if coin in coingecko_map]}"
    if coin_medium:
        task = execute_orders_binance(coin_medium, platform, number_binance_medium, slip_binance_medium)
        tasks.append(task)
        msg += f"\n中市值代币:{', '.join([str(coin) for coin in coin_medium])} 市值数据:{[coingecko_map[coin][0] for coin in coin_medium if coin in coingecko_map]}"
    if coin_low:
        task = execute_orders_binance(coin_low, platform, number_binance_low, slip_binance_low)
        tasks.append(task)
        msg += f"\n低市值代币:{', '.join([str(coin) for coin in coin_low])} 市值数据:{[coingecko_map[coin][0] for coin in coin_low if coin in coingecko_map]}"
    if coin_other:
        task = execute_orders_binance(coin_other, platform, number_binance_medium, slip_binance_medium)
        tasks.append(task)
        msg += f"\n代币:{coin_other} 未检测到市值 (使用中市值参数 gate高市值参数)\n"

    if tasks:
        data_info = await asyncio.gather(*tasks)
        body = (f"{platform} 逻辑处理\n\n"
               f"处理代币:{supper}\n\n"
               f"{platform} 逻辑处理成交响应\n")
        for data_ in data_info:
            for item in data_:
                if item is None:
                    continue  # 跳过 None 值
                elif isinstance(item, (list, tuple)):  # 检查 item 是否为可迭代对象
                    body += f"🟢🟢🟢\n" + "\n".join(item) + "\n"
                else:
                    body += f"🟢🟢🟢\n{item}\n"
        await ding_auto_api(body + f"\n⏱️{datetime.now()}\n")
    else:
        logging.info(f"execute_order_uncoin_binance_map 函数 线程池中无任务")
    logging.info(f"{platform}使用的市值配置参数: 高市值:{map_high},中市值:{map_medium},低市值:{map_low}")

    await ding_auto_api(msg + f"\n⏱️{datetime.now()}\n")


async def fetch_messages(client, config):
    # 获取频道名称
    channel_username = config['channel_username']
    while True:
        # 获取最新的1条消息
        messages = await client.get_messages(channel_username, limit=1)
        for message in messages:
            message_id = message.id
            message_date = message.date  # 获取消息的发布时间
            message_content = message.message  # 获取消息内容

            # 初始化消息ID列表
            if not processed_message_ids:
                processed_message_ids.add(message_id)
                continue

            # 如果消息ID已处理过，跳过
            if message_id in processed_message_ids:
                continue

            # 输出消息时间和内容到日志
            logging.info(f'原文: {message_content}\n发布时间: {message_date}')

            # 添加到消息列表
            processed_message_ids.add(message_id)

            # 判断不买入关键字是否存在
            state_purchase_interval = not any(woid in message_content for woid in keyword)

            if state_purchase_interval:
                if binance_spot_keyword in message_content:
                    _supper = log_symbols_and_filter(message_content, coin_u, binance_spot_keyword, "BINANCE 现货上新")
                    logging.info(f"BINANCE 现货上新 即将使用的全部符号: {_supper}")

                    # 去除币安交易所存在的coin
                    supper = [coin for coin in _supper if coin not in binance_coin]
                    binance_coin.update(supper)

                    if supper:
                        if len(supper) > 1:
                            await execute_order_uncoin_binance_map(supper, "BINANCE 现货上新(多币)",
                                                                   config_dict['map_high_a'],
                                                                   config_dict['map_medium_a'],
                                                                   config_dict['map_low_a'],
                                                                   config_dict['ok_number_b_high'],
                                                                   config_dict['ok_number_b_medium'],
                                                                   config_dict['ok_number_b_low'],
                                                                   config_dict['ok_slip_b_high'],
                                                                   config_dict['ok_slip_b_medium'],
                                                                   config_dict['ok_slip_b_low'],
                                                                   config_dict['bybit_number_b_high'],
                                                                   config_dict['bybit_number_b_medium'],
                                                                   config_dict['bybit_number_b_low'],
                                                                   config_dict['bybit_slip_b_high'],
                                                                   config_dict['bybit_slip_b_medium'],
                                                                   config_dict['bybit_slip_b_low'],
                                                                   config_dict['gate_number_b_high'],
                                                                   config_dict['gate_number_b_medium'],
                                                                   config_dict['gate_number_b_low'],
                                                                   config_dict['gate_slip_b_high'],
                                                                   config_dict['gate_slip_b_medium'],
                                                                   config_dict['gate_slip_b_low'],
                                                                   config_dict['okx_contract_number_b_high'],
                                                                   config_dict['okx_contract_number_b_medium'],
                                                                   config_dict['okx_contract_number_b_low'],
                                                                   config_dict['okx_contract_slip_b_high'],
                                                                   config_dict['okx_contract_slip_b_medium'],
                                                                   config_dict['okx_contract_slip_b_low'],
                                                                   config_dict['bybit_contract_number_b_high'],
                                                                   config_dict['bybit_contract_number_b_medium'],
                                                                   config_dict['bybit_contract_number_b_low'],
                                                                   config_dict['bybit_contract_slip_b_high'],
                                                                   config_dict['bybit_contract_slip_b_medium'],
                                                                   config_dict['bybit_contract_slip_b_low'],
                                                                   config_dict['ok_number_b_1_high'],
                                                                   config_dict['ok_number_b_1_medium'],
                                                                   config_dict['ok_number_b_1_low'],
                                                                   config_dict['ok_slip_b_1_high'],
                                                                   config_dict['ok_slip_b_1_medium'],
                                                                   config_dict['ok_slip_b_1_low'],
                                                                   config_dict['okx_contract_number_b_1_high'],
                                                                   config_dict['okx_contract_number_b_1_medium'],
                                                                   config_dict['okx_contract_number_b_1_low'],
                                                                   config_dict['okx_contract_slip_b_1_high'],
                                                                   config_dict['okx_contract_slip_b_1_medium'],
                                                                   config_dict['okx_contract_slip_b_1_low'],
                                                                   )
                        if len(supper) == 1:
                            await execute_order_uncoin_binance_map(supper, "BINANCE 现货上新(单币)",
                                                                   config_dict['map_high_a'],
                                                                   config_dict['map_medium_a'],
                                                                   config_dict['map_low_a'],
                                                                   config_dict['ok_number_a_high'],
                                                                   config_dict['ok_number_a_medium'],
                                                                   config_dict['ok_number_a_low'],
                                                                   config_dict['ok_slip_a_high'],
                                                                   config_dict['ok_slip_a_medium'],
                                                                   config_dict['ok_slip_a_low'],
                                                                   config_dict['bybit_number_a_high'],
                                                                   config_dict['bybit_number_a_medium'],
                                                                   config_dict['bybit_number_a_low'],
                                                                   config_dict['bybit_slip_a_high'],
                                                                   config_dict['bybit_slip_a_medium'],
                                                                   config_dict['bybit_slip_a_low'],
                                                                   config_dict['gate_number_a_high'],
                                                                   config_dict['gate_number_a_medium'],
                                                                   config_dict['gate_number_a_low'],
                                                                   config_dict['gate_slip_a_high'],
                                                                   config_dict['gate_slip_a_medium'],
                                                                   config_dict['gate_slip_a_low'],
                                                                   config_dict['okx_contract_number_a_high'],
                                                                   config_dict['okx_contract_number_a_medium'],
                                                                   config_dict['okx_contract_number_a_low'],
                                                                   config_dict['okx_contract_slip_a_high'],
                                                                   config_dict['okx_contract_slip_a_medium'],
                                                                   config_dict['okx_contract_slip_a_low'],
                                                                   config_dict['bybit_contract_number_a_high'],
                                                                   config_dict['bybit_contract_number_a_medium'],
                                                                   config_dict['bybit_contract_number_a_low'],
                                                                   config_dict['bybit_contract_slip_a_high'],
                                                                   config_dict['bybit_contract_slip_a_medium'],
                                                                   config_dict['bybit_contract_slip_a_low'],
                                                                   config_dict['ok_number_a_1_high'],
                                                                   config_dict['ok_number_a_1_medium'],
                                                                   config_dict['ok_number_a_1_low'],
                                                                   config_dict['ok_slip_a_1_high'],
                                                                   config_dict['ok_slip_a_1_medium'],
                                                                   config_dict['ok_slip_a_1_low'],
                                                                   config_dict['okx_contract_number_a_1_high'],
                                                                   config_dict['okx_contract_number_a_1_medium'],
                                                                   config_dict['okx_contract_number_a_1_low'],
                                                                   config_dict['okx_contract_slip_a_1_high'],
                                                                   config_dict['okx_contract_slip_a_1_medium'],
                                                                   config_dict['okx_contract_slip_a_1_low'],
                                                                   )
                    else:
                        logging.info("BINANCE 现货上新 没有代币需要处理")
                    msg = (f"BINANCE 现货上新 逻辑处理\n\n"
                           f"原文:{message_content}\n\n"
                           f"  ⏱️{datetime.now()}")
                    await ding_auto_api(msg)

                if binance_contract_keyword in message_content:
                    _supper = log_symbols_and_filter(message_content, coin_u, binance_contract_keyword,
                                                     "BINANCE 合约上新")
                    logging.info(f"BINANCE 合约上新 即将使用的全部符号: {_supper}")

                    # 去除币安交易所存在的coin
                    supper = [coin for coin in _supper if coin not in binance_coin]

                    # 保留币安交易所存在的coin
                    unsupper = [coin for coin in _supper if coin in binance_coin]

                    # 总处理代币数量
                    n = len(supper) + len(unsupper)

                    if supper:
                        if n > 1:
                            await execute_order_uncoin_binance_map(supper, "BINANCE 永续合约上新(币安不存在)(多币)",
                                                                   config_dict['map_high_c'],
                                                                   config_dict['map_medium_c'],
                                                                   config_dict['map_low_c'],
                                                                   config_dict['ok_number_d_high'],
                                                                   config_dict['ok_number_d_medium'],
                                                                   config_dict['ok_number_d_low'],
                                                                   config_dict['ok_slip_d_high'],
                                                                   config_dict['ok_slip_d_medium'],
                                                                   config_dict['ok_slip_d_low'],
                                                                   config_dict['bybit_number_d_high'],
                                                                   config_dict['bybit_number_d_medium'],
                                                                   config_dict['bybit_number_d_low'],
                                                                   config_dict['bybit_slip_d_high'],
                                                                   config_dict['bybit_slip_d_medium'],
                                                                   config_dict['bybit_slip_d_low'],
                                                                   config_dict['gate_number_d_high'],
                                                                   config_dict['gate_number_d_medium'],
                                                                   config_dict['gate_number_d_low'],
                                                                   config_dict['gate_slip_d_high'],
                                                                   config_dict['gate_slip_d_medium'],
                                                                   config_dict['gate_slip_d_low'],
                                                                   config_dict['okx_contract_number_d_high'],
                                                                   config_dict['okx_contract_number_d_medium'],
                                                                   config_dict['okx_contract_number_d_low'],
                                                                   config_dict['okx_contract_slip_d_high'],
                                                                   config_dict['okx_contract_slip_d_medium'],
                                                                   config_dict['okx_contract_slip_d_low'],
                                                                   config_dict['bybit_contract_number_d_high'],
                                                                   config_dict['bybit_contract_number_d_medium'],
                                                                   config_dict['bybit_contract_number_d_low'],
                                                                   config_dict['bybit_contract_slip_d_high'],
                                                                   config_dict['bybit_contract_slip_d_medium'],
                                                                   config_dict['bybit_contract_slip_d_low'],
                                                                   config_dict['ok_number_d_1_high'],
                                                                   config_dict['ok_number_d_1_medium'],
                                                                   config_dict['ok_number_d_1_low'],
                                                                   config_dict['ok_slip_d_1_high'],
                                                                   config_dict['ok_slip_d_1_medium'],
                                                                   config_dict['ok_slip_d_1_low'],
                                                                   config_dict['okx_contract_number_d_1_high'],
                                                                   config_dict['okx_contract_number_d_1_medium'],
                                                                   config_dict['okx_contract_number_d_1_low'],
                                                                   config_dict['okx_contract_slip_d_1_high'],
                                                                   config_dict['okx_contract_slip_d_1_medium'],
                                                                   config_dict['okx_contract_slip_d_1_low'],
                                                                   )
                        if n == 1:
                            await execute_order_uncoin_binance_map(supper, "BINANCE 永续合约上新(币安不存在)(单币)",
                                                                   config_dict['map_high_c'],
                                                                   config_dict['map_medium_c'],
                                                                   config_dict['map_low_c'],
                                                                   config_dict['ok_number_c_high'],
                                                                   config_dict['ok_number_c_medium'],
                                                                   config_dict['ok_number_c_low'],
                                                                   config_dict['ok_slip_c_high'],
                                                                   config_dict['ok_slip_c_medium'],
                                                                   config_dict['ok_slip_c_low'],
                                                                   config_dict['bybit_number_c_high'],
                                                                   config_dict['bybit_number_c_medium'],
                                                                   config_dict['bybit_number_c_low'],
                                                                   config_dict['bybit_slip_c_high'],
                                                                   config_dict['bybit_slip_c_medium'],
                                                                   config_dict['bybit_slip_c_low'],
                                                                   config_dict['gate_number_c_high'],
                                                                   config_dict['gate_number_c_medium'],
                                                                   config_dict['gate_number_c_low'],
                                                                   config_dict['gate_slip_c_high'],
                                                                   config_dict['gate_slip_c_medium'],
                                                                   config_dict['gate_slip_c_low'],
                                                                   config_dict['okx_contract_number_c_high'],
                                                                   config_dict['okx_contract_number_c_medium'],
                                                                   config_dict['okx_contract_number_c_low'],
                                                                   config_dict['okx_contract_slip_c_high'],
                                                                   config_dict['okx_contract_slip_c_medium'],
                                                                   config_dict['okx_contract_slip_c_low'],
                                                                   config_dict['bybit_contract_number_c_high'],
                                                                   config_dict['bybit_contract_number_c_medium'],
                                                                   config_dict['bybit_contract_number_c_low'],
                                                                   config_dict['bybit_contract_slip_c_high'],
                                                                   config_dict['bybit_contract_slip_c_medium'],
                                                                   config_dict['bybit_contract_slip_c_low'],
                                                                   config_dict['ok_number_c_1_high'],
                                                                   config_dict['ok_number_c_1_medium'],
                                                                   config_dict['ok_number_c_1_low'],
                                                                   config_dict['ok_slip_c_1_high'],
                                                                   config_dict['ok_slip_c_1_medium'],
                                                                   config_dict['ok_slip_c_1_low'],
                                                                   config_dict['okx_contract_number_c_1_high'],
                                                                   config_dict['okx_contract_number_c_1_medium'],
                                                                   config_dict['okx_contract_number_c_1_low'],
                                                                   config_dict['okx_contract_slip_c_1_high'],
                                                                   config_dict['okx_contract_slip_c_1_medium'],
                                                                   config_dict['okx_contract_slip_c_1_low'],
                                                                   )
                    else:
                        logging.info("BINANCE 永续合约上新逻辑(币安不存在) 没有代币需要处理")

                    if unsupper:
                        if n > 1:
                            await execute_order_binance_map(unsupper, "BINANCE 永续合约上新(币安存在)(多币)",
                                                            config_dict['map_high_b'],
                                                            config_dict['map_medium_b'],
                                                            config_dict['map_low_b'],
                                                            config_dict['binance_number_b_high'],
                                                            config_dict['binance_number_b_medium'],
                                                            config_dict['binance_number_b_low'],
                                                            config_dict['binance_slip_b_high'],
                                                            config_dict['binance_slip_b_medium'],
                                                            config_dict['binance_slip_b_low']
                                                            )
                        if n == 1:
                            await execute_order_binance_map(unsupper, "BINANCE 永续合约上新(币安存在)(单币)",
                                                            config_dict['map_high_b'],
                                                            config_dict['map_medium_b'],
                                                            config_dict['map_low_b'],
                                                            config_dict['binance_number_a_high'],
                                                            config_dict['binance_number_a_medium'],
                                                            config_dict['binance_number_a_low'],
                                                            config_dict['binance_slip_a_high'],
                                                            config_dict['binance_slip_a_medium'],
                                                            config_dict['binance_slip_a_low']
                                                            )
                    else:
                        logging.info("BINANCE 永续合约上新逻辑(币安存在) 没有代币需要处理")

                    msg = (f"BINANCE 永续合约上新 逻辑处理\n\n"
                           f"原文:{message_content}\n\n"
                           f"  ⏱️{datetime.now()}")
                    await ding_auto_api(msg)

                if upbit_keyword in message_content:
                    if "(KRW, BTC, USDT 마켓)" in message_content:
                        _supper = log_symbols_and_filter(message_content, coin_u, upbit_keyword,
                                                         "UPBIT 现货上新 带有韩元对")
                        # 去除UPBIT交易所存在的coin
                        supper_upbit = [coin for coin in _supper if coin not in upbit_coin]
                        logging.info(f"去除UPBIT交易所存在的coin {supper_upbit}")
                        upbit_coin.update(supper_upbit)
                        # 如果有去除交易所存在的coin之后 有代币进行购买 没有代币播报失败
                        if supper_upbit:
                            # 去除币安交易所存在的coin
                            supper = [coin for coin in supper_upbit if coin not in binance_coin]

                            # 保留币安交易所存在的coin
                            unsupper = [coin for coin in supper_upbit if coin in binance_coin]

                            if supper:
                                if len(_supper) > 1:
                                    await execute_order_uncoin_binance_map(supper,
                                                                           "UPBIT带韩元 现货上新(币安不存在)(多币)",
                                                                           config_dict['map_high_e'],
                                                                           config_dict['map_medium_e'],
                                                                           config_dict['map_low_e'],
                                                                           config_dict['ok_number_f_high'],
                                                                           config_dict['ok_number_f_medium'],
                                                                           config_dict['ok_number_f_low'],
                                                                           config_dict['ok_slip_f_high'],
                                                                           config_dict['ok_slip_f_medium'],
                                                                           config_dict['ok_slip_f_low'],
                                                                           config_dict['bybit_number_f_high'],
                                                                           config_dict['bybit_number_f_medium'],
                                                                           config_dict['bybit_number_f_low'],
                                                                           config_dict['bybit_slip_f_high'],
                                                                           config_dict['bybit_slip_f_medium'],
                                                                           config_dict['bybit_slip_f_low'],
                                                                           config_dict['gate_number_f_high'],
                                                                           config_dict['gate_number_f_medium'],
                                                                           config_dict['gate_number_f_low'],
                                                                           config_dict['gate_slip_f_high'],
                                                                           config_dict['gate_slip_f_medium'],
                                                                           config_dict['gate_slip_f_low'],
                                                                           config_dict['okx_contract_number_f_high'],
                                                                           config_dict['okx_contract_number_f_medium'],
                                                                           config_dict['okx_contract_number_f_low'],
                                                                           config_dict['okx_contract_slip_f_high'],
                                                                           config_dict['okx_contract_slip_f_medium'],
                                                                           config_dict['okx_contract_slip_f_low'],
                                                                           config_dict['bybit_contract_number_f_high'],
                                                                           config_dict[
                                                                               'bybit_contract_number_f_medium'],
                                                                           config_dict['bybit_contract_number_f_low'],
                                                                           config_dict['bybit_contract_slip_f_high'],
                                                                           config_dict['bybit_contract_slip_f_medium'],
                                                                           config_dict['bybit_contract_slip_f_low'],
                                                                           config_dict['ok_number_f_1_high'],
                                                                           config_dict['ok_number_f_1_medium'],
                                                                           config_dict['ok_number_f_1_low'],
                                                                           config_dict['ok_slip_f_1_high'],
                                                                           config_dict['ok_slip_f_1_medium'],
                                                                           config_dict['ok_slip_f_1_low'],
                                                                           config_dict['okx_contract_number_f_1_high'],
                                                                           config_dict[
                                                                               'okx_contract_number_f_1_medium'],
                                                                           config_dict['okx_contract_number_f_1_low'],
                                                                           config_dict['okx_contract_slip_f_1_high'],
                                                                           config_dict['okx_contract_slip_f_1_medium'],
                                                                           config_dict['okx_contract_slip_f_1_low'],
                                                                           )
                                if len(_supper) == 1:
                                    await execute_order_uncoin_binance_map(supper,
                                                                           "UPBIT带韩元 现货上新(币安不存在)(单币)",
                                                                           config_dict['map_high_e'],
                                                                           config_dict['map_medium_e'],
                                                                           config_dict['map_low_e'],
                                                                           config_dict['ok_number_e_high'],
                                                                           config_dict['ok_number_e_medium'],
                                                                           config_dict['ok_number_e_low'],
                                                                           config_dict['ok_slip_e_high'],
                                                                           config_dict['ok_slip_e_medium'],
                                                                           config_dict['ok_slip_e_low'],
                                                                           config_dict['bybit_number_e_high'],
                                                                           config_dict['bybit_number_e_medium'],
                                                                           config_dict['bybit_number_e_low'],
                                                                           config_dict['bybit_slip_e_high'],
                                                                           config_dict['bybit_slip_e_medium'],
                                                                           config_dict['bybit_slip_e_low'],
                                                                           config_dict['gate_number_e_high'],
                                                                           config_dict['gate_number_e_medium'],
                                                                           config_dict['gate_number_e_low'],
                                                                           config_dict['gate_slip_e_high'],
                                                                           config_dict['gate_slip_e_medium'],
                                                                           config_dict['gate_slip_e_low'],
                                                                           config_dict['okx_contract_number_e_high'],
                                                                           config_dict['okx_contract_number_e_medium'],
                                                                           config_dict['okx_contract_number_e_low'],
                                                                           config_dict['okx_contract_slip_e_high'],
                                                                           config_dict['okx_contract_slip_e_medium'],
                                                                           config_dict['okx_contract_slip_e_low'],
                                                                           config_dict['bybit_contract_number_e_high'],
                                                                           config_dict[
                                                                               'bybit_contract_number_e_medium'],
                                                                           config_dict['bybit_contract_number_e_low'],
                                                                           config_dict['bybit_contract_slip_e_high'],
                                                                           config_dict['bybit_contract_slip_e_medium'],
                                                                           config_dict['bybit_contract_slip_e_low'],
                                                                           config_dict['ok_number_e_1_high'],
                                                                           config_dict['ok_number_e_1_medium'],
                                                                           config_dict['ok_number_e_1_low'],
                                                                           config_dict['ok_slip_e_1_high'],
                                                                           config_dict['ok_slip_e_1_medium'],
                                                                           config_dict['ok_slip_e_1_low'],
                                                                           config_dict['okx_contract_number_e_1_high'],
                                                                           config_dict[
                                                                               'okx_contract_number_e_1_medium'],
                                                                           config_dict['okx_contract_number_e_1_low'],
                                                                           config_dict['okx_contract_slip_e_1_high'],
                                                                           config_dict['okx_contract_slip_e_1_medium'],
                                                                           config_dict['okx_contract_slip_e_1_low'],
                                                                           )
                            else:
                                logging.info("UPBIT带韩元 现货上新逻辑(币安不存在) 没有代币需要处理")

                            if unsupper:
                                if len(_supper) > 1:
                                    await execute_order_binance_map(unsupper, "UPBIT带韩元 现货上新(币安存在)(多币)",
                                                                    config_dict['map_high_d'],
                                                                    config_dict['map_medium_d'],
                                                                    config_dict['map_low_d'],
                                                                    config_dict['binance_number_d_high'],
                                                                    config_dict['binance_number_d_medium'],
                                                                    config_dict['binance_number_d_low'],
                                                                    config_dict['binance_slip_d_high'],
                                                                    config_dict['binance_slip_d_medium'],
                                                                    config_dict['binance_slip_d_low']
                                                                    )
                                if len(_supper) == 1:
                                    await execute_order_binance_map(unsupper, "UPBIT带韩元 现货上新(币安存在)(单币)",
                                                                    config_dict['map_high_d'],
                                                                    config_dict['map_medium_d'],
                                                                    config_dict['map_low_d'],
                                                                    config_dict['binance_number_c_high'],
                                                                    config_dict['binance_number_c_medium'],
                                                                    config_dict['binance_number_c_low'],
                                                                    config_dict['binance_slip_c_high'],
                                                                    config_dict['binance_slip_c_medium'],
                                                                    config_dict['binance_slip_c_low']
                                                                    )
                            else:
                                logging.info("UPBIT带韩元 现货上新逻辑(币安存在) 没有代币需要处理")

                        else:
                            logging.info("UPBIT 现货上新 带有韩元对 UPBIT 存在此交易对")

                        msg = (f"UPBIT 现货上新 带有韩元对 逻辑处理\n\n"
                               f"原文:{message_content}\n\n"
                               f"  ⏱️{datetime.now()}")
                        await ding_auto_api(msg)

                    elif "(BTC, USDT 마켓)" in message_content:
                        _supper = log_symbols_and_filter(message_content, coin_u, upbit_keyword,
                                                         "UPBIT 现货上新 不带有韩元对")
                        # 去除UPBIT交易所存在的coin
                        supper_upbit = [coin for coin in _supper if coin not in upbit_coin]
                        logging.info(f"去除UPBIT交易所存在的coin {supper_upbit}")
                        upbit_coin.update(supper_upbit)
                        # 如果有去除交易所存在的coin之后 有代币进行购买 没有代币播报失败
                        if supper_upbit:
                            # 去除币安交易所存在的coin
                            supper = [coin for coin in supper_upbit if coin not in binance_coin]

                            # 保留币安交易所存在的coin
                            unsupper = [coin for coin in supper_upbit if coin in binance_coin]

                            if supper:
                                if len(_supper) > 1:
                                    await execute_order_uncoin_binance_map(supper,
                                                                           "UPBIT不带韩元 现货上新(币安不存在)(多币)",
                                                                           config_dict['map_high_g'],
                                                                           config_dict['map_medium_g'],
                                                                           config_dict['map_low_g'],
                                                                           config_dict['ok_number_h_high'],
                                                                           config_dict['ok_number_h_medium'],
                                                                           config_dict['ok_number_h_low'],
                                                                           config_dict['ok_slip_h_high'],
                                                                           config_dict['ok_slip_h_medium'],
                                                                           config_dict['ok_slip_h_low'],
                                                                           config_dict['bybit_number_h_high'],
                                                                           config_dict['bybit_number_h_medium'],
                                                                           config_dict['bybit_number_h_low'],
                                                                           config_dict['bybit_slip_h_high'],
                                                                           config_dict['bybit_slip_h_medium'],
                                                                           config_dict['bybit_slip_h_low'],
                                                                           config_dict['gate_number_h_high'],
                                                                           config_dict['gate_number_h_medium'],
                                                                           config_dict['gate_number_h_low'],
                                                                           config_dict['gate_slip_h_high'],
                                                                           config_dict['gate_slip_h_medium'],
                                                                           config_dict['gate_slip_h_low'],
                                                                           config_dict['okx_contract_number_h_high'],
                                                                           config_dict['okx_contract_number_h_medium'],
                                                                           config_dict['okx_contract_number_h_low'],
                                                                           config_dict['okx_contract_slip_h_high'],
                                                                           config_dict['okx_contract_slip_h_medium'],
                                                                           config_dict['okx_contract_slip_h_low'],
                                                                           config_dict['bybit_contract_number_h_high'],
                                                                           config_dict[
                                                                               'bybit_contract_number_h_medium'],
                                                                           config_dict['bybit_contract_number_h_low'],
                                                                           config_dict['bybit_contract_slip_h_high'],
                                                                           config_dict['bybit_contract_slip_h_medium'],
                                                                           config_dict['bybit_contract_slip_h_low'],
                                                                           config_dict['ok_number_h_1_high'],
                                                                           config_dict['ok_number_h_1_medium'],
                                                                           config_dict['ok_number_h_1_low'],
                                                                           config_dict['ok_slip_h_1_high'],
                                                                           config_dict['ok_slip_h_1_medium'],
                                                                           config_dict['ok_slip_h_1_low'],
                                                                           config_dict['okx_contract_number_h_1_high'],
                                                                           config_dict[
                                                                               'okx_contract_number_h_1_medium'],
                                                                           config_dict['okx_contract_number_h_1_low'],
                                                                           config_dict['okx_contract_slip_h_1_high'],
                                                                           config_dict['okx_contract_slip_h_1_medium'],
                                                                           config_dict['okx_contract_slip_h_1_low'],
                                                                           )
                                if len(_supper) == 1:
                                    await execute_order_uncoin_binance_map(supper,
                                                                           "UPBIT不带韩元 现货上新(币安不存在)(单币)",
                                                                           config_dict['map_high_g'],
                                                                           config_dict['map_medium_g'],
                                                                           config_dict['map_low_g'],
                                                                           config_dict['ok_number_g_high'],
                                                                           config_dict['ok_number_g_medium'],
                                                                           config_dict['ok_number_g_low'],
                                                                           config_dict['ok_slip_g_high'],
                                                                           config_dict['ok_slip_g_medium'],
                                                                           config_dict['ok_slip_g_low'],
                                                                           config_dict['bybit_number_g_high'],
                                                                           config_dict['bybit_number_g_medium'],
                                                                           config_dict['bybit_number_g_low'],
                                                                           config_dict['bybit_slip_g_high'],
                                                                           config_dict['bybit_slip_g_medium'],
                                                                           config_dict['bybit_slip_g_low'],
                                                                           config_dict['gate_number_g_high'],
                                                                           config_dict['gate_number_g_medium'],
                                                                           config_dict['gate_number_g_low'],
                                                                           config_dict['gate_slip_g_high'],
                                                                           config_dict['gate_slip_g_medium'],
                                                                           config_dict['gate_slip_g_low'],
                                                                           config_dict['okx_contract_number_g_high'],
                                                                           config_dict['okx_contract_number_g_medium'],
                                                                           config_dict['okx_contract_number_g_low'],
                                                                           config_dict['okx_contract_slip_g_high'],
                                                                           config_dict['okx_contract_slip_g_medium'],
                                                                           config_dict['okx_contract_slip_g_low'],
                                                                           config_dict['bybit_contract_number_g_high'],
                                                                           config_dict[
                                                                               'bybit_contract_number_g_medium'],
                                                                           config_dict['bybit_contract_number_g_low'],
                                                                           config_dict['bybit_contract_slip_g_high'],
                                                                           config_dict['bybit_contract_slip_g_medium'],
                                                                           config_dict['bybit_contract_slip_g_low'],
                                                                           config_dict['ok_number_g_1_high'],
                                                                           config_dict['ok_number_g_1_medium'],
                                                                           config_dict['ok_number_g_1_low'],
                                                                           config_dict['ok_slip_g_1_high'],
                                                                           config_dict['ok_slip_g_1_medium'],
                                                                           config_dict['ok_slip_g_1_low'],
                                                                           config_dict['okx_contract_number_g_1_high'],
                                                                           config_dict[
                                                                               'okx_contract_number_g_1_medium'],
                                                                           config_dict['okx_contract_number_g_1_low'],
                                                                           config_dict['okx_contract_slip_g_1_high'],
                                                                           config_dict['okx_contract_slip_g_1_medium'],
                                                                           config_dict['okx_contract_slip_g_1_low'],
                                                                           )
                            else:
                                logging.info("UPBIT不带韩元 现货上新逻辑(币安不存在) 没有代币需要处理")

                            if unsupper:
                                if len(_supper) > 1:
                                    await execute_order_binance_map(unsupper, "UPBIT不带韩元 现货上新(币安存在)(多币)",
                                                                    config_dict['map_high_f'],
                                                                    config_dict['map_medium_f'],
                                                                    config_dict['map_low_f'],
                                                                    config_dict['binance_number_f_high'],
                                                                    config_dict['binance_number_f_medium'],
                                                                    config_dict['binance_number_f_low'],
                                                                    config_dict['binance_slip_f_high'],
                                                                    config_dict['binance_slip_f_medium'],
                                                                    config_dict['binance_slip_f_low']
                                                                    )
                                if len(_supper) == 1:
                                    await execute_order_binance_map(unsupper, "UPBIT不带韩元 现货上新(币安存在)(单币)",
                                                                    config_dict['map_high_f'],
                                                                    config_dict['map_medium_f'],
                                                                    config_dict['map_low_f'],
                                                                    config_dict['binance_number_e_high'],
                                                                    config_dict['binance_number_e_medium'],
                                                                    config_dict['binance_number_e_low'],
                                                                    config_dict['binance_slip_e_high'],
                                                                    config_dict['binance_slip_e_medium'],
                                                                    config_dict['binance_slip_e_low']
                                                                    )
                            else:
                                logging.info("UPBIT不带韩元 现货上新逻辑(币安存在) 没有代币需要处理")

                        else:
                            logging.info("UPBIT 现货上新 不带有韩元对 UPBIT 存在此交易对")

                        msg = (f"UPBIT 现货上新 不带有韩元对 逻辑处理\n\n"
                               f"原文:{message_content}\n\n"
                               f"  ⏱️{datetime.now()}")
                        await ding_auto_api(msg)

                check_and_update_coingecko_map()
            else:
                logging.info("触发不买入关键字")

        # 等待一段时间后再次检查新消息
        await asyncio.sleep(1)  # 每1秒检查一次新消息


# 主函数，启动所有客户端并设置延迟
async def main():
    # 创建多个 TelegramClient 实例
    clients = []
    for idx, config in enumerate(clients_config):
        client = TelegramClient(config['session_name'], config['api_id'], config['api_hash'])
        await client.start()
        clients.append((client, config))
        logging.info(f"{config['session_name']} 客户端已启动...")

        # 为每个客户端启动添加 200 毫秒的延迟
        await asyncio.sleep(0.2 * idx)  # idx用于确保每个客户端启动间隔200毫秒

    # 创建并运行多个轮询任务
    tasks = [fetch_messages(client, config) for client, config in clients]
    await asyncio.gather(*tasks)


if __name__ == '__main__':
    # 日志配置
    log_file_handler = handlers.TimedRotatingFileHandler("cnn.log", when='D', interval=1, backupCount=5,
                                                         encoding="utf-8")
    log_file_handler.setLevel(logging.INFO)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            log_file_handler,
            logging.StreamHandler()
        ]
    )

    asyncio.run(main())
