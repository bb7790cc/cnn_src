# 编译正则表达式
import re

TOKEN_SYMBOLS_PATTERN = re.compile(r'\(([^)]+)\)')
URL_PATTERN = re.compile(r'https?://\S+')
PATTEN_COIN = re.compile(r'([A-Za-z0-9]+)(?=USDT)')
REMOVE_DIGITS_PATTERN = re.compile(r'\d+')  # 预编译去除数字的正则表达式


def token_symbol_upper(text):
    # 使用事先编译的正则表达式匹配符号
    matches = TOKEN_SYMBOLS_PATTERN.findall(text)

    # 过滤掉URL，并去除数字部分且转换为大写
    # 使用列表推导式处理匹配结果
    cleaned_matches = [
                          match.upper() for match in matches if not URL_PATTERN.match(match)  # 保留原始匹配并转换为大写
                      ] + [
                          REMOVE_DIGITS_PATTERN.sub('', match).upper() for match in matches
                          if any(char.isdigit() for char in match) and not URL_PATTERN.match(match)  # 去掉数字并转换为大写，排除 URL
                      ]
    return cleaned_matches


def token_symbol_contract_upper(text):
    # 使用正则表达式匹配
    matches = PATTEN_COIN.findall(text)

    # 过滤掉URL，并去除数字部分且转换为大写
    # 使用列表推导式处理匹配结果
    cleaned_matches = [
                          match.upper() for match in matches if not URL_PATTERN.match(match)  # 保留原始匹配并转换为大写
                      ] + [
                          REMOVE_DIGITS_PATTERN.sub('', match).upper() for match in matches
                          if any(char.isdigit() for char in match) and not URL_PATTERN.match(match)  # 去掉数字并转换为大写，排除 URL
                      ]

    return cleaned_matches


