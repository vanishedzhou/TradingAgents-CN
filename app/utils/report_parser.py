"""
分析报告文本解析工具
=========================

从 MongoDB `analysis_reports` 的 `reports` 字段（AI 生成的 markdown 文本）里
用正则抽出结构化字段：当前价、目标价、止损价。

这些逻辑原先散落在 scripts/query_us_buy_ranking.py，为了被 favorites 历史
曲线接口复用，抽成独立 util。

向后兼容：query_us_buy_ranking.py 仍可 import 本模块里的函数。
"""

from __future__ import annotations

import re
from typing import Optional, Dict, Any, Iterable, List


_PRICE_PREFIX = r'(?:HK|US)?[\$￥¥]?'


def extract_price_from_text(text: str, pattern_keywords: Iterable[str],
                            negative_lookbehind: Optional[List[str]] = None) -> Optional[float]:
    """从文本中提取第一个匹配关键字后出现的价格数值。

    支持:
      目标价格：$185.00
      目标价: 185
      target_price: $185
      目标价 HK$185.5
      当前价格 ￥12.34

    negative_lookbehind: 如果关键字前面紧邻这些前缀词，则跳过该匹配。
      例如 ['平均', '年均', '历史'] 可避免把 "平均收盘价" 误当作 "收盘价"。
    """
    if not text or not isinstance(text, str):
        return None
    for kw in pattern_keywords:
        patterns = [
            rf'{kw}[：:]\s*{_PRICE_PREFIX}\s*([\d]+\.?\d*)',
            rf'{kw}\s*{_PRICE_PREFIX}\s*([\d]+\.?\d*)',
        ]
        for pat in patterns:
            for m in re.finditer(pat, text, re.IGNORECASE):
                # 检查是否应被排除（关键字前面紧邻 negative_lookbehind 中的词）
                if negative_lookbehind:
                    start = max(0, m.start() - 6)
                    prefix = text[start:m.start()]
                    if any(neg in prefix for neg in negative_lookbehind):
                        continue
                try:
                    return float(m.group(1))
                except (ValueError, TypeError):
                    continue
    return None


# 报告 key 搜索顺序（优先 market_report，因为它通常包含实时行情段落）
_CURRENT_PRICE_REPORT_KEYS: List[str] = [
    'market_report', 'fundamentals_report', 'trader_investment_plan',
    'final_trade_decision', 'news_report',
]
_CURRENT_PRICE_KEYWORDS: List[str] = [
    '当前价格', '现价', '当前股价', '最新股价', '目前价格',
    '最新价', 'current price', 'close price',
    '收盘价',  # 放最后，因为容易误匹配"平均收盘价"
]

# "收盘价" 等宽泛关键字前面如果紧邻以下词，说明不是"当前收盘价"，应跳过
_CURRENT_PRICE_NEGATIVE_LOOKBEHIND: List[str] = [
    '平均', '年均', '历史', '均值', '年平均', '月均',
]


def extract_current_price_from_reports(reports: Optional[Dict[str, Any]]) -> Optional[float]:
    """从 reports 字典中按预定顺序搜索当前价。"""
    if not reports:
        return None
    for key in _CURRENT_PRICE_REPORT_KEYS:
        content = reports.get(key, '')
        if not content:
            continue
        price = extract_price_from_text(
            content, _CURRENT_PRICE_KEYWORDS,
            negative_lookbehind=_CURRENT_PRICE_NEGATIVE_LOOKBEHIND,
        )
        if price and price > 0:
            return price
    return None


_TARGET_PRICE_REPORT_KEYS: List[str] = [
    'trader_investment_plan', 'final_trade_decision',
    'investment_plan', 'research_team_decision',
]
_TARGET_PRICE_KEYWORDS: List[str] = [
    '目标价格', '目标价位', '目标价',
    'target price', '预期目标', '上行目标', '目标区间',
]


def extract_target_price_from_reports(reports: Optional[Dict[str, Any]]) -> Optional[float]:
    """从 reports 字典中提取 AI 给出的目标价。"""
    if not reports:
        return None
    for key in _TARGET_PRICE_REPORT_KEYS:
        content = reports.get(key, '')
        if not content:
            continue
        price = extract_price_from_text(content, _TARGET_PRICE_KEYWORDS)
        if price and price > 0:
            return price
    return None


_STOP_LOSS_REPORT_KEYS: List[str] = [
    'trader_investment_plan', 'final_trade_decision',
    'investment_plan', 'risk_management_decision',
]
_STOP_LOSS_KEYWORDS: List[str] = [
    '止损价位', '止损价格', '止损价', '止损',
    'stop loss', '下行风险', '支撑位',
]


def extract_stop_loss_from_reports(reports: Optional[Dict[str, Any]]) -> Optional[float]:
    """从 reports 字典中提取止损价。"""
    if not reports:
        return None
    for key in _STOP_LOSS_REPORT_KEYS:
        content = reports.get(key, '')
        if not content:
            continue
        price = extract_price_from_text(content, _STOP_LOSS_KEYWORDS)
        if price and price > 0:
            return price
    return None
