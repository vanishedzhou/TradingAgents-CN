#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
查询最近一个批次的美股/港股分析结果
筛选投资建议为"买入"的股票，计算预计收益率并排名

使用方法:
    python scripts/query_us_buy_ranking.py              # 默认查询美股
    python scripts/query_us_buy_ranking.py --market us  # 查询美股
    python scripts/query_us_buy_ranking.py --market hk  # 查询港股
"""

import argparse
import re
import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient


# 市场配置
MARKET_CONFIG = {
    'us': {
        'market_type': '美股',
        'display_name': '美股',
        'currency_symbol': '$',
    },
    'hk': {
        'market_type': '港股',
        'display_name': '港股',
        'currency_symbol': 'HK$',
    },
}


def extract_price_from_text(text: str, pattern_keywords: list) -> float | None:
    """从文本中提取价格数值"""
    if not text or not isinstance(text, str):
        return None
    for kw in pattern_keywords:
        # Match patterns like: 目标价格：$185.00 or 目标价: 185 or target_price: $185
        patterns = [
            rf'{kw}[：:]\s*(?:HK)?[\$￥¥]?\s*([\d]+\.?\d*)',
            rf'{kw}\s*(?:HK)?[\$￥¥]?\s*([\d]+\.?\d*)',
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    return float(m.group(1))
                except (ValueError, TypeError):
                    continue
    return None


def extract_current_price_from_reports(reports: dict) -> float | None:
    """从报告中提取当前价格"""
    if not reports:
        return None

    # Try market_report first, then fundamentals_report
    for report_key in ['market_report', 'fundamentals_report', 'trader_investment_plan',
                       'final_trade_decision', 'news_report']:
        content = reports.get(report_key, '')
        if not content:
            continue
        price = extract_price_from_text(content, [
            '当前价格', '现价', '收盘价', '最新价', 'current price', 'close price',
            '当前股价', '最新股价', '目前价格'
        ])
        if price and price > 0:
            return price
    return None


def extract_target_price_from_reports(reports: dict) -> float | None:
    """从报告中提取目标价格"""
    if not reports:
        return None

    for report_key in ['trader_investment_plan', 'final_trade_decision',
                       'investment_plan', 'research_team_decision']:
        content = reports.get(report_key, '')
        if not content:
            continue
        price = extract_price_from_text(content, [
            '目标价格', '目标价位', '目标价', 'target price', '预期目标',
            '上行目标', '目标区间'
        ])
        if price and price > 0:
            return price
    return None


def extract_stop_loss_from_reports(reports: dict) -> float | None:
    """从报告中提取止损价格"""
    if not reports:
        return None

    for report_key in ['trader_investment_plan', 'final_trade_decision',
                       'investment_plan', 'risk_management_decision']:
        content = reports.get(report_key, '')
        if not content:
            continue
        price = extract_price_from_text(content, [
            '止损价位', '止损价格', '止损价', '止损', 'stop loss',
            '下行风险', '支撑位'
        ])
        if price and price > 0:
            return price
    return None


def normalize_symbol(symbol: str, market_key: str) -> str:
    """
    把同一只股票的不同写法归一到同一个 key，用于去重。

    港股示例（市场 hk）:
        '1810.HK'  → '1810'
        '01810'    → '1810'
        '0700.HK'  → '700'
        '00700'    → '700'
        '06869'    → '6869'
        '6869.HK'  → '6869'

    美股（市场 us）: 去空格、转大写即可
        'AAPL'     → 'AAPL'
        ' aapl '   → 'AAPL'
    """
    if not symbol:
        return ''
    s = symbol.strip().upper()

    if market_key == 'hk':
        # 剥掉 .HK / .HKG 之类后缀
        s = re.sub(r'\.(HK|HKG)$', '', s, flags=re.IGNORECASE)
        # 纯数字代码: 去前导零（但保留至少 1 位）
        if s.isdigit():
            s = s.lstrip('0') or '0'
        return s

    # us 及其他市场
    return s


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(
        description='查询最近一个批次的美股/港股分析结果，筛选"买入"建议并排名',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        '--market', '-m',
        type=str,
        default='us',
        choices=['us', 'hk'],
        help='市场类型: us (美股, 默认) 或 hk (港股)',
    )
    parser.add_argument(
        '--limit', '-l',
        type=int,
        default=50,
        help='查询最近的分析报告数量 (默认: 50)',
    )
    return parser.parse_args()


def main():
    args = parse_args()

    market_key = args.market
    market_cfg = MARKET_CONFIG[market_key]
    market_type = market_cfg['market_type']
    display_name = market_cfg['display_name']
    currency = market_cfg['currency_symbol']

    # Connect to MongoDB (with auth)
    client = MongoClient('mongodb://admin:tradingagents123@localhost:27017/tradingagents?authSource=admin')
    db = client['tradingagents']

    print("=" * 90)
    print(f"📊 {display_name}批量分析结果 - 买入建议排名（按预计收益率）")
    print("=" * 90)

    # Step 1: Find the latest batch of analyses for the selected market
    # Query analysis_reports for market_type, sorted by created_at desc
    market_reports = list(
        db.analysis_reports.find(
            {"market_type": market_type}
        ).sort("created_at", -1).limit(args.limit)
    )

    if not market_reports:
        print(f"❌ 未找到{display_name}分析报告")
        return

    print(f"\n📋 共找到 {len(market_reports)} 条{display_name}分析报告，全部展示")

    # Step 2: 去重（同一股票保留最新一条）
    # 港股的同一只股票可能有多种写法（例如 1810.HK / 01810 / 00700 / 0700.HK），
    # 用 normalize_symbol 归一化后再去重；market_reports 已按 created_at desc 排序，
    # 第一次遇到的就是最新一条，之后同一 canonical key 跳过。
    seen_keys = {}  # canonical_key -> report (最新)
    alias_map = {}  # canonical_key -> set[原始写法]
    for report in market_reports:
        symbol = report.get('stock_symbol', '')
        if not symbol:
            continue
        key = normalize_symbol(symbol, market_key)
        if not key:
            continue
        alias_map.setdefault(key, set()).add(symbol)
        if key not in seen_keys:
            seen_keys[key] = report

    latest_batch_reports = list(seen_keys.values())

    # 提示那些"被合并"的股票，方便用户确认去重没误伤
    merged = [(k, sorted(v)) for k, v in alias_map.items() if len(v) > 1]
    if merged:
        print(f"🔗 检测到 {len(merged)} 只股票存在多种代码写法，已合并:")
        for k, variants in merged:
            print(f"    · {k}  ⇐  {', '.join(variants)}")
    print(f"📦 去重后共 {len(latest_batch_reports)} 只{display_name}（每只保留最新一条）")

    if not latest_batch_reports:
        print("❌ 未找到分析报告")
        return

    print(f"\n📊 全部 {len(latest_batch_reports)} 个{display_name}分析结果:")
    print("-" * 90)

    # Step 3: Display all results and filter for "buy"
    all_results = []
    buy_results = []

    for i, report in enumerate(latest_batch_reports, 1):
        stock_symbol = report.get('stock_symbol', 'N/A')
        stock_name = report.get('stock_name', stock_symbol)
        decision = report.get('decision', {})
        recommendation = report.get('recommendation', '')
        confidence = 0.0
        risk_score = 0.0
        action = '未知'
        target_price = None

        if isinstance(decision, dict):
            action = decision.get('action', '未知')
            confidence = decision.get('confidence', 0.0)
            risk_score = decision.get('risk_score', 0.0)
            target_price = decision.get('target_price')
        elif isinstance(recommendation, str):
            if '买入' in recommendation or 'buy' in recommendation.lower():
                action = '买入'
            elif '卖出' in recommendation or 'sell' in recommendation.lower():
                action = '卖出'
            elif '持有' in recommendation or 'hold' in recommendation.lower():
                action = '持有'

        # Format confidence
        if isinstance(confidence, (int, float)):
            conf_str = f"{confidence:.1%}" if confidence <= 1 else f"{confidence:.1f}%"
        else:
            conf_str = str(confidence)

        action_emoji = {'买入': '🟢', '卖出': '🔴', '持有': '🟡'}.get(action, '⚪')
        print(f"  {i:2d}. {action_emoji} {stock_symbol:6s} ({stock_name}) | 建议: {action} | 置信度: {conf_str}")

        result_entry = {
            'stock_symbol': stock_symbol,
            'stock_name': stock_name,
            'action': action,
            'confidence': confidence if isinstance(confidence, (int, float)) else 0.0,
            'risk_score': risk_score if isinstance(risk_score, (int, float)) else 0.0,
            'target_price': target_price,
            'reports': report.get('reports', {}),
            'decision': decision,
            'recommendation': recommendation,
            'created_at': report.get('created_at', ''),
        }
        all_results.append(result_entry)

        if action == '买入':
            buy_results.append(result_entry)

    print(f"\n{'=' * 90}")
    print(f"🟢 投资建议为「买入」的股票共 {len(buy_results)} 只")
    print(f"{'=' * 90}")

    if not buy_results:
        print("⚠️ 本批次没有建议买入的股票")
        return

    # Step 4: Calculate expected return rate for buy recommendations
    print(f"\n📈 计算预计收益率...")
    print("-" * 90)

    ranked_results = []

    for result in buy_results:
        symbol = result['stock_symbol']
        decision = result['decision']
        reports = result['reports']
        target_price = result['target_price']
        confidence = result['confidence']

        # Try to get target price from decision first
        if target_price is None or not isinstance(target_price, (int, float)):
            target_price = extract_target_price_from_reports(reports)

        # Try to get current price from reports
        current_price = extract_current_price_from_reports(reports)

        # Try to get stop loss
        stop_loss = extract_stop_loss_from_reports(reports)

        # Calculate expected return rate
        expected_return = None
        if target_price and current_price and current_price > 0:
            expected_return = (target_price - current_price) / current_price * 100

        # Calculate risk-adjusted return (using confidence as weight)
        risk_adjusted_return = None
        if expected_return is not None and isinstance(confidence, (int, float)):
            conf_val = confidence if confidence <= 1 else confidence / 100
            risk_adjusted_return = expected_return * conf_val

        # Calculate downside risk
        downside_risk = None
        if stop_loss and current_price and current_price > 0:
            downside_risk = (current_price - stop_loss) / current_price * 100

        # Risk-reward ratio
        risk_reward_ratio = None
        if expected_return and downside_risk and downside_risk > 0:
            risk_reward_ratio = expected_return / downside_risk

        ranked_results.append({
            'symbol': symbol,
            'name': result['stock_name'],
            'current_price': current_price,
            'target_price': target_price,
            'stop_loss': stop_loss,
            'expected_return': expected_return,
            'risk_adjusted_return': risk_adjusted_return,
            'downside_risk': downside_risk,
            'risk_reward_ratio': risk_reward_ratio,
            'confidence': confidence,
            'risk_score': result['risk_score'],
        })

    # Step 5: Sort by expected return (risk-adjusted), then by raw expected return
    # Use risk_adjusted_return as primary sort key, fallback to expected_return
    def sort_key(item):
        rar = item.get('risk_adjusted_return')
        er = item.get('expected_return')
        if rar is not None:
            return (1, rar)
        elif er is not None:
            return (0, er)
        else:
            return (-1, 0)

    ranked_results.sort(key=sort_key, reverse=True)

    # Step 6: Display ranking
    print(f"\n{'=' * 90}")
    print(f"🏆 买入建议预计收益率排名")
    print(f"{'=' * 90}")
    print(f"{'排名':>4s} | {'股票':8s} | {'名称':10s} | {'现价':>10s} | {'目标价':>10s} | {'预计收益率':>10s} | {'风险调整收益':>12s} | {'置信度':>8s}")
    print("-" * 90)

    for rank, item in enumerate(ranked_results, 1):
        symbol = item['symbol']
        name = item['name'][:10]
        cp = f"{currency}{item['current_price']:.2f}" if item['current_price'] else "N/A"
        tp = f"{currency}{item['target_price']:.2f}" if item['target_price'] else "N/A"
        er = f"{item['expected_return']:+.2f}%" if item['expected_return'] is not None else "N/A"
        rar = f"{item['risk_adjusted_return']:+.2f}%" if item['risk_adjusted_return'] is not None else "N/A"
        conf = f"{item['confidence']:.1%}" if isinstance(item['confidence'], (int, float)) and item['confidence'] <= 1 else f"{item['confidence']:.1f}%"

        medal = {1: '🥇', 2: '🥈', 3: '🥉'}.get(rank, '  ')
        print(f"{medal}{rank:2d}  | {symbol:8s} | {name:10s} | {cp:>10s} | {tp:>10s} | {er:>10s} | {rar:>12s} | {conf:>8s}")

    # Step 7: Detailed breakdown
    print(f"\n{'=' * 90}")
    print(f"📋 详细分析")
    print(f"{'=' * 90}")

    for rank, item in enumerate(ranked_results, 1):
        medal = {1: '🥇', 2: '🥈', 3: '🥉'}.get(rank, '📌')
        cp = item['current_price']
        tp = item['target_price']
        sl = item['stop_loss']
        er = item['expected_return']
        rar = item['risk_adjusted_return']
        dr = item['downside_risk']
        rrr = item['risk_reward_ratio']
        conf = item['confidence']
        rs = item['risk_score']

        cp_str = f"{currency}{cp:.2f}" if cp else "未提取到"
        tp_str = f"{currency}{tp:.2f}" if tp else "未提取到"
        sl_str = f"{currency}{sl:.2f}" if sl else "未提取到"
        er_str = f"{er:+.2f}%" if er is not None else "无法计算（缺少价格数据）"
        rar_str = f"{rar:+.2f}%" if rar is not None else "N/A"
        dr_str = f"{dr:.2f}%" if dr is not None else "N/A"
        rrr_str = f"{rrr:.2f}" if rrr is not None else "N/A"
        if isinstance(conf, (int, float)) and conf <= 1:
            conf_str = f"{conf:.1%}"
        else:
            conf_str = f"{conf:.1f}%"
        if isinstance(rs, (int, float)) and rs <= 1:
            rs_str = f"{rs:.1%}"
        else:
            rs_str = f"{rs:.1f}%"

        print(f"\n{medal} 第{rank}名: {item['symbol']} ({item['name']})")
        print(f"   ├─ 当前价格:     {cp_str}")
        print(f"   ├─ 目标价格:     {tp_str}")
        print(f"   ├─ 止损价格:     {sl_str}")
        print(f"   ├─ 预计收益率:   {er_str}")
        print(f"   ├─ 风险调整收益: {rar_str}")
        print(f"   ├─ 下行风险:     {dr_str}")
        print(f"   ├─ 风险收益比:   {rrr_str}")
        print(f"   ├─ 置信度:       {conf_str}")
        print(f"   └─ 风险评分:     {rs_str}")

    # Summary
    valid_returns = [r['expected_return'] for r in ranked_results if r['expected_return'] is not None]
    if valid_returns:
        avg_return = sum(valid_returns) / len(valid_returns)
        max_return = max(valid_returns)
        min_return = min(valid_returns)
        print(f"\n{'=' * 90}")
        print(f"📊 汇总统计")
        print(f"{'=' * 90}")
        print(f"   市场类型:       {display_name}")
        print(f"   买入建议数量:   {len(ranked_results)} 只")
        print(f"   平均预计收益率: {avg_return:+.2f}%")
        print(f"   最高预计收益率: {max_return:+.2f}%")
        print(f"   最低预计收益率: {min_return:+.2f}%")

    print(f"\n⚠️  免责声明: 以上数据基于AI分析模型生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。")
    print(f"📅 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    client.close()


if __name__ == '__main__':
    main()
