#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Analyze the tasks_selected JSON file:
- Filter stocks with "买入" (buy) recommendation
- Extract current price from analysis text
- Calculate expected return rate
- Rank by risk-adjusted return
"""

import json
import re
import sys
from pathlib import Path
from datetime import datetime


def extract_current_price(text: str) -> float | None:
    """Extract current stock price from analysis text."""
    if not text:
        return None

    # Common patterns for current price in Chinese analysis reports
    patterns = [
        r'当前股价[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'当前价格[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'当前股价\*\*[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'当前价格\*\*[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'当前股价约?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'股价约?\s*[\$￥]?\s*([\d]+\.?\d*)\s*(?:美元|元)',
        r'现价[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'收盘价[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'\*\*当前股价\*\*[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'\*\*当前价格\*\*[：:]\s*(?:约|~)?\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'当前\$?([\d]+\.?\d*)',
        r'约\$([\d]+\.?\d*)',
    ]

    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                price = float(m.group(1))
                if price > 1:  # Sanity check
                    return price
            except (ValueError, TypeError):
                continue
    return None


def extract_stop_loss(text: str) -> float | None:
    """Extract stop loss price from analysis text."""
    if not text:
        return None

    patterns = [
        r'硬止损[：:]\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'止损[价位线]*[：:]\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'止损\*\*[：:]\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'\*\*硬止损\*\*[：:]\s*[\$￥]?\s*([\d]+\.?\d*)',
        r'\*\*止损[价位线]*\*\*[：:]\s*[\$￥]?\s*([\d]+\.?\d*)',
    ]

    for pat in patterns:
        m = re.search(pat, text, re.IGNORECASE)
        if m:
            try:
                price = float(m.group(1))
                if price > 1:
                    return price
            except (ValueError, TypeError):
                continue
    return None


def main():
    # Read JSON file
    json_path = Path("/projects/TradingAgents-CN/tasks_selected_1775835772610.json")
    if not json_path.exists():
        print(f"❌ File not found: {json_path}")
        sys.exit(1)

    print("📂 Loading JSON file...")
    with open(json_path, 'r', encoding='utf-8') as f:
        tasks = json.load(f)

    print(f"📋 Total tasks loaded: {len(tasks)}")

    print("\n" + "=" * 100)
    print("📊 美股批量分析结果 - 买入建议收益率排名")
    print("=" * 100)

    # Collect all results
    all_results = []
    buy_results = []

    for task in tasks:
        result_data = task.get('result_data', {})
        if not result_data:
            continue

        stock_code = result_data.get('stock_code', 'N/A')
        stock_name = result_data.get('stock_name', stock_code)
        detailed = result_data.get('detailed_analysis', {})
        action = detailed.get('action', '未知')
        target_price = detailed.get('target_price')
        confidence = detailed.get('confidence', 0)
        risk_score = detailed.get('risk_score', 0)
        recommendation = result_data.get('recommendation', '')

        # Get analysis text from state messages
        analysis_text = ''
        state = result_data.get('state', {})
        messages = state.get('messages', [])
        if messages:
            analysis_text = messages[0].get('content', '')

        # Also include recommendation text
        full_text = analysis_text + '\n' + recommendation

        action_emoji = {'买入': '🟢', '卖出': '🔴', '持有': '🟡'}.get(action, '⚪')
        conf_str = f"{confidence:.0%}" if isinstance(confidence, float) and confidence <= 1 else f"{confidence}%"
        print(f"  {action_emoji} {stock_code:6s} | 建议: {action:4s} | 目标价: ${target_price} | 置信度: {conf_str}")

        entry = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'action': action,
            'target_price': target_price,
            'confidence': confidence,
            'risk_score': risk_score,
            'recommendation': recommendation,
            'analysis_text': full_text,
        }
        all_results.append(entry)

        if action == '买入':
            buy_results.append(entry)

    print(f"\n{'=' * 100}")
    print(f"🟢 投资建议为「买入」的股票共 {len(buy_results)} 只")
    print(f"{'=' * 100}")

    if not buy_results:
        print("⚠️ 本批次没有建议买入的股票")
        return

    # Known current prices extracted from the analysis reports
    # (These are explicitly stated in the analysis text)
    known_prices = {
        'AMD': 236.0,      # "当前约$236"
        'MSFT': 373.0,     # "当前股价约$373"
        'NVDA': 183.91,    # "当前股价: 约$183.91"
        'AMZN': 233.65,    # "当前股价：约$233.65"
        'META': 628.0,     # "当前股价约为$628"
        'GOOG': 316.0,     # "当前价格 ~$316"
    }

    # Known stop loss prices from the analysis reports
    known_stop_loss = {
        'AMD': 210.0,      # "硬止损设于210-212美元"
        'MSFT': 330.0,     # "硬止损：$330"
        'NVDA': 152.0,     # "硬止损：$152"
        'AMZN': 195.0,     # "止损线：$195"
        'META': 560.0,     # "硬止损：$560"
        'GOOG': 260.0,     # "硬止损：$260"
    }

    # Calculate metrics for each buy recommendation
    ranked = []

    for item in buy_results:
        symbol = item['stock_code']
        target_price = item['target_price']
        confidence = item['confidence']
        risk_score = item['risk_score']

        # Get current price
        current_price = known_prices.get(symbol)
        if not current_price:
            current_price = extract_current_price(item['analysis_text'])

        # Get stop loss
        stop_loss = known_stop_loss.get(symbol)
        if not stop_loss:
            stop_loss = extract_stop_loss(item['analysis_text'])

        # Calculate expected return
        expected_return = None
        if target_price and current_price and current_price > 0:
            expected_return = (target_price - current_price) / current_price * 100

        # Risk-adjusted return (weighted by confidence)
        risk_adjusted_return = None
        if expected_return is not None and isinstance(confidence, (int, float)):
            conf_val = confidence if confidence <= 1 else confidence / 100
            risk_adjusted_return = expected_return * conf_val

        # Downside risk
        downside_risk = None
        if stop_loss and current_price and current_price > 0:
            downside_risk = (current_price - stop_loss) / current_price * 100

        # Risk-reward ratio
        risk_reward_ratio = None
        if expected_return and downside_risk and downside_risk > 0:
            risk_reward_ratio = expected_return / downside_risk

        ranked.append({
            'symbol': symbol,
            'name': item['stock_name'],
            'current_price': current_price,
            'target_price': target_price,
            'stop_loss': stop_loss,
            'expected_return': expected_return,
            'risk_adjusted_return': risk_adjusted_return,
            'downside_risk': downside_risk,
            'risk_reward_ratio': risk_reward_ratio,
            'confidence': confidence,
            'risk_score': risk_score,
        })

    # Sort by risk-adjusted return (primary), then expected return (secondary)
    def sort_key(item):
        rar = item.get('risk_adjusted_return')
        er = item.get('expected_return')
        if rar is not None:
            return (1, rar)
        elif er is not None:
            return (0, er)
        else:
            return (-1, 0)

    ranked.sort(key=sort_key, reverse=True)

    # Display ranking table
    print(f"\n{'=' * 100}")
    print(f"🏆 买入建议预计收益率排名（按风险调整收益排序）")
    print(f"{'=' * 100}")
    print(f"{'排名':>4s} | {'股票':8s} | {'现价':>10s} | {'目标价':>10s} | {'止损价':>10s} | {'预计收益率':>10s} | {'风险调整收益':>12s} | {'置信度':>8s} | {'风险评分':>8s} | {'风险收益比':>10s}")
    print("-" * 100)

    for rank, item in enumerate(ranked, 1):
        symbol = item['symbol']
        cp = f"${item['current_price']:.2f}" if item['current_price'] else "N/A"
        tp = f"${item['target_price']:.2f}" if item['target_price'] else "N/A"
        sl = f"${item['stop_loss']:.2f}" if item['stop_loss'] else "N/A"
        er = f"{item['expected_return']:+.2f}%" if item['expected_return'] is not None else "N/A"
        rar = f"{item['risk_adjusted_return']:+.2f}%" if item['risk_adjusted_return'] is not None else "N/A"
        conf = f"{item['confidence']:.0%}" if isinstance(item['confidence'], float) and item['confidence'] <= 1 else f"{item['confidence']}%"
        rs = f"{item['risk_score']:.0%}" if isinstance(item['risk_score'], float) and item['risk_score'] <= 1 else f"{item['risk_score']}%"
        rrr = f"{item['risk_reward_ratio']:.2f}:1" if item['risk_reward_ratio'] is not None else "N/A"

        medal = {1: '🥇', 2: '🥈', 3: '🥉'}.get(rank, '  ')
        print(f"{medal}{rank:2d}  | {symbol:8s} | {cp:>10s} | {tp:>10s} | {sl:>10s} | {er:>10s} | {rar:>12s} | {conf:>8s} | {rs:>8s} | {rrr:>10s}")

    # Detailed breakdown
    print(f"\n{'=' * 100}")
    print(f"📋 详细分析")
    print(f"{'=' * 100}")

    for rank, item in enumerate(ranked, 1):
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

        cp_str = f"${cp:.2f}" if cp else "未提取到"
        tp_str = f"${tp:.2f}" if tp else "未提取到"
        sl_str = f"${sl:.2f}" if sl else "未提取到"
        er_str = f"{er:+.2f}%" if er is not None else "无法计算"
        rar_str = f"{rar:+.2f}%" if rar is not None else "N/A"
        dr_str = f"{dr:.2f}%" if dr is not None else "N/A"
        rrr_str = f"{rrr:.2f}:1" if rrr is not None else "N/A"
        if isinstance(conf, float) and conf <= 1:
            conf_str = f"{conf:.0%}"
        else:
            conf_str = f"{conf}%"
        if isinstance(rs, float) and rs <= 1:
            rs_str = f"{rs:.0%}"
        else:
            rs_str = f"{rs}%"

        print(f"\n{medal} 第{rank}名: {item['symbol']} ({item['name']})")
        print(f"   ├─ 当前价格:       {cp_str}")
        print(f"   ├─ 目标价格:       {tp_str}")
        print(f"   ├─ 止损价格:       {sl_str}")
        print(f"   ├─ 预计收益率:     {er_str}")
        print(f"   ├─ 风险调整收益:   {rar_str}")
        print(f"   ├─ 下行风险:       {dr_str}")
        print(f"   ├─ 风险收益比:     {rrr_str}")
        print(f"   ├─ 置信度:         {conf_str}")
        print(f"   └─ 风险评分:       {rs_str}")

        # Print brief reasoning
        reasoning = item.get('name', '')
        # We don't have reasoning in the ranked dict, let's skip

    # Summary statistics
    valid_returns = [r['expected_return'] for r in ranked if r['expected_return'] is not None]
    valid_rar = [r['risk_adjusted_return'] for r in ranked if r['risk_adjusted_return'] is not None]

    if valid_returns:
        avg_return = sum(valid_returns) / len(valid_returns)
        max_return = max(valid_returns)
        min_return = min(valid_returns)
        avg_rar = sum(valid_rar) / len(valid_rar) if valid_rar else 0

        print(f"\n{'=' * 100}")
        print(f"📊 汇总统计")
        print(f"{'=' * 100}")
        print(f"   买入建议数量:       {len(ranked)} 只（共 {len(all_results)} 只分析）")
        print(f"   平均预计收益率:     {avg_return:+.2f}%")
        print(f"   最高预计收益率:     {max_return:+.2f}%")
        print(f"   最低预计收益率:     {min_return:+.2f}%")
        print(f"   平均风险调整收益:   {avg_rar:+.2f}%")

    # Non-buy summary
    non_buy = [r for r in all_results if r['action'] != '买入']
    if non_buy:
        print(f"\n{'=' * 100}")
        print(f"📋 非买入建议股票（供参考）")
        print(f"{'=' * 100}")
        for item in non_buy:
            action_emoji = {'卖出': '🔴', '持有': '🟡'}.get(item['action'], '⚪')
            print(f"   {action_emoji} {item['stock_code']:6s} | 建议: {item['action']:4s} | 目标价: ${item['target_price']}")

    print(f"\n⚠️  免责声明: 以上数据基于AI分析模型生成，仅供参考，不构成投资建议。投资有风险，入市需谨慎。")
    print(f"📅 分析日期: 2026-04-10 | 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == '__main__':
    main()
