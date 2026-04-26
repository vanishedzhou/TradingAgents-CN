#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
查询本地 MongoDB 里的股票数据概览
=====================================

默认显示各股票相关集合的总览（条数 / 最新更新 / 市场分布），
也支持按股票代码筛选查看单只股票的完整数据。

使用方式:
    # 1. 查看总览（所有集合的统计）
    python scripts/show_stock_data.py

    # 2. 查询某只股票在所有集合里的数据
    python scripts/show_stock_data.py --symbol 000001
    python scripts/show_stock_data.py -s AAPL
    python scripts/show_stock_data.py -s 1810.HK

    # 3. 按市场筛选列表（A股/美股/港股）
    python scripts/show_stock_data.py --market A股 --limit 20
    python scripts/show_stock_data.py --market 美股 --limit 10

    # 4. 列出所有股票代码（按市场分组的 distinct 清单）
    python scripts/show_stock_data.py --list-symbols --market A股 --limit 50

环境变量(可选):
    MONGODB_URI  — 默认 mongodb://admin:tradingagents123@localhost:27017/tradingagents?authSource=admin
    MONGODB_DB   — 默认 tradingagents
"""

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient


# 股票相关集合及其关键字段
# (collection_name, symbol_field_candidates, time_field_candidates, display_label)
STOCK_COLLECTIONS = [
    ('stock_basic_info',      ['code', 'full_symbol', 'symbol'], ['updated_at'],   '股票基础信息'),
    ('stock_screening_view',  ['code', 'full_symbol', 'symbol'], ['updated_at'],   '股票筛选视图'),
    ('market_quotes',         ['code', 'symbol'],                 ['updated_at', 'trade_date'], '行情快照'),
    ('analysis_tasks',        ['stock_symbol', 'stock_code'],     ['updated_at', 'created_at'], '分析任务'),
    ('analysis_reports',      ['stock_symbol', 'stock_code'],     ['created_at'],   'AI 分析报告'),
    ('financial_data_cache',  ['symbol'],                          ['updated_at'],   '财务数据缓存'),
    ('paper_positions',       ['code'],                            ['updated_at'],   '模拟持仓'),
    ('paper_orders',          ['code'],                            ['created_at'],   '模拟订单'),
    ('paper_trades',          ['code'],                            ['timestamp'],    '模拟成交'),
]


def connect():
    uri = os.environ.get(
        'MONGODB_URI',
        'mongodb://admin:tradingagents123@localhost:27017/tradingagents?authSource=admin'
    )
    db_name = os.environ.get('MONGODB_DB', 'tradingagents')
    return MongoClient(uri)[db_name]


def fmt_time(t):
    if not t:
        return 'N/A'
    if isinstance(t, datetime):
        return t.strftime('%Y-%m-%d %H:%M:%S')
    return str(t)[:19]


def show_overview(db):
    """总览：每个股票相关集合的条数、最新更新、市场分布"""
    print('=' * 90)
    print(f"📊 本地 MongoDB 股票数据总览  (db={db.name})")
    print('=' * 90)
    print(f"{'集合名':25s} | {'标签':14s} | {'条数':>7s} | {'最新更新':19s}")
    print('-' * 90)

    for coll, symbol_fields, time_fields, label in STOCK_COLLECTIONS:
        try:
            cnt = db[coll].count_documents({})
        except Exception as e:
            print(f"{coll:25s} | {label:14s} | ERR     | {e}")
            continue
        if cnt == 0:
            continue

        latest = None
        for tf in time_fields:
            doc = db[coll].find_one({tf: {'$exists': True}}, sort=[(tf, -1)])
            if doc and doc.get(tf):
                latest = doc.get(tf)
                break
        print(f"{coll:25s} | {label:14s} | {cnt:>7d} | {fmt_time(latest):19s}")

    # 按市场分布
    print()
    print('📍 按市场分布 (stock_basic_info):')
    print('-' * 90)
    pipeline = [
        {'$group': {'_id': {'market': '$market', 'sse': '$sse', 'category': '$category'},
                    'cnt': {'$sum': 1}}},
        {'$sort': {'cnt': -1}},
    ]
    for r in db.stock_basic_info.aggregate(pipeline):
        k = r['_id']
        label = f"market={k.get('market','-')}, 交易所={k.get('sse','-')}, 类别={k.get('category','-')}"
        print(f"  {label:75s}  {r['cnt']:>6d}")

    # 按 AI 分析覆盖
    print()
    print('🤖 已被 AI 分析过的股票 (analysis_reports 去重):')
    print('-' * 90)
    pipe = [
        {'$group': {'_id': {'symbol': '$stock_symbol', 'market': '$market_type'},
                    'reports': {'$sum': 1},
                    'latest': {'$max': '$created_at'}}},
        {'$sort': {'_id.market': 1, '_id.symbol': 1}},
    ]
    by_market = {}
    for r in db.analysis_reports.aggregate(pipe):
        m = r['_id'].get('market') or '未知'
        by_market.setdefault(m, []).append((r['_id'].get('symbol',''), r['reports'], r['latest']))
    for m, items in sorted(by_market.items()):
        print(f"  [{m}]  ({len(items)} 只)")
        for sym, reports, latest in items[:30]:
            print(f"    · {str(sym or '-'):12s} · 报告数={reports:<3d} · 最新分析: {fmt_time(latest)}")
        if len(items) > 30:
            print(f"    ... 以及其他 {len(items)-30} 只")

    # 行情数据的最新交易日
    print()
    print('📈 行情数据 (market_quotes) 最新 3 个交易日:')
    print('-' * 90)
    for r in db.market_quotes.aggregate([
        {'$group': {'_id': '$trade_date', 'cnt': {'$sum': 1}}},
        {'$sort': {'_id': -1}}, {'$limit': 3}
    ]):
        print(f"  trade_date={r['_id']}  : {r['cnt']} 条")


def show_symbol(db, symbol):
    """查询某只股票在所有集合里的数据"""
    print('=' * 90)
    print(f"🔍 查询股票 '{symbol}' 在本地数据库的全部数据")
    print('=' * 90)

    found = 0
    for coll, symbol_fields, time_fields, label in STOCK_COLLECTIONS:
        # 构造 $or 查询，匹配所有候选 symbol 字段
        or_conds = []
        for f in symbol_fields:
            or_conds.append({f: symbol})
            or_conds.append({f: symbol.upper()})
            or_conds.append({f: symbol.lower()})
        query = {'$or': or_conds}

        try:
            cnt = db[coll].count_documents(query)
        except Exception:
            continue
        if cnt == 0:
            continue

        found += 1
        print(f"\n📁 [{coll}]  {label}  — 匹配 {cnt} 条")
        print('-' * 90)
        # 显示最多 3 条
        sort_field = time_fields[0] if time_fields else '_id'
        for doc in db[coll].find(query).sort(sort_field, -1).limit(3):
            doc.pop('_id', None)
            # 精简输出：过长字段截断
            for k, v in doc.items():
                v_str = str(v)
                if len(v_str) > 80:
                    v_str = v_str[:80] + '...'
                print(f"    {k:22s}: {v_str}")
            print('    ' + '.' * 85)

    if found == 0:
        print(f"\n❌ 未在任何集合找到 '{symbol}'")
        print('提示: 试试不同写法，例如 000001 / 000001.SZ / AAPL / 1810.HK / 01810')


def list_symbols(db, market_filter=None, limit=50):
    """列出本地有哪些股票代码（distinct）"""
    print('=' * 90)
    print(f"📋 本地已有的股票代码清单  市场={market_filter or '全部'}  limit={limit}")
    print('=' * 90)

    q = {}
    if market_filter:
        # 模糊匹配 market / sse / category 字段
        q = {'$or': [
            {'market': {'$regex': market_filter, '$options': 'i'}},
            {'sse': {'$regex': market_filter, '$options': 'i'}},
            {'category': {'$regex': market_filter, '$options': 'i'}},
        ]}

    cursor = db.stock_basic_info.find(q, {'code': 1, 'full_symbol': 1, 'name': 1, 'market': 1, 'industry': 1, 'pe': 1}).limit(limit)
    print(f"{'代码':12s} | {'全称':15s} | {'名称':20s} | {'市场':10s} | {'行业':15s} | PE")
    print('-' * 90)
    for d in cursor:
        print(f"{str(d.get('code','')):12s} | "
              f"{str(d.get('full_symbol','')):15s} | "
              f"{str(d.get('name','')):20s} | "
              f"{str(d.get('market','')):10s} | "
              f"{str(d.get('industry',''))[:15]:15s} | "
              f"{d.get('pe','')}")


def parse_args():
    p = argparse.ArgumentParser(
        description='查询本地 MongoDB 里的股票数据',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument('-s', '--symbol', type=str, help='按股票代码查询单只股票')
    p.add_argument('-m', '--market', type=str,
                   help='按市场过滤（主板/创业板/科创板/美股/港股 等，模糊匹配）')
    p.add_argument('-l', '--limit', type=int, default=50, help='列表返回条数 (默认 50)')
    p.add_argument('--list-symbols', action='store_true', help='列出股票代码清单（需配合 --market）')
    return p.parse_args()


def main():
    args = parse_args()
    db = connect()

    if args.symbol:
        show_symbol(db, args.symbol)
    elif args.list_symbols:
        list_symbols(db, args.market, args.limit)
    else:
        show_overview(db)


if __name__ == '__main__':
    main()
