#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
CodeBuddy 模型同步脚本
========================

把 tradingagents/llm_adapters/codebuddy_adapter.py 里声明的模型列表，
写入 MongoDB 的 system_configs.llm_configs（当前 is_active 版本）。

特点:
  - 幂等: 已存在的 (provider=codebuddy, model_name=X) 跳过，不会重复插入
  - API Key 复用: 从已有的任一 codebuddy 记录中读取，不重复写明文
  - 排除别名 default/auto（按用户偏好，避免下拉框出现迷惑项）
  - 默认从活跃 system_configs 里取 codebuddy api_key;
    如果一条都没有，则退回读 llm_providers.codebuddy.api_key

使用方式:
    python scripts/sync_codebuddy_models.py
    # 在 restart.sh 中被自动调用

环境变量(可选):
    MONGODB_URI  — MongoDB 连接串(默认 mongodb://admin:tradingagents123@localhost:27017/tradingagents?authSource=admin)
    MONGODB_DB   — 数据库名(默认 tradingagents)
"""

import os
import sys
from datetime import datetime
from pathlib import Path

# 允许从项目根目录 import
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from pymongo import MongoClient


# CodeBuddy 下全部可用模型的"展示元数据"
# 与 tradingagents/llm_adapters/codebuddy_adapter.py 的 CODEBUDDY_MODELS 对齐
# 故意不包含 default / auto 别名(按用户偏好)
CODEBUDDY_MODEL_SPECS = [
    # (model_name, display_name, description, capability_level)
    # Claude 系列
    ('claude-opus-4.6',      'Claude Opus 4.6',            'Claude Opus 4.6 (CodeBuddy, 旗舰)',            5),
    ('claude-opus-4.6-1m',   'Claude Opus 4.6 (1M)',       'Claude Opus 4.6 1M 上下文 (CodeBuddy)',        5),
    ('claude-sonnet-4.6',    'Claude Sonnet 4.6',          'Claude Sonnet 4.6 (CodeBuddy, 平衡)',          5),
    ('claude-sonnet-4.6-1m', 'Claude Sonnet 4.6 (1M)',     'Claude Sonnet 4.6 1M 上下文 (CodeBuddy)',      5),
    ('claude-opus-4.5',      'Claude Opus 4.5',            'Claude Opus 4.5 (CodeBuddy, 前代旗舰)',        5),
    ('claude-haiku-4.5',     'Claude Haiku 4.5',           'Claude Haiku 4.5 (CodeBuddy, 快速)',           4),
    # GPT
    ('gpt-5.4',              'GPT-5.4',                    'GPT-5.4 (CodeBuddy, OpenAI 旗舰)',             5),
    # Gemini
    ('gemini-2.5-pro',       'Gemini 2.5 Pro',             'Gemini 2.5 Pro (CodeBuddy 代理)',              5),
    ('gemini-2.5-flash',     'Gemini 2.5 Flash',           'Gemini 2.5 Flash (CodeBuddy 代理, 快速)',      4),
    # 腾讯混元
    ('hunyuan-2.0-instruct', 'Hunyuan 2.0 Instruct',       '腾讯混元 2.0 指令 (CodeBuddy)',                4),
    ('hunyuan-2.0-thinking', 'Hunyuan 2.0 Thinking',       '腾讯混元 2.0 推理 (CodeBuddy)',                4),
    # 智谱 GLM (CodeBuddy 版本)
    ('glm-5.0-turbo',        'GLM 5.0 Turbo',              'GLM 5.0 Turbo (CodeBuddy, 智谱快速)',          3),
    ('glm-5.0',              'GLM 5.0',                    'GLM 5.0 (CodeBuddy, 智谱旗舰)',                4),
    ('glm-4.7',              'GLM 4.7',                    'GLM 4.7 (CodeBuddy, 智谱经典)',                4),
    # DeepSeek (CodeBuddy 版本)
    ('deepseek-v3',          'DeepSeek V3',                'DeepSeek V3 (CodeBuddy, 通用)',                4),
    ('deepseek-v3-0324',     'DeepSeek V3 (0324)',         'DeepSeek V3 0324 更新版 (CodeBuddy)',          4),
    ('deepseek-r1',          'DeepSeek R1',                'DeepSeek R1 (CodeBuddy, 推理)',                5),
    # Kimi
    ('kimi-k2.5',            'Kimi K2.5',                  'Kimi K2.5 (CodeBuddy, 月之暗面)',              4),
    # MiniMax
    ('minimax-m2.5',         'MiniMax M2.5',               'MiniMax M2.5 (CodeBuddy)',                     4),
]


def get_codebuddy_api_key(db) -> str | None:
    """优先从 system_configs 现有 codebuddy 条目复用 api_key；否则回退 llm_providers"""
    doc = db.system_configs.find_one({'is_active': True}, sort=[('version', -1)])
    if doc:
        for c in doc.get('llm_configs', []):
            if c.get('provider') == 'codebuddy' and c.get('api_key'):
                return c['api_key']

    provider = db.llm_providers.find_one({'name': 'codebuddy'})
    if provider and provider.get('api_key'):
        return provider['api_key']

    # 最后回退读环境变量
    return os.environ.get('CODEBUDDY_API_KEY')


def build_entry(model_name: str, display: str, desc: str, cap: int, api_key: str) -> dict:
    """构造一条 llm_configs 条目，字段与现有 codebuddy 记录对齐"""
    return {
        'provider': 'codebuddy',
        'model_name': model_name,
        'model_display_name': display,
        'api_key': api_key,
        'api_base': 'https://copilot.tencent.com/v2',
        'max_tokens': 8000,
        'temperature': 0.1,
        'timeout': 180,
        'retry_times': 3,
        'enabled': True,
        'description': desc,
        'model_category': None,
        'custom_endpoint': None,
        'enable_memory': False,
        'enable_debug': False,
        'priority': 10,
        'input_price_per_1k': None,
        'output_price_per_1k': None,
        'currency': 'CNY',
        'capability_level': cap,
        'suitable_roles': ['both'],
        'features': ['tool_calling', 'reasoning'] if cap >= 5 else ['tool_calling'],
        'recommended_depths': ['基础', '标准'] if cap <= 3 else (['标准', '深度'] if cap == 4 else ['深度', '全面']),
        'performance_metrics': None,
    }


def sync(mongo_uri: str, db_name: str) -> int:
    """返回新增条目数量（已存在的不计入）"""
    client = MongoClient(mongo_uri)
    db = client[db_name]

    api_key = get_codebuddy_api_key(db)
    if not api_key:
        print('⚠  未找到 CodeBuddy API Key (system_configs / llm_providers / env 均无)，跳过模型同步')
        client.close()
        return 0

    doc = db.system_configs.find_one({'is_active': True}, sort=[('version', -1)])
    if not doc:
        print('⚠  未找到活跃的 system_configs 文档，跳过')
        client.close()
        return 0

    existing = {
        c['model_name']
        for c in doc.get('llm_configs', [])
        if c.get('provider') == 'codebuddy'
    }

    new_entries = [
        build_entry(name, display, desc, cap, api_key)
        for name, display, desc, cap in CODEBUDDY_MODEL_SPECS
        if name not in existing
    ]

    if not new_entries:
        print(f'✓ CodeBuddy 模型已齐全（{len(existing)} 个），无需同步')
        client.close()
        return 0

    result = db.system_configs.update_one(
        {'_id': doc['_id']},
        {
            '$push': {'llm_configs': {'$each': new_entries}},
            '$set': {'updated_at': datetime.utcnow()},
        }
    )

    print(f'✓ 已追加 {len(new_entries)} 个 CodeBuddy 模型 (matched={result.matched_count}, modified={result.modified_count}):')
    for e in new_entries:
        print(f'    + {e["model_name"]:22s} | {e["model_display_name"]}')

    client.close()
    return len(new_entries)


def main() -> int:
    mongo_uri = os.environ.get(
        'MONGODB_URI',
        'mongodb://admin:tradingagents123@localhost:27017/tradingagents?authSource=admin'
    )
    db_name = os.environ.get('MONGODB_DB', 'tradingagents')

    try:
        sync(mongo_uri, db_name)
        return 0
    except Exception as e:
        print(f'✗ CodeBuddy 模型同步失败: {type(e).__name__}: {e}')
        return 1


if __name__ == '__main__':
    sys.exit(main())
