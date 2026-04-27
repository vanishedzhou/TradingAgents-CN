"""
自选股定时分析调度器
==========================

对每个用户的 user_favorites 按市场分组，在固定时间触发 AI 5 级（全面）分析。

调度时间（Asia/Shanghai，day_of_week=mon-fri 工作日）:
  - A 股 / 港股: 10:30 (盘中)、16:30 (收盘后)
  - 美股:       09:30 (美股前一交易日收盘后复盘)、22:30 (美股开盘后)

只注册协程实现，cron 由 app/main.py lifespan 中挂载。
节假日不单独判定，数据源返回异常时 AI 自己会 degrade（V1 不做额外过滤）。

## 限流 V2 机制
模块级 `_ANALYSIS_SEMAPHORE` 限制"调度器视角下的并发度"（默认 3）。
每只股票的 create_analysis_task 立即返回（DB 登记任务），
execute_analysis_background 才是真正的 15-25 分钟 AI 调用，那部分
在 semaphore 内执行，超过并发上限的会排队等待。

这样可以避免 scheduler 一次性把 9+ 个 5 级分析同时扔进线程池把
event loop 打满（曾经导致 /api/health 整个超时）。
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Iterable, List

from app.core.database import get_mongo_db
from app.models.analysis import AnalysisParameters, SingleAnalysisRequest
from app.services.simple_analysis_service import get_simple_analysis_service

logger = logging.getLogger(__name__)


A_HK_MARKETS = ("A股", "港股")
US_MARKETS = ("美股",)
RESEARCH_DEPTH = "全面"  # 5 级全面分析

# 跨所有 favorites_analysis_* cron 共享的并发闸门
# 通过环境变量 FAVORITES_ANALYSIS_CONCURRENCY 覆盖（默认 5）
_FAV_CONCURRENCY = int(os.environ.get("FAVORITES_ANALYSIS_CONCURRENCY", "5"))
_ANALYSIS_SEMAPHORE: asyncio.Semaphore | None = None


def _get_semaphore() -> asyncio.Semaphore:
    """懒初始化，确保 Semaphore 绑定到当前 event loop"""
    global _ANALYSIS_SEMAPHORE
    if _ANALYSIS_SEMAPHORE is None:
        _ANALYSIS_SEMAPHORE = asyncio.Semaphore(_FAV_CONCURRENCY)
    return _ANALYSIS_SEMAPHORE


async def _run_single_analysis_with_throttle(
    simple_service,
    task_id: str,
    user_id: str,
    req: SingleAnalysisRequest,
    label: str,
    stock_code: str,
) -> None:
    """
    在全局 semaphore 保护下跑一次 execute_analysis_background。
    超过并发上限时会在 acquire 处等待，不会阻塞 event loop。
    """
    sem = _get_semaphore()
    async with sem:
        logger.info(
            f"🚀 [{label}] 开始执行 task={task_id} stock={stock_code} "
            f"(并发槽: {_FAV_CONCURRENCY - sem._value}/{_FAV_CONCURRENCY} 占用)"
        )
        try:
            await simple_service.execute_analysis_background(task_id, user_id, req)
            logger.info(f"✅ [{label}] 完成 task={task_id} stock={stock_code}")
        except Exception as e:
            logger.error(
                f"❌ [{label}] 执行失败 task={task_id} stock={stock_code}: {e}",
                exc_info=True,
            )


async def _run_for_markets(markets: Iterable[str], label: str) -> dict:
    """
    遍历 user_favorites，按市场过滤后逐只触发分析。

    - create_analysis_task 串行 await（只是写 DB，几十毫秒）
    - execute_analysis_background 包在模块级 semaphore 内，超限排队
    - scheduler 协程本身在所有入队完成后立即返回，不等分析跑完
    """
    markets_tuple = tuple(markets)
    db = get_mongo_db()
    simple_service = get_simple_analysis_service()

    stats = {"users": 0, "stocks": 0, "success": 0, "failed": 0, "errors": []}

    try:
        # user_favorites 里每个用户一个文档，遍历所有用户
        async for doc in db.user_favorites.find({}):
            user_id = str(doc.get("user_id")) if doc.get("user_id") else None
            if not user_id:
                continue

            user_favs: List[dict] = [
                f for f in (doc.get("favorites") or [])
                if f.get("market") in markets_tuple and f.get("stock_code")
            ]
            if not user_favs:
                continue

            stats["users"] += 1

            for fav in user_favs:
                stats["stocks"] += 1
                stock_code = fav["stock_code"]
                market = fav.get("market") or "auto"

                try:
                    req = SingleAnalysisRequest(
                        symbol=stock_code,
                        parameters=AnalysisParameters(
                            market_type=market,
                            research_depth=RESEARCH_DEPTH,
                        ),
                    )
                    create_res = await simple_service.create_analysis_task(user_id, req)
                    task_id = create_res.get("task_id")
                    if not task_id:
                        raise RuntimeError("create_analysis_task 未返回 task_id")

                    # 包在 semaphore 内，超过并发上限会在 async with 处等待
                    asyncio.create_task(
                        _run_single_analysis_with_throttle(
                            simple_service, task_id, user_id, req, label, stock_code
                        ),
                        name=f"fav_analysis/{label}/{user_id}/{stock_code}",
                    )
                    stats["success"] += 1
                    logger.info(
                        f"🟢 [{label}] 已入队: user={user_id} stock={stock_code} market={market} task={task_id}"
                    )
                except Exception as e:
                    stats["failed"] += 1
                    stats["errors"].append(f"{user_id}/{stock_code}: {e}")
                    logger.error(
                        f"❌ [{label}] 触发分析失败 user={user_id} stock={stock_code}: {e}",
                        exc_info=True,
                    )
    except Exception as e:
        logger.error(f"❌ [{label}] 调度器异常: {e}", exc_info=True)
        stats["errors"].append(f"scheduler: {e}")

    logger.info(
        f"✅ [{label}] 调度完成: users={stats['users']}, "
        f"stocks={stats['stocks']}, success={stats['success']}, failed={stats['failed']} "
        f"(并发上限={_FAV_CONCURRENCY})"
    )
    return stats


# -------- APScheduler 直接调用的 4 个入口 --------

async def run_favorites_analysis_a_hk_morning():
    """A 股 / 港股 · 盘中 10:30"""
    logger.info("📊 [自选股定时分析] A股/港股 · 盘中 10:30 开始")
    return await _run_for_markets(A_HK_MARKETS, "A股港股-盘中10:30")


async def run_favorites_analysis_a_hk_afternoon():
    """A 股 / 港股 · 收盘后 16:30"""
    logger.info("📊 [自选股定时分析] A股/港股 · 收盘后 16:30 开始")
    return await _run_for_markets(A_HK_MARKETS, "A股港股-收盘后16:30")


async def run_favorites_analysis_us_morning():
    """美股 · 09:30 (前一美股交易日收盘后复盘)"""
    logger.info("📊 [自选股定时分析] 美股 · 09:30 开始")
    return await _run_for_markets(US_MARKETS, "美股-09:30")


async def run_favorites_analysis_us_evening():
    """美股 · 22:30 (美股开盘后实时)"""
    logger.info("📊 [自选股定时分析] 美股 · 22:30 开始")
    return await _run_for_markets(US_MARKETS, "美股-22:30")
