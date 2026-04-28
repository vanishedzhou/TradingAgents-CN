"""
自选股服务
"""

from typing import List, Optional, Dict, Any
from datetime import datetime
from bson import ObjectId

from app.core.database import get_mongo_db
from app.models.user import FavoriteStock
from app.services.quotes_service import get_quotes_service


class FavoritesService:
    """自选股服务类"""
    
    def __init__(self):
        self.db = None
    
    async def _get_db(self):
        """获取数据库连接"""
        if self.db is None:
            self.db = get_mongo_db()
        return self.db

    def _is_valid_object_id(self, user_id: str) -> bool:
        """
        检查是否是有效的ObjectId格式
        注意：这里只检查格式，不代表数据库中实际存储的是ObjectId类型
        为了兼容性，我们统一使用 user_favorites 集合存储自选股
        """
        # 强制返回 False，统一使用 user_favorites 集合
        return False

    def _format_favorite(self, favorite: Dict[str, Any]) -> Dict[str, Any]:
        """格式化收藏条目（仅基础信息，不包含实时行情）。
        行情将在 get_user_favorites 中批量富集。
        """
        added_at = favorite.get("added_at")
        if isinstance(added_at, datetime):
            added_at = added_at.isoformat()
        return {
            "stock_code": favorite.get("stock_code"),
            "stock_name": favorite.get("stock_name"),
            "market": favorite.get("market", "A股"),
            "added_at": added_at,
            "tags": favorite.get("tags", []),
            "notes": favorite.get("notes", ""),
            "alert_price_high": favorite.get("alert_price_high"),
            "alert_price_low": favorite.get("alert_price_low"),
            # 行情占位，稍后填充
            "current_price": None,
            "change_percent": None,
            "volume": None,
        }

    async def get_user_favorites(self, user_id: str) -> List[Dict[str, Any]]:
        """获取用户自选股列表，并批量拉取实时行情进行富集（兼容字符串ID与ObjectId）。"""
        db = await self._get_db()

        favorites: List[Dict[str, Any]] = []
        if self._is_valid_object_id(user_id):
            # 先尝试使用 ObjectId 查询
            user = await db.users.find_one({"_id": ObjectId(user_id)})
            # 如果 ObjectId 查询失败，尝试使用字符串查询
            if user is None:
                user = await db.users.find_one({"_id": user_id})
            favorites = (user or {}).get("favorite_stocks", [])
        else:
            doc = await db.user_favorites.find_one({"user_id": user_id})
            favorites = (doc or {}).get("favorites", [])

        # 先格式化基础字段
        items = [self._format_favorite(fav) for fav in favorites]

        # 批量获取股票基础信息（板块等）
        codes = [it.get("stock_code") for it in items if it.get("stock_code")]
        if codes:
            try:
                # 🔥 获取数据源优先级配置
                from app.core.unified_config import UnifiedConfigManager
                config = UnifiedConfigManager()
                data_source_configs = await config.get_data_source_configs_async()

                # 提取启用的数据源，按优先级排序
                enabled_sources = [
                    ds.type.lower() for ds in data_source_configs
                    if ds.enabled and ds.type.lower() in ['tushare', 'akshare', 'baostock']
                ]

                if not enabled_sources:
                    enabled_sources = ['tushare', 'akshare', 'baostock']

                preferred_source = enabled_sources[0] if enabled_sources else 'tushare'

                # 从 stock_basic_info 获取板块信息（只查询优先级最高的数据源）
                basic_info_coll = db["stock_basic_info"]
                cursor = basic_info_coll.find(
                    {"code": {"$in": codes}, "source": preferred_source},  # 🔥 添加数据源筛选
                    {"code": 1, "sse": 1, "market": 1, "_id": 0}
                )
                basic_docs = await cursor.to_list(length=None)
                basic_map = {str(d.get("code")).zfill(6): d for d in (basic_docs or [])}

                for it in items:
                    code = it.get("stock_code")
                    basic = basic_map.get(code)
                    if basic:
                        # market 字段表示板块（主板、创业板、科创板等）
                        it["board"] = basic.get("market", "-")
                        # sse 字段表示交易所（上海证券交易所、深圳证券交易所等）
                        it["exchange"] = basic.get("sse", "-")
                    else:
                        it["board"] = "-"
                        it["exchange"] = "-"
            except Exception as e:
                # 查询失败时设置默认值
                for it in items:
                    it["board"] = "-"
                    it["exchange"] = "-"

        # 批量获取行情（按市场从不同集合读取）
        # A股   → market_quotes        code 6 位补零
        # 美股  → market_quotes_us     code 大写
        # 港股  → market_quotes_hk     code 5 位补零
        if codes:
            try:
                # 按市场把 items 分成三组
                a_codes: list = []
                us_codes: list = []
                hk_codes: list = []
                for it in items:
                    code = it.get("stock_code")
                    if not code:
                        continue
                    market = it.get("market") or "A股"
                    if market == "美股":
                        us_codes.append(str(code).strip().upper())
                    elif market == "港股":
                        hk_codes.append(str(code).strip().upper().replace('.HK', '').lstrip('0').zfill(5))
                    else:
                        a_codes.append(code)

                quotes_a: dict = {}
                quotes_us: dict = {}
                quotes_hk: dict = {}

                # --- A 股 ---
                if a_codes:
                    coll = db["market_quotes"]
                    cursor = coll.find(
                        {"code": {"$in": a_codes}},
                        {"code": 1, "close": 1, "pct_chg": 1, "amount": 1}
                    )
                    docs = await cursor.to_list(length=None)
                    quotes_a = {str(d.get("code")).zfill(6): d for d in (docs or [])}

                # --- 美股 ---
                if us_codes:
                    coll = db["market_quotes_us"]
                    cursor = coll.find(
                        {"code": {"$in": us_codes}},
                        {"code": 1, "close": 1, "pct_chg": 1, "volume": 1}
                    )
                    docs = await cursor.to_list(length=None)
                    quotes_us = {str(d.get("code")).upper(): d for d in (docs or [])}

                # --- 港股 ---
                if hk_codes:
                    coll = db["market_quotes_hk"]
                    cursor = coll.find(
                        {"code": {"$in": hk_codes}},
                        {"code": 1, "close": 1, "pct_chg": 1, "volume": 1}
                    )
                    docs = await cursor.to_list(length=None)
                    quotes_hk = {str(d.get("code")).zfill(5): d for d in (docs or [])}

                # 写回 items.current_price / change_percent
                for it in items:
                    code = it.get("stock_code")
                    if not code:
                        continue
                    market = it.get("market") or "A股"
                    q = None
                    if market == "美股":
                        q = quotes_us.get(str(code).strip().upper())
                    elif market == "港股":
                        q = quotes_hk.get(str(code).strip().upper().replace('.HK', '').lstrip('0').zfill(5))
                    else:
                        q = quotes_a.get(code)
                    if q:
                        it["current_price"] = q.get("close")
                        it["change_percent"] = q.get("pct_chg")

                # 兜底：对未命中的 A 股代码用在线源补齐；美股/港股暂不在线兜底（需用户点击同步）
                missing_a = [c for c in a_codes if c not in quotes_a]
                if missing_a:
                    try:
                        quotes_online = await get_quotes_service().get_quotes(missing_a)
                        for it in items:
                            if (it.get("market") or "A股") != "A股":
                                continue
                            code = it.get("stock_code")
                            if it.get("current_price") is None:
                                q2 = quotes_online.get(code, {}) if quotes_online else {}
                                it["current_price"] = q2.get("close")
                                it["change_percent"] = q2.get("pct_chg")
                    except Exception:
                        pass
            except Exception:
                # 查询失败时保持占位 None，避免影响基础功能
                pass

        return items

    async def add_favorite(
        self,
        user_id: str,
        stock_code: str,
        stock_name: str,
        market: str = "A股",
        tags: List[str] = None,
        notes: str = "",
        alert_price_high: Optional[float] = None,
        alert_price_low: Optional[float] = None
    ) -> bool:
        """添加股票到自选股（兼容字符串ID与ObjectId）"""
        import logging
        logger = logging.getLogger("webapi")

        try:
            logger.info(f"🔧 [add_favorite] 开始添加自选股: user_id={user_id}, stock_code={stock_code}")

            db = await self._get_db()
            logger.info(f"🔧 [add_favorite] 数据库连接获取成功")

            favorite_stock = {
                "stock_code": stock_code,
                "stock_name": stock_name,
                "market": market,
                "added_at": datetime.utcnow(),
                "tags": tags or [],
                "notes": notes,
                "alert_price_high": alert_price_high,
                "alert_price_low": alert_price_low
            }

            logger.info(f"🔧 [add_favorite] 自选股数据构建完成: {favorite_stock}")

            is_oid = self._is_valid_object_id(user_id)
            logger.info(f"🔧 [add_favorite] 用户ID类型检查: is_valid_object_id={is_oid}")

            if is_oid:
                logger.info(f"🔧 [add_favorite] 使用 ObjectId 方式添加到 users 集合")

                # 先尝试使用 ObjectId 查询
                result = await db.users.update_one(
                    {"_id": ObjectId(user_id)},
                    {
                        "$push": {"favorite_stocks": favorite_stock},
                        "$setOnInsert": {"favorite_stocks": []}
                    }
                )
                logger.info(f"🔧 [add_favorite] ObjectId查询结果: matched_count={result.matched_count}, modified_count={result.modified_count}")

                # 如果 ObjectId 查询失败，尝试使用字符串查询
                if result.matched_count == 0:
                    logger.info(f"🔧 [add_favorite] ObjectId查询失败，尝试使用字符串ID查询")
                    result = await db.users.update_one(
                        {"_id": user_id},
                        {
                            "$push": {"favorite_stocks": favorite_stock}
                        }
                    )
                    logger.info(f"🔧 [add_favorite] 字符串ID查询结果: matched_count={result.matched_count}, modified_count={result.modified_count}")

                success = result.matched_count > 0
                logger.info(f"🔧 [add_favorite] 返回结果: {success}")
                return success
            else:
                logger.info(f"🔧 [add_favorite] 使用字符串ID方式添加到 user_favorites 集合")
                result = await db.user_favorites.update_one(
                    {"user_id": user_id},
                    {
                        "$setOnInsert": {"user_id": user_id, "created_at": datetime.utcnow()},
                        "$push": {"favorites": favorite_stock},
                        "$set": {"updated_at": datetime.utcnow()}
                    },
                    upsert=True
                )
                logger.info(f"🔧 [add_favorite] 更新结果: matched_count={result.matched_count}, modified_count={result.modified_count}, upserted_id={result.upserted_id}")
                logger.info(f"🔧 [add_favorite] 返回结果: True")
                return True
        except Exception as e:
            logger.error(f"❌ [add_favorite] 添加自选股异常: {type(e).__name__}: {str(e)}", exc_info=True)
            raise

    async def remove_favorite(self, user_id: str, stock_code: str) -> bool:
        """从自选股中移除股票（兼容字符串ID与ObjectId）"""
        db = await self._get_db()

        if self._is_valid_object_id(user_id):
            # 先尝试使用 ObjectId 查询
            result = await db.users.update_one(
                {"_id": ObjectId(user_id)},
                {"$pull": {"favorite_stocks": {"stock_code": stock_code}}}
            )
            # 如果 ObjectId 查询失败，尝试使用字符串查询
            if result.matched_count == 0:
                result = await db.users.update_one(
                    {"_id": user_id},
                    {"$pull": {"favorite_stocks": {"stock_code": stock_code}}}
                )
            return result.modified_count > 0
        else:
            result = await db.user_favorites.update_one(
                {"user_id": user_id},
                {
                    "$pull": {"favorites": {"stock_code": stock_code}},
                    "$set": {"updated_at": datetime.utcnow()}
                }
            )
            return result.modified_count > 0

    async def update_favorite(
        self,
        user_id: str,
        stock_code: str,
        tags: Optional[List[str]] = None,
        notes: Optional[str] = None,
        alert_price_high: Optional[float] = None,
        alert_price_low: Optional[float] = None
    ) -> bool:
        """更新自选股信息（兼容字符串ID与ObjectId）"""
        db = await self._get_db()

        # 统一构建更新字段（根据不同集合的字段路径设置前缀）
        is_oid = self._is_valid_object_id(user_id)
        prefix = "favorite_stocks.$." if is_oid else "favorites.$."
        update_fields: Dict[str, Any] = {}
        if tags is not None:
            update_fields[prefix + "tags"] = tags
        if notes is not None:
            update_fields[prefix + "notes"] = notes
        if alert_price_high is not None:
            update_fields[prefix + "alert_price_high"] = alert_price_high
        if alert_price_low is not None:
            update_fields[prefix + "alert_price_low"] = alert_price_low

        if not update_fields:
            return True

        if is_oid:
            result = await db.users.update_one(
                {
                    "_id": ObjectId(user_id),
                    "favorite_stocks.stock_code": stock_code
                },
                {"$set": update_fields}
            )
            return result.modified_count > 0
        else:
            result = await db.user_favorites.update_one(
                {
                    "user_id": user_id,
                    "favorites.stock_code": stock_code
                },
                {
                    "$set": {
                        **update_fields,
                        "updated_at": datetime.utcnow()
                    }
                }
            )
            return result.modified_count > 0

    async def is_favorite(self, user_id: str, stock_code: str) -> bool:
        """检查股票是否在自选股中（兼容字符串ID与ObjectId）"""
        import logging
        logger = logging.getLogger("webapi")

        try:
            logger.info(f"🔧 [is_favorite] 检查自选股: user_id={user_id}, stock_code={stock_code}")

            db = await self._get_db()

            is_oid = self._is_valid_object_id(user_id)
            logger.info(f"🔧 [is_favorite] 用户ID类型: is_valid_object_id={is_oid}")

            if is_oid:
                # 先尝试使用 ObjectId 查询
                user = await db.users.find_one(
                    {
                        "_id": ObjectId(user_id),
                        "favorite_stocks.stock_code": stock_code
                    }
                )

                # 如果 ObjectId 查询失败，尝试使用字符串查询
                if user is None:
                    logger.info(f"🔧 [is_favorite] ObjectId查询未找到，尝试使用字符串ID查询")
                    user = await db.users.find_one(
                        {
                            "_id": user_id,
                            "favorite_stocks.stock_code": stock_code
                        }
                    )

                result = user is not None
                logger.info(f"🔧 [is_favorite] 查询结果: {result}")
                return result
            else:
                doc = await db.user_favorites.find_one(
                    {
                        "user_id": user_id,
                        "favorites.stock_code": stock_code
                    }
                )
                result = doc is not None
                logger.info(f"🔧 [is_favorite] 字符串ID查询结果: {result}")
                return result
        except Exception as e:
            logger.error(f"❌ [is_favorite] 检查自选股异常: {type(e).__name__}: {str(e)}", exc_info=True)
            raise

    async def get_user_tags(self, user_id: str) -> List[str]:
        """获取用户使用的所有标签（兼容字符串ID与ObjectId）"""
        db = await self._get_db()

        if self._is_valid_object_id(user_id):
            pipeline = [
                {"$match": {"_id": ObjectId(user_id)}},
                {"$unwind": "$favorite_stocks"},
                {"$unwind": "$favorite_stocks.tags"},
                {"$group": {"_id": "$favorite_stocks.tags"}},
                {"$sort": {"_id": 1}}
            ]
            result = await db.users.aggregate(pipeline).to_list(None)
        else:
            pipeline = [
                {"$match": {"user_id": user_id}},
                {"$unwind": "$favorites"},
                {"$unwind": "$favorites.tags"},
                {"$group": {"_id": "$favorites.tags"}},
                {"$sort": {"_id": 1}}
            ]
            result = await db.user_favorites.aggregate(pipeline).to_list(None)

        return [item["_id"] for item in result if item.get("_id")]

    def _get_mock_price(self, stock_code: str) -> float:
        """获取模拟股价"""
        # 基于股票代码生成模拟价格
        base_price = hash(stock_code) % 100 + 10
        return round(base_price + (hash(stock_code) % 1000) / 100, 2)
    
    def _get_mock_change(self, stock_code: str) -> float:
        """获取模拟涨跌幅"""
        # 基于股票代码生成模拟涨跌幅
        change = (hash(stock_code) % 2000 - 1000) / 100
        return round(change, 2)
    
    def _get_mock_volume(self, stock_code: str) -> int:
        """获取模拟成交量"""
        # 基于股票代码生成模拟成交量
        return (hash(stock_code) % 10000 + 1000) * 100

    async def get_analysis_history(
        self,
        user_id: str,
        symbols: Optional[List[str]] = None,
        market: Optional[str] = None,
        limit: int = 100,
        days: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取用户自选股的 AI 分析历史（按股票分组的时间序列）。

        参数
            user_id: 当前用户（用于默认取自选股范围）
            symbols: 可选，只返回这些股票代码的历史；None 时取 user_favorites 全部
            market:  可选市场过滤（"A股" / "美股" / "港股"）
            limit:   每只股票最多返回多少条（按时间倒序）
            days:    可选，只返回近 N 天（按 created_at，UTC now 向前推）；None 或 <=0 表示不限

        返回
            [
              {
                "stock_code": "AAPL",
                "stock_name": "苹果",
                "market":     "美股",
                "points": [
                    {
                      "analysis_id":      str,
                      "analyzed_at":      ISO str,
                      "current_price":    float | None,
                      "target_price":     float | None,
                      "expected_return":  float | None,   # % 单位
                      "action":           "买入" | "卖出" | "持有" | None,
                      "confidence":       float | None,
                    },
                    ...
                ]
              },
              ...
            ]
        """
        from app.utils.report_parser import (
            extract_current_price_from_reports,
            extract_target_price_from_reports,
        )

        db = await self._get_db()

        # 1) 解析待查 stock_code → {stock_name, market}
        # 先读一次自选股，按需过滤
        favorites = await self.get_user_favorites(user_id)  # 已带 market / stock_name
        fav_map: Dict[str, Dict[str, Any]] = {}
        for f in favorites:
            code = f.get("stock_code")
            if not code:
                continue
            fav_market = f.get("market")
            if market and fav_market != market:
                continue
            if symbols and code not in symbols:
                continue
            fav_map[code] = {
                "stock_code": code,
                "stock_name": f.get("stock_name") or code,
                "market": fav_market,
            }

        if not fav_map:
            return []

        # 2) 从 analysis_reports 拉记录。analysis_reports 是"全局"的，不绑 user_id，
        #    所以按 stock_symbol + (可选) market_type 过滤即可。
        #    同一股票在数据库里可能有多种代码写法（例如 1810.HK / 01810），这里
        #    用宽松 $in 匹配用户自选股里记录的那份，不强制归一。
        # 时间窗过滤：days>0 时只取最近 N 天，避免"全部"返回时爆体积
        min_created_at = None
        if days and days > 0:
            from datetime import timezone as _tz, timedelta as _td
            min_created_at = datetime.now(_tz.utc) - _td(days=days)

        series_result: List[Dict[str, Any]] = []
        for code, meta in fav_map.items():
            query: Dict[str, Any] = {"stock_symbol": code}
            if meta.get("market"):
                query["market_type"] = meta["market"]
            if min_created_at is not None:
                # MongoDB 里 created_at 是 naive UTC datetime，pymongo 在比较时会
                # 把带时区的 datetime 归一到 naive UTC，这里直接传带时区也能工作
                query["created_at"] = {"$gte": min_created_at.replace(tzinfo=None)}

            cursor = db.analysis_reports.find(query).sort("created_at", -1).limit(limit)
            docs = await cursor.to_list(length=limit)

            points: List[Dict[str, Any]] = []
            for d in docs:
                decision = d.get("decision") or {}
                reports = d.get("reports") or {}

                # 目标价：优先 decision.target_price，回退到 reports 文本
                target_price = decision.get("target_price")
                if not isinstance(target_price, (int, float)) or target_price is None:
                    target_price = extract_target_price_from_reports(reports)

                # 当时股价：先尝试 decision.current_price（新数据可能有），
                # 再 fallback 到 reports.market_report 正则提取
                current_price = decision.get("current_price")
                if not isinstance(current_price, (int, float)) or current_price is None:
                    current_price = extract_current_price_from_reports(reports)

                action = decision.get("action")
                confidence = decision.get("confidence")

                expected_return = None
                if (isinstance(current_price, (int, float)) and current_price > 0
                        and isinstance(target_price, (int, float))):
                    expected_return = round((target_price - current_price) / current_price * 100, 2)

                created_at = d.get("created_at")
                if isinstance(created_at, datetime):
                    # MongoDB 存的是 UTC naive datetime，直接 isoformat 会丢时区后缀，
                    # 浏览器 new Date() 会误当成本地时间解析，导致图表时间戳差 8 小时。
                    # 这里显式标成 UTC 后再 isoformat，输出 "...+00:00"。
                    if created_at.tzinfo is None:
                        from datetime import timezone as _tz
                        created_at = created_at.replace(tzinfo=_tz.utc)
                    analyzed_at = created_at.isoformat()
                else:
                    analyzed_at = str(created_at) if created_at else None

                points.append({
                    "analysis_id": d.get("analysis_id") or str(d.get("_id")),
                    "analyzed_at": analyzed_at,
                    "current_price": current_price,
                    "target_price": target_price,
                    "expected_return": expected_return,
                    "action": action,
                    "confidence": confidence,
                })

            # 按时间正序（前端折线图更自然），反转之前倒序结果
            points.reverse()
            series_result.append({
                **meta,
                "points": points,
            })

        return series_result


# 创建全局实例
favorites_service = FavoritesService()
