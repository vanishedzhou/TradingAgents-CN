"""
自选股管理API路由
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
import logging

from app.routers.auth_db import get_current_user
from app.models.user import User, FavoriteStock
from app.services.favorites_service import favorites_service
from app.core.response import ok

logger = logging.getLogger("webapi")

router = APIRouter(prefix="/favorites", tags=["自选股管理"])


class AddFavoriteRequest(BaseModel):
    """添加自选股请求"""
    stock_code: str
    stock_name: str
    market: str = "A股"
    tags: List[str] = []
    notes: str = ""
    alert_price_high: Optional[float] = None
    alert_price_low: Optional[float] = None


class UpdateFavoriteRequest(BaseModel):
    """更新自选股请求"""
    tags: Optional[List[str]] = None
    notes: Optional[str] = None
    alert_price_high: Optional[float] = None
    alert_price_low: Optional[float] = None


class FavoriteStockResponse(BaseModel):
    """自选股响应"""
    stock_code: str
    stock_name: str
    market: str
    added_at: str
    tags: List[str]
    notes: str
    alert_price_high: Optional[float]
    alert_price_low: Optional[float]
    # 实时数据
    current_price: Optional[float] = None
    change_percent: Optional[float] = None
    volume: Optional[int] = None


@router.get("/", response_model=dict)
async def get_favorites(
    current_user: dict = Depends(get_current_user)
):
    """获取用户自选股列表"""
    try:
        favorites = await favorites_service.get_user_favorites(current_user["id"])
        return ok(favorites)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取自选股失败: {str(e)}"
        )


@router.post("/", response_model=dict)
async def add_favorite(
    request: AddFavoriteRequest,
    current_user: dict = Depends(get_current_user)
):
    """添加股票到自选股"""
    import logging
    logger = logging.getLogger("webapi")

    try:
        logger.info(f"📝 添加自选股请求: user_id={current_user['id']}, stock_code={request.stock_code}, stock_name={request.stock_name}")

        # 检查是否已存在
        is_fav = await favorites_service.is_favorite(current_user["id"], request.stock_code)
        logger.info(f"🔍 检查是否已存在: {is_fav}")

        if is_fav:
            logger.warning(f"⚠️ 股票已在自选股中: {request.stock_code}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="该股票已在自选股中"
            )

        # 添加到自选股
        logger.info(f"➕ 开始添加自选股...")
        success = await favorites_service.add_favorite(
            user_id=current_user["id"],
            stock_code=request.stock_code,
            stock_name=request.stock_name,
            market=request.market,
            tags=request.tags,
            notes=request.notes,
            alert_price_high=request.alert_price_high,
            alert_price_low=request.alert_price_low
        )

        logger.info(f"✅ 添加结果: success={success}")

        if success:
            return ok({"stock_code": request.stock_code}, "添加成功")
        else:
            logger.error(f"❌ 添加失败: success=False")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="添加失败"
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 添加自选股异常: {type(e).__name__}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"添加自选股失败: {str(e)}"
        )


@router.put("/{stock_code}", response_model=dict)
async def update_favorite(
    stock_code: str,
    request: UpdateFavoriteRequest,
    current_user: dict = Depends(get_current_user)
):
    """更新自选股信息"""
    try:
        success = await favorites_service.update_favorite(
            user_id=current_user["id"],
            stock_code=stock_code,
            tags=request.tags,
            notes=request.notes,
            alert_price_high=request.alert_price_high,
            alert_price_low=request.alert_price_low
        )

        if success:
            return ok({"stock_code": stock_code}, "更新成功")
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="自选股不存在"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"更新自选股失败: {str(e)}"
        )


@router.delete("/{stock_code}", response_model=dict)
async def remove_favorite(
    stock_code: str,
    current_user: dict = Depends(get_current_user)
):
    """从自选股中移除股票"""
    try:
        success = await favorites_service.remove_favorite(current_user["id"], stock_code)

        if success:
            return ok({"stock_code": stock_code}, "移除成功")
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="自选股不存在"
            )

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"移除自选股失败: {str(e)}"
        )


@router.get("/check/{stock_code}", response_model=dict)
async def check_favorite(
    stock_code: str,
    current_user: dict = Depends(get_current_user)
):
    """检查股票是否在自选股中"""
    try:
        is_favorite = await favorites_service.is_favorite(current_user["id"], stock_code)
        return ok({"stock_code": stock_code, "is_favorite": is_favorite})
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"检查自选股状态失败: {str(e)}"
        )


@router.get("/tags", response_model=dict)
async def get_user_tags(
    current_user: dict = Depends(get_current_user)
):
    """获取用户使用的所有标签"""
    try:
        tags = await favorites_service.get_user_tags(current_user["id"])
        return ok(tags)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取标签失败: {str(e)}"
        )


class SyncFavoritesRequest(BaseModel):
    """同步自选股实时行情请求"""
    data_source: str = "tushare"  # tushare/akshare 仅对 A 股生效；美股走 yfinance，港股走 yfinance


@router.post("/sync-realtime", response_model=dict)
async def sync_favorites_realtime(
    request: SyncFavoritesRequest,
    current_user: dict = Depends(get_current_user)
):
    """
    同步自选股实时行情（按市场分发）

    - **data_source**: A 股数据源选择（tushare/akshare），美股/港股固定走 yfinance
    """
    try:
        logger.info(f"📊 开始同步自选股实时行情: user_id={current_user['id']}, a_share_data_source={request.data_source}")

        # 获取用户自选股列表
        favorites = await favorites_service.get_user_favorites(current_user["id"])

        if not favorites:
            logger.info("⚠️ 用户没有自选股")
            return ok({
                "total": 0,
                "success_count": 0,
                "failed_count": 0,
                "message": "没有自选股需要同步"
            })

        # 按市场分组
        by_market: dict = {"A股": [], "美股": [], "港股": []}
        unknown: list = []
        for fav in favorites:
            code = fav.get("stock_code") or fav.get("symbol")
            market = fav.get("market")
            if not code:
                continue
            if market in by_market:
                by_market[market].append(code)
            else:
                unknown.append(f"{code}({market})")

        logger.info(
            f"🎯 按市场分组: A股={len(by_market['A股'])} 只, "
            f"美股={len(by_market['美股'])} 只, "
            f"港股={len(by_market['港股'])} 只"
            + (f", 未知市场={unknown}" if unknown else "")
        )

        total_symbols = sum(len(v) for v in by_market.values())
        aggregate = {
            "total": total_symbols,
            "success_count": 0,
            "failed_count": 0,
            "by_market": {},
            "errors": [],
        }
        if unknown:
            aggregate["errors"].append(f"跳过未知市场股票: {unknown}")

        # --- A 股：按 request.data_source 走 tushare/akshare ---
        if by_market["A股"]:
            try:
                if request.data_source == "tushare":
                    from app.worker.tushare_sync_service import get_tushare_sync_service
                    a_service = await get_tushare_sync_service()
                elif request.data_source == "akshare":
                    from app.worker.akshare_sync_service import get_akshare_sync_service
                    a_service = await get_akshare_sync_service()
                else:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"不支持的 A 股数据源: {request.data_source}"
                    )

                if not a_service:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail=f"A 股数据源 {request.data_source} 不可用"
                    )

                a_result = await a_service.sync_realtime_quotes(
                    symbols=by_market["A股"], force=True
                )
                succ = a_result.get("success_count", 0)
                fail = a_result.get("failed_count", 0)
                aggregate["success_count"] += succ
                aggregate["failed_count"] += fail
                aggregate["by_market"]["A股"] = {
                    "data_source": request.data_source,
                    "success_count": succ,
                    "failed_count": fail,
                    "symbols": by_market["A股"],
                }
                logger.info(f"✅ A股同步: 成功 {succ}/{len(by_market['A股'])} 只")
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"❌ A股同步异常: {e}", exc_info=True)
                aggregate["failed_count"] += len(by_market["A股"])
                aggregate["errors"].append(f"A股: {e}")
                aggregate["by_market"]["A股"] = {
                    "data_source": request.data_source,
                    "success_count": 0,
                    "failed_count": len(by_market["A股"]),
                    "error": str(e),
                }

        # --- 美股：yfinance ---
        if by_market["美股"]:
            try:
                from app.worker.us_sync_service import get_us_sync_service
                us_service = await get_us_sync_service()
                us_result = await us_service.sync_realtime_quotes(
                    symbols=by_market["美股"], force=True
                )
                succ = us_result.get("success_count", 0)
                fail = us_result.get("failed_count", 0)
                aggregate["success_count"] += succ
                aggregate["failed_count"] += fail
                aggregate["by_market"]["美股"] = {
                    "data_source": "yfinance",
                    "success_count": succ,
                    "failed_count": fail,
                    "symbols": by_market["美股"],
                }
                logger.info(f"✅ 美股同步: 成功 {succ}/{len(by_market['美股'])} 只")
            except Exception as e:
                logger.error(f"❌ 美股同步异常: {e}", exc_info=True)
                aggregate["failed_count"] += len(by_market["美股"])
                aggregate["errors"].append(f"美股: {e}")
                aggregate["by_market"]["美股"] = {
                    "data_source": "yfinance",
                    "success_count": 0,
                    "failed_count": len(by_market["美股"]),
                    "error": str(e),
                }

        # --- 港股：yfinance ---
        if by_market["港股"]:
            try:
                from app.worker.hk_sync_service import get_hk_sync_service
                hk_service = await get_hk_sync_service()
                hk_result = await hk_service.sync_realtime_quotes(
                    symbols=by_market["港股"], force=True
                )
                succ = hk_result.get("success_count", 0)
                fail = hk_result.get("failed_count", 0)
                aggregate["success_count"] += succ
                aggregate["failed_count"] += fail
                aggregate["by_market"]["港股"] = {
                    "data_source": "yfinance",
                    "success_count": succ,
                    "failed_count": fail,
                    "symbols": by_market["港股"],
                }
                logger.info(f"✅ 港股同步: 成功 {succ}/{len(by_market['港股'])} 只")
            except Exception as e:
                logger.error(f"❌ 港股同步异常: {e}", exc_info=True)
                aggregate["failed_count"] += len(by_market["港股"])
                aggregate["errors"].append(f"港股: {e}")
                aggregate["by_market"]["港股"] = {
                    "data_source": "yfinance",
                    "success_count": 0,
                    "failed_count": len(by_market["港股"]),
                    "error": str(e),
                }

        aggregate["message"] = (
            f"同步完成: 成功 {aggregate['success_count']} / 总计 {aggregate['total']} 只 "
            f"(A股 {len(by_market['A股'])} / 美股 {len(by_market['美股'])} / 港股 {len(by_market['港股'])})"
        )
        logger.info(f"✅ 自选股实时行情同步总览: {aggregate['message']}")
        return ok(aggregate)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ 同步自选股实时行情失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"同步失败: {str(e)}"
        )


@router.get("/analysis-history", response_model=dict)
async def get_favorites_analysis_history(
    symbols: Optional[str] = Query(
        None,
        description="逗号分隔的股票代码过滤，空则返回所有自选股"
    ),
    market: Optional[str] = Query(None, description="市场过滤: A股/美股/港股"),
    limit: int = Query(100, ge=1, le=500, description="每只股票最多返回多少个分析点"),
    current_user: dict = Depends(get_current_user),
):
    """
    返回当前用户自选股的 AI 分析历史（按股票分组的时间序列）。

    每个点包含：分析时间、当时股价、AI 目标价、预计收益率、买/卖/持有、置信度。
    """
    try:
        symbol_list: Optional[List[str]] = None
        if symbols:
            symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]

        series = await favorites_service.get_analysis_history(
            user_id=current_user["id"],
            symbols=symbol_list,
            market=market,
            limit=limit,
        )

        total_points = sum(len(s.get("points", [])) for s in series)
        logger.info(
            f"📈 分析历史查询 user={current_user['id']} "
            f"symbols={symbol_list} market={market} → {len(series)} 只股票, {total_points} 个点"
        )
        return ok({
            "series": series,
            "total_symbols": len(series),
            "total_points": total_points,
        })
    except Exception as e:
        logger.error(f"❌ 获取自选股分析历史失败: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"获取分析历史失败: {str(e)}"
        )
