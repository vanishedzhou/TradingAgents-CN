"""
CodeBuddy 开放平台适配器
========================================
Base URL : https://copilot.tencent.com
Endpoint : /v2/chat/completions  (仅支持 stream=True)
认证方式  : Authorization: Bearer <API_KEY>

可用模型（截至 2026-03-30）:
  腾讯混元 : hunyuan-2.0-instruct, hunyuan-2.0-thinking
  智谱 GLM : glm-5.0-turbo, glm-5.0, glm-4.7
  DeepSeek : deepseek-v3, deepseek-v3-0324, deepseek-r1
  Claude   : claude-opus-4.6, claude-opus-4.6-1m, claude-sonnet-4.6,
             claude-sonnet-4.6-1m, claude-opus-4.5, claude-haiku-4.5
  GPT      : gpt-5.4
  Kimi     : kimi-k2.5
  MiniMax  : minimax-m2.5
  Gemini   : gemini-2.5-pro, gemini-2.5-flash
  通用别名  : default (→glm-4.7), auto (→自动路由)

注意事项:
  - 模型名大小写敏感，必须使用小写（如 claude-opus-4.6）
  - stream 必须为 True，不支持非流式调用
  - 接口路径为 /v2/chat/completions，与 OpenAI 格式兼容
"""

import os
import logging
from typing import Optional

from tradingagents.llm_adapters.openai_compatible_base import OpenAICompatibleBase

logger = logging.getLogger(__name__)

# CodeBuddy API 配置
CODEBUDDY_BASE_URL = "https://copilot.tencent.com/v2"
CODEBUDDY_API_KEY_ENV = "CODEBUDDY_API_KEY"

# 支持的模型列表（可用于校验）
CODEBUDDY_MODELS = [
    # 腾讯混元
    "hunyuan-2.0-instruct",
    "hunyuan-2.0-thinking",
    # 智谱 GLM
    "glm-5.0-turbo",
    "glm-5.0",
    "glm-4.7",
    # DeepSeek
    "deepseek-v3",
    "deepseek-v3-0324",
    "deepseek-r1",
    # Claude
    "claude-opus-4.6",
    "claude-opus-4.6-1m",
    "claude-sonnet-4.6",
    "claude-sonnet-4.6-1m",
    "claude-opus-4.5",
    "claude-haiku-4.5",
    # GPT
    "gpt-5.4",
    # Kimi
    "kimi-k2.5",
    # MiniMax
    "minimax-m2.5",
    # Google Gemini
    "gemini-2.5-pro",
    "gemini-2.5-flash",
    # 通用别名
    "default",
    "auto",
]


class ChatCodeBuddy(OpenAICompatibleBase):
    """
    CodeBuddy 开放平台 LLM 适配器

    继承自 OpenAICompatibleBase（底层为 langchain_openai.ChatOpenAI），
    复用所有 OpenAI 兼容逻辑，只需指定 base_url 和 api_key_env_var。

    使用方式:
        llm = ChatCodeBuddy(model="hunyuan-2.0-instruct", api_key="your_key")
        response = llm.invoke([HumanMessage(content="你好")])
    """

    def __init__(
        self,
        model: str = "hunyuan-2.0-instruct",
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        temperature: float = 0.1,
        max_tokens: Optional[int] = None,
        **kwargs,
    ):
        """
        初始化 CodeBuddy 适配器

        Args:
            model       : 模型名（见 CODEBUDDY_MODELS，大小写敏感，需小写）
            api_key     : CodeBuddy API Key（未提供则读 CODEBUDDY_API_KEY 环境变量）
            base_url    : 自定义 API 地址（一般不需要修改）
            temperature : 温度参数，默认 0.1
            max_tokens  : 最大输出 token 数
        """
        resolved_base_url = base_url or CODEBUDDY_BASE_URL

        # 模型名大小写校验（非硬性阻断，只是日志提醒）
        if model != model.lower() and model not in ("default", "auto"):
            logger.warning(
                f"⚠️ [CodeBuddy] 模型名 '{model}' 包含大写，"
                f"CodeBuddy API 大小写敏感，建议改为 '{model.lower()}'"
            )

        # ⚠️ CodeBuddy 强制要求 stream=True，必须启用 streaming
        # 否则返回 400 Bad Request
        kwargs.setdefault("streaming", True)

        super().__init__(
            provider_name="codebuddy",
            model=model,
            api_key_env_var=CODEBUDDY_API_KEY_ENV,
            base_url=resolved_base_url,
            api_key=api_key,
            temperature=temperature,
            max_tokens=max_tokens,
            **kwargs,
        )
        logger.info(f"✅ CodeBuddy 适配器初始化完成 | 模型: {model} | Base URL: {resolved_base_url}")
