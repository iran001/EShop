"""
路由模块
包含所有API路由和页面路由
"""
from .pages import router as pages_router
from .chat import router as chat_router
from .sessions import router as sessions_router

__all__ = ["pages_router", "chat_router", "sessions_router"]
