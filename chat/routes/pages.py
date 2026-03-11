"""
页面路由模块
处理HTML页面渲染和静态路由
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from config import BASE_DIR

router = APIRouter(tags=["pages"])

# 设置模板目录
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@router.get("/")
async def root():
    """根路径，返回基本信息"""
    return {
        "name": "AI Agent Chat API",
        "version": "1.0.0",
        "docs": "/docs",
        "chat_page": "/chat"
    }


@router.get("/chat", response_class=HTMLResponse)
async def chat_page(request: Request):
    """聊天页面 - 使用Jinja2模板渲染"""
    return templates.TemplateResponse("chat.html", {
        "request": request,
        "title": "AI Agent Chat",
        "welcome_message": "你好！我是你的AI助手。我可以帮你查询数据库、检索知识库信息。请问有什么可以帮你的吗？",
        "input_placeholder": "输入你的问题..."
    })
