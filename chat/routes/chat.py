"""
聊天API路由模块
处理聊天相关的API
"""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from agent_core import AgentManager, ReActAgent

router = APIRouter(tags=["chat"])


# 请求模型
class ChatRequest(BaseModel):
    message: str = Field(..., description="用户消息", min_length=1)
    session_id: Optional[str] = Field(None, description="会话ID，为空则创建新会话")


class ChatResponse(BaseModel):
    success: bool
    message: str
    session_id: str
    timestamp: str


@router.post("/api/chat", response_model=ChatResponse)
async def chat(request: ChatRequest):
    """
    聊天API（非流式）
    """
    try:
        agent: ReActAgent = AgentManager.get_agent(request.session_id)
        
        # 收集所有响应
        responses = []
        thought_process = []
        final_answer = ""
        
        async for response in agent.run(request.message):
            if response["type"] == "final_answer":
                final_answer = response["content"]
            elif response["type"] in ["thought", "action", "observation"]:
                thought_process.append(response)
            elif response["type"] == "error":
                raise HTTPException(status_code=500, detail=response["content"])
        
        return {
            "success": True,
            "answer": final_answer,
            "thought_process": thought_process,
            "session_id": agent.session_id,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
