"""
会话管理路由模块
处理会话列表、记忆获取、删除等操作
"""
from fastapi import APIRouter, HTTPException, Query

from agent_core import AgentManager
from memory import MemoryManager, SessionMemory

router = APIRouter(prefix="/api/sessions", tags=["sessions"])


@router.get("/")
async def list_sessions():
    """获取最近10个会话列表"""
    sessions = SessionMemory.list_recent_sessions(limit=10)
    return {
        "success": True,
        "sessions": sessions
    }


@router.get("/{session_id}/memories")
async def get_session_memories(session_id: str, limit: int = Query(50, ge=1, le=100)):
    """获取指定会话的记忆"""
    try:
        memory = MemoryManager.get_session(session_id)
        memories = memory.get_memories(limit=limit)
        
        return {
            "success": True,
            "session_id": session_id,
            "memories": [
                {
                    "id": m.id,
                    "role": m.role,
                    "content": m.content,
                    "thought_process": m.thought_process,
                    "timestamp": m.timestamp,
                    "metadata": m.metadata
                }
                for m in memories
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{session_id}")
async def delete_session(session_id: str):
    """删除会话"""
    try:
        MemoryManager.delete_session(session_id)
        AgentManager.remove_agent(session_id)
        return {
            "success": True,
            "message": f"会话 {session_id} 已删除"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{session_id}/clear")
async def clear_session_memories(session_id: str):
    """清空会话记忆"""
    try:
        memory = MemoryManager.get_session(session_id)
        memory.clear()
        return {
            "success": True,
            "message": f"会话 {session_id} 的记忆已清空"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
