"""
长期记忆管理模块
支持按session单独保存记忆到本地文件
"""
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, asdict
import uuid

from config import MEMORY_DIR


@dataclass
class MemoryEntry:
    """记忆条目"""
    id: str
    session_id: str
    role: str  # 'user' 或 'assistant'
    content: str
    thought_process: Optional[str]  # 思考过程
    timestamp: str
    metadata: Dict


class SessionMemory:
    """会话记忆管理器"""
    
    def __init__(self, session_id: str):
        self.session_id = session_id
        self.session_dir = MEMORY_DIR / session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.memory_file = self.session_dir / "memory.json"
        self.memories: List[MemoryEntry] = []
        self._load_memories()
    
    def _load_memories(self):
        """从文件加载记忆"""
        if self.memory_file.exists():
            try:
                with open(self.memory_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    self.memories = [MemoryEntry(**entry) for entry in data]
            except Exception as e:
                print(f"加载记忆失败: {e}")
                self.memories = []
    
    def _save_memories(self):
        """保存记忆到文件"""
        try:
            with open(self.memory_file, 'w', encoding='utf-8') as f:
                json.dump([asdict(m) for m in self.memories], f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"保存记忆失败: {e}")
    
    def add_memory(self, role: str, content: str, thought_process: Optional[str] = None, metadata: Dict = None):
        """添加记忆条目"""
        entry = MemoryEntry(
            id=str(uuid.uuid4()),
            session_id=self.session_id,
            role=role,
            content=content,
            thought_process=thought_process,
            timestamp=datetime.now().isoformat(),
            metadata=metadata or {}
        )
        self.memories.append(entry)
        self._save_memories()
        return entry
    
    def get_memories(self, limit: int = 50) -> List[MemoryEntry]:
        """获取最近N条记忆"""
        return self.memories[-limit:]
    
    def get_formatted_history(self, limit: int = 20) -> List[Dict]:
        """获取格式化的对话历史（用于LLM上下文）"""
        memories = self.get_memories(limit)
        history = []
        for m in memories:
            history.append({
                "role": m.role,
                "content": m.content
            })
        return history
    
    def clear(self):
        """清空当前会话的记忆"""
        self.memories = []
        if self.memory_file.exists():
            self.memory_file.unlink()
    
    @staticmethod
    def list_sessions() -> List[str]:
        """列出所有会话ID"""
        if not MEMORY_DIR.exists():
            return []
        return [d.name for d in MEMORY_DIR.iterdir() if d.is_dir()]
    
    @staticmethod
    def list_recent_sessions(limit: int = 10) -> List[Dict]:
        """
        列出最近的会话列表，包含预览信息
        
        Returns:
            会话列表，按最后活动时间排序，包含session_id、最后消息时间、预览内容
        """
        if not MEMORY_DIR.exists():
            return []
        
        sessions = []
        for session_dir in MEMORY_DIR.iterdir():
            if not session_dir.is_dir():
                continue
            
            session_id = session_dir.name
            memory_file = session_dir / "memory.json"
            
            if not memory_file.exists():
                continue
            
            try:
                with open(memory_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if not data:
                    continue
                
                # 获取最后一条消息
                last_message = data[-1]
                first_message = data[0] if data else None
                
                # 获取第一条用户消息作为标题预览
                preview = "新对话"
                for entry in data:
                    if entry.get('role') == 'user':
                        content = entry.get('content', '')
                        preview = content[:30] + '...' if len(content) > 30 else content
                        break
                
                sessions.append({
                    'session_id': session_id,
                    'preview': preview,
                    'last_message_time': last_message.get('timestamp', ''),
                    'message_count': len(data)
                })
            except Exception as e:
                print(f"读取会话 {session_id} 失败: {e}")
                continue
        
        # 按最后消息时间排序（最新的在前）
        sessions.sort(key=lambda x: x['last_message_time'], reverse=True)
        return sessions[:limit]
    
    @staticmethod
    def delete_session(session_id: str):
        """删除指定会话"""
        session_dir = MEMORY_DIR / session_id
        if session_dir.exists():
            import shutil
            shutil.rmtree(session_dir)


class MemoryManager:
    """记忆管理器工厂"""
    _instances: Dict[str, SessionMemory] = {}
    
    @classmethod
    def get_session(cls, session_id: Optional[str] = None) -> SessionMemory:
        """获取或创建会话记忆"""
        if session_id is None:
            session_id = str(uuid.uuid4())
        
        if session_id not in cls._instances:
            cls._instances[session_id] = SessionMemory(session_id)
        
        return cls._instances[session_id]
    
    @classmethod
    def clear_cache(cls):
        """清除缓存"""
        cls._instances.clear()
