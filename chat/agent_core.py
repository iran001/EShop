"""
LangChain ReAct Agent 核心模块
集成 Qwen3-max LLM - 兼容 LangChain 1.x
"""
import json
import re
from typing import AsyncGenerator, Dict, List, Optional, Any
from dataclasses import dataclass

from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langgraph.prebuilt import create_react_agent

from config import QWEN_API_KEY, QWEN_BASE_URL, QWEN_MODEL
from memory import MemoryManager, SessionMemory
from skills import DatabaseSkill


@dataclass
class AgentResponse:
    """Agent响应"""
    content: str
    thought_process: str
    tool_calls: List[Dict]
    session_id: str


class ReActAgent:
    """ReAct Agent 实现 - LangChain 1.x 版本"""
    
    def __init__(self, session_id: Optional[str] = None):
        self.session_id = session_id or self._generate_session_id()
        self.memory = MemoryManager.get_session(self.session_id)
        self.llm = self._create_llm()
        self.tools = self._create_tools()
        self.agent = self._create_agent()
    
    def _generate_session_id(self) -> str:
        """生成新的会话ID"""
        import uuid
        return str(uuid.uuid4())
    
    def _create_llm(self) -> ChatOpenAI:
        """创建Qwen3-max LLM实例"""
        return ChatOpenAI(
            model=QWEN_MODEL,
            api_key=QWEN_API_KEY,
            base_url=QWEN_BASE_URL,
            temperature=0.7,
            streaming=True
        )
    
    def _create_tools(self) -> List:
        """创建Agent工具 - 使用 DatabaseSkill 的类方法"""
        # 使用 DatabaseSkill 创建工具
        return DatabaseSkill.create_tools()
    
    def _create_agent(self):
        """创建ReAct Agent - 使用LangGraph"""
        system_prompt = """你是一个智能AI助手，可以使用工具来帮助用户解决问题。

你有以下工具可以使用来协助回答用户问题。请根据用户的问题选择合适的工具，或者直接用你的知识回答。

工具使用方法：
1. 分析用户问题
2. 如果需要，使用工具获取信息
3. 整合信息给出最终回答"""
        
        return create_react_agent(
            model=self.llm,
            tools=self.tools,
            prompt=system_prompt
        )
    
    def _format_chat_history(self) -> List:
        """格式化对话历史为消息列表"""
        memories = self.memory.get_memories(limit=5)
        history = []
        for m in memories:
            if m.role == "user":
                history.append(HumanMessage(content=m.content))
            elif m.role == "assistant":
                history.append(AIMessage(content=m.content))
        return history
    
    async def run(self, query: str) -> AsyncGenerator[Dict, None]:
        """
        运行Agent并流式返回结果
        
        Yields:
            {
                "type": "thought" | "action" | "observation" | "final_answer",
                "content": str
            }
        """
        # 获取对话历史
        chat_history = self._format_chat_history()
        
        # 构建输入消息
        messages = chat_history + [HumanMessage(content=query)]
        
        tool_calls = []
        
        try:
            # 执行Agent
            result = await self.agent.ainvoke({"messages": messages})
            
            # 获取最终输出
            final_messages = result.get("messages", [])
            final_answer = ""
            
            # 从消息中提取工具调用和最终回答
            for msg in final_messages:
                if hasattr(msg, 'tool_calls') and msg.tool_calls:
                    for tc in msg.tool_calls:
                        tool_calls.append({
                            "tool": tc.get('name', 'unknown'),
                            "input": str(tc.get('args', {}))
                        })
                        yield {
                            "type": "action",
                            "content": f"使用工具: {tc.get('name', 'unknown')}\n输入: {tc.get('args', {})}"
                        }
                elif hasattr(msg, 'content') and msg.content:
                    if isinstance(msg, AIMessage):
                        final_answer = msg.content
            
            # 如果没有找到最终回答，取最后一条AI消息
            if not final_answer and final_messages:
                for msg in reversed(final_messages):
                    if isinstance(msg, AIMessage) and msg.content:
                        final_answer = msg.content
                        break
            
            # 保存到记忆
            thought_text = "\n".join([f"{tc['tool']}: {tc['input']}" for tc in tool_calls])
            self.memory.add_memory(
                role="user",
                content=query,
                metadata={"type": "user_query"}
            )
            self.memory.add_memory(
                role="assistant",
                content=final_answer,
                thought_process=thought_text,
                metadata={"tool_calls": tool_calls}
            )
            
            yield {
                "type": "final_answer",
                "content": final_answer,
                "session_id": self.session_id
            }
            
        except Exception as e:
            error_msg = f"处理请求时出错: {str(e)}"
            yield {
                "type": "error",
                "content": error_msg
            }


class AgentManager:
    """Agent管理器"""
    _agents: Dict[str, ReActAgent] = {}
    
    @classmethod
    def get_agent(cls, session_id: Optional[str] = None) -> ReActAgent:
        """获取或创建Agent实例"""
        if session_id and session_id in cls._agents:
            return cls._agents[session_id]
        
        agent = ReActAgent(session_id)
        cls._agents[agent.session_id] = agent
        return agent
    
    @classmethod
    def remove_agent(cls, session_id: str):
        """移除Agent实例"""
        if session_id in cls._agents:
            del cls._agents[session_id]
