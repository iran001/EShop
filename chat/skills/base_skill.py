"""
Skill 基类定义
"""
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class BaseSkill(ABC):
    """技能基类"""
    
    name: str = ""
    description: str = ""
    
    @abstractmethod
    def execute(self, **kwargs) -> Any:
        """执行技能"""
        pass
    
    @abstractmethod
    def get_schema(self) -> Dict:
        """获取技能参数Schema"""
        pass
