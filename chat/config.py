"""
应用配置模块
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 项目根目录
BASE_DIR = Path(__file__).parent

# LLM配置
QWEN_API_KEY = os.getenv("QWEN_API_KEY", "sk-ccd80127c3314b98b67358b2e1f3e529")
QWEN_BASE_URL = os.getenv("QWEN_BASE_URL", "https://dashscope.aliyuncs.com/compatible-mode/v1")
QWEN_MODEL = os.getenv("QWEN_MODEL", "qwen3-max")

# 数据库配置
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "5432")),
    "database": os.getenv("DB_NAME", "EShopDB"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "123456"),
}

# 应用配置
APP_HOST = os.getenv("APP_HOST", "localhost")
APP_PORT = int(os.getenv("APP_PORT", "8000"))
DEBUG = os.getenv("DEBUG", "false").lower() == "true"

# CORS 允许的源
ALLOWED_ORIGINS = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# 记忆存储配置
MEMORY_DIR = Path(os.getenv("MEMORY_DIR", "./memories"))
MEMORY_DIR.mkdir(parents=True, exist_ok=True)

# 知识库配置（预留）
KNOWLEDGE_BASE_DIR = Path(os.getenv("KNOWLEDGE_BASE_DIR", "./knowledge_base"))
KNOWLEDGE_BASE_DIR.mkdir(parents=True, exist_ok=True)
