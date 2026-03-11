# AI Agent Chat - 基于 LangChain ReAct 的智能对话系统

一个基于LangChain ReAct Agent架构的智能AI对话系统，支持Qwen3-max大模型、PostgreSQL数据库查询、本地长期记忆等功能。

## 功能特性

- 🤖 **ReAct Agent**: 基于LangChain的ReAct推理-行动架构
- 🧠 **Qwen3-max**: 接入阿里云通义千问3-max大模型
- 🗄️ **数据库Skill**: 支持PostgreSQL数据库查询
- 📚 **知识库**: 预留知识库对接接口（可扩展向量检索）
- 💾 **长期记忆**: 基于本地文件的会话记忆管理
- 🌐 **Web界面**: 美观的浏览器对话界面，实时显示思考过程
- ⚡ **流式响应**: WebSocket支持实时流式响应

## 项目结构

```
chat/
├── main.py                    # FastAPI主应用
├── agent_core.py              # ReAct Agent核心
├── memory.py                  # 长期记忆管理
├── config.py                  # 配置文件
├── skills/                    # Agent技能
│   ├── __init__.py
│   ├── base_skill.py          # 技能基类
│   ├── database_skill.py      # 数据库技能
│   └── knowledge_base_skill.py # 知识库技能
├── requirements.txt           # 依赖
├── .env.example               # 环境变量示例
└── README.md                  # 说明文档
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env`，并填写配置：

```bash
cp .env.example .env
```

编辑 `.env` 文件：

```env
# Qwen3-max LLM配置
QWEN_API_KEY=your_qwen_api_key_here
QWEN_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
QWEN_MODEL=qwen3-max

# PostgreSQL数据库配置
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password

# 应用配置
APP_HOST=0.0.0.0
APP_PORT=8000
```

### 3. 启动服务

```bash
python main.py
```

### 4. 访问Web界面

打开浏览器访问：`http://localhost:8000/chat`

## API接口

### WebSocket 实时对话
- 地址: `ws://localhost:8000/ws/chat`
- 发送格式: `{"message": "你的问题", "session_id": "可选的会话ID"}`

### REST API

#### 发送消息（非流式）
```bash
POST /api/chat
Content-Type: application/json

{
    "message": "查询用户信息",
    "session_id": "可选的会话ID"
}
```

#### 获取会话列表
```bash
GET /api/sessions
```

#### 获取会话记忆
```bash
GET /api/sessions/{session_id}/memories
```

#### 删除会话
```bash
DELETE /api/sessions/{session_id}
```

## 可用工具

Agent内置以下工具：

1. **DatabaseQuery** - 查询PostgreSQL数据库
2. **KnowledgeBaseQuery** - 知识库查询（预留）
3. **GetDatabaseSchema** - 获取数据库结构

## 扩展开发

### 添加新的Skill

1. 在 `skills/` 目录创建新的skill文件
2. 继承 `BaseSkill` 基类
3. 在 `agent_core.py` 中注册工具

示例：

```python
from skills.base_skill import BaseSkill

class MySkill(BaseSkill):
    name = "my_skill"
    description = "我的技能描述"
    
    def execute(self, **kwargs):
        # 实现技能逻辑
        return {"success": True, "data": "结果"}
    
    def get_schema(self):
        return {
            "type": "object",
            "properties": {...}
        }
```

## 注意事项

1. 请确保PostgreSQL数据库可访问且配置正确
2. Qwen API Key需要从阿里云百炼平台获取
3. 会话记忆保存在 `./memories/` 目录下
4. 知识库功能需额外集成向量数据库

## 技术栈

- **Backend**: FastAPI, WebSocket
- **AI/ML**: LangChain, LangChain-OpenAI
- **Database**: PostgreSQL, SQLAlchemy
- **Frontend**: HTML5, CSS3, JavaScript (原生)
- **LLM**: Qwen3-max (阿里云)
