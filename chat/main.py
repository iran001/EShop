"""
FastAPI Web 应用主入口
提供Web API和静态文件服务
"""
import json

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from config import APP_HOST, APP_PORT, DEBUG, ALLOWED_ORIGINS
from routes import pages_router, chat_router, sessions_router
from agent_core import AgentManager


# 创建FastAPI应用
app = FastAPI(
    title="AI Agent Chat",
    description="基于LangChain ReAct的AI Agent对话系统",
    version="1.0.0",
    debug=DEBUG
)

# 配置CORS - 开发环境允许所有源
# 注意：WebSocket 的 CORS 由浏览器强制执行，服务器只能检查 Origin 头部
allowed_origins = ["*"] if ALLOWED_ORIGINS == ["*"] else ALLOWED_ORIGINS

print(f"CORS 允许的源: {allowed_origins}")

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"],
    max_age=600,
)

# 注册路由
app.include_router(pages_router)
app.include_router(chat_router)
app.include_router(sessions_router)


# WebSocket 路由直接定义在 app 上，避免 router 中间件问题
@app.websocket("/ws/chat")
async def websocket_chat(websocket: WebSocket):
    """
    WebSocket 聊天接口（流式响应）
    实时返回思考过程和最终答案
    """
    client_origin = websocket.headers.get("origin", "")
    print(f"WebSocket 连接请求，Origin: {client_origin}, Client: {websocket.client}")
    
    try:
        await websocket.accept()
        print(f"WebSocket 连接已接受")
        
        while True:
            # 接收消息
            data = await websocket.receive_text()
            message_data = json.loads(data)
            
            user_message = message_data.get("message", "")
            session_id = message_data.get("session_id")
            
            # 获取Agent
            agent = AgentManager.get_agent(session_id)
            
            # 发送会话ID
            await websocket.send_json({
                "type": "session",
                "session_id": agent.session_id
            })
            
            # 流式执行
            async for response in agent.run(user_message):
                await websocket.send_json(response)
            
    except WebSocketDisconnect:
        print("WebSocket 连接已断开")
    except Exception as e:
        print(f"WebSocket 错误: {e}")
        try:
            await websocket.send_json({
                "type": "error",
                "content": str(e)
            })
        except:
            pass
    finally:
        try:
            await websocket.close()
        except:
            pass


# 启动应用
if __name__ == "__main__":
    import uvicorn
    print(f"启动AI Agent服务...")
    print(f"访问 http://{APP_HOST}:{APP_PORT}/chat 开始对话")
    uvicorn.run(app, host=APP_HOST, port=APP_PORT)
