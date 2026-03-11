#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
API服务器
"""
import os
import sys
import json
from http.server import HTTPServer, BaseHTTPRequestHandler

# 确保使用UTF-8编码
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 添加项目根目录到Python路径
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from services.sync_service import sync_sale_records, sync_sale_details, sync_vip_info, sync_pro_info, sync_weather_data
from data.repository import init_database
from job_scheduler import init_scheduler
from config import JOB_CONFIG

# 模板目录路径
TEMPLATE_DIR = os.path.join(os.path.dirname(__file__), 'templates')


def render_template(template_name, **kwargs):
    """渲染模板文件，替换变量"""
    template_path = os.path.join(TEMPLATE_DIR, template_name)
    try:
        with open(template_path, 'r', encoding='utf-8') as f:
            template = f.read()
        
        # 替换 {{ variable }} 格式的变量
        for key, value in kwargs.items():
            placeholder = '{{ ' + key + ' }}'
            template = template.replace(placeholder, value)
        
        return template
    except FileNotFoundError:
        return f"<html><body><h1>Template not found: {template_name}</h1></body></html>"
    except Exception as e:
        return f"<html><body><h1>Template error: {str(e)}</h1></body></html>"

class APIHandler(BaseHTTPRequestHandler):
    """API请求处理器"""
    
    def _send_response(self, status_code, data):
        """发送HTTP响应"""
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False).encode('utf-8'))
    
    def do_GET(self):
        """处理GET请求"""
        # 获取请求路径
        path = self.path
        
        try:
            # 根路径 - 显示API文档
            if path == '/':
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                
                # 模板变量
                template_vars = {
                    'sync_response_example': '''{
  "status": "success",
  "message": "销售记录同步成功",
  "data": null
}''',
                    'all_sync_response_example': '''{
  "status": "success",
  "message": "全量数据同步成功",
  "data": null
}''',
                    'vip_response_example': '''{
  "status": "success",
  "message": "会员信息同步成功",
  "data": null
}''',
                    'product_response_example': '''{
  "status": "success",
  "message": "商品档案同步成功",
  "data": null
}''',
                    'weather_request_example': '''{
  "days_back": 7
}''',
                    'weather_response_example': '''{
  "status": "success",
  "message": "历史天气数据同步成功（最近7天）",
  "data": null
}''',
                    'job_status_example': '''{
  "status": "success",
  "message": "获取任务状态成功",
  "data": {
    "scheduler_running": true,
    "tasks": {
      "sync_sale_records": {
        "name": "同步销售记录",
        "description": "从API拉取销售记录数据并保存到数据库",
        "enabled": true,
        "cron": "0 1 * * *",
        "is_running": false
      }
    },
    "job_history": [
      {
        "job_id": "job_sync_all_1234567890",
        "task_name": "sync_all",
        "task_display_name": "全量同步",
        "start_time": "2026-03-09 10:00:00",
        "end_time": "2026-03-09 10:01:30",
        "duration": 90.5,
        "status": "success",
        "message": "全量同步成功"
      }
    ]
  }
}''',
                    'job_run_request_example': '''{
  "task_name": "sync_all"
}''',
                    'job_run_response_example': '''{
  "status": "success",
  "message": "任务 sync_all 已触发",
  "data": null
}'''
                }
                
                html = render_template('api_docs.html', **template_vars)
                self.wfile.write(html.encode('utf-8'))
            
            # 任务管理 - 获取任务状态
            elif path == '/api/job/status':
                # 初始化调度器
                scheduler = init_scheduler(JOB_CONFIG)
                
                # 获取任务状态
                status = scheduler.get_job_status()
                
                response = {
                    'status': 'success',
                    'message': '获取任务状态成功',
                    'data': status
                }
                self._send_response(200, response)
            
            # 任务管理页面
            elif path == '/job':
                self.send_response(200)
                self.send_header('Content-type', 'text/html; charset=utf-8')
                self.end_headers()
                
                # 初始化调度器
                scheduler = init_scheduler(JOB_CONFIG)
                status = scheduler.get_job_status()
                
                # 构建任务列表HTML
                tasks_html = ''
                for task_name, task_info in status['tasks'].items():
                    enabled_class = 'enabled' if task_info['enabled'] else 'disabled'
                    enabled_text = '启用' if task_info['enabled'] else '禁用'
                    running_class = 'running' if task_info['is_running'] else 'idle'
                    running_text = '是' if task_info['is_running'] else '否'
                    
                    tasks_html += f'''
                    <div class="job-item">
                        <h3>{task_info['name']}</h3>
                        <p>{task_info['description']}</p>
                        <div class="job-info">
                            <span class="label">状态:</span>
                            <span class="value {enabled_class}">{enabled_text}</span>
                        </div>
                        <div class="job-info">
                            <span class="label">执行周期:</span>
                            <span class="value">{task_info['cron']}</span>
                        </div>
                        <div class="job-info">
                            <span class="label">运行中:</span>
                            <span class="value {running_class}">{running_text}</span>
                        </div>
                        <button class="run-btn" onclick="runJob('{task_name}')">执行任务</button>
                    </div>
                    '''
                
                # 构建任务历史HTML
                history_html = ''
                if status['job_history']:
                    history_html += '''
                    <table class="history-table">
                        <tr>
                            <th>任务名称</th>
                            <th>开始时间</th>
                            <th>结束时间</th>
                            <th>耗时(秒)</th>
                            <th>状态</th>
                            <th>消息</th>
                        </tr>
                    '''
                    for job in status['job_history'][:10]:  # 只显示最近10条
                        task_name = job.get('task_display_name', job.get('task_name', ''))
                        status_class = 'success' if job['status'] == 'success' else 'failed'
                        history_html += f'''
                        <tr>
                            <td>{task_name}</td>
                            <td>{job['start_time']}</td>
                            <td>{job['end_time']}</td>
                            <td>{job['duration']}</td>
                            <td class="status {status_class}">{job['status']}</td>
                            <td>{job['message']}</td>
                        </tr>
                        '''
                    history_html += '</table>'
                else:
                    history_html = '<p>暂无执行历史</p>'
                
                # 使用模板渲染
                html = render_template('job_management.html', 
                                       tasks_html=tasks_html, 
                                       history_html=history_html)
                self.wfile.write(html.encode('utf-8'))
            
            # 404 Not Found
            else:
                response = {
                    'status': 'error',
                    'message': 'API路径不存在',
                    'data': None
                }
                self._send_response(404, response)
                
        except Exception as e:
            response = {
                'status': 'error',
                'message': f'操作失败: {str(e)}',
                'data': None
            }
            self._send_response(500, response)
    
    def do_POST(self):
        """处理POST请求"""
        # 获取请求路径
        path = self.path
        
        try:
            # 同步销售记录
            if path == '/api/sync/sale-records':
                # 初始化数据库连接
                db = init_database()
                
                # 同步销售记录
                sync_sale_records(db)
                
                # 关闭数据库连接
                if db:
                    db.close()
                
                response = {
                    'status': 'success',
                    'message': '销售记录同步成功',
                    'data': None
                }
                self._send_response(200, response)
            
            # 同步销售记录明细
            elif path == '/api/sync/sale-details':
                # 初始化数据库连接
                db = init_database()
                
                # 同步销售记录明细
                sync_sale_details(db)
                
                # 关闭数据库连接
                if db:
                    db.close()
                
                response = {
                    'status': 'success',
                    'message': '销售记录明细同步成功',
                    'data': None
                }
                self._send_response(200, response)
            
            # 全量同步
            elif path == '/api/sync/all':
                # 初始化数据库连接
                db = init_database()
                
                # 同步销售记录
                sync_sale_records(db)
                
                # 同步销售记录明细
                sync_sale_details(db)
                
                # 同步会员信息
                sync_vip_info(db)
                
                # 同步商品档案
                sync_pro_info(db)
                
                # 关闭数据库连接
                if db:
                    db.close()
                
                response = {
                    'status': 'success',
                    'message': '全量数据同步成功',
                    'data': None
                }
                self._send_response(200, response)
            
            # 同步会员信息
            elif path == '/api/sync/vip':
                # 初始化数据库连接
                db = init_database()
                
                # 同步会员信息
                sync_vip_info(db)
                
                # 关闭数据库连接
                if db:
                    db.close()
                
                response = {
                    'status': 'success',
                    'message': '会员信息同步成功',
                    'data': None
                }
                self._send_response(200, response)
            
            # 同步商品档案
            elif path == '/api/sync/pro-info':
                # 初始化数据库连接
                db = init_database()
                
                # 同步商品档案
                sync_pro_info(db)
                
                # 关闭数据库连接
                if db:
                    db.close()
                
                response = {
                    'status': 'success',
                    'message': '商品档案同步成功',
                    'data': None
                }
                self._send_response(200, response)
            
            # 同步历史天气
            elif path == '/api/sync/weather':
                # 初始化数据库连接
                db = init_database()
                
                # 解析请求体获取参数
                try:
                    content_length = int(self.headers.get('Content-Length', 0))
                    if content_length > 0:
                        post_data = self.rfile.read(content_length).decode('utf-8')
                        request_data = json.loads(post_data)
                        days_back = request_data.get('days_back', 7)
                    else:
                        days_back = 7
                except:
                    days_back = 7
                
                # 同步天气数据
                sync_weather_data(db, days_back=days_back)
                
                # 关闭数据库连接
                if db:
                    db.close()
                
                response = {
                    'status': 'success',
                    'message': f'历史天气数据同步成功（最近{days_back}天）',
                    'data': None
                }
                self._send_response(200, response)
            
            # 任务管理 - 触发任务
            elif path == '/api/job/run':
                # 解析请求体
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length).decode('utf-8')
                data = json.loads(post_data)
                task_name = data.get('task_name', 'sync_all')
                
                # 验证任务名称
                valid_tasks = ['sync_all', 'sync_sale_records', 'sync_sale_details', 'sync_vip', 'sync_pro_info', 'sync_weather']
                if task_name not in valid_tasks:
                    response = {
                        'status': 'error',
                        'message': f'无效的任务名称，有效值: {valid_tasks}',
                        'data': None
                    }
                    self._send_response(400, response)
                    return
                
                # 初始化调度器
                scheduler = init_scheduler(JOB_CONFIG)
                
                # 立即执行任务
                import threading
                def run_task():
                    scheduler.run_now(task_name)
                
                # 异步执行任务
                thread = threading.Thread(target=run_task)
                thread.start()
                
                response = {
                    'status': 'success',
                    'message': f'任务 {task_name} 已触发',
                    'data': None
                }
                self._send_response(200, response)
            
            # 404 Not Found
            else:
                response = {
                    'status': 'error',
                    'message': 'API路径不存在',
                    'data': None
                }
                self._send_response(404, response)
                
        except Exception as e:
            response = {
                'status': 'error',
                'message': f'操作失败: {str(e)}',
                'data': None
            }
            self._send_response(500, response)

def main():
    """主函数"""
    # 从配置文件读取端口
    from config import JOB_CONFIG
    port = JOB_CONFIG.get('web_port', 5000)
    
    # 启动定时job任务
    print("启动定时job任务...")
    scheduler = init_scheduler(JOB_CONFIG)
    scheduler.setup_schedule()
    scheduler.start()
    print("定时job任务已启动")
    
    # 创建HTTP服务器
    server = HTTPServer(('0.0.0.0', port), APIHandler)
    print(f"API服务器已启动，监听端口 {port}")
    print(f"访问 http://localhost:{port} 查看API文档")
    print(f"访问 http://localhost:{port}/job 管理定时任务")
    
    # 启动服务器
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nAPI服务器已停止")
        # 停止定时job任务
        scheduler.stop()
        print("定时job任务已停止")
        server.shutdown()

if __name__ == '__main__':
    main()
