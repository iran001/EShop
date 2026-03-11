#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
定时任务调度器
"""
import os
import sys
import time
import threading
import schedule
import logging
import json
from datetime import datetime

# 确保使用UTF-8编码
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('job_scheduler.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 导入同步服务
import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from services.sync_service import run_full_sync as main, sync_vip_info
from config import JOB_CONFIG

class JobScheduler:
    def __init__(self, config):
        """初始化定时任务调度器"""
        self.config = config
        self.jobs_running = {}
        self.job_history = []
        self.scheduler_thread = None
        self.stop_event = threading.Event()
        self.history_file = 'job_history.json'
        # 加载任务历史
        self._load_history()
    
    def _load_history(self):
        """加载任务历史"""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    self.job_history = json.load(f)
                logger.info(f"成功加载任务历史，共 {len(self.job_history)} 条记录")
        except Exception as e:
            logger.error(f"加载任务历史失败: {e}")
            self.job_history = []
    
    def _save_history(self):
        """保存任务历史"""
        try:
            # 只保留最近100条历史记录
            if len(self.job_history) > 100:
                self.job_history = self.job_history[-100:]
            with open(self.history_file, 'w', encoding='utf-8') as f:
                json.dump(self.job_history, f, ensure_ascii=False, indent=2)
            logger.info(f"成功保存任务历史，共 {len(self.job_history)} 条记录")
        except Exception as e:
            logger.error(f"保存任务历史失败: {e}")
    
    def run_sync_job(self, task_name='sync_all'):
        """执行同步任务"""
        if self.jobs_running.get(task_name, False):
            logger.info(f"任务 {task_name} 正在执行中，跳过本次调度")
            return
        
        self.jobs_running[task_name] = True
        job_id = f"job_{task_name}_{int(time.time())}"
        start_time = datetime.now()
        
        logger.info(f"开始执行任务 {task_name}: {job_id}")
        
        try:
            # 根据任务名称执行不同的同步任务
            if task_name == 'sync_sale_records':
                from data.repository import init_database
                from services.sync_service import sync_sale_records
                db = init_database()
                sync_sale_records(db)
                if db:
                    db.close()
                message = "销售记录同步成功"
            elif task_name == 'sync_sale_details':
                from data.repository import init_database
                from services.sync_service import sync_sale_details
                db = init_database()
                sync_sale_details(db)
                if db:
                    db.close()
                message = "销售记录明细同步成功"
            elif task_name == 'sync_vip':
                from data.repository import init_database
                from services.sync_service import sync_vip_info
                db = init_database()
                sync_vip_info(db)
                if db:
                    db.close()
                message = "会员信息同步成功"
            elif task_name == 'sync_pro_info':
                from data.repository import init_database
                from services.sync_service import sync_pro_info
                db = init_database()
                sync_pro_info(db)
                if db:
                    db.close()
                message = "商品信息同步成功"
            elif task_name == 'sync_weather':
                from data.repository import init_database
                from services.sync_service import sync_weather_data
                from datetime import date as dt_date
                db = init_database()
                # 获取天气同步配置参数
                task_config = self.config.get('tasks', {}).get('sync_weather', {})
                params = task_config.get('params', {})
                # 解析起始日期
                start_date_str = params.get('start_date', '2022-11-01')
                start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
                # 解析结束日期，None表示今天
                end_date_str = params.get('end_date')
                end_date = datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else dt_date.today()
                sync_weather_data(db, start_date=start_date, end_date=end_date)
                if db:
                    db.close()
                message = "历史天气数据同步成功"
            else:  # sync_all
                main()
                message = "全量同步成功"
            status = "success"
        except Exception as e:
            logger.error(f"任务 {task_name} 执行失败: {e}")
            status = "failed"
            message = str(e)
        finally:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # 记录任务执行历史
            self.job_history.append({
                'job_id': job_id,
                'task_name': task_name,
                'task_display_name': self.config.get('tasks', {}).get(task_name, {}).get('name', task_name),
                'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
                'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
                'duration': round(duration, 2),
                'status': status,
                'message': message
            })
            
            # 只保留最近100条历史记录
            if len(self.job_history) > 100:
                self.job_history = self.job_history[-100:]
            
            logger.info(f"任务 {task_name} 执行完成: {job_id}, 状态: {status}, 耗时: {duration}秒")
            self.jobs_running[task_name] = False
            # 保存任务历史
            self._save_history()
    
    def setup_schedule(self):
        """设置定时任务"""
        # 清除所有现有任务
        schedule.clear()
        
        # 从配置中获取任务列表
        tasks = self.config.get('tasks', {})
        
        logger.info(f"开始设置定时任务，共 {len(tasks)} 个任务")
        
        # 为每个任务设置定时执行
        for task_name, task_config in tasks.items():
            if task_config.get('enabled', False):
                cron_expression = task_config.get('cron', '0 0 * * *')
                logger.info(f"设置任务 {task_name}，执行周期: {cron_expression}")
                
                # 根据cron表达式设置定时任务
                # 这里简化处理，实际项目中可以使用更复杂的cron解析库
                if cron_expression == '0 0 * * *':
                    schedule.every().day.at("00:00").do(self.run_sync_job, task_name=task_name)
                    logger.info(f"已设置任务 {task_name} 每天凌晨执行")
                elif cron_expression == '0 1 * * *':
                    schedule.every().day.at("01:00").do(self.run_sync_job, task_name=task_name)
                    logger.info(f"已设置任务 {task_name} 每天凌晨1点执行")
                elif cron_expression == '0 2 * * *':
                    schedule.every().day.at("02:00").do(self.run_sync_job, task_name=task_name)
                    logger.info(f"已设置任务 {task_name} 每天凌晨2点执行")
                elif cron_expression == '0 */6 * * *':
                    schedule.every(6).hours.do(self.run_sync_job, task_name=task_name)
                    logger.info(f"已设置任务 {task_name} 每6小时执行")
                elif cron_expression == '*/30 * * * *':
                    schedule.every(30).minutes.do(self.run_sync_job, task_name=task_name)
                    logger.info(f"已设置任务 {task_name} 每30分钟执行")
                elif cron_expression == '* * * * *':
                    schedule.every().minute.do(self.run_sync_job, task_name=task_name)
                    logger.info(f"已设置任务 {task_name} 每分钟执行")
                else:
                    # 默认每天凌晨执行
                    schedule.every().day.at("00:00").do(self.run_sync_job, task_name=task_name)
                    logger.info(f"已设置任务 {task_name} 每天凌晨执行（默认）")
            else:
                logger.info(f"任务 {task_name} 已禁用，跳过设置")
    
    def start(self):
        """启动定时任务调度器"""
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            logger.info("调度器已经在运行中")
            return
        
        self.stop_event.clear()
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        logger.info("定时任务调度器启动成功")
    
    def _run_scheduler(self):
        """运行调度器"""
        while not self.stop_event.is_set():
            schedule.run_pending()
            time.sleep(1)
    
    def stop(self):
        """停止定时任务调度器"""
        self.stop_event.set()
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        logger.info("定时任务调度器已停止")
    
    def get_job_status(self):
        """获取任务状态"""
        # 构建任务状态信息
        tasks_status = {}
        tasks = self.config.get('tasks', {})
        
        for task_name, task_config in tasks.items():
            tasks_status[task_name] = {
                'name': task_config.get('name', task_name),
                'description': task_config.get('description', ''),
                'enabled': task_config.get('enabled', False),
                'cron': task_config.get('cron', '0 0 * * *'),
                'is_running': self.jobs_running.get(task_name, False)
            }
        
        return {
            'scheduler_running': self.scheduler_thread and self.scheduler_thread.is_alive(),
            'tasks': tasks_status,
            'job_history': self.job_history
        }
    
    def run_now(self, task_name='sync_all'):
        """立即执行同步任务"""
        # 不使用守护线程，避免解释器关闭时的锁问题
        thread = threading.Thread(target=self.run_sync_job, args=(task_name,))
        thread.start()
        # 等待任务完成
        thread.join()
        logger.info(f"手动触发任务: {task_name}")
    
    def show_status(self):
        """显示任务状态"""
        status = self.get_job_status()
        print("\n任务调度器状态:")
        print(f"调度器运行状态: {'运行中' if status['scheduler_running'] else '已停止'}")
        print("\n任务配置:")
        print("{:<20} {:<30} {:<10} {:<20} {:<10}".format(
            '任务名称', '描述', '状态', '执行周期', '运行中'
        ))
        print('-' * 110)
        for task_name, task_info in status['tasks'].items():
            print("{:<20} {:<30} {:<10} {:<20} {:<10}".format(
                task_info['name'],
                task_info['description'][:30],
                '启用' if task_info['enabled'] else '禁用',
                task_info['cron'],
                '是' if task_info['is_running'] else '否'
            ))
        print("\n任务执行历史:")
        if status['job_history']:
            print("{:<20} {:<20} {:<20} {:<20} {:<10} {:<10} {:<30}".format(
                '任务ID', '任务名称', '开始时间', '结束时间', '耗时(秒)', '状态', '消息'
            ))
            print('-' * 150)
            for job in status['job_history']:
                print("{:<20} {:<20} {:<20} {:<20} {:<10} {:<10} {:<30}".format(
                    job['job_id'],
                    job.get('task_display_name', job.get('task_name', '')),
                    job['start_time'],
                    job['end_time'],
                    job['duration'],
                    job['status'],
                    job['message'][:30]
                ))
        else:
            print("暂无执行历史")

# 全局调度器实例
global_scheduler = None

def init_scheduler(config):
    """初始化全局调度器"""
    global global_scheduler
    if not global_scheduler:
        global_scheduler = JobScheduler(config)
    return global_scheduler

def get_scheduler():
    """获取全局调度器"""
    global global_scheduler
    return global_scheduler

if __name__ == '__main__':
    # 从配置文件读取配置
    from config import JOB_CONFIG
    
    # 初始化调度器
    scheduler = init_scheduler(JOB_CONFIG)
    
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='定时任务调度器')
    parser.add_argument('command', choices=['start', 'stop', 'run', 'status'], help='命令: start(启动), stop(停止), run(立即执行), status(查看状态)')
    parser.add_argument('--task', choices=['sync_all', 'sync_sale_records', 'sync_sale_details', 'sync_vip', 'sync_pro_info', 'sync_weather'], default='sync_all', help='任务名称')
    args = parser.parse_args()
    
    if args.command == 'start':
        scheduler.setup_schedule()
        scheduler.start()
        print("定时任务调度器已启动")
        print("按 Ctrl+C 退出")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            scheduler.stop()
            print("定时任务调度器已停止")
    elif args.command == 'stop':
        scheduler.stop()
        print("定时任务调度器已停止")
    elif args.command == 'run':
        scheduler.run_now(args.task)
        print(f"任务 {args.task} 已触发")
    elif args.command == 'status':
        scheduler.show_status()
