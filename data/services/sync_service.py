#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主脚本，用于从API拉取数据并保存到数据库
"""
import os
import sys
import time
import logging
from datetime import datetime, timedelta

# 确保使用UTF-8编码
os.environ['PYTHONIOENCODING'] = 'utf-8'
sys.stdout.reconfigure(encoding='utf-8')
sys.stderr.reconfigure(encoding='utf-8')

# 导入配置和服务模块
try:
    import sys
    import os
    
    # 添加项目根目录到Python路径
    sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
    
    from config import FETCH_CONFIG
    from services.api_service import fetch_sale_record_data, fetch_sale_detail_data, fetch_vip_data, fetch_pro_info_data, process_data
    from data.repository import save_sale_record_to_file, save_sale_detail_to_file, save_vip_to_file, save_pro_info_to_file, save_sale_record_to_database, save_sale_detail_to_database, save_vip_to_database, save_pro_info_to_database, init_database
except Exception as e:
    print(f"导入模块失败: {e}")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def sync_sale_records(db):
    """同步销售记录数据"""
    logger.info("开始同步销售记录数据")
    
    # 从配置文件中获取实际任务时间范围
    start_date = datetime.strptime(FETCH_CONFIG['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(FETCH_CONFIG['end_date'], '%Y-%m-%d')
    
    # 分日期范围拉取数据，每天拉取一次
    current_date = start_date
    while current_date <= end_date:
        day_end = current_date.replace(hour=23, minute=59, second=59)
        date_str = current_date.strftime('%Y-%m-%d')
        sync_date = current_date.date()
        
        # 检查是否已经存在成功的拉取日志
        if db and db.check_api_log('sale_record', sync_date):
            logger.info(f"销售记录 {date_str} 的数据已经成功拉取过，跳过")
            current_date += timedelta(days=1)
            continue
        
        logger.info(f"拉取销售记录 {date_str} 的数据")
        
        # 分页拉取数据
        page = 1
        all_records = []
        error_message = None
        
        try:
            while True:
                logger.info(f"  拉取第 {page} 页数据")
                
                # 拉取数据
                response = fetch_sale_record_data(current_date, day_end, page, FETCH_CONFIG['page_size'])
                if not response:
                    logger.warning(f"  第 {page} 页数据拉取失败")
                    error_message = f"第 {page} 页数据拉取失败"
                    break
                
                # 处理数据
                records = process_data(response)
                if not records:
                    logger.info(f"  第 {page} 页无数据")
                    break
                
                # 保存数据到列表
                all_records.extend(records)
                
                # 如果数据库连接成功，保存数据到数据库
                if db:
                    save_sale_record_to_database(records, db)
                
                # 检查是否还有更多数据
                if len(records) < FETCH_CONFIG['page_size']:
                    break
                
                page += 1
                # 避免请求过于频繁
                time.sleep(1)
            
            # 保存数据到文件，无论数据库连接是否成功
            if all_records:
                save_sale_record_to_file(all_records, date_str)
            
            # 保存拉取日志
            if db:
                status = 'success' if error_message is None else 'failed'
                db.save_api_log('sale_record', sync_date, status, len(all_records), error_message)
        except Exception as e:
            logger.error(f"拉取销售记录 {date_str} 数据失败: {e}")
            if db:
                db.save_api_log('sale_record', sync_date, 'failed', 0, str(e))
        
        current_date += timedelta(days=1)

def sync_sale_details(db):
    """同步销售记录明细数据"""
    logger.info("开始同步销售记录明细数据")
    
    # 从配置文件中获取实际任务时间范围
    start_date = datetime.strptime(FETCH_CONFIG['start_date'], '%Y-%m-%d')
    end_date = datetime.strptime(FETCH_CONFIG['end_date'], '%Y-%m-%d')
    
    # 分日期范围拉取数据，每天拉取一次
    current_date = start_date
    while current_date <= end_date:
        day_end = current_date.replace(hour=23, minute=59, second=59)
        date_str = current_date.strftime('%Y-%m-%d')
        sync_date = current_date.date()
        
        # 检查是否已经存在成功的拉取日志
        if db and db.check_api_log('sale_detail', sync_date):
            logger.info(f"销售记录明细 {date_str} 的数据已经成功拉取过，跳过")
            current_date += timedelta(days=1)
            continue
        
        logger.info(f"拉取销售记录明细 {date_str} 的数据")
        
        # 分页拉取数据
        page = 1
        all_records = []
        error_message = None
        
        try:
            while True:
                logger.info(f"  拉取第 {page} 页数据")
                
                # 拉取数据
                response = fetch_sale_detail_data(current_date, day_end, page, FETCH_CONFIG['page_size'])
                if not response:
                    logger.warning(f"  第 {page} 页数据拉取失败")
                    error_message = f"第 {page} 页数据拉取失败"
                    break
                
                # 处理数据
                records = process_data(response)
                if not records:
                    logger.info(f"  第 {page} 页无数据")
                    break
                
                # 保存数据到列表
                all_records.extend(records)
                
                # 如果数据库连接成功，保存数据到数据库
                if db:
                    save_sale_detail_to_database(records, db)
                
                # 检查是否还有更多数据
                if len(records) < FETCH_CONFIG['page_size']:
                    break
                
                page += 1
                # 避免请求过于频繁
                time.sleep(1)
            
            # 保存数据到文件，无论数据库连接是否成功
            if all_records:
                save_sale_detail_to_file(all_records, date_str)
            
            # 保存拉取日志
            if db:
                status = 'success' if error_message is None else 'failed'
                db.save_api_log('sale_detail', sync_date, status, len(all_records), error_message)
        except Exception as e:
            logger.error(f"拉取销售记录明细 {date_str} 数据失败: {e}")
            if db:
                db.save_api_log('sale_detail', sync_date, 'failed', 0, str(e))
        
        current_date += timedelta(days=1)

def sync_vip_info(db):
    """同步会员信息数据（全量同步：先清空再插入）"""
    logger.info("开始同步会员信息数据（全量同步）")
    
    from datetime import date
    today = date.today()
    date_str = today.strftime('%Y-%m-%d')
    
    # 分页拉取数据
    page = 1
    all_vips = []
    error_message = None
    
    try:
        # 先清空数据库中的会员信息（全量同步）
        if db:
            db.clear_vip_info()
            logger.info("已清空会员信息表，准备全量同步")
        
        while True:
            logger.info(f"  拉取第 {page} 页会员数据")
            
            # 拉取数据
            response = fetch_vip_data(page, FETCH_CONFIG['page_size'])
            if not response:
                logger.warning(f"  第 {page} 页会员数据拉取失败")
                error_message = f"第 {page} 页会员数据拉取失败"
                break
            
            # 处理数据
            vips = process_data(response)
            if not vips:
                logger.info(f"  第 {page} 页无会员数据")
                break
            
            # 保存数据到列表
            all_vips.extend(vips)
            
            # 如果数据库连接成功，保存数据到数据库
            if db:
                save_vip_to_database(vips, db)
            
            # 检查是否还有更多数据
            if len(vips) < FETCH_CONFIG['page_size']:
                break
            
            page += 1
            # 避免请求过于频繁
            time.sleep(1)
        
        # 保存数据到文件，无论数据库连接是否成功
        if all_vips:
            save_vip_to_file(all_vips, date_str)
        
        # 保存拉取日志
        if db:
            status = 'success' if error_message is None else 'failed'
            db.save_api_log('vip_info', today, status, len(all_vips), error_message)
            
        logger.info(f"会员信息全量同步完成，共 {len(all_vips)} 条")
    except Exception as e:
        logger.error(f"拉取会员信息数据失败: {e}")
        if db:
            db.save_api_log('vip_info', today, 'failed', 0, str(e))


def sync_pro_info(db):
    """同步商品档案数据（全量同步：先清空再插入）"""
    logger.info("开始同步商品档案数据（全量同步）")
    
    from datetime import date
    today = date.today()
    date_str = today.strftime('%Y-%m-%d')
    
    page = 1
    all_products = []
    error_message = None
    
    try:
        if db:
            db.clear_pro_info()
            logger.info("已清空商品档案表，准备全量同步")
        
        while True:
            logger.info(f"  拉取第 {page} 页商品档案数据")
            
            response = fetch_pro_info_data(page, FETCH_CONFIG['page_size'])
            if not response:
                logger.warning(f"  第 {page} 页商品档案数据拉取失败")
                error_message = f"  第 {page} 页商品档案数据拉取失败"
                break
            
            # 打印API返回的原始数据结构
            logger.info(f"  API返回数据: {response}")
            
            products = process_data(response)
            if not products:
                logger.info(f"  第 {page} 页无商品档案数据")
                break
            
            # 打印处理后的数据结构
            if products:
                logger.info(f"  处理后数据: {products[0]}")
            
            all_products.extend(products)
            
            if db:
                save_pro_info_to_database(products, db)
            
            if len(products) < FETCH_CONFIG['page_size']:
                break
            
            page += 1
            time.sleep(1)
        
        if all_products:
            save_pro_info_to_file(all_products, date_str)
        
        if db:
            status = 'success' if error_message is None else 'failed'
            db.save_api_log('pro_info', today, status, len(all_products), error_message)
            
        logger.info(f"商品档案全量同步完成，共 {len(all_products)} 条")
    except Exception as e:
        logger.error(f"拉取商品档案数据失败: {e}")
        if db:
            db.save_api_log('pro_info', today, 'failed', 0, str(e))


def fetch_weather_data(start_date, end_date, config=None):
    """从Open-Meteo API获取历史天气数据"""
    import requests
    from config import WEATHER_API_CONFIG
    
    if config is None:
        config = WEATHER_API_CONFIG
    
    try:
        params = {
            "latitude": config['latitude'],
            "longitude": config['longitude'],
            "daily": config['daily_fields'],
            "start_date": start_date,
            "end_date": end_date,
            "timezone": config['timezone'],
            "temperature_unit": config['temperature_unit'],
            "precipitation_unit": config['precipitation_unit']
        }
        
        response = requests.get(config['url'], params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"获取天气数据失败: {e}")
        return None


def sync_weather_data(db, days_back=7, start_date=None, end_date=None):
    """同步历史天气数据，超过30天则分批拉取"""
    from datetime import date, timedelta as td
    from config import WEATHER_CODE_MAP, WEATHER_API_CONFIG
    
    # 计算日期范围
    if end_date is None:
        end_date = date.today()
    if start_date is None:
        start_date = end_date - td(days=days_back)
    
    # 确保日期是 date 类型
    if isinstance(start_date, datetime):
        start_date = start_date.date()
    if isinstance(end_date, datetime):
        end_date = end_date.date()
    
    logger.info(f"开始同步历史天气数据，总日期范围: {start_date} 至 {end_date}")
    
    # 计算总天数
    total_days = (end_date - start_date).days + 1
    logger.info(f"需要拉取的总天数: {total_days}")
    
    # 最大批次大小（30天）
    BATCH_SIZE = 30
    
    total_count = 0
    batch_num = 0
    current_start = start_date
    
    try:
        while current_start <= end_date:
            batch_num += 1
            # 计算当前批次的结束日期
            current_end = min(current_start + td(days=BATCH_SIZE - 1), end_date)
            
            start_date_str = current_start.strftime('%Y-%m-%d')
            end_date_str = current_end.strftime('%Y-%m-%d')
            
            logger.info(f"第 {batch_num} 批: {start_date_str} 至 {end_date_str}")
            
            # 获取天气数据
            data = fetch_weather_data(start_date_str, end_date_str)
            
            if not data or 'daily' not in data:
                logger.warning(f"第 {batch_num} 批未获取到天气数据，跳过")
                current_start = current_end + td(days=1)
                continue
            
            daily_data = data['daily']
            dates = daily_data.get('time', [])
            temp_max_list = daily_data.get('temperature_2m_max', [])
            temp_min_list = daily_data.get('temperature_2m_min', [])
            precipitation_list = daily_data.get('precipitation_sum', [])
            weathercode_list = daily_data.get('weathercode', [])
            
            if not dates:
                logger.warning(f"第 {batch_num} 批天气数据为空，跳过")
                current_start = current_end + td(days=1)
                continue
            
            # 处理并保存数据
            batch_count = 0
            for i, date_str in enumerate(dates):
                weather_code = weathercode_list[i] if i < len(weathercode_list) else None
                weather_desc = WEATHER_CODE_MAP.get(weather_code, "未知")
                
                weather_record = {
                    'date': date_str,
                    'temp_max': temp_max_list[i] if i < len(temp_max_list) else None,
                    'temp_min': temp_min_list[i] if i < len(temp_min_list) else None,
                    'precipitation': precipitation_list[i] if i < len(precipitation_list) else None,
                    'weather_code': weather_code,
                    'weather_desc': weather_desc,
                    'latitude': WEATHER_API_CONFIG['latitude'],
                    'longitude': WEATHER_API_CONFIG['longitude'],
                    'location_name': WEATHER_API_CONFIG['location_name']
                }
                
                if db:
                    db.insert_weather_data(weather_record)
                batch_count += 1
            
            logger.info(f"第 {batch_num} 批完成，{batch_count} 条记录")
            total_count += batch_count
            
            # 移动到下一批次的起始日期
            current_start = current_end + td(days=1)
            
            # 避免请求过于频繁，批次间添加延迟（除了最后一批）
            if current_start <= end_date:
                time.sleep(1)
        
        # 保存日志
        if db:
            db.save_api_log('weather', date.today(), 'success', total_count)
        
        logger.info(f"天气数据同步完成，共 {batch_num} 批次，{total_count} 条记录")
        
    except Exception as e:
        logger.error(f"同步天气数据失败: {e}")
        if db:
            db.save_api_log('weather', date.today(), 'failed', total_count, str(e))


def run_full_sync():
    """执行全量同步"""
    logger.info("开始全量数据同步任务")
    
    # 尝试初始化数据库连接
    db = init_database()
    
    # 同步销售记录
    sync_sale_records(db)
    
    # 同步销售记录明细
    sync_sale_details(db)
    
    # 同步会员信息
    sync_vip_info(db)
    
    # 同步商品档案
    sync_pro_info(db)
    
    # 同步天气数据
    sync_weather_data(db)
    
    # 关闭数据库连接
    if db:
        try:
            db.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {e}")
    
    logger.info("全量数据拉取和保存完成")

if __name__ == "__main__":
    # 主模块现在只作为模块导入使用，不直接执行
    pass
