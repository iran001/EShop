import json
import logging
import os
from datetime import datetime
from database import Database
from config import DB_CONFIG

logger = logging.getLogger(__name__)


def save_sale_record_to_file(records, date_str):
    """将销售记录数据保存到文件"""
    try:
        # 确保data目录存在
        os.makedirs('data', exist_ok=True)
        
        # 生成文件名
        file_path = f'data/sale_record_{date_str}.json'
        
        # 保存数据
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        
        logger.info(f"销售记录数据已保存到 {file_path}，共 {len(records)} 条")
    except Exception as e:
        logger.error(f"保存销售记录数据到文件失败: {e}")


def save_sale_detail_to_file(records, date_str):
    """将销售记录明细数据保存到文件"""
    try:
        # 确保data目录存在
        os.makedirs('data', exist_ok=True)
        
        # 生成文件名
        file_path = f'data/sale_detail_{date_str}.json'
        
        # 保存数据
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
        
        logger.info(f"销售记录明细数据已保存到 {file_path}，共 {len(records)} 条")
    except Exception as e:
        logger.error(f"保存销售记录明细数据到文件失败: {e}")


def save_sale_record_to_database(records, db):
    """将销售记录数据保存到数据库"""
    try:
        for record in records:
            db.insert_sell_record(record)
        logger.info(f"销售记录数据保存到数据库完成，共 {len(records)} 条")
    except Exception as e:
        logger.error(f"保存销售记录数据到数据库失败: {e}")


def save_sale_detail_to_database(records, db):
    """将销售记录明细数据保存到数据库"""
    try:
        for record in records:
            db.insert_sell_detail(record)
        logger.info(f"销售记录明细数据保存到数据库完成，共 {len(records)} 条")
    except Exception as e:
        logger.error(f"保存销售记录明细数据到数据库失败: {e}")


def save_vip_to_file(vips, date_str):
    """将会员信息数据保存到文件"""
    try:
        # 确保data目录存在
        os.makedirs('data', exist_ok=True)
        
        # 生成文件名
        file_path = f'data/vip_info_{date_str}.json'
        
        # 保存数据
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(vips, f, ensure_ascii=False, indent=2)
        
        logger.info(f"会员信息数据已保存到 {file_path}，共 {len(vips)} 条")
    except Exception as e:
        logger.error(f"保存会员信息数据到文件失败: {e}")


def map_vip_fields(vip):
    """将 API 返回的 VIP 字段映射为数据库字段"""
    field_mapping = {
        'vip_id': 'vip_id',
        'vip_name': 'vip_name',
        'phone': 'vip_phone',           # API: phone -> DB: vip_phone
        'sex': 'vip_sex',               # API: sex -> DB: vip_sex
        'vip_bir': 'vip_birthday',      # API: vip_bir -> DB: vip_birthday
        'cls_id': 'vip_cls_id',         # API: cls_id -> DB: vip_cls_id
        'cls_name': 'vip_cls_name',     # API: cls_name -> DB: vip_cls_name
        'mall_id': 'vip_mall_id',       # API: mall_id -> DB: vip_mall_id
        'mall_name': 'vip_mall_name',   # API: mall_name -> DB: vip_mall_name
        'now_jf': 'vip_jf_value',       # API: now_jf -> DB: vip_jf_value
        'vip_pre_amt': 'vip_ye_value',  # API: vip_pre_amt -> DB: vip_ye_value
        'xf_total_money': 'vip_xf_total_money',  # API: xf_total_money -> DB: vip_xf_total_money
        'last_xf_time': 'vip_last_xf_time',      # API: last_xf_time -> DB: vip_last_xf_time
        'creat_time': 'vip_zc_date',    # API: creat_time -> DB: vip_zc_date
        'yw_user_id': 'vip_zc_user_id', # API: yw_user_id -> DB: vip_zc_user_id
        'yw_user_name': 'vip_zc_user_name',  # API: yw_user_name -> DB: vip_zc_user_name
        'user_memo': 'vip_remark',      # API: user_memo -> DB: vip_remark
    }
    
    mapped_vip = {}
    for api_field, db_field in field_mapping.items():
        mapped_vip[db_field] = vip.get(api_field)
    
    return mapped_vip


def save_vip_to_database(vips, db):
    """将会员信息数据保存到数据库"""
    try:
        for vip in vips:
            # 字段映射转换
            mapped_vip = map_vip_fields(vip)
            db.insert_vip_info(mapped_vip)
        logger.info(f"会员信息数据保存到数据库完成，共 {len(vips)} 条")
    except Exception as e:
        logger.error(f"保存会员信息数据到数据库失败: {e}")


def save_pro_info_to_file(products, date_str):
    """将商品档案数据保存到文件"""
    try:
        os.makedirs('data', exist_ok=True)
        file_path = f'data/pro_info_{date_str}.json'
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(products, f, ensure_ascii=False, indent=2)
        logger.info(f"商品档案数据已保存到 {file_path}，共 {len(products)} 条")
    except Exception as e:
        logger.error(f"保存商品档案数据到文件失败: {e}")


def save_pro_info_to_database(products, db):
    """将商品档案数据保存到数据库"""
    try:
        for pro in products:
            db.insert_pro_info(pro)
        logger.info(f"商品档案数据保存到数据库完成，共 {len(products)} 条")
    except Exception as e:
        logger.error(f"保存商品档案数据到数据库失败: {e}")


def init_database():
    """初始化数据库连接"""
    try:
        db = Database(DB_CONFIG)
        logger.info("数据库初始化成功")
        return db
    except Exception as e:
        logger.error(f"数据库初始化失败: {e}")
        logger.info("将数据保存到本地文件")
        return None
