#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据库操作类
"""
import psycopg2
from psycopg2 import sql
from datetime import datetime
import logging

# 配置日志
logger = logging.getLogger(__name__)

class Database:
    def __init__(self, config):
        """初始化数据库连接"""
        self.config = config
        self.conn = None
        self.cursor = None
        self.connect()
        self.create_table()
    
    def connect(self):
        """建立数据库连接"""
        try:
            # 直接使用配置参数，不进行额外的类型转换
            host = self.config.get('host', 'localhost')
            port = self.config.get('port', 5432)
            database = self.config.get('database', 'EShopDB')
            user = self.config.get('user', 'postgres')
            password = self.config.get('password', '123456')
            
            # 打印配置信息，确保没有编码问题
            logger.info(f"数据库配置 - host: {host}, port: {port}, database: {database}, user: {user}")
            
            # 尝试使用单独的参数连接，不使用options参数
            # 注意：这里不使用dsn，而是直接使用关键字参数
            self.conn = psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                # 显式设置客户端编码
                client_encoding='UTF8'
            )
            
            # 获取连接后设置编码
            self.cursor = self.conn.cursor()
            
            # 验证连接
            self.cursor.execute("SELECT version()")
            version = self.cursor.fetchone()
            logger.info(f"数据库连接成功，版本: {version}")
            
            # 检查当前编码设置
            self.cursor.execute("SHOW client_encoding")
            encoding = self.cursor.fetchone()
            logger.info(f"当前客户端编码: {encoding}")
        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            logger.error(f"配置信息: {self.config}")
            
            # 尝试使用不同的编码设置
            try:
                logger.info("尝试使用GBK编码连接")
                self.conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password,
                    client_encoding='GBK'
                )
                
                self.cursor = self.conn.cursor()
                
                # 验证连接
                self.cursor.execute("SELECT version()")
                version = self.cursor.fetchone()
                logger.info(f"数据库连接成功，版本: {version}")
                
                # 检查当前编码设置
                self.cursor.execute("SHOW client_encoding")
                encoding = self.cursor.fetchone()
                logger.info(f"当前客户端编码: {encoding}")
            except Exception as e2:
                logger.error(f"GBK编码连接失败: {e2}")
                
                # 尝试捕获更详细的错误信息
                try:
                    logger.info("尝试捕获详细错误信息")
                    import traceback
                    traceback.print_exc()
                except:
                    pass
                raise
    
    def create_table(self):
        """创建销售记录表和销售记录明细表"""
        # 创建销售记录表
        create_sell_records_table_sql = """
        CREATE TABLE IF NOT EXISTS sell_records (
            id SERIAL PRIMARY KEY,
            tr VARCHAR(10),
            tp VARCHAR(10),
            dh_id VARCHAR(50) UNIQUE,
            oper_type VARCHAR(50),
            t_type VARCHAR(50),
            dw_dh_id VARCHAR(50),
            mall_id VARCHAR(50),
            mall_name VARCHAR(100),
            vip_id VARCHAR(50),
            vip_name VARCHAR(100),
            state VARCHAR(50),
            yw_time TIMESTAMP,
            yw_user_id VARCHAR(50),
            yw_user_name VARCHAR(100),
            inout_mark VARCHAR(10),
            pro_old_total_money DECIMAL(10, 2),
            tax_money DECIMAL(10, 2),
            un_tax_pro_total_money DECIMAL(10, 2),
            kw_old_total_money DECIMAL(10, 2),
            yh_money DECIMAL(10, 2),
            yf_money DECIMAL(10, 2),
            in_money DECIMAL(10, 2),
            t_ml_money DECIMAL(10, 2),
            t_tc_money DECIMAL(10, 2),
            t_jf_value DECIMAL(10, 2),
            pay_way VARCHAR(50),
            pay_result VARCHAR(100),
            pay_msg VARCHAR(255),
            pay_state VARCHAR(50),
            kq_id VARCHAR(50),
            kq_dk_money DECIMAL(10, 2),
            user_memo VARCHAR(255),
            refund_dh_list VARCHAR(255),
            refund_money DECIMAL(10, 2),
            gl_dh_type VARCHAR(50),
            gl_dh_id VARCHAR(50),
            table_id VARCHAR(50),
            creat_user_id VARCHAR(50),
            creat_user_name VARCHAR(100),
            creat_time TIMESTAMP,
            chg_user_id VARCHAR(50),
            chg_user_name VARCHAR(100),
            chg_time TIMESTAMP,
            t_from VARCHAR(50),
            cx_type VARCHAR(50),
            cx_dh_id VARCHAR(50),
            custom_id VARCHAR(50),
            rc VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # 创建销售记录明细表
        create_sell_details_table_sql = """
        CREATE TABLE IF NOT EXISTS sell_details (
            id SERIAL PRIMARY KEY,
            tr VARCHAR(10),
            tp VARCHAR(10),
            detail_id VARCHAR(50) UNIQUE,
            pro_id VARCHAR(50),
            pro_name VARCHAR(100),
            pym VARCHAR(50),
            zbm VARCHAR(50),
            pro_unit VARCHAR(20),
            pro_num DECIMAL(10, 3),
            pro_price DECIMAL(10, 2),
            pro_total_price DECIMAL(10, 2),
            dh_id VARCHAR(50),
            yw_time TIMESTAMP,
            t_type VARCHAR(50),
            dj_state VARCHAR(50),
            check_user_name VARCHAR(100),
            creat_time TIMESTAMP,
            mall_name VARCHAR(100),
            cls_name VARCHAR(50),
            gys_name VARCHAR(50),
            rc VARCHAR(10),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        # 创建API拉取日志表
        create_api_log_table_sql = """
        CREATE TABLE IF NOT EXISTS api_logs (
            id SERIAL PRIMARY KEY,
            api_type VARCHAR(50),  -- 区分不同API类型，如'sale_record'或'sale_detail'
            sync_date DATE,       -- 同步日期（天）
            status VARCHAR(20),   -- 拉取状态：'success'或'failed'
            data_count INTEGER,   -- 拉取的数据量
            error_message TEXT,   -- 错误信息（如果有）
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(api_type, sync_date)  -- 确保每个API每天只有一条日志记录
        );
        """
        
        # 创建会员信息表
        create_vip_table_sql = """
        CREATE TABLE IF NOT EXISTS vip_info (
            id SERIAL PRIMARY KEY,
            vip_id VARCHAR(50) UNIQUE,
            vip_name VARCHAR(100),
            vip_phone VARCHAR(20),
            vip_sex VARCHAR(10),
            vip_birthday DATE,
            vip_type VARCHAR(50),
            vip_level VARCHAR(50),
            vip_state VARCHAR(20),
            vip_zc_date DATE,
            vip_cls_id VARCHAR(20),
            vip_cls_name VARCHAR(100),
            vip_mall_id VARCHAR(50),
            vip_mall_name VARCHAR(100),
            vip_jf_value DECIMAL(10, 2),
            vip_ye_value DECIMAL(10, 2),
            vip_xf_total_money DECIMAL(10, 2),
            vip_last_xf_time TIMESTAMP,
            vip_zc_user_id VARCHAR(50),
            vip_zc_user_name VARCHAR(100),
            vip_remark TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        try:
            # 创建销售记录表
            self.cursor.execute(create_sell_records_table_sql)
            self.conn.commit()
            logger.info("销售记录表创建成功")
            
            # 创建销售记录明细表
            self.cursor.execute(create_sell_details_table_sql)
            self.conn.commit()
            logger.info("销售记录明细表创建成功")
            
            # 创建API拉取日志表
            self.cursor.execute(create_api_log_table_sql)
            self.conn.commit()
            logger.info("API拉取日志表创建成功")
            
            # 创建会员信息表
            self.cursor.execute(create_vip_table_sql)
            self.conn.commit()
            logger.info("会员信息表创建成功")
            
            # 创建商品档案表
            create_pro_info_table_sql = """
            CREATE TABLE IF NOT EXISTS pro_info (
                id SERIAL PRIMARY KEY,
                tr VARCHAR(10),
                tp VARCHAR(10),
                pro_id VARCHAR(50) UNIQUE,
                in_price DECIMAL(10, 2),
                sale_price DECIMAL(10, 2),
                vip_price DECIMAL(10, 2),
                vip_price1 DECIMAL(10, 2),
                vip_price2 DECIMAL(10, 2),
                vip_price3 DECIMAL(10, 2),
                tj_sale_price DECIMAL(10, 2),
                ps_price DECIMAL(10, 2),
                pf_price DECIMAL(10, 2),
                tax_rate DECIMAL(10, 2),
                state VARCHAR(20),
                mall_id VARCHAR(50),
                low_stock DECIMAL(10, 2),
                max_stock DECIMAL(10, 2),
                img_url TEXT,
                asc_desc VARCHAR(10),
                chg_user_name VARCHAR(100),
                chg_time TIMESTAMP,
                low_sale_price DECIMAL(10, 2),
                online_price DECIMAL(10, 2),
                online_state VARCHAR(20),
                trade_day VARCHAR(20),
                day_can_sale_num DECIMAL(10, 2),
                day_sale_num DECIMAL(10, 2),
                day_now_num DECIMAL(10, 2),
                hot_yn VARCHAR(10),
                tj_yn VARCHAR(10),
                index_show_yn VARCHAR(10),
                pro_create_time TIMESTAMP,
                pro_stop_time TIMESTAMP,
                ex_mark VARCHAR(50),
                sale_mrl DECIMAL(10, 2),
                pf_mrl DECIMAL(10, 2),
                stock DECIMAL(10, 2),
                pro_asc_desc VARCHAR(10),
                pro_name VARCHAR(200),
                pym VARCHAR(50),
                zbm VARCHAR(50),
                size VARCHAR(100),
                unit VARCHAR(20),
                price_way VARCHAR(50),
                pro_type VARCHAR(50),
                cls_id VARCHAR(20),
                ledger_cls_id VARCHAR(20),
                dx_unit1_name VARCHAR(50),
                dx_unit1_num DECIMAL(10, 2),
                dx_unit2_name VARCHAR(50),
                dx_unit2_num DECIMAL(10, 2),
                dx_unit1_stock DECIMAL(10, 2),
                dx_unit2_stock DECIMAL(10, 2),
                dx_unit_stock VARCHAR(50),
                online_pro_name VARCHAR(200),
                online_memo TEXT,
                pro_address VARCHAR(200),
                user_memo TEXT,
                bzq_yn VARCHAR(10),
                bzq VARCHAR(20),
                tc_way VARCHAR(50),
                tc_value DECIMAL(10, 2),
                jf_yn VARCHAR(10),
                jf_value DECIMAL(10, 2),
                mall_chg_price_yn VARCHAR(10),
                sale_show_yn VARCHAR(10),
                sale_zk_yn VARCHAR(10),
                gl_kc_yn VARCHAR(10),
                sale_chg_price_yn VARCHAR(10),
                creat_time TIMESTAMP,
                pp_id VARCHAR(50),
                gys_id VARCHAR(50),
                cls_name VARCHAR(100),
                gys_name VARCHAR(200),
                mall_name VARCHAR(100),
                pp_name VARCHAR(100),
                mall_type VARCHAR(50),
                pro_cls_mall_id VARCHAR(50),
                unit_count VARCHAR(10),
                color_size_count VARCHAR(10),
                sale_ledger_rate DECIMAL(10, 2),
                tmp_price_yn VARCHAR(10),
                color_size_sub_id VARCHAR(50),
                total_in_money DECIMAL(10, 2),
                rc VARCHAR(10),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            
            self.cursor.execute(create_pro_info_table_sql)
            self.conn.commit()
            logger.info("商品档案表创建成功")
            
            # 创建天气数据表
            create_weather_table_sql = """
            CREATE TABLE IF NOT EXISTS weather_history (
                id SERIAL PRIMARY KEY,
                weather_date DATE UNIQUE,
                temp_max DECIMAL(5, 2),
                temp_min DECIMAL(5, 2),
                precipitation DECIMAL(5, 2),
                weather_code INTEGER,
                weather_desc VARCHAR(50),
                latitude DECIMAL(8, 5),
                longitude DECIMAL(8, 5),
                location_name VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
            self.cursor.execute(create_weather_table_sql)
            self.conn.commit()
            logger.info("天气数据表创建成功")
        except Exception as e:
            logger.error(f"创建表失败: {e}")
            self.conn.rollback()
    
    def insert_sell_record(self, record):
        """插入销售记录"""
        insert_sql = """
        INSERT INTO sell_records (
            tr, tp, dh_id, oper_type, t_type, dw_dh_id, mall_id, mall_name, vip_id, vip_name,
            state, yw_time, yw_user_id, yw_user_name, inout_mark, pro_old_total_money, tax_money,
            un_tax_pro_total_money, kw_old_total_money, yh_money, yf_money, in_money, t_ml_money,
            t_tc_money, t_jf_value, pay_way, pay_result, pay_msg, pay_state, kq_id, kq_dk_money,
            user_memo, refund_dh_list, refund_money, gl_dh_type, gl_dh_id, table_id, creat_user_id,
            creat_user_name, creat_time, chg_user_id, chg_user_name, chg_time, t_from, cx_type,
            cx_dh_id, custom_id, rc
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s
        ) ON CONFLICT (dh_id) DO NOTHING
        """
        try:
            # 处理时间字段
            def parse_time(time_str):
                if time_str:
                    return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                return None
            
            # 处理数值字段
            def parse_decimal(value):
                if value:
                    return float(value)
                return 0.0
            
            # 构建参数
            params = (
                record.get('TR'),
                record.get('TP'),
                record.get('dh_id'),
                record.get('oper_type'),
                record.get('t_type'),
                record.get('dw_dh_id'),
                record.get('mall_id'),
                record.get('mall_name'),
                record.get('vip_id'),
                record.get('vip_name'),
                record.get('state'),
                parse_time(record.get('yw_time')),
                record.get('yw_user_id'),
                record.get('yw_user_name'),
                record.get('inout_mark'),
                parse_decimal(record.get('pro_old_total_money')),
                parse_decimal(record.get('tax_money')),
                parse_decimal(record.get('un_tax_pro_total_money')),
                parse_decimal(record.get('kw_old_total_money')),
                parse_decimal(record.get('yh_money')),
                parse_decimal(record.get('yf_money')),
                parse_decimal(record.get('in_money')),
                parse_decimal(record.get('t_ml_money')),
                parse_decimal(record.get('t_tc_money')),
                parse_decimal(record.get('t_jf_value')),
                record.get('pay_way'),
                record.get('pay_result'),
                record.get('pay_msg'),
                record.get('pay_state'),
                record.get('kq_id'),
                parse_decimal(record.get('kq_dk_money')),
                record.get('user_memo'),
                record.get('refund_dh_list'),
                parse_decimal(record.get('refund_money')),
                record.get('gl_dh_type'),
                record.get('gl_dh_id'),
                record.get('table_id'),
                record.get('creat_user_id'),
                record.get('creat_user_name'),
                parse_time(record.get('creat_time')),
                record.get('chg_user_id'),
                record.get('chg_user_name'),
                parse_time(record.get('chg_time')),
                record.get('t_from'),
                record.get('cx_type'),
                record.get('cx_dh_id'),
                record.get('custom_id'),
                record.get('RC')
            )
            
            self.cursor.execute(insert_sql, params)
            self.conn.commit()
        except Exception as e:
            logger.error(f"插入数据失败: {e}")
            self.conn.rollback()
    
    def insert_sell_detail(self, record):
        """插入销售记录明细"""
        # 简化SQL语句，使用更少的字段
        insert_sql = """
        INSERT INTO sell_details (
            tr, tp, detail_id, pro_id, pro_name, pym, zbm, pro_unit, 
            pro_num, pro_price, pro_total_price, dh_id, yw_time, t_type, 
            dj_state, check_user_name, creat_time, mall_name, cls_name, gys_name, rc
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s
        ) ON CONFLICT (detail_id) DO NOTHING
        """
        try:
            # 处理时间字段
            def parse_time(time_str):
                if time_str:
                    return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                return None
            
            # 处理数值字段
            def parse_decimal(value):
                if value:
                    return float(value)
                return 0.0
            
            # 构建参数
            params = (
                record.get('TR'),
                record.get('TP'),
                record.get('id'),  # 使用id作为detail_id
                record.get('pro_id'),
                record.get('pro_name'),
                record.get('pym'),
                record.get('zbm'),
                record.get('pro_unit'),
                parse_decimal(record.get('pro_num')),
                parse_decimal(record.get('pro_price')),
                parse_decimal(record.get('pro_total_price')),
                record.get('dh_id'),
                parse_time(record.get('yw_time')),
                record.get('t_type'),
                record.get('dj_state'),
                record.get('check_user_name'),
                parse_time(record.get('creat_time')),
                record.get('mall_name'),
                record.get('cls_name'),
                record.get('gys_name'),
                record.get('RC')
            )
            
            self.cursor.execute(insert_sql, params)
            self.conn.commit()
        except Exception as e:
            logger.error(f"插入销售记录明细失败: {e}")
            self.conn.rollback()
    
    def check_api_log(self, api_type, sync_date):
        """检查指定API类型和日期的拉取日志是否存在且成功"""
        try:
            check_sql = """
            SELECT status FROM api_logs 
            WHERE api_type = %s AND sync_date = %s
            """
            self.cursor.execute(check_sql, (api_type, sync_date))
            result = self.cursor.fetchone()
            return result and result[0] == 'success'
        except Exception as e:
            logger.error(f"检查API拉取日志失败: {e}")
            return False
    
    def save_api_log(self, api_type, sync_date, status, data_count=0, error_message=None):
        """保存API拉取日志，存在则更新，不存在则插入"""
        try:
            upsert_sql = """
            INSERT INTO api_logs (api_type, sync_date, status, data_count, error_message)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (api_type, sync_date)
            DO UPDATE SET 
                status = EXCLUDED.status,
                data_count = EXCLUDED.data_count,
                error_message = EXCLUDED.error_message,
                created_at = CURRENT_TIMESTAMP
            """
            self.cursor.execute(upsert_sql, (api_type, sync_date, status, data_count, error_message))
            self.conn.commit()
            logger.info(f"API拉取日志已保存: {api_type}, {sync_date}, {status}, {data_count}")
        except Exception as e:
            logger.error(f"保存API拉取日志失败: {e}")
            self.conn.rollback()
    
    def clear_vip_info(self):
        """清空会员信息表"""
        try:
            clear_sql = "TRUNCATE TABLE vip_info RESTART IDENTITY"
            self.cursor.execute(clear_sql)
            self.conn.commit()
            logger.info("会员信息表已清空")
        except Exception as e:
            logger.error(f"清空会员信息表失败: {e}")
            self.conn.rollback()
    
    def clear_pro_info(self):
        """清空商品档案表"""
        try:
            clear_sql = "TRUNCATE TABLE pro_info RESTART IDENTITY"
            self.cursor.execute(clear_sql)
            self.conn.commit()
            logger.info("商品档案表已清空")
        except Exception as e:
            logger.error(f"清空商品档案表失败: {e}")
            self.conn.rollback()

    def insert_vip_info(self, vip):
        """插入会员信息"""
        insert_sql = """
        INSERT INTO vip_info (
            vip_id, vip_name, vip_phone, vip_sex, vip_birthday, vip_type, vip_level,
            vip_state, vip_zc_date, vip_cls_id, vip_cls_name, vip_mall_id, vip_mall_name,
            vip_jf_value, vip_ye_value, vip_xf_total_money, vip_last_xf_time,
            vip_zc_user_id, vip_zc_user_name, vip_remark
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s
        )
        ON CONFLICT (vip_id) DO UPDATE SET
            vip_name = EXCLUDED.vip_name,
            vip_phone = EXCLUDED.vip_phone,
            vip_sex = EXCLUDED.vip_sex,
            vip_birthday = EXCLUDED.vip_birthday,
            vip_type = EXCLUDED.vip_type,
            vip_level = EXCLUDED.vip_level,
            vip_state = EXCLUDED.vip_state,
            vip_zc_date = EXCLUDED.vip_zc_date,
            vip_cls_id = EXCLUDED.vip_cls_id,
            vip_cls_name = EXCLUDED.vip_cls_name,
            vip_mall_id = EXCLUDED.vip_mall_id,
            vip_mall_name = EXCLUDED.vip_mall_name,
            vip_jf_value = EXCLUDED.vip_jf_value,
            vip_ye_value = EXCLUDED.vip_ye_value,
            vip_xf_total_money = EXCLUDED.vip_xf_total_money,
            vip_last_xf_time = EXCLUDED.vip_last_xf_time,
            vip_zc_user_id = EXCLUDED.vip_zc_user_id,
            vip_zc_user_name = EXCLUDED.vip_zc_user_name,
            vip_remark = EXCLUDED.vip_remark,
            updated_at = CURRENT_TIMESTAMP
        """
        try:
            # 处理日期字段
            def parse_date(date_str):
                if date_str:
                    try:
                        return datetime.strptime(date_str, '%Y-%m-%d').date()
                    except:
                        try:
                            return datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S').date()
                        except:
                            return None
                return None

            # 处理时间字段
            def parse_time(time_str):
                if time_str:
                    try:
                        return datetime.strptime(time_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        return None
                return None

            # 处理数值字段
            def parse_decimal(value):
                if value:
                    try:
                        return float(value)
                    except:
                        return 0.0
                return 0.0

            # 构建参数
            params = (
                vip.get('vip_id'),
                vip.get('vip_name'),
                vip.get('vip_phone'),
                vip.get('vip_sex'),
                parse_date(vip.get('vip_birthday')),
                vip.get('vip_type'),
                vip.get('vip_level'),
                vip.get('vip_state'),
                parse_date(vip.get('vip_zc_date')),
                vip.get('vip_cls_id'),
                vip.get('vip_cls_name'),
                vip.get('vip_mall_id'),
                vip.get('vip_mall_name'),
                parse_decimal(vip.get('vip_jf_value')),
                parse_decimal(vip.get('vip_ye_value')),
                parse_decimal(vip.get('vip_xf_total_money')),
                parse_time(vip.get('vip_last_xf_time')),
                vip.get('vip_zc_user_id'),
                vip.get('vip_zc_user_name'),
                vip.get('vip_remark')
            )

            self.cursor.execute(insert_sql, params)
            self.conn.commit()
        except Exception as e:
            logger.error(f"插入会员信息失败: {e}")
            self.conn.rollback()
    
    def insert_pro_info(self, pro):
        """插入商品档案信息"""
        try:
            # 处理日期时间字段
            def parse_datetime(dt_str):
                if dt_str:
                    try:
                        return datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                    except:
                        try:
                            return datetime.strptime(dt_str, '%Y-%m-%d')
                        except:
                            return None
                return None

            # 处理数值字段
            def parse_decimal(value):
                if value:
                    try:
                        return float(value)
                    except:
                        return 0.0
                return 0.0

            # 构建参数 - 按照API返回的实际字段名
            params = (
                pro.get('TR'),  # tr
                pro.get('TP'),  # tp
                pro.get('pro_id'),  # pro_id
                parse_decimal(pro.get('in_price')),  # in_price
                parse_decimal(pro.get('sale_price')),  # sale_price
                parse_decimal(pro.get('vip_price')),  # vip_price
                parse_decimal(pro.get('vip_price1')),  # vip_price1
                parse_decimal(pro.get('vip_price2')),  # vip_price2
                parse_decimal(pro.get('vip_price3')),  # vip_price3
                parse_decimal(pro.get('tj_sale_price')),  # tj_sale_price
                parse_decimal(pro.get('ps_price')),  # ps_price
                parse_decimal(pro.get('pf_price')),  # pf_price
                parse_decimal(pro.get('tax_rate')),  # tax_rate
                pro.get('state'),  # state
                pro.get('mall_id'),  # mall_id
                parse_decimal(pro.get('low_stock')),  # low_stock
                parse_decimal(pro.get('max_stock')),  # max_stock
                pro.get('img_url'),  # img_url
                pro.get('asc_desc'),  # asc_desc
                pro.get('chg_user_name'),  # chg_user_name
                parse_datetime(pro.get('chg_time')),  # chg_time
                parse_decimal(pro.get('low_sale_price')),  # low_sale_price
                parse_decimal(pro.get('online_price')),  # online_price
                pro.get('online_state'),  # online_state
                pro.get('trade_day'),  # trade_day
                parse_decimal(pro.get('day_can_sale_num')),  # day_can_sale_num
                parse_decimal(pro.get('day_sale_num')),  # day_sale_num
                parse_decimal(pro.get('day_now_num')),  # day_now_num
                pro.get('hot_yn'),  # hot_yn
                pro.get('tj_yn'),  # tj_yn
                pro.get('index_show_yn'),  # index_show_yn
                parse_datetime(pro.get('pro_create_time')),  # pro_create_time
                parse_datetime(pro.get('pro_stop_time')),  # pro_stop_time
                pro.get('ex_mark'),  # ex_mark
                parse_decimal(pro.get('sale_mrl')),  # sale_mrl
                parse_decimal(pro.get('pf_mrl')),  # pf_mrl
                parse_decimal(pro.get('stock')),  # stock
                pro.get('pro_asc_desc'),  # pro_asc_desc
                pro.get('pro_name'),  # pro_name
                pro.get('pym'),  # pym
                pro.get('zbm'),  # zbm
                pro.get('size'),  # size
                pro.get('unit'),  # unit
                pro.get('price_way'),  # price_way
                pro.get('pro_type'),  # pro_type
                pro.get('cls_id'),  # cls_id
                pro.get('ledger_cls_id'),  # ledger_cls_id
                pro.get('dx_unit1_name'),  # dx_unit1_name
                parse_decimal(pro.get('dx_unit1_num')),  # dx_unit1_num
                pro.get('dx_unit2_name'),  # dx_unit2_name
                parse_decimal(pro.get('dx_unit2_num')),  # dx_unit2_num
                parse_decimal(pro.get('dx_unit1_stock')),  # dx_unit1_stock
                parse_decimal(pro.get('dx_unit2_stock')),  # dx_unit2_stock
                pro.get('dx_unit_stock'),  # dx_unit_stock
                pro.get('online_pro_name'),  # online_pro_name
                pro.get('online_memo'),  # online_memo
                pro.get('pro_address'),  # pro_address
                pro.get('user_memo'),  # user_memo
                pro.get('bzq_yn'),  # bzq_yn
                pro.get('bzq'),  # bzq
                pro.get('tc_way'),  # tc_way
                parse_decimal(pro.get('tc_value')),  # tc_value
                pro.get('jf_yn'),  # jf_yn
                parse_decimal(pro.get('jf_value')),  # jf_value
                pro.get('mall_chg_price_yn'),  # mall_chg_price_yn
                pro.get('sale_show_yn'),  # sale_show_yn
                pro.get('sale_zk_yn'),  # sale_zk_yn
                pro.get('gl_kc_yn'),  # gl_kc_yn
                pro.get('sale_chg_price_yn'),  # sale_chg_price_yn
                parse_datetime(pro.get('creat_time')),  # creat_time
                pro.get('pp_id'),  # pp_id
                pro.get('gys_id'),  # gys_id
                pro.get('cls_name'),  # cls_name
                pro.get('gys_name'),  # gys_name
                pro.get('mall_name'),  # mall_name
                pro.get('pp_name'),  # pp_name
                pro.get('mall_type'),  # mall_type
                pro.get('pro_cls_mall_id'),  # pro_cls_mall_id
                pro.get('unit_count'),  # unit_count
                pro.get('color_size_count'),  # color_size_count
                parse_decimal(pro.get('sale_ledger_rate')),  # sale_ledger_rate
                pro.get('tmp_price_yn'),  # tmp_price_yn
                pro.get('color_size_sub_id'),  # color_size_sub_id
                parse_decimal(pro.get('total_in_money')),  # total_in_money
                pro.get('RC')  # rc
            )

            # 构建SQL语句，确保字段数量和参数数量一致
            insert_sql = f"""
            INSERT INTO pro_info (
                tr, tp, pro_id, in_price, sale_price, vip_price, vip_price1, vip_price2, vip_price3,
                tj_sale_price, ps_price, pf_price, tax_rate, state, mall_id, low_stock, max_stock, img_url,
                asc_desc, chg_user_name, chg_time, low_sale_price, online_price, online_state, trade_day,
                day_can_sale_num, day_sale_num, day_now_num, hot_yn, tj_yn, index_show_yn, pro_create_time,
                pro_stop_time, ex_mark, sale_mrl, pf_mrl, stock, pro_asc_desc, pro_name, pym, zbm, size,
                unit, price_way, pro_type, cls_id, ledger_cls_id, dx_unit1_name, dx_unit1_num, dx_unit2_name,
                dx_unit2_num, dx_unit1_stock, dx_unit2_stock, dx_unit_stock, online_pro_name, online_memo,
                pro_address, user_memo, bzq_yn, bzq, tc_way, tc_value, jf_yn, jf_value, mall_chg_price_yn,
                sale_show_yn, sale_zk_yn, gl_kc_yn, sale_chg_price_yn, creat_time, pp_id, gys_id, cls_name,
                gys_name, mall_name, pp_name, mall_type, pro_cls_mall_id, unit_count, color_size_count,
                sale_ledger_rate, tmp_price_yn, color_size_sub_id, total_in_money, rc
            ) VALUES (
                {'%s, ' * (len(params) - 1)}%s
            )
            ON CONFLICT (pro_id) DO UPDATE SET
                tr = EXCLUDED.tr, tp = EXCLUDED.tp, in_price = EXCLUDED.in_price, sale_price = EXCLUDED.sale_price,
                vip_price = EXCLUDED.vip_price, vip_price1 = EXCLUDED.vip_price1, vip_price2 = EXCLUDED.vip_price2,
                vip_price3 = EXCLUDED.vip_price3, tj_sale_price = EXCLUDED.tj_sale_price, ps_price = EXCLUDED.ps_price,
                pf_price = EXCLUDED.pf_price, tax_rate = EXCLUDED.tax_rate, state = EXCLUDED.state,
                mall_id = EXCLUDED.mall_id, low_stock = EXCLUDED.low_stock, max_stock = EXCLUDED.max_stock,
                img_url = EXCLUDED.img_url, asc_desc = EXCLUDED.asc_desc, chg_user_name = EXCLUDED.chg_user_name,
                chg_time = EXCLUDED.chg_time, low_sale_price = EXCLUDED.low_sale_price, online_price = EXCLUDED.online_price,
                online_state = EXCLUDED.online_state, trade_day = EXCLUDED.trade_day, day_can_sale_num = EXCLUDED.day_can_sale_num,
                day_sale_num = EXCLUDED.day_sale_num, day_now_num = EXCLUDED.day_now_num, hot_yn = EXCLUDED.hot_yn,
                tj_yn = EXCLUDED.tj_yn, index_show_yn = EXCLUDED.index_show_yn, pro_create_time = EXCLUDED.pro_create_time,
                pro_stop_time = EXCLUDED.pro_stop_time, ex_mark = EXCLUDED.ex_mark, sale_mrl = EXCLUDED.sale_mrl,
                pf_mrl = EXCLUDED.pf_mrl, stock = EXCLUDED.stock, pro_asc_desc = EXCLUDED.pro_asc_desc,
                pro_name = EXCLUDED.pro_name, pym = EXCLUDED.pym, zbm = EXCLUDED.zbm, size = EXCLUDED.size,
                unit = EXCLUDED.unit, price_way = EXCLUDED.price_way, pro_type = EXCLUDED.pro_type,
                cls_id = EXCLUDED.cls_id, ledger_cls_id = EXCLUDED.ledger_cls_id, dx_unit1_name = EXCLUDED.dx_unit1_name,
                dx_unit1_num = EXCLUDED.dx_unit1_num, dx_unit2_name = EXCLUDED.dx_unit2_name, dx_unit2_num = EXCLUDED.dx_unit2_num,
                dx_unit1_stock = EXCLUDED.dx_unit1_stock, dx_unit2_stock = EXCLUDED.dx_unit2_stock, dx_unit_stock = EXCLUDED.dx_unit_stock,
                online_pro_name = EXCLUDED.online_pro_name, online_memo = EXCLUDED.online_memo, pro_address = EXCLUDED.pro_address,
                user_memo = EXCLUDED.user_memo, bzq_yn = EXCLUDED.bzq_yn, bzq = EXCLUDED.bzq, tc_way = EXCLUDED.tc_way,
                tc_value = EXCLUDED.tc_value, jf_yn = EXCLUDED.jf_yn, jf_value = EXCLUDED.jf_value,
                mall_chg_price_yn = EXCLUDED.mall_chg_price_yn, sale_show_yn = EXCLUDED.sale_show_yn,
                sale_zk_yn = EXCLUDED.sale_zk_yn, gl_kc_yn = EXCLUDED.gl_kc_yn, sale_chg_price_yn = EXCLUDED.sale_chg_price_yn,
                creat_time = EXCLUDED.creat_time, pp_id = EXCLUDED.pp_id, gys_id = EXCLUDED.gys_id,
                cls_name = EXCLUDED.cls_name, gys_name = EXCLUDED.gys_name, mall_name = EXCLUDED.mall_name,
                pp_name = EXCLUDED.pp_name, mall_type = EXCLUDED.mall_type, pro_cls_mall_id = EXCLUDED.pro_cls_mall_id,
                unit_count = EXCLUDED.unit_count, color_size_count = EXCLUDED.color_size_count,
                sale_ledger_rate = EXCLUDED.sale_ledger_rate, tmp_price_yn = EXCLUDED.tmp_price_yn,
                color_size_sub_id = EXCLUDED.color_size_sub_id, total_in_money = EXCLUDED.total_in_money,
                rc = EXCLUDED.rc, updated_at = CURRENT_TIMESTAMP
            """

            self.cursor.execute(insert_sql, params)
            self.conn.commit()
        except Exception as e:
            logger.error(f"插入商品档案信息失败: {e}")
            self.conn.rollback()

    def insert_weather_data(self, weather_data):
        """插入天气数据"""
        insert_sql = """
        INSERT INTO weather_history (
            weather_date, temp_max, temp_min, precipitation, weather_code, weather_desc,
            latitude, longitude, location_name
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (weather_date) DO UPDATE SET
            temp_max = EXCLUDED.temp_max,
            temp_min = EXCLUDED.temp_min,
            precipitation = EXCLUDED.precipitation,
            weather_code = EXCLUDED.weather_code,
            weather_desc = EXCLUDED.weather_desc,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            location_name = EXCLUDED.location_name,
            updated_at = CURRENT_TIMESTAMP
        """
        try:
            from datetime import datetime
            
            def parse_date(date_str):
                if date_str:
                    try:
                        return datetime.strptime(date_str, '%Y-%m-%d').date()
                    except:
                        return None
                return None
            
            def parse_decimal(value):
                if value is not None:
                    try:
                        return float(value)
                    except:
                        return None
                return None
            
            params = (
                parse_date(weather_data.get('date')),
                parse_decimal(weather_data.get('temp_max')),
                parse_decimal(weather_data.get('temp_min')),
                parse_decimal(weather_data.get('precipitation')),
                weather_data.get('weather_code'),
                weather_data.get('weather_desc'),
                parse_decimal(weather_data.get('latitude')),
                parse_decimal(weather_data.get('longitude')),
                weather_data.get('location_name')
            )
            
            self.cursor.execute(insert_sql, params)
            self.conn.commit()
        except Exception as e:
            logger.error(f"插入天气数据失败: {e}")
            self.conn.rollback()
    
    def close(self):
        """关闭数据库连接"""
        try:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
            logger.info("数据库连接已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接失败: {e}")
