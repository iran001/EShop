# 数据库配置
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'EShopDB',
    'user': 'postgres',
    'password': '123456',
    'options': '-c client_encoding=utf8'
}

# 销售记录API配置
SALE_RECORD_API_CONFIG = {
    'url': 'https://www.91byk.com/smSellCaf/api/smSellApi/',
    'method': 'post',
    'headers': {
        'Content-Type': 'application/json'
    },
    'api_name': 'smSellQueryApi',
    'method_name': 'GET_LS_DETAIL',
    'api_user_phone': '18260284170',
    'api_user_id': '1001',
    'api_req_from': 'WEB',
    'api_req_ver': 'smSellAdmin'
}

# 销售记录明细API配置
SALE_DETAIL_API_CONFIG = {
    'url': 'https://www.91byk.com/smSellCaf/api/smSellApi/',
    'method': 'post',
    'headers': {
        'Content-Type': 'application/json'
    },
    'api_name': 'smSellQueryApi',
    'method_name': 'GET_LS_DETAIL',  # 假设与API1相同，实际可能需要调整
    'api_user_phone': '18260284170',
    'api_user_id': '1001',
    'api_req_from': 'WEB',
    'api_req_ver': 'smSellAdmin'
}

# 会员信息API配置
VIP_API_CONFIG = {
    'url': 'https://www.91byk.com/smSellCaf/api/smSellApi',
    'method': 'post',
    'headers': {
        'Content-Type': 'application/json'
    },
    'api_name': 'smSellQueryApi',
    'method_name': 'GET_VIP_INFO',
    'api_user_phone': '18260284170',
    'api_user_id': '1001',
    'api_req_from': 'WEB',
    'api_req_ver': 'smSellAdmin'
}

# 商品档案API配置
PRODUCT_API_CONFIG = {
    'url': 'https://www.91byk.com/smSellCaf/api/smSellApi',
    'method': 'post',
    'headers': {
        'Content-Type': 'application/json'
    },
    'api_name': 'smSellQueryApi',
    'method_name': 'GET_PRO_INFO',
    'api_user_phone': '18260284170',
    'api_user_id': '1001',
    'api_req_from': 'WEB',
    'api_req_ver': 'smSellAdmin'
}

# 天气API配置
WEATHER_API_CONFIG = {
    'url': 'https://archive-api.open-meteo.com/v1/archive',
    'latitude': 31.38,  # 昆山纬度
    'longitude': 120.98,  # 昆山经度
    'location_name': '昆山',
    'timezone': 'Asia/Shanghai',
    'temperature_unit': 'celsius',
    'precipitation_unit': 'mm',
    'daily_fields': 'temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode'
}

# 天气代码映射表
WEATHER_CODE_MAP = {
    0: "晴", 1: "晴", 2: "多云", 3: "阴",
    45: "雾", 48: "雾凇",
    51: "毛毛雨", 53: "毛毛雨", 55: "毛毛雨",
    61: "小雨", 63: "中雨", 65: "大雨",
    71: "小雪", 73: "中雪", 75: "大雪",
    80: "阵雨", 81: "强阵雨", 82: "暴雨",
    95: "雷雨", 96: "雷阵雨伴冰雹", 99: "强雷暴"
}

# 拉取配置
FETCH_CONFIG = {
    'page_size': 50,
    'start_date': '2022-11-01',
    'end_date': '2022-12-31',
    'test_days': 3  # 测试时只拉取最近几天的数据
}

# 定时任务配置
JOB_CONFIG = {
    'cron_expression': '0 0 * * *',  # 每天凌晨执行
    'web_port': 5000,  # Web服务端口
    'tasks': {
        'sync_sale_records': {
            'name': '同步销售记录',
            'description': '从API拉取销售记录数据并保存到数据库',
            'enabled': True,
            'cron': '0 1 * * *'  # 每天凌晨1点执行
        },
        'sync_sale_details': {
            'name': '同步销售记录明细',
            'description': '从API拉取销售记录明细数据并保存到数据库',
            'enabled': True,
            'cron': '0 2 * * *'  # 每天凌晨2点执行
        },
        'sync_all': {
            'name': '全量同步',
            'description': '同步所有数据',
            'enabled': True,
            'cron': '0 0 * * *'  # 每天凌晨执行
        },
        'sync_vip': {
            'name': '同步会员信息',
            'description': '从API拉取会员信息数据并保存到数据库',
            'enabled': True,
            'cron': '0 3 * * *'  # 每天凌晨3点执行
        },
        'sync_pro_info': {
            'name': '同步商品档案',
            'description': '从API拉取商品档案数据并保存到数据库（先清除后插入）',
            'enabled': True,
            'cron': '0 4 * * *'  # 每天凌晨4点执行
        },
        'sync_weather': {
            'name': '同步历史天气',
            'description': '从Open-Meteo API拉取昆山历史天气数据（2022-11-01至今）并保存到数据库',
            'enabled': True,
            'cron': '0 5 * * *',  # 每天凌晨5点执行
            'params': {
                'start_date': '2022-11-01',  # 起始日期
                'end_date': '2026-02-28',  # 结束日期，None表示今天
                'location': 'kunshan'
            }
        }
    }
}
