import base64
import json
import logging
import requests
from config import SALE_RECORD_API_CONFIG, SALE_DETAIL_API_CONFIG, VIP_API_CONFIG, PRODUCT_API_CONFIG

logger = logging.getLogger(__name__)


def encode_req_json(req_json):
    """将请求参数编码为base64字符串"""
    try:
        json_str = json.dumps(req_json, ensure_ascii=False)
        return base64.b64encode(json_str.encode('utf-8')).decode('utf-8')
    except Exception as e:
        logger.error(f"编码请求参数失败: {e}")
        return None

# 销售记录API相关函数
def build_sale_record_request_body(start_date, end_date, page=1, page_size=50):
    """构建销售记录API请求体"""
    req_json = {
        "OPER": "BILL_LIST",
        "DJ_TYPE": "全部",
        "OPER_TYPE": "",
        "USER_ID": "全部",
        "MALL_ID": "00010001",
        "STATE": "全部",
        "USER_TYPE_YN": "Y",
        "START_TIME": start_date.strftime('%Y-%m-%d %H:%M:%S'),
        "OVER_TIME": end_date.strftime('%Y-%m-%d %H:%M:%S'),
        "PAGE_SIZE": str(page_size),
        "NOW_PAGE": page,
        "MH_YN": "Y"
    }
    
    req_json_str = encode_req_json(req_json)
    if not req_json_str:
        logger.error("编码请求参数失败")
        return None
    
    return {
        "API_NAME": SALE_RECORD_API_CONFIG['api_name'],
        "METHOD_NAME": SALE_RECORD_API_CONFIG['method_name'],
        "REQ_JSON_STR": req_json_str,
        "API_USER_PHONE": SALE_RECORD_API_CONFIG['api_user_phone'],
        "API_USER_ID": SALE_RECORD_API_CONFIG['api_user_id'],
        "API_REQ_FROM": SALE_RECORD_API_CONFIG['api_req_from'],
        "API_REQ_VER": SALE_RECORD_API_CONFIG['api_req_ver']
    }

def fetch_sale_record_data(start_date, end_date, page=1, page_size=50):
    """从API拉取销售记录数据"""
    url = SALE_RECORD_API_CONFIG['url']
    headers = SALE_RECORD_API_CONFIG['headers']
    
    try:
        # 构建请求体
        data = build_sale_record_request_body(start_date, end_date, page, page_size)
        if not data:
            return None
        
        # 发送请求
        response = requests.post(url, json=data, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 检测响应编码
        if response.encoding is None:
            response.encoding = response.apparent_encoding
        
        # 解析响应
        try:
            result = response.json()
        except json.JSONDecodeError:
            # 尝试使用GBK编码解析
            try:
                result = json.loads(response.content.decode('gbk'))
            except Exception as e:
                logger.error(f"JSON解析失败: {e}")
                return None
        
        logger.info(f"销售记录API响应状态: {result.get('result')}")
        logger.info(f"销售记录API响应消息: {result.get('msg')}")
        
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"销售记录API请求失败: {e}")
        return None
    except Exception as e:
        logger.error(f"拉取销售记录数据失败: {e}")
        return None

# 销售记录明细API相关函数
def build_sale_detail_request_body(start_date, end_date, page=1, page_size=50, dh_id=""):
    """构建销售记录明细API请求体"""
    req_json = {
        "OPER": "SP_LIST",
        "SEARCH_STR": "",
        "DH_ID": dh_id,
        "DJ_TYPE": "全部",
        "MALL_ID": "00010001",
        "GYS_ID": "",
        "STATE": "全部",
        "USER_ID": "全部",
        "USER_TYPE_YN": "Y",
        "START_TIME": start_date.strftime('%Y-%m-%d %H:%M:%S'),
        "OVER_TIME": end_date.strftime('%Y-%m-%d %H:%M:%S'),
        "PAGE_SIZE": str(page_size),
        "NOW_PAGE": page,
        "MH_YN": "Y"
    }
    
    req_json_str = encode_req_json(req_json)
    if not req_json_str:
        logger.error("编码请求参数失败")
        return None
    
    return {
        "API_NAME": SALE_DETAIL_API_CONFIG['api_name'],
        "METHOD_NAME": SALE_DETAIL_API_CONFIG['method_name'],
        "REQ_JSON_STR": req_json_str,
        "API_USER_PHONE": SALE_DETAIL_API_CONFIG['api_user_phone'],
        "API_USER_ID": SALE_DETAIL_API_CONFIG['api_user_id'],
        "API_REQ_FROM": SALE_DETAIL_API_CONFIG['api_req_from'],
        "API_REQ_VER": SALE_DETAIL_API_CONFIG['api_req_ver']
    }

def fetch_sale_detail_data(start_date, end_date, page=1, page_size=50, dh_id=""):
    """从API拉取销售记录明细数据"""
    url = SALE_DETAIL_API_CONFIG['url']
    headers = SALE_DETAIL_API_CONFIG['headers']
    
    try:
        # 构建请求体
        data = build_sale_detail_request_body(start_date, end_date, page, page_size, dh_id)
        if not data:
            return None
        
        # 发送请求
        response = requests.post(url, json=data, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 检测响应编码
        if response.encoding is None:
            response.encoding = response.apparent_encoding
        
        # 解析响应
        try:
            result = response.json()
        except json.JSONDecodeError:
            # 尝试使用GBK编码解析
            try:
                result = json.loads(response.content.decode('gbk'))
            except Exception as e:
                logger.error(f"JSON解析失败: {e}")
                return None
        
        logger.info(f"销售记录明细API响应状态: {result.get('result')}")
        logger.info(f"销售记录明细API响应消息: {result.get('msg')}")
        
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"销售记录明细API请求失败: {e}")
        return None
    except Exception as e:
        logger.error(f"拉取销售记录明细数据失败: {e}")
        return None

def process_data(data):
    """处理API返回的数据"""
    try:
        if not data or data.get('result') != 'sucess':
            logger.warning(f"API返回数据异常: {data}")
            return []
        return data.get('data', [])
    except Exception as e:
        logger.error(f"处理数据失败: {e}")
        return []


# 会员信息API相关函数
def build_vip_request_body(page=1, page_size=50, search_str="", cls_id="00"):
    """构建会员信息API请求体"""
    req_json = {
        "OPER": "MALL_VIP_LIST",
        "MALL_ID": "00010001",
        "CLS_ID": cls_id,
        "SEARCH_STR": search_str,
        "PAGE_SIZE": str(page_size),
        "NOW_PAGE": str(page),
        "ZC_YN": "Y",
        "MH_YN": "Y"
    }
    
    req_json_str = encode_req_json(req_json)
    if not req_json_str:
        logger.error("编码请求参数失败")
        return None
    
    return {
        "API_NAME": VIP_API_CONFIG['api_name'],
        "METHOD_NAME": VIP_API_CONFIG['method_name'],
        "REQ_JSON_STR": req_json_str,
        "API_USER_PHONE": VIP_API_CONFIG['api_user_phone'],
        "API_USER_ID": VIP_API_CONFIG['api_user_id'],
        "API_REQ_FROM": VIP_API_CONFIG['api_req_from'],
        "API_REQ_VER": VIP_API_CONFIG['api_req_ver'],
        "API_REQ_TOKEN": ""
    }


def fetch_vip_data(page=1, page_size=50, search_str="", cls_id="00"):
    """从API拉取会员信息数据"""
    url = VIP_API_CONFIG['url']
    headers = VIP_API_CONFIG['headers']
    
    try:
        # 构建请求体
        data = build_vip_request_body(page, page_size, search_str, cls_id)
        if not data:
            return None
        
        # 发送请求
        response = requests.post(url, json=data, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 检测响应编码
        if response.encoding is None:
            response.encoding = response.apparent_encoding
        
        # 解析响应
        try:
            result = response.json()
        except json.JSONDecodeError:
            # 尝试使用GBK编码解析
            try:
                result = json.loads(response.content.decode('gbk'))
            except Exception as e:
                logger.error(f"JSON解析失败: {e}")
                return None
        
        logger.info(f"会员信息API响应状态: {result.get('result')}")
        logger.info(f"会员信息API响应消息: {result.get('msg')}")
        
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"会员信息API请求失败: {e}")
        return None
    except Exception as e:
        logger.error(f"拉取会员信息数据失败: {e}")
        return None


def build_pro_info_request_body(page=1, page_size=50, search_str="", cls_id="00", gys_id="", pp_id="", zc_yn="全部", mh_yn="Y", pro_type="", state="正常"):
    """构建商品档案API请求体"""
    req_json = {
        "OPER": "PRO_LIST",
        "MALL_ID": "00010001",
        "CLS_ID": cls_id,
        "GYS_ID": gys_id,
        "USER_ID": "",
        "PP_ID": pp_id,
        "SEARCH_STR": search_str,
        "PAGE_SIZE": page_size,
        "NOW_PAGE": page,
        "ZC_YN": zc_yn,
        "MH_YN": mh_yn,
        "PRO_TYPE": pro_type,
        "STATE": state
    }
    
    req_json_str = encode_req_json(req_json)
    if not req_json_str:
        logger.error("编码请求参数失败")
        return None
    
    return {
        "API_NAME": PRODUCT_API_CONFIG['api_name'],
        "METHOD_NAME": PRODUCT_API_CONFIG['method_name'],
        "REQ_JSON_STR": req_json_str,
        "API_USER_PHONE": PRODUCT_API_CONFIG['api_user_phone'],
        "API_USER_ID": PRODUCT_API_CONFIG['api_user_id'],
        "API_REQ_FROM": PRODUCT_API_CONFIG['api_req_from'],
        "API_REQ_VER": PRODUCT_API_CONFIG['api_req_ver'],
        "API_REQ_TOKEN": ""
    }


def fetch_pro_info_data(page=1, page_size=50, search_str="", cls_id="00", gys_id="", pp_id="", zc_yn="全部", mh_yn="Y", pro_type="", state="正常"):
    """从API拉取商品档案数据"""
    url = PRODUCT_API_CONFIG['url']
    headers = PRODUCT_API_CONFIG['headers']
    
    try:
        # 构建请求体
        data = build_pro_info_request_body(page, page_size, search_str, cls_id, gys_id, pp_id, zc_yn, mh_yn, pro_type, state)
        if not data:
            return None
        
        # 发送请求
        response = requests.post(url, json=data, headers=headers, timeout=30)
        response.raise_for_status()
        
        # 检测响应编码
        if response.encoding is None:
            response.encoding = response.apparent_encoding
        
        # 解析响应
        try:
            result = response.json()
        except json.JSONDecodeError:
            # 尝试使用GBK编码解析
            try:
                result = json.loads(response.content.decode('gbk'))
            except Exception as e:
                logger.error(f"JSON解析失败: {e}")
                return None
        
        logger.info(f"商品档案API响应状态: {result.get('result')}")
        logger.info(f"商品档案API响应消息: {result.get('msg')}")
        
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"商品档案API请求失败: {e}")
        return None
    except Exception as e:
        logger.error(f"拉取商品档案数据失败: {e}")
        return None
