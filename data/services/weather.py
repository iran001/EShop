import requests
import pandas as pd  # pyright: ignore[reportMissingImports]

def get_kunshan_history(start_date, end_date):
    # 昆山坐标
    lat = 31.38
    lon = 120.98
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,weathercode",
        "start_date": start_date,
        "end_date": end_date,
        "timezone": "Asia/Shanghai",
        "temperature_unit": "celsius",
        "precipitation_unit": "mm"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if 'daily' not in data:
        return None

    # WMO 天气代码映射表 (简化版)
    weather_codes = {
        0: "晴", 1: "晴", 2: "多云", 3: "阴",
        45: "雾", 48: "雾凇",
        51: "毛毛雨", 53: "毛毛雨", 55: "毛毛雨",
        61: "小雨", 63: "中雨", 65: "大雨",
        71: "小雪", 73: "中雪", 75: "大雪",
        80: "阵雨", 81: "强阵雨", 82: "暴雨",
        95: "雷雨", 96: "雷阵雨伴冰雹", 99: "强雷暴"
    }
    
    df = pd.DataFrame({
        '日期': data['daily']['time'],
        '最高气温': data['daily']['temperature_2m_max'],
        '最低气温': data['daily']['temperature_2m_min'],
        '降雨量': data['daily']['precipitation_sum'],
        '天气代码': data['daily']['weathercode']
    })
    
    # 转换天气代码为中文
    df['天气状况'] = df['天气代码'].map(lambda x: weather_codes.get(x, "未知"))
    
    return df
