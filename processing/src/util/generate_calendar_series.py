"""
generate_calendar_series.py - 污染日历矩阵数据生成器

功能：
- 读取2013年城市日级清洗数据
- 计算AQI（遵循HJ 633-2012标准）
- 生成日历热力图JSON数据

用法：python processing/src/util/generate_calendar_series.py --year 2013
"""

import os
import json
import argparse
import pandas as pd
from datetime import datetime, timedelta


# ==================== AQI 计算模块 ====================

# HJ 633-2012 AQI分级浓度限值表
AQI_BREAKPOINTS = {
    'pm25': [
        (0, 35, 0, 50),      # 优
        (35, 75, 50, 100),   # 良
        (75, 115, 100, 150), # 轻度污染
        (115, 150, 150, 200),# 中度污染
        (150, 250, 200, 300),# 重度污染
        (250, 350, 300, 400),# 严重污染
        (350, 500, 400, 500),# 严重污染
    ],
    'pm10': [
        (0, 50, 0, 50),
        (50, 150, 50, 100),
        (150, 250, 100, 150),
        (250, 350, 150, 200),
        (350, 420, 200, 300),
        (420, 500, 300, 400),
        (500, 600, 400, 500),
    ]
}

# AQI质量等级映射
AQI_LEVELS = [
    (0, 50, '优', '#00E400'),
    (51, 100, '良', '#FFFF00'),
    (101, 150, '轻度污染', '#FF7E00'),
    (151, 200, '中度污染', '#FF0000'),
    (201, 300, '重度污染', '#99004C'),
    (301, 500, '严重污染', '#7E0023'),
]

# 2013年法定节假日
HOLIDAYS_2013 = {
    # 元旦
    '2013-01-01', '2013-01-02', '2013-01-03',
    # 春节
    '2013-02-09', '2013-02-10', '2013-02-11', '2013-02-12', 
    '2013-02-13', '2013-02-14', '2013-02-15',
    # 清明
    '2013-04-04', '2013-04-05', '2013-04-06',
    # 劳动节
    '2013-04-29', '2013-04-30', '2013-05-01',
    # 端午
    '2013-06-10', '2013-06-11', '2013-06-12',
    # 中秋
    '2013-09-19', '2013-09-20', '2013-09-21',
    # 国庆
    '2013-10-01', '2013-10-02', '2013-10-03', '2013-10-04',
    '2013-10-05', '2013-10-06', '2013-10-07',
}


def calculate_iaqi(concentration, pollutant):
    """
    计算单项污染物的分指数 IAQI
    遵循 HJ 633-2012 标准
    """
    if pd.isna(concentration) or concentration < 0:
        return None
    
    breakpoints = AQI_BREAKPOINTS.get(pollutant)
    if not breakpoints:
        return None
    
    for bp_lo, bp_hi, iaqi_lo, iaqi_hi in breakpoints:
        if bp_lo <= concentration <= bp_hi:
            # 线性插值公式: IAQI = (IAQI_hi - IAQI_lo) / (BP_hi - BP_lo) * (C - BP_lo) + IAQI_lo
            iaqi = (iaqi_hi - iaqi_lo) / (bp_hi - bp_lo) * (concentration - bp_lo) + iaqi_lo
            return round(iaqi)
    
    # 超出范围返回500
    if concentration > breakpoints[-1][1]:
        return 500
    
    return None


def calculate_aqi(pm25, pm10):
    """
    计算AQI值（取PM2.5和PM10分指数的最大值）
    返回: (aqi, level, color, primary_pollutant)
    """
    iaqi_pm25 = calculate_iaqi(pm25, 'pm25')
    iaqi_pm10 = calculate_iaqi(pm10, 'pm10')
    
    if iaqi_pm25 is None and iaqi_pm10 is None:
        return None, None, None, None
    
    if iaqi_pm25 is None:
        aqi = iaqi_pm10
        primary = 'PM10'
    elif iaqi_pm10 is None:
        aqi = iaqi_pm25
        primary = 'PM2.5'
    elif iaqi_pm25 >= iaqi_pm10:
        aqi = iaqi_pm25
        primary = 'PM2.5'
    else:
        aqi = iaqi_pm10
        primary = 'PM10'
    
    # 获取质量等级
    level = '未知'
    color = '#CCCCCC'
    for lo, hi, lvl, clr in AQI_LEVELS:
        if lo <= aqi <= hi:
            level = lvl
            color = clr
            break
    if aqi > 300:
        level = '严重污染'
        color = '#7E0023'
    
    return aqi, level, color, primary


def is_weekend(date_str):
    """判断是否为周末"""
    dt = datetime.strptime(date_str, '%Y-%m-%d')
    return dt.weekday() >= 5  # 5=周六, 6=周日


def is_holiday(date_str):
    """判断是否为法定节假日"""
    return date_str in HOLIDAYS_2013


# ==================== 数据处理模块 ====================

def load_daily_data(year, processed_dir):
    """
    加载指定年份的所有日级数据
    返回按城市分组的DataFrame字典
    """
    base_path = os.path.join(processed_dir, 'city', str(year))
    
    if not os.path.exists(base_path):
        print(f"错误: 找不到目录 {base_path}")
        return {}
    
    all_data = []
    
    # 遍历月份目录
    for month_dir in sorted(os.listdir(base_path)):
        month_path = os.path.join(base_path, month_dir)
        if not os.path.isdir(month_path):
            continue
        
        # 遍历日期目录
        for day_dir in sorted(os.listdir(month_path)):
            day_path = os.path.join(month_path, day_dir)
            if not os.path.isdir(day_path):
                continue
            
            # 读取CSV文件
            for csv_file in os.listdir(day_path):
                if csv_file.endswith('.csv'):
                    csv_path = os.path.join(day_path, csv_file)
                    try:
                        df = pd.read_csv(csv_path)
                        # 从文件名提取日期
                        date_str = csv_file.replace('.csv', '')
                        if len(date_str) == 8:  # YYYYMMDD格式
                            df['date'] = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
                        all_data.append(df)
                    except Exception as e:
                        print(f"读取文件失败 {csv_path}: {e}")
    
    if not all_data:
        print(f"警告: 未找到 {year} 年的任何数据")
        return {}
    
    combined = pd.concat(all_data, ignore_index=True)
    
    # 按城市分组
    city_data = {}
    if 'city' in combined.columns:
        for city, group in combined.groupby('city'):
            city_data[city] = group.copy()
    
    print(f"加载了 {len(city_data)} 个城市的数据")
    return city_data


def generate_calendar_series(city_df, year):
    """
    为单个城市生成完整的日历序列
    返回: [[日期, AQI, 等级, 首要污染物, 是否周末, 是否节假日], ...]
    """
    result = []
    
    # 生成完整的年度日期序列
    start_date = datetime(year, 1, 1)
    end_date = datetime(year, 12, 31)
    current_date = start_date
    
    # 将数据转换为日期索引的字典
    date_data = {}
    if 'date' in city_df.columns:
        for _, row in city_df.iterrows():
            date_str = str(row['date'])[:10]  # 确保格式为 YYYY-MM-DD
            if date_str not in date_data:
                pm25 = row.get('pm25', None)
                pm10 = row.get('pm10', None)
                date_data[date_str] = (pm25, pm10)
    
    # 遍历全年每一天
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        
        weekend = is_weekend(date_str)
        holiday = is_holiday(date_str)
        
        if date_str in date_data:
            pm25, pm10 = date_data[date_str]
            aqi, level, color, primary = calculate_aqi(pm25, pm10)
            
            if aqi is not None:
                result.append([
                    date_str,
                    aqi,
                    level,
                    primary,
                    weekend,
                    holiday
                ])
            else:
                result.append([date_str, None, None, None, weekend, holiday])
        else:
            # 数据缺失
            result.append([date_str, None, None, None, weekend, holiday])
        
        current_date += timedelta(days=1)
    
    return result


def save_calendar_json(city_name, calendar_data, output_dir):
    """保存日历数据为JSON"""
    # 清理城市名中的特殊字符
    safe_name = city_name.replace('|', '_').replace('/', '_').replace('\\', '_')
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{safe_name}.json")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({
            'city': city_name,
            'data': calendar_data
        }, f, ensure_ascii=False, indent=2)
    
    return output_path


# ==================== 主函数 ====================

def build_calendar_series(year, processed_dir=None, output_dir=None):
    """
    主函数：为指定年份生成所有城市的日历数据
    """
    if processed_dir is None:
        processed_dir = os.path.join('resources', 'processed')
    if output_dir is None:
        output_dir = os.path.join('resources', 'output', 'calendar', str(year))
    
    print(f"开始生成 {year} 年日历数据...")
    print(f"数据源: {processed_dir}")
    print(f"输出目录: {output_dir}")
    
    # 加载数据
    city_data = load_daily_data(year, processed_dir)
    
    if not city_data:
        print("未找到数据，退出")
        return
    
    # 为每个城市生成日历
    success_count = 0
    for city_name, city_df in city_data.items():
        try:
            calendar = generate_calendar_series(city_df, year)
            output_path = save_calendar_json(city_name, calendar, output_dir)
            success_count += 1
            print(f"  ✓ {city_name} -> {os.path.basename(output_path)}")
        except Exception as e:
            print(f"  ✗ {city_name}: {e}")
    
    print(f"\n完成！成功生成 {success_count}/{len(city_data)} 个城市的日历数据")
    print(f"输出目录: {output_dir}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='生成污染日历矩阵数据')
    parser.add_argument('--year', type=int, default=2013, help='年份 (默认: 2013)')
    parser.add_argument('--processed-dir', type=str, default=None, help='已处理数据目录')
    parser.add_argument('--output-dir', type=str, default=None, help='输出目录')
    
    args = parser.parse_args()
    
    build_calendar_series(
        year=args.year,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir
    )
