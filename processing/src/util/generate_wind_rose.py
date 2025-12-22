"""
generate_wind_rose.py - 污染风向玫瑰图数据生成器

功能：
- 读取2013年城市日级清洗数据
- 将风矢量(u, v)转换为风速和风向
- 按16个方位统计风频和PM2.5平均浓度
- 支持季节对比（采暖季 vs 非采暖季）

用法：python processing/src/util/generate_wind_rose.py --year 2013
"""

import os
import json
import argparse
import math
import pandas as pd
import numpy as np


# ==================== 常量定义 ====================

# 16个风向方位（从北开始，顺时针）
DIRECTIONS = [
    'N', 'NNE', 'NE', 'ENE', 
    'E', 'ESE', 'SE', 'SSE',
    'S', 'SSW', 'SW', 'WSW', 
    'W', 'WNW', 'NW', 'NNW'
]

# 每个扇区的角度范围（22.5度）
SECTOR_SIZE = 360.0 / 16  # 22.5度

# 采暖季月份（1, 2, 11, 12月）
HEATING_MONTHS = {1, 2, 11, 12}

# AQI质量等级映射（与日历模块保持一致）
AQI_LEVELS = [
    (0, 50, '优', '#00E400'),
    (51, 100, '良', '#FFFF00'),
    (101, 150, '轻度污染', '#FF7E00'),
    (151, 200, '中度污染', '#FF0000'),
    (201, 300, '重度污染', '#99004C'),
    (301, 500, '严重污染', '#7E0023'),
]


# ==================== 风矢量转换模块 ====================

def calculate_wind_speed(u, v):
    """
    计算风速
    Wind_Speed = sqrt(u^2 + v^2)
    
    Args:
        u: 东西方向风分量（正值表示西风，即向东吹）
        v: 南北方向风分量（正值表示南风，即向北吹）
    
    Returns:
        风速 (m/s)
    """
    if pd.isna(u) or pd.isna(v):
        return np.nan
    return math.sqrt(u**2 + v**2)


def calculate_wind_direction(u, v):
    """
    计算风向（风的来向）
    Wind_Direction = atan2(u, v) * (180/π)
    
    注意：气象学中风向指的是风的来向
    - u > 0, v > 0: 西南风（来自西南，吹向东北）
    - u > 0, v < 0: 西北风
    - u < 0, v > 0: 东南风
    - u < 0, v < 0: 东北风
    
    Args:
        u: 东西方向风分量
        v: 南北方向风分量
    
    Returns:
        风向角度 (0-360度, 0=北, 90=东, 180=南, 270=西)
    """
    if pd.isna(u) or pd.isna(v):
        return np.nan
    
    # 如果风速接近0，返回NaN
    if abs(u) < 1e-6 and abs(v) < 1e-6:
        return np.nan
    
    # atan2返回的是风吹向的方向，需要加180度得到风的来向
    # 同时转换为气象学惯例（0度=北，顺时针增加）
    direction = math.atan2(u, v) * (180.0 / math.pi)
    
    # 将角度转换为0-360范围
    direction = (direction + 360) % 360
    
    return direction


def direction_to_sector(direction):
    """
    将风向角度转换为16方位扇区索引
    
    每个扇区覆盖22.5度：
    - N (北): 348.75° - 11.25°
    - NNE (北北东): 11.25° - 33.75°
    - ... 以此类推
    
    Args:
        direction: 风向角度 (0-360度)
    
    Returns:
        扇区索引 (0-15)，0=N, 1=NNE, ..., 15=NNW
    """
    if pd.isna(direction):
        return None
    
    # 偏移11.25度使得北向位于中心
    adjusted = (direction + SECTOR_SIZE / 2) % 360
    sector = int(adjusted / SECTOR_SIZE)
    
    return sector


def get_aqi_level(pm25_value):
    """
    根据PM2.5浓度获取AQI等级信息
    
    Args:
        pm25_value: PM2.5浓度值
    
    Returns:
        (等级名称, 颜色代码)
    """
    if pd.isna(pm25_value):
        return '未知', '#CCCCCC'
    
    # 使用PM2.5浓度近似映射到AQI等级
    # PM2.5浓度区间（参考HJ 633-2012）
    if pm25_value <= 35:
        return '优', '#00E400'
    elif pm25_value <= 75:
        return '良', '#FFFF00'
    elif pm25_value <= 115:
        return '轻度污染', '#FF7E00'
    elif pm25_value <= 150:
        return '中度污染', '#FF0000'
    elif pm25_value <= 250:
        return '重度污染', '#99004C'
    else:
        return '严重污染', '#7E0023'


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
                            df['month'] = int(date_str[4:6])
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


def calculate_wind_rose_stats(city_df, filter_months=None):
    """
    计算单个城市的风玫瑰图统计数据
    
    Args:
        city_df: 城市数据DataFrame
        filter_months: 可选，用于筛选特定月份的集合
    
    Returns:
        list: 16个方位的统计数据
        [{"dir": "N", "freq": 15.5, "value": 85.2, "level": "良", "color": "#FFFF00"}, ...]
    """
    df = city_df.copy()
    
    # 按月份筛选
    if filter_months is not None and 'month' in df.columns:
        df = df[df['month'].isin(filter_months)]
    
    if df.empty:
        return []
    
    # 检查必要列
    if 'u' not in df.columns or 'v' not in df.columns or 'pm25' not in df.columns:
        print(f"警告: 缺少必要列 (u, v, pm25)")
        return []
    
    # 计算风向
    df['wind_direction'] = df.apply(lambda row: calculate_wind_direction(row['u'], row['v']), axis=1)
    df['sector'] = df['wind_direction'].apply(direction_to_sector)
    
    # 过滤无效数据
    valid_df = df.dropna(subset=['sector', 'pm25'])
    
    if valid_df.empty:
        return []
    
    total_count = len(valid_df)
    
    # 按扇区统计
    sector_stats = []
    for i, dir_name in enumerate(DIRECTIONS):
        sector_data = valid_df[valid_df['sector'] == i]
        
        count = len(sector_data)
        freq = (count / total_count) * 100 if total_count > 0 else 0
        mean_pm25 = sector_data['pm25'].mean() if count > 0 else 0
        
        level, color = get_aqi_level(mean_pm25)
        
        sector_stats.append({
            'dir': dir_name,
            'freq': round(freq, 2),
            'value': round(mean_pm25, 2) if not pd.isna(mean_pm25) else 0,
            'level': level,
            'color': color
        })
    
    return sector_stats


def generate_wind_rose_data(city_df):
    """
    生成单个城市的完整风玫瑰图数据（含季节对比）
    
    Returns:
        dict: {
            "all": [...],  # 全年数据
            "heating": [...],  # 采暖季数据
            "nonHeating": [...]  # 非采暖季数据
        }
    """
    # 全年数据
    all_data = calculate_wind_rose_stats(city_df)
    
    # 采暖季数据
    heating_data = calculate_wind_rose_stats(city_df, filter_months=HEATING_MONTHS)
    
    # 非采暖季数据
    non_heating_months = set(range(1, 13)) - HEATING_MONTHS
    non_heating_data = calculate_wind_rose_stats(city_df, filter_months=non_heating_months)
    
    return {
        'all': all_data,
        'heating': heating_data,
        'nonHeating': non_heating_data
    }


def save_wind_rose_json(city_name, wind_rose_data, output_dir):
    """保存风玫瑰图数据为JSON"""
    # 清理城市名中的特殊字符
    safe_name = city_name.replace('|', '_').replace('/', '_').replace('\\', '_')
    
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{safe_name}.json")
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump({
            'city': city_name,
            **wind_rose_data
        }, f, ensure_ascii=False, indent=2)
    
    return output_path


# ==================== 主函数 ====================

def build_wind_rose_data(year, processed_dir=None, output_dir=None):
    """
    主函数：为指定年份生成所有城市的风玫瑰图数据
    """
    if processed_dir is None:
        processed_dir = os.path.join('resources', 'processed')
    if output_dir is None:
        output_dir = os.path.join('resources', 'output', 'wind_rose', str(year))
    
    print(f"开始生成 {year} 年风玫瑰图数据...")
    print(f"数据源: {processed_dir}")
    print(f"输出目录: {output_dir}")
    
    # 加载数据
    city_data = load_daily_data(year, processed_dir)
    
    if not city_data:
        print("未找到数据，退出")
        return
    
    # 为每个城市生成风玫瑰图数据
    success_count = 0
    for city_name, city_df in city_data.items():
        try:
            wind_rose = generate_wind_rose_data(city_df)
            
            # 检查是否有有效数据
            if wind_rose['all']:
                output_path = save_wind_rose_json(city_name, wind_rose, output_dir)
                success_count += 1
                print(f"  ✓ {city_name} -> {os.path.basename(output_path)}")
            else:
                print(f"  ⚠ {city_name}: 无有效风向数据")
        except Exception as e:
            print(f"  ✗ {city_name}: {e}")
    
    print(f"\n完成！成功生成 {success_count}/{len(city_data)} 个城市的风玫瑰图数据")
    print(f"输出目录: {output_dir}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='生成污染风向玫瑰图数据')
    parser.add_argument('--year', type=int, default=2013, help='年份 (默认: 2013)')
    parser.add_argument('--processed-dir', type=str, default=None, help='已处理数据目录')
    parser.add_argument('--output-dir', type=str, default=None, help='输出目录')
    
    args = parser.parse_args()
    
    build_wind_rose_data(
        year=args.year,
        processed_dir=args.processed_dir,
        output_dir=args.output_dir
    )
