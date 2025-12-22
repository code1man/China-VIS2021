"""
China-VIS2021 数据处理主入口
支持命令行参数: --step extract|aggregate|export|calendar|windrose|all --year YYYY
"""

import os
import glob
import argparse
import pandas as pd
from src.config import BASE_PATH, AGGREGATED_DIR, OUTPUT_DIR, PROCESSED_DIR
from src.preprocess import process_zips_parallel
from src.aggregate import aggregate_month_from_saved_days
from src.visualize import convert_to_echarts_format

# ===== 默认参数 =====
DEFAULT_YEAR = 2013
DEFAULT_WORKERS = 4
DEFAULT_PREPROCESS_DEBUG = 0
DEFAULT_PREPROCESS_SKIP_IQR = 1


def _ensure_env_defaults():
    """设置环境变量默认值"""
    if os.environ.get('PREPROCESS_DEBUG', '') == '':
        os.environ['PREPROCESS_DEBUG'] = str(int(DEFAULT_PREPROCESS_DEBUG))
    if os.environ.get('PREPROCESS_SKIP_IQR', '') == '':
        os.environ['PREPROCESS_SKIP_IQR'] = str(int(DEFAULT_PREPROCESS_SKIP_IQR))


def _find_admin_geojson():
    """查找行政区划 GeoJSON 文件"""
    candidates = [
        os.path.join(os.path.dirname(__file__), '..', 'resources', 'GADM', 'gadm41_CHN_2.json'),
        os.path.join(os.path.dirname(__file__), 'Data', 'GADM', 'gadm41_CHN_2.json'),
    ]
    for path in candidates:
        if os.path.exists(path):
            print(f"✅ 成功定位行政区划文件: {os.path.abspath(path)}")
            return path
    return None


def step_extract(year, workers):
    """Step 1: 提取与清洗"""
    print(f"\n[extract] 开始并行按日处理 ZIP 文件，年份: {year}...")
    base_path = os.path.join(BASE_PATH, str(year))
    admin_geojson = _find_admin_geojson()
    granularity = 'city' if admin_geojson else 'grid'
    
    process_zips_parallel(
        base_path, year=year, granularity=granularity,
        admin_geojson=admin_geojson, workers=workers, aggregate_mean=True
    )
    print(f"提取完成。结果保存在: {os.path.join(PROCESSED_DIR, 'city', str(year))}")


def step_aggregate(year):
    """Step 2: 月度聚合"""
    print(f"\n[aggregate] 开始逐月聚合已保存的日数据，年份: {year}...")
    processed_root = os.path.join(PROCESSED_DIR, 'city')
    output_dir = os.path.join(AGGREGATED_DIR, str(year))
    os.makedirs(output_dir, exist_ok=True)
    
    for month in range(1, 13):
        month_dir = os.path.join(processed_root, str(year), f"{month:02d}")
        try:
            if not os.path.exists(month_dir) or not os.listdir(month_dir):
                print(f"  跳过 {year}-{month:02d} (无数据)")
                continue
            aggregate_month_from_saved_days(year, month, month_dir, output_dir=output_dir)
            print(f"  ✓ 聚合完成: {year}-{month:02d}")
        except Exception as e:
            print(f"  ✗ 聚合失败 {year}-{month:02d}: {e}")
    
    print(f"聚合完成。结果保存在: {output_dir}")


def step_export(year):
    """Step 3: ECharts 可视化导出"""
    print(f"\n[export] 开始生成 ECharts 可视化数据，年份: {year}...")
    
    monthly_dir = os.path.join(AGGREGATED_DIR, str(year))
    monthly_frames = []
    
    if os.path.exists(monthly_dir):
        for month in range(1, 13):
            csv_path = os.path.join(monthly_dir, f"{year}{month:02d}.csv")
            if os.path.exists(csv_path):
                try:
                    df = pd.read_csv(csv_path)
                    monthly_frames.append(df)
                    print(f"  - 已加载: {os.path.basename(csv_path)}")
                except Exception as e:
                    print(f"  - 加载失败: {csv_path}: {e}")
    
    if not monthly_frames:
        print("警告: 未找到月度聚合文件，尝试直接读取日文件...")
        processed_root = os.path.join(PROCESSED_DIR, 'city')
        all_files = glob.glob(os.path.join(processed_root, str(year), '**', '*.csv'), recursive=True)
        
        if not all_files:
            raise RuntimeError(f"未找到年份 {year} 的任何数据文件")
        
        for f in all_files[:365]:  # 限制数量
            try:
                monthly_frames.append(pd.read_csv(f))
            except:
                pass
    
    if monthly_frames:
        print(f"读取到 {len(monthly_frames)} 个文件，正在合并...")
        combined = pd.concat(monthly_frames, ignore_index=True)
        if 'time' in combined.columns:
            combined['time'] = pd.to_datetime(combined['time'])
        
        output_path = convert_to_echarts_format(combined, output_dir=os.path.join(OUTPUT_DIR, 'echarts'))
        print(f"ECharts 数据已保存到: {output_path}")
    else:
        raise RuntimeError("没有有效数据可用于生成可视化。")


def step_calendar(year):
    """Step 4: 日历热力图数据生成"""
    print(f"\n[calendar] 开始生成污染日历数据，年份: {year}...")
    
    # 动态导入日历生成模块
    from src.util.generate_calendar_series import build_calendar_series
    
    processed_dir = os.path.join(os.path.dirname(__file__), '..', 'resources', 'processed')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'resources', 'output', 'calendar', str(year))
    
    build_calendar_series(year=year, processed_dir=processed_dir, output_dir=output_dir)
    print(f"日历数据生成完成！输出目录: {output_dir}")


def step_windrose(year):
    """Step 5: 风向玫瑰图数据生成"""
    print(f"\n[windrose] 开始生成风玫瑰图数据，年份: {year}...")
    
    # 动态导入风玫瑰图生成模块
    from src.util.generate_wind_rose import build_wind_rose_data
    
    processed_dir = os.path.join(os.path.dirname(__file__), '..', 'resources', 'processed')
    output_dir = os.path.join(os.path.dirname(__file__), '..', 'resources', 'output', 'wind_rose', str(year))
    
    build_wind_rose_data(year=year, processed_dir=processed_dir, output_dir=output_dir)
    print(f"风玫瑰图数据生成完成！输出目录: {output_dir}")


def main():
    parser = argparse.ArgumentParser(description='China-VIS2021 数据处理管道')
    parser.add_argument('--step', type=str, default='all',
                        choices=['extract', 'aggregate', 'export', 'calendar', 'windrose', 'all'],
                        help='执行步骤: extract/aggregate/export/calendar/windrose/all')
    parser.add_argument('--year', type=int, default=DEFAULT_YEAR,
                        help=f'处理年份 (默认: {DEFAULT_YEAR})')
    parser.add_argument('--workers', type=int, default=DEFAULT_WORKERS,
                        help=f'并行 worker 数 (默认: {DEFAULT_WORKERS})')
    
    args = parser.parse_args()
    
    _ensure_env_defaults()
    
    print(f"=" * 50)
    print(f"China-VIS2021 数据处理管道")
    print(f"年份: {args.year}, 步骤: {args.step}")
    print(f"=" * 50)
    
    if args.step in ('extract', 'all'):
        step_extract(args.year, args.workers)
    
    if args.step in ('aggregate', 'all'):
        step_aggregate(args.year)
    
    if args.step in ('export', 'all'):
        step_export(args.year)
    
    if args.step == 'calendar':
        step_calendar(args.year)
    
    if args.step == 'windrose':
        step_windrose(args.year)
    
    print("\n✅ 处理完成！")


if __name__ == '__main__':
    main()
