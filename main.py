import os
import glob
import pandas as pd
from src.config import BASE_PATH, AGGREGATED_DIR, OUTPUT_DIR, PROCESSED_DIR
from src.preprocess import process_zips_parallel
from src.aggregate import aggregate_month_from_saved_days
from src.visualize import convert_to_echarts_format

# ===== 用户可修改的运行参数 =====
# 年份
DEFAULT_YEAR = 2013
# 并发 worker 数（建议根据磁盘 I/O 与 CPU 调整，Windows 上 HDF5 打开仍有全局序列化）
DEFAULT_WORKERS = 4
# 是否在运行时输出 debug 日志（0/1）
DEFAULT_PREPROCESS_DEBUG = 0
# 是否跳过 IQR 离群值移除（1 跳过以加速，0 保留完整清洗）
DEFAULT_PREPROCESS_SKIP_IQR = 1
# granularity: 'city' or 'grid' (默认自动检测 admin_geojson 存在时使用 city)
# ======================================================================



def main_processing_pipeline(year=2013, workers=4):
    # ensure environment flags are set from defaults if not provided externally
    if os.environ.get('PREPROCESS_DEBUG', '') == '':
        os.environ['PREPROCESS_DEBUG'] = str(int(DEFAULT_PREPROCESS_DEBUG))
    if os.environ.get('PREPROCESS_SKIP_IQR', '') == '':
        os.environ['PREPROCESS_SKIP_IQR'] = str(int(DEFAULT_PREPROCESS_SKIP_IQR))
    base_path = os.path.join(BASE_PATH, str(year))
    print("开始并行按日处理 ZIP 文件并保存中间（已清洗）结果，按市级粒度...")

    admin_geojson = os.path.join(os.path.dirname(__file__), 'Data', 'GADM', 'gadm41_CHN_2.json')
    if not os.path.exists(admin_geojson):
        admin_geojson = None

    # 逐日进行处理以降低内存压力
    process_zips_parallel(base_path, year=year, granularity='city' if admin_geojson else 'grid', admin_geojson=admin_geojson, workers=workers, aggregate_mean=True)
    # 保存的日文件根目录（preprocess 将按 PROCESSED_DIR/<granularity>/<year>/<month>/<day> 保存）
    processed_root = os.path.join(PROCESSED_DIR, 'city' if admin_geojson else 'grid')

    print("开始逐月聚合已保存的日数据（逐月处理以降低内存压力）...")
    monthly_frames = []
    # root for saved daily files for this run/year
    processed_days_dir = os.path.join(processed_root, str(year))
    for month in range(1, 13):
        month_dir = os.path.join(processed_root, str(year), f"{month:02d}")
        try:
            month_df = aggregate_month_from_saved_days(year, month, month_dir, output_dir=os.path.join(AGGREGATED_DIR, 'processed_months'))
            monthly_frames.append(month_df)
        except FileNotFoundError:
            print(f"未找到 {year}-{month:02d} 的日文件，跳过月度聚合")
            continue
        except Exception as e:
            print(f"聚合 {year}-{month:02d} 时出错: {e}")
            continue

    if monthly_frames:
        combined = pd.concat(monthly_frames, ignore_index=True)
        if 'time' in combined.columns:
            combined['time'] = pd.to_datetime(combined['time'])
        province_data = combined.groupby('time').mean().reset_index()
    else:
        # try to use processed days directly (递归查找该年下所有月日文件)
        all_day_files = sorted(glob.glob(os.path.join(processed_root, str(year), '**', '*.parquet'), recursive=True) +
                               glob.glob(os.path.join(processed_root, str(year), '**', '*.csv'), recursive=True))
        parts = []
        for f in all_day_files:
            try:
                if f.endswith('.parquet'):
                    parts.append(pd.read_parquet(f))
                else:
                    parts.append(pd.read_csv(f, parse_dates=['time']))
            except Exception as e:
                print(f"读取日文件 {f} 时出错: {e}")
        if parts:
            combined = pd.concat(parts, ignore_index=True)
            combined['time'] = pd.to_datetime(combined['time'])
            province_data = combined.groupby('time').mean().reset_index()
        else:
            raise RuntimeError("未找到可用于生成可视化的数据")

    print("转换为ECharts格式并保存...")
    output_dir = convert_to_echarts_format(province_data, output_dir=os.path.join(OUTPUT_DIR, 'echarts'))

    print("数据处理完成！ 中间日文件目录：", processed_days_dir)
    print("月度聚合文件目录：", os.path.join(AGGREGATED_DIR, 'processed_months'))
    return output_dir


if __name__ == '__main__':
    # run with defaults defined at top of file
    # allow quick edit of DEFAULT_YEAR/DEFAULT_WORKERS/DEFAULT_PREPROCESS_DEBUG/DEFAULT_PREPROCESS_SKIP_IQR
    os.environ['PREPROCESS_DEBUG'] = str(int(DEFAULT_PREPROCESS_DEBUG))
    os.environ['PREPROCESS_SKIP_IQR'] = str(int(DEFAULT_PREPROCESS_SKIP_IQR))
    main_processing_pipeline(year=DEFAULT_YEAR, workers=DEFAULT_WORKERS)
