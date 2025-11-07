import os
import glob
import xarray as xr
import numpy as np
import pandas as pd
from src.preprocess import temporal_aggregation, _save_df_by_year_granularity
from src.remove_outliers import remove_physical_bounds
from src.config import VAR_BOUNDS
from src import config as _config
CHINA_GADM = os.path.join(os.path.dirname(__file__), 'Data', 'GADM', 'gadm41_CHN_2.json')
CHINA_BBOX = getattr(_config, 'CHINA_BBOX', None)


"""
测试用例：快速处理单日 CN-Reanalysis 网格数据文件夹下的所有 .nc 文件
"""

def main():
    repo_root = os.path.dirname(__file__)
    data_dir = os.path.join(repo_root, 'Data', 'CN-Reanalysis20131231')
    if not os.path.exists(data_dir):
        raise FileNotFoundError(data_dir)

    nc_files = sorted(glob.glob(os.path.join(data_dir, '*.nc')))
    if not nc_files:
        print('未找到任何 .nc 文件于', data_dir)
        return

    hourly_list = []
    first_temp_vals = []
    for nc in nc_files:
        try:
            ds = xr.open_dataset(nc, engine='h5netcdf')
        except Exception:
            try:
                ds = xr.open_dataset(nc)
            except Exception as e:
                print('跳过，无法打开', nc, e)
                continue
        basename = os.path.basename(nc)
        try:
            part = basename.replace('CN-Reanalysis', '').replace('.nc', '')
            yyyy = int(part[0:4])
            mm = int(part[4:6])
            dd = int(part[6:8])
            hh = int(part[8:10])
            time_str = f"{yyyy}-{mm:02d}-{dd:02d} {hh:02d}:00"
        except Exception:
            time_str = ''

        item = {}
        for var in ['pm25', 'pm10', 'so2', 'no2', 'co', 'o3', 'temp', 'rh', 'psfc', 'u', 'v']:
            if var in ds.variables:
                try:
                    item[var] = ds[var].values
                except Exception:
                    item[var] = None
        if 'lat2d' in ds.variables:
            item['lat'] = ds['lat2d'].values
        elif 'lat' in ds.variables:
            item['lat'] = ds['lat'].values
        if 'lon2d' in ds.variables:
            item['lon'] = ds['lon2d'].values
        elif 'lon' in ds.variables:
            item['lon'] = ds['lon'].values
        item['time'] = time_str

        if 'temp' in item and item['temp'] is not None:
            try:
                arr = np.array(item['temp']).ravel()
                finite = arr[np.isfinite(arr)]
                if finite.size:
                    first_temp_vals.append(finite[:min(1000, finite.size)])
            except Exception:
                pass

        hourly_list.append(item)
        try:
            ds.close()
        except Exception:
            pass

    print('读取到 hourly 网格数:', len(hourly_list))
    # use aggregate_mean to compute per-grid means across the hourly items (faster and avoids huge flattening)
    day_df = temporal_aggregation(hourly_list, aggregation='daily', aggregate_mean=True)
    print('展平后行数:', len(day_df))

    # Per user request: do NOT convert temperature units. Keep original units (Kelvin) as-is.
    # Previously we attempted to detect Kelvin and convert to Celsius; that behavior is disabled.

    # filter points to China (prefer GADM polygon)
    try:
        if os.path.exists(CHINA_GADM):
            from src.geo_utils import map_points_to_admin
            mapped = map_points_to_admin(day_df, CHINA_GADM, level='city')
            # filter mapped rows: prefer admin_name if present, else require province or city
            if 'admin_name' in mapped.columns:
                mask = mapped['admin_name'].notna()
            else:
                mask = (mapped.get('province').notna() if 'province' in mapped.columns else False) | (mapped.get('city').notna() if 'city' in mapped.columns else False)
            day_df = mapped.loc[mask].copy()
        elif CHINA_BBOX and all([isinstance(x, (int, float)) for x in CHINA_BBOX]):
            min_lat, max_lat, min_lon, max_lon = CHINA_BBOX
            day_df = day_df[(day_df['lat'] >= min_lat) & (day_df['lat'] <= max_lat) & (day_df['lon'] >= min_lon) & (day_df['lon'] <= max_lon)]
    except Exception as e:
        print('过滤中国边界失败，继续并使用 bbox（若可用）或跳过过滤:', e)

    # ensure expected columns exist (avoid losing 'temp' if dtype non-numeric)
    expected_vars = ['pm25', 'pm10', 'so2', 'no2', 'co', 'o3', 'temp', 'rh', 'psfc', 'u', 'v']
    for v in expected_vars:
        if v not in day_df.columns:
            day_df[v] = pd.NA

    # physical bounds
    # coerce expected vars to numeric to ensure aggregation works
    for v in expected_vars:
        try:
            day_df[v] = pd.to_numeric(day_df[v], errors='coerce')
        except Exception:
            day_df[v] = pd.NA

    numeric_cols = [c for c in expected_vars if c in day_df.columns]
    day_df_bounds = remove_physical_bounds(day_df, VAR_BOUNDS, inplace=False) if VAR_BOUNDS else day_df

    # 快速离群处理：按列全局 percentile clip (0.5% - 99.5%)，避免 groupby quantile 的性能瓶颈
    clip_low = 0.005
    clip_high = 0.995
    for col in numeric_cols:
        try:
            ser = pd.to_numeric(day_df_bounds[col], errors='coerce')
            low = ser.quantile(clip_low)
            high = ser.quantile(clip_high)
            day_df_bounds[col] = ser.clip(lower=low, upper=high)
        except Exception as e:
            print('列', col, 'clip 失败:', e)

    # map to admin (city) and aggregate by city mean
    try:
        from src.geo_utils import map_points_to_admin, aggregate_to_admin
        admin_file = os.path.join(os.path.dirname(__file__), 'Data', 'GADM', 'gadm41_CHN_2.json')
        if os.path.exists(admin_file):
            coords = day_df_bounds[['lat', 'lon']].dropna().copy()
            coords['lat_r'] = coords['lat'].round(4)
            coords['lon_r'] = coords['lon'].round(4)
            coords_unique = coords[['lat_r', 'lon_r']].drop_duplicates().reset_index(drop=True).rename(columns={'lat_r': 'lat', 'lon_r': 'lon'})
            mapped_coords = map_points_to_admin(coords_unique, admin_file, level='city')
            # DEBUG: print mapped_coords shape, columns and a small sample to help diagnose mapping issues
            try:
                print('DEBUG mapped_coords.shape =', getattr(mapped_coords, 'shape', None))
                print('DEBUG mapped_coords.columns =', list(mapped_coords.columns))
                try:
                    print('DEBUG mapped_coords sample:\n', mapped_coords.head(10).to_string(index=False))
                except Exception as _e:
                    print('DEBUG: cannot print mapped_coords head:', _e)
            except Exception:
                pass
            # ensure we have an admin_name column; if not, try to infer from common name columns (NL_NAME_*/NAME_*/province/city)
            if 'admin_name' not in mapped_coords.columns:
                candidates = [c for c in mapped_coords.columns if any(tok in c.upper() for tok in ['NL_NAME_2','NL_NAME_1','NAME_2','NAME_1','PROVINCE','CITY','NL_NAME'])]
                if candidates:
                    mapped_coords = mapped_coords.rename(columns={candidates[0]: 'admin_name'})
            # keep province/city/admin_name if available
            keep_cols = ['lat', 'lon']
            for c in ('admin_name', 'province', 'city'):
                if c in mapped_coords.columns:
                    keep_cols.append(c)
            mapped_coords = mapped_coords[[c for c in keep_cols if c in mapped_coords.columns]]

            # merge back to full dataframe using rounded coords
            day_df_bounds['_lat_r'] = day_df_bounds['lat'].round(4)
            day_df_bounds['_lon_r'] = day_df_bounds['lon'].round(4)
            mapped_coords = mapped_coords.rename(columns={'lat': '_lat_r', 'lon': '_lon_r'})
            merged = day_df_bounds.merge(mapped_coords, how='left', left_on=['_lat_r', '_lon_r'], right_on=['_lat_r', '_lon_r'])
            # DEBUG: inspect merged columns and a few rows to understand why mapping may be empty
            try:
                print('DEBUG merged.shape =', getattr(merged, 'shape', None))
                print('DEBUG merged.columns =', list(merged.columns))
                try:
                    print('DEBUG merged sample:\n', merged.head(10).to_string(index=False))
                except Exception as _e:
                    print('DEBUG: cannot print merged head:', _e)
            except Exception:
                pass
            # drop rows without admin mapping (非中国点)
            before_rows = len(merged)

            # normalize admin-related columns and optionally fill missing Chinese names with English fallbacks
            from src.geo_utils import canonicalize_admin_mapping
            merged, stats = canonicalize_admin_mapping(merged, fill_english_if_missing=True, sample_limit=50)
            after_rows = stats.get('after_rows', len(merged))
            filled_count = stats.get('filled_count', 0)
            print(f'空间映射后保留 {after_rows} / {before_rows} 行 (其中用英文替代中文名的填充次数: {filled_count})')
            if stats.get('english_samples'):
                print('使用英文名作为替代:')
                shown = 0
                for pe, ce in stats.get('english_samples', []):
                    if shown >= 50:
                        break
                    if pe and ce:
                        print(f'  {pe} / {ce}')
                    elif ce:
                        print(f'  {ce}')
                    elif pe:
                        print(f'  {pe}')
                    shown += 1

            # aggregate numeric columns by province + city (two columns only, Chinese names preferred)
            agg_numeric_cols = [c for c in numeric_cols]
            agg = merged.groupby(['province', 'city'])[agg_numeric_cols].mean().reset_index()
            # drop groups where both province and city are missing
            agg = agg[~(agg['province'].isna() & agg['city'].isna())].copy()

            # save as city-level (two columns: province, city)
            first_basename = os.path.basename(nc_files[0])
            datepart = first_basename.replace('CN-Reanalysis', '').replace('.nc', '')
            day_basename = datepart[0:8]
            year = int(day_basename[0:4])
            saved = _save_df_by_year_granularity(agg, day_basename, 'city')
            print('已按市级聚合并保存到:', saved)
        else:
            # fallback: save grid-level
            first_basename = os.path.basename(nc_files[0])
            datepart = first_basename.replace('CN-Reanalysis', '').replace('.nc', '')
            day_basename = datepart[0:8]
            year = int(day_basename[0:4])
            saved = _save_df_by_year_granularity(day_df_bounds, day_basename, 'grid')
            print('未找到 admin geojson，已保存 grid 到:', saved)
    except Exception as e:
        print('映射/聚合到市级发生错误，回退为保存 grid：', e)
        first_basename = os.path.basename(nc_files[0])
        datepart = first_basename.replace('CN-Reanalysis', '').replace('.nc', '')
        day_basename = datepart[0:8]
        year = int(day_basename[0:4])
    saved = _save_df_by_year_granularity(day_df_bounds, day_basename, 'grid')
    print('已保存到:', saved)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        import traceback
        print('运行出错:', e)
        traceback.print_exc()
