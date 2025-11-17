#!/usr/bin/env python3
"""
根据处理后的 CSV/聚合数据预先计算热图 JSON 文件和城市质心。

该脚本将：
 -通过对每个城市的经度/纬度进行平均来计算“resources/city_centroids.json”
   `resources/processed/city/{year}/**/*.csv` 其中存在 `lon` 和 `lat` 列。
 -在“resources/heatmap/monthly/{YYYYMM}.json”下生成每月热图 JSON 文件。

热图 JSON 格式：对象列表 {"city":..., "province":..., "lon":..., "lat":..., "value":...}
用法：python script/precompute_heatmaps.py --year 2013
"""
import os
import glob
import json
import argparse
import pandas as pd


def ensure_dir(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)


def compute_city_centroids(year=None):
    base = os.path.join('resources', 'processed', 'city')
    pattern = os.path.join(base, '**', '*.csv') if year is None else os.path.join(base, str(year), '**', '*.csv')
    files = sorted(glob.glob(pattern, recursive=True))
    records = {}
    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip().lower() for c in df.columns]
            if 'city' not in df.columns:
                continue
            if 'lon' not in df.columns or 'lat' not in df.columns:
                continue
            for _, r in df.iterrows():
                city = r.get('city')
                if pd.isna(city):
                    continue
                try:
                    lon = float(r.get('lon'))
                    lat = float(r.get('lat'))
                except Exception:
                    continue
                key = str(city)
                if key not in records:
                    records[key] = { 'sum_lon': 0.0, 'sum_lat': 0.0, 'n': 0, 'province': r.get('province') }
                records[key]['sum_lon'] += lon
                records[key]['sum_lat'] += lat
                records[key]['n'] += 1
        except Exception as e:
            print('skipping', f, e)
    centroids = {}
    for k, v in records.items():
        if v['n'] > 0:
            centroids[k] = { 'city': k, 'lon': v['sum_lon'] / v['n'], 'lat': v['sum_lat'] / v['n'], 'count': v['n'], 'province': v.get('province') }
    out = os.path.join('resources', 'city_centroids.json')
    ensure_dir(os.path.dirname(out))
    with open(out, 'w', encoding='utf-8') as fh:
        json.dump(centroids, fh, ensure_ascii=False, indent=2)
    print('Wrote city centroids to', out, 'entries=', len(centroids))
    return centroids


def build_monthly_heatmaps(year, centroids=None):
    agg_dir = os.path.join('resources', 'aggregated', str(year))
    pattern = os.path.join(agg_dir, '*.csv')
    files = sorted(glob.glob(pattern))
    out_base = os.path.join('resources', 'heatmap', 'monthly')
    ensure_dir(out_base)
    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip().lower() for c in df.columns]
            fname = os.path.basename(f)
            ym = os.path.splitext(fname)[0]
            out_file = os.path.join(out_base, f"{ym}.json")
            rows = []
            # prefer lon/lat in aggregated rows if present, else look up centroids by city
            for _, r in df.iterrows():
                city = r.get('city')
                prov = r.get('province')
                value = None
                # choose a primary pollutant value if available (pm25) else first numeric
                if 'pm25' in r and not pd.isna(r['pm25']):
                    value = float(r['pm25'])
                else:
                    # try to find first numeric among common pollutants
                    for k in ['pm10','so2','no2','co','o3']:
                        if k in r and not pd.isna(r[k]):
                            value = float(r[k]); break
                lon = r.get('lon') if 'lon' in r else None
                lat = r.get('lat') if 'lat' in r else None
                try:
                    lon = float(lon) if lon is not None and str(lon) != '' else None
                    lat = float(lat) if lat is not None and str(lat) != '' else None
                except Exception:
                    lon = lat = None
                if (lon is None or lat is None) and centroids and city and city in centroids:
                    lon = centroids[city]['lon']; lat = centroids[city]['lat']
                if lon is None or lat is None:
                    continue
                rows.append({'city': city, 'province': prov, 'lon': lon, 'lat': lat, 'value': value})
            with open(out_file, 'w', encoding='utf-8') as fh:
                json.dump(rows, fh, ensure_ascii=False, indent=2)
            print('Wrote heatmap', out_file, 'points=', len(rows))
        except Exception as e:
            print('Failed to build heatmap for', f, e)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--year', type=int, required=False, help='year to process (e.g. 2013). If omitted, all years found under resources/aggregated will be processed.')
    args = p.parse_args()
    year = args.year
    # compute centroids across processed data (if year specified, restrict; else compute across all)
    centroids = compute_city_centroids(year)
    if year:
        build_monthly_heatmaps(year, centroids)
    else:
        # discover years under resources/aggregated
        agg_root = os.path.join('resources', 'aggregated')
        years = []
        if os.path.exists(agg_root):
            for entry in sorted(os.listdir(agg_root)):
                pth = os.path.join(agg_root, entry)
                if os.path.isdir(pth) and entry.isdigit():
                    years.append(int(entry))
        if not years:
            print('No aggregated year directories found under resources/aggregated. Nothing to build.')
            return
        for y in years:
            print('Building heatmaps for', y)
            build_monthly_heatmaps(y, centroids)


if __name__ == '__main__':
    main()
