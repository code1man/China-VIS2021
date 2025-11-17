"""
生成每个省份和每个城市的趋势 CSV。

用途：
  python 脚本/generate_trend_csvs.py --2013 年

此脚本读取 `resources/aggreated/{year}/` 下的每月聚合 CSV（如果存在）
并回退到“resources/processed/city/{year}/”下每天处理的 CSV
在 `resources/trends/{level}/` 中生成趋势 CSV。

输出文件（示例）：
  资源/趋势/省/Guangdong_monthly.csv
  资源/趋势/城市/Beijing_monthly.csv
  资源/趋势/city/Beijing_daily.csv
列：日期、pm25、pm10、so2、no2、co、o3、temp、rh、psfc、u、v
"""
import argparse
import os
import glob
import pandas as pd
import json


DEFAULT_VARS = ['pm25','pm10','so2','no2','co','o3','temp','rh','psfc','u','v']


def ensure_dir(p):
    if not os.path.exists(p):
        os.makedirs(p, exist_ok=True)


def read_aggregated_monthly(year):
    base = os.path.join('resources', 'aggregated', str(year))
    pattern = os.path.join(base, '*.csv')
    files = sorted(glob.glob(pattern))
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            # normalize column names
            df.columns = [c.strip().lower() for c in df.columns]
            # ensure a time column representing month
            fname = os.path.basename(f)
            ym = os.path.splitext(fname)[0]
            # convert 201301 -> 2013-01
            if len(ym) >= 6 and ym[:4].isdigit():
                date = ym[:4] + '-' + ym[4:6]
            else:
                date = ym
            df['__period'] = date
            dfs.append(df)
        except Exception as e:
            print('Failed to read', f, e)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def read_processed_daily(year):
    base = os.path.join('resources', 'processed', 'city', str(year))
    pattern = os.path.join(base, '**', '*.csv')
    files = sorted(glob.glob(pattern, recursive=True))
    dfs = []
    for f in files:
        try:
            df = pd.read_csv(f)
            df.columns = [c.strip().lower() for c in df.columns]
            # try to infer date from filename if not present
            fname = os.path.basename(f)
            m = None
            if len(fname) >= 8 and fname[:8].isdigit():
                m = fname[:8]
            if m:
                date = m[:4] + '-' + m[4:6] + '-' + m[6:8]
                df['time'] = df.get('time').fillna(date) if 'time' in df.columns else date
            dfs.append(df)
        except Exception as e:
            print('Failed to read', f, e)
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def produce_monthly_trends(df, out_dir, group_field='province'):
    """Group by group_field and __period (YYYY-MM) and compute mean for variables."""
    if df.empty:
        return
    g = df.copy()
    # ensure period exists (aggregated has __period; for processed we create YYYY-MM from time)
    if '__period' not in g.columns:
        if 'time' in g.columns:
            g['__period'] = g['time'].astype(str).apply(lambda s: (s[:7] if len(s) >= 7 else s))
        else:
            g['__period'] = 'unknown'
    # normalize group field
    if group_field not in g.columns:
        g[group_field] = g.get('province') if group_field == 'province' else g.get('city')
    vars_present = [v for v in DEFAULT_VARS if v in g.columns]
    if not vars_present:
        print('No numeric variables found to aggregate for monthly trends.')
        return
    grp = g.groupby([group_field, '__period'])[vars_present].mean().reset_index()
    ensure_dir(out_dir)
    # write one CSV per group (province/city)
    def sanitize_filename(s):
        # remove characters invalid on Windows filenames: <>:"/\\|?* and control chars
        bad = '<>:"/\\|?*'
        out = ''.join((c if c not in bad and ord(c) >= 32 else '_') for c in str(s))
        # also strip surrounding whitespace and collapse consecutive underscores
        out = out.strip()
        while '__' in out:
            out = out.replace('__', '_')
        if not out:
            out = 'item'
        return out

    for name, group in grp.groupby(group_field):
        safe = sanitize_filename(name)
        out_path = os.path.join(out_dir, f"{safe}_monthly.csv")
        group = group.rename(columns={'__period': 'date'})
        group = group.sort_values('date')
        group.to_csv(out_path, index=False)
        print('Wrote', out_path)


def produce_daily_trends(df, out_dir, group_field='city'):
    if df.empty:
        return
    g = df.copy()
    if 'time' not in g.columns:
        print('No time column for daily trends')
        return
    # normalize time to YYYY-MM-DD
    def norm_day(x):
        s = str(x)
        # try YYYYMMDD or ISO
        if len(s) >= 8 and s[:8].isdigit():
            return s[:4] + '-' + s[4:6] + '-' + s[6:8]
        return s[:10]
    g['__day'] = g['time'].apply(norm_day)
    if group_field not in g.columns:
        g[group_field] = g.get('city')
    vars_present = [v for v in DEFAULT_VARS if v in g.columns]
    if not vars_present:
        print('No numeric variables found to aggregate for daily trends.')
        return
    grp = g.groupby([group_field, '__day'])[vars_present].mean().reset_index()
    ensure_dir(out_dir)
    for name, group in grp.groupby(group_field):
        def sanitize_filename(s):
            bad = '<>:"/\\|?*'
            out = ''.join((c if c not in bad and ord(c) >= 32 else '_') for c in str(s))
            out = out.strip()
            while '__' in out:
                out = out.replace('__', '_')
            if not out:
                out = 'item'
            return out
        safe = sanitize_filename(name)
        out_path = os.path.join(out_dir, f"{safe}_daily.csv")
        group = group.rename(columns={'__day': 'date'})
        group = group.sort_values('date')
        group.to_csv(out_path, index=False)
        print('Wrote', out_path)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--year', type=int, default=None, help='year to process (e.g. 2013)')
    args = p.parse_args()

    year = args.year
    # try aggregated monthly first
    df_monthly = pd.DataFrame()
    if year:
        df_monthly = read_aggregated_monthly(year)
    else:
        # try all years in aggregated
        agg_root = os.path.join('resources', 'aggregated')
        years = [d for d in os.listdir(agg_root) if os.path.isdir(os.path.join(agg_root, d))]
        dfs = []
        for y in years:
            dfs.append(read_aggregated_monthly(y))
        if dfs:
            df_monthly = pd.concat(dfs, ignore_index=True)

    trends_base = os.path.join('resources', 'trends')
    prov_dir = os.path.join(trends_base, 'province')
    city_dir = os.path.join(trends_base, 'city')
    ensure_dir(prov_dir)
    ensure_dir(city_dir)

    if not df_monthly.empty:
        print('Using aggregated monthly CSVs to build monthly trends')
        produce_monthly_trends(df_monthly, prov_dir, group_field='province')
        produce_monthly_trends(df_monthly, city_dir, group_field='city')
    else:
        print('No aggregated monthly CSVs found for the requested year(s)')

    # build daily trends from processed per-day CSVs when available
    df_daily = None
    if year:
        df_daily = read_processed_daily(year)
    else:
        # try to read all processed/city years
        pbase = os.path.join('resources', 'processed', 'city')
        if os.path.exists(pbase):
            years = [d for d in os.listdir(pbase) if os.path.isdir(os.path.join(pbase, d))]
            dfs = []
            for y in years:
                dfs.append(read_processed_daily(y))
            if dfs:
                df_daily = pd.concat(dfs, ignore_index=True)

    if df_daily is not None and not df_daily.empty:
        print('Using processed per-day CSVs to build daily trends')
        produce_daily_trends(df_daily, city_dir, group_field='city')
    else:
        print('No processed per-day CSVs found (skipping daily trends)')


if __name__ == '__main__':
    main()
