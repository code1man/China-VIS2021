import geopandas as gpd
import pandas as pd
import re
from shapely.geometry import Point
import os


def _choose_admin_name_column(gdf):
    # prefer common columns; prefer NL_NAME_1/NL_NAME_2 (localized/Chinese) when available
    for c in ['NL_NAME_2', 'NL_NAME_1', 'NL_NAME', 'NAME_2', 'NAME_1', 'NAME', 'name', 'ADM_NAME', 'CITY_NAME', 'province', '市名', '省名']:
        if c in gdf.columns:
            return c
    # fallback: pick first non-geometry, non-numeric column
    for c in gdf.columns:
        if c == 'geometry':
            continue
        if gdf[c].dtype == object:
            return c
    # final fallback
    return gdf.columns[0]


def map_points_to_admin(df: pd.DataFrame, admin_geojson_path: str, level: str = 'city') -> pd.DataFrame:
    """Map lat/lon points in df to administrative polygons from a GeoJSON.
    Returns original df with an added column 'admin_name' and 'admin_level'.
    """
    if 'lat' not in df.columns or 'lon' not in df.columns:
        raise ValueError('DataFrame 必须包含 lat 和 lon 列')

    if not os.path.exists(admin_geojson_path):
        raise FileNotFoundError(admin_geojson_path)

    gdf_admin = gpd.read_file(admin_geojson_path)
    # ensure crs is WGS84
    try:
        gdf_admin = gdf_admin.to_crs(epsg=4326)
    except Exception:
        pass

    # build points gdf
    pts = gpd.GeoDataFrame(df.copy(), geometry=[Point(xy) for xy in zip(df['lon'], df['lat'])], crs='EPSG:4326')

    # spatial join (points within polygons)
    joined = gpd.sjoin(pts, gdf_admin, how='left', predicate='within')

    # Normalize/restore admin columns from the admin GeoDataFrame into the joined result.
    # After spatial join geopandas may produce column names with suffixes (e.g. NAME_2_right)
    # or other variations; try to copy original admin columns back to predictable names
    try:
        admin_cols = [c for c in gdf_admin.columns if c != gdf_admin.geometry.name]
        joined_cols_lower = {jc.lower(): jc for jc in joined.columns}
        for ac in admin_cols:
            ac_lower = ac.lower()
            # find a joined column that best matches this admin col name
            match = None
            # exact match
            if ac_lower in joined_cols_lower:
                match = joined_cols_lower[ac_lower]
            else:
                # try common suffix/patterns produced by sjoin
                for jc_lower, jc in joined_cols_lower.items():
                    if jc_lower.endswith('.' + ac_lower) or jc_lower.endswith('_' + ac_lower) or jc_lower.endswith(ac_lower + '_right'):
                        match = jc
                        break
                # fallback: any joined column that contains the token
                if match is None:
                    for jc_lower, jc in joined_cols_lower.items():
                        if ac_lower in jc_lower:
                            match = jc
                            break
            if match is not None and match in joined.columns:
                # copy to a canonical column name matching the original admin col
                try:
                    joined[ac] = joined[match]
                except Exception:
                    pass
    except Exception:
        # non-fatal: best-effort normalization
        pass

    # Intersects fallback: some points sit exactly on polygon boundaries and 'within'
    # may miss them. For rows that currently have no admin-like columns filled,
    # try a second spatial join with predicate='intersects' and fill missing values.
    try:
        # determine which rows currently have any admin info
        admin_like_cols = [c for c in joined.columns if any(tok.lower() in c.lower() for tok in ['name', 'nl_name', 'province', 'city', 'adm'])]
        if not admin_like_cols:
            admin_like_cols = [c for c in joined.columns if c not in ('index_right', 'geometry')]

        def _row_has_admin(series_row):
            for c in admin_like_cols:
                try:
                    if c in series_row.index and pd.notna(series_row[c]) and str(series_row[c]).strip():
                        return True
                except Exception:
                    continue
            return False

        mask_has = joined.apply(_row_has_admin, axis=1)
        if mask_has.all():
            # everyone matched; no need for fallback
            pass
        else:
            unmatched = joined.loc[~mask_has].copy()
            if not unmatched.empty:
                try:
                    # do intersects join for unmatched subset
                    unmatched = gpd.GeoDataFrame(unmatched, geometry=unmatched.geometry, crs=joined.crs)
                    s2 = gpd.sjoin(unmatched, gdf_admin, how='left', predicate='intersects')
                    # normalize admin columns from s2 into s2 (same logic as above)
                    try:
                        admin_cols2 = [c for c in gdf_admin.columns if c != gdf_admin.geometry.name]
                        joined_cols_lower2 = {jc.lower(): jc for jc in s2.columns}
                        for ac in admin_cols2:
                            ac_lower = ac.lower()
                            match = None
                            if ac_lower in joined_cols_lower2:
                                match = joined_cols_lower2[ac_lower]
                            else:
                                for jc_lower, jc in joined_cols_lower2.items():
                                    if jc_lower.endswith('.' + ac_lower) or jc_lower.endswith('_' + ac_lower) or jc_lower.endswith(ac_lower + '_right') or (ac_lower in jc_lower):
                                        match = jc
                                        break
                            if match is not None and match in s2.columns:
                                try:
                                    s2[ac] = s2[match]
                                except Exception:
                                    pass
                    except Exception:
                        pass

                    # for each admin-like column, fill into original joined where missing
                    for ac in admin_cols2:
                        if ac in s2.columns:
                            # s2 index aligns with unmatched index; fill only where joined has null/empty
                            for idx, val in s2[ac].items():
                                try:
                                    if (ac not in joined.columns) or pd.isna(joined.at[idx, ac]) or str(joined.at[idx, ac]).strip() == '':
                                        joined.at[idx, ac] = val
                                except Exception:
                                    continue
                except Exception:
                    # fallback: ignore intersects errors
                    pass
    except Exception:
        pass

    # try to extract province and city columns from the admin GeoDataFrame
    # common GADM fields: NAME_1 (province), NAME_2 (city)
    # prefer localized (NL_NAME_*) Chinese names if present
    province_candidates = ['NL_NAME_1', 'NAME_1', 'province', 'Province', 'PROV', 'ADM1_NAME', 'NAME_0', 'NAME']
    city_candidates = ['NL_NAME_2', 'NAME_2', 'city', 'City', 'ADM2_NAME', 'NAME_1']
    admin_col = _choose_admin_name_column(gdf_admin)

    def _find_join_col(joined_cols, candidates):
        # look for exact match first, then for cols that endwith or contain the candidate (handle suffixes from sjoin)
        for c in candidates:
            if c in joined_cols:
                return c
        for c in candidates:
            for jc in joined_cols:
                if jc.endswith('.' + c) or jc.endswith('_' + c) or jc == c + '_right' or jc.endswith(c + '_right') or (c in jc and jc.count(c) == 1 and jc.startswith(c)):
                    return jc
        return None

    joined_cols = list(joined.columns)
    province_col = _find_join_col(joined_cols, province_candidates)
    city_col = _find_join_col(joined_cols, city_candidates)

    # set province/city columns on joined result if available (prefer localized fields)
    if province_col:
        if province_col != 'province':
            joined = joined.rename(columns={province_col: 'province'})
    else:
        joined['province'] = None

    if city_col:
        if city_col != 'city':
            joined = joined.rename(columns={city_col: 'city'})
    else:
        # fallback: if admin_col corresponds to city-level names and level=='city', use it
        if level == 'city' and admin_col in joined.columns:
            joined = joined.rename(columns={admin_col: 'city'})
            if 'province' not in joined.columns:
                joined['province'] = None
        else:
            joined['city'] = None

    # Ensure admin_name column always exists: prefer admin_col, then city, then province
    if admin_col in joined.columns and admin_col != 'admin_name':
        joined = joined.rename(columns={admin_col: 'admin_name'})
    if 'admin_name' not in joined.columns or joined['admin_name'].isnull().all():
        if 'city' in joined.columns and joined['city'].notna().any():
            joined['admin_name'] = joined['city']
        elif 'province' in joined.columns and joined['province'].notna().any():
            joined['admin_name'] = joined['province']
        else:
            joined['admin_name'] = None

    joined['admin_level'] = level

    # convert back to pandas DataFrame (drop geometry)
    out = pd.DataFrame(joined.drop(columns=['geometry']))
    # ensure string types for province/city/admin_name to avoid encoding issues later
    for col in ('province', 'city', 'admin_name'):
        if col in out.columns:
            out[col] = out[col].astype(object).where(out[col].notna(), None)
    return out


def aggregate_to_admin(mapped_df: pd.DataFrame, level: str = 'city') -> pd.DataFrame:
    """Aggregate numeric variables to admin unit (group by 'admin_name').
    Returns DataFrame with admin_name and mean of numeric columns.
    """
    if 'admin_name' not in mapped_df.columns:
        raise ValueError('mapped_df must contain admin_name column')

    # numeric columns to aggregate
    numeric_cols = mapped_df.select_dtypes(include=['number']).columns.tolist()
    # Exclude lon/lat if present
    numeric_cols = [c for c in numeric_cols if c not in ('lat', 'lon')]

    grouped = mapped_df.groupby('admin_name')[numeric_cols].mean().reset_index()
    # Optionally add level indicator
    grouped['admin_level'] = level
    return grouped


def canonicalize_admin_mapping(df: pd.DataFrame, fill_english_if_missing: bool = True, sample_limit: int = 50):
    """规范化映射 DataFrame 中的行政区相关列。

    行为（改进版）:
    - 优先选择包含中文字符的省/市列；如果找不到中文且 fill_english_if_missing 为 True，
      则使用候选英文列（NAME_1/NAME_2 等）作为回退；否则保持缺失。
    - 保证不会把 city 的值误用为 province（候选列会区分优先级并去重）。
    - 返回 (df_out, stats)，其中 stats 包含 before_rows/after_rows、filled_count（使用英文回退的次数）和 english_samples。
    """
    out = df.copy()
    cols = list(out.columns)

    def _cols_matching(tokens):
        outc = []
        for c in cols:
            low = c.lower()
            for t in tokens:
                if t.lower() in low:
                    outc.append(c)
                    break
        return outc

    # build candidate lists (distinct)
    prov_candidates = _cols_matching(['nl_name_1', 'name_1', 'province', 'province_x', 'province_y', 'prov', 'adm1'])
    city_candidates = _cols_matching(['nl_name_2', 'name_2', 'city', 'city_x', 'city_y', 'adm2', 'cnty', 'mun'])

    # remove overlaps so we don't accidentally pick the same column for both
    prov_candidates = [c for c in prov_candidates if c not in city_candidates]
    city_candidates = [c for c in city_candidates if c not in prov_candidates]

    # helper to pick chinese-first, then english-only-if-allowed
    def _choose_per_row(df_frame, candidates, allow_english=True):
        idx = df_frame.index
        out_s = pd.Series([pd.NA] * len(idx), index=idx, dtype=object)

        def col_series(name):
            if name in df_frame.columns:
                return df_frame[name].astype(object).where(df_frame[name].notna(), pd.NA)
            else:
                return pd.Series([pd.NA] * len(idx), index=idx, dtype=object)

        chinese_re = r'[\u4e00-\u9fff]'
        # prefer Chinese values first
        for c in candidates:
            s = col_series(c)
            try:
                mask = s.notna() & s.astype(str).str.strip().ne('') & s.astype(str).str.contains(chinese_re)
            except Exception:
                mask = s.notna() & s.astype(str).str.strip().ne('')
            if mask.any():
                to_fill = mask & out_s.isna()
                out_s[to_fill] = s[to_fill]

        # then allow english fallback if requested
        if allow_english:
            for c in candidates:
                s = col_series(c)
                mask = s.notna() & s.astype(str).str.strip().ne('')
                if mask.any():
                    to_fill = mask & out_s.isna()
                    out_s[to_fill] = s[to_fill]

        return out_s

    prov_series = _choose_per_row(out, prov_candidates, allow_english=fill_english_if_missing) if prov_candidates else pd.Series([pd.NA] * len(out), index=out.index)
    city_series = _choose_per_row(out, city_candidates, allow_english=fill_english_if_missing) if city_candidates else pd.Series([pd.NA] * len(out), index=out.index)

    # existing admin_name column preference
    if 'admin_name' in out.columns:
        admin_existing = out['admin_name'].astype(object).where(out['admin_name'].notna(), pd.NA)
    else:
        admin_existing = pd.Series([pd.NA] * len(out), index=out.index)

    admin_series = admin_existing.where(admin_existing.notna(), city_series)
    admin_series = admin_series.where(admin_series.notna(), prov_series)

    out['province'] = prov_series
    out['city'] = city_series
    out['admin_name'] = admin_series

    before_rows = len(out)

    # keep rows that have at least one of province/city/admin_name after fallback
    mask_keep = out['province'].notna() | out['city'].notna() | out['admin_name'].notna()
    out = out.loc[mask_keep].copy()

    # compute filled_count and examples where english fallback was used
    filled_count = 0
    english_samples = []
    try:
        def _has_chinese(s):
            try:
                return bool(pd.notna(s) and bool(re.search(r'[\u4e00-\u9fff]', str(s))))
            except Exception:
                return False

        for idx in out.index:
            orig = df.loc[idx] if idx in df.index else None
            if orig is None:
                continue
            # province
            prov_orig_has_cn = False
            if 'province' in orig.index and pd.notna(orig['province']):
                prov_orig_has_cn = _has_chinese(orig['province'])
            prov_now = out.at[idx, 'province'] if 'province' in out.columns else None
            if not prov_orig_has_cn and pd.notna(prov_now):
                filled_count += 1
                if len(english_samples) < sample_limit:
                    english_samples.append((None, str(prov_now)))
            # city
            city_orig_has_cn = False
            if 'city' in orig.index and pd.notna(orig['city']):
                city_orig_has_cn = _has_chinese(orig['city'])
            city_now = out.at[idx, 'city'] if 'city' in out.columns else None
            if not city_orig_has_cn and pd.notna(city_now):
                filled_count += 1
                if len(english_samples) < sample_limit:
                    english_samples.append((None, str(city_now)))
    except Exception:
        filled_count = int(filled_count) if 'filled_count' in locals() else 0

    after_rows = len(out)
    stats = {'before_rows': before_rows, 'after_rows': after_rows, 'filled_count': int(filled_count), 'english_samples': english_samples}
    return out, stats
