# 项目说明文档

本文档介绍本仓库的目录结构、主要文件/模块的作用、运行与调试指南，以及与临时文件相关的注意事项。适合在 Windows/conda 环境下运行此数据处理流水线的使用者阅读。

## 项目概览

这是一个针对气象/空气质量类 NetCDF 数据的处理管道，主要功能：

- 从 ZIP 包中读取 NetCDF（.nc）文件（逐小时或按日聚合）
- 进行物理边界过滤、离群值处理（IQR / 全局分位裁剪）
- 将格点数据映射到行政区（市级/省级）并按行政区聚合
- 保存日级中间结果（parquet/csv），并按月合并输出用于可视化（ECharts JSON）

## 目录结构（重要文件/文件夹说明）

- `main.py`

  - 主入口脚本。控制并行任务数量、调用数据预处理并触发逐月聚合与 ECharts 输出生成。
- `run_single_day_quick.py`

  - 快速调试单日处理脚本（参考与对照实现）。用于对比行为并验证单日流程。
- `requirements.txt`

  - Python 依赖清单（用于 pip 安装）。建议在 Windows 上优先使用 `conda`/`conda-forge` 安装 `geopandas`、`netCDF4` 等有二进制依赖的包。
- `README.md`

  - 项目说明与运行指南（本文件为详细说明）。
- `Data/`

  - `raw/`：放原始 ZIP 文件（例如 `CN-ReanalysisYYYYMMDD.zip`）。请按照年份建立子目录（如 `Data/raw/2013/`）。
  - `tmp/`：运行时的临时目录（解压/中间文件）。代码会在可能的情况下尽量减少临时目录的产生并在安全时删除。
  - `processed/`：日级中间结果（按粒度 `city` 或 `grid` 存放子目录），文件格式为 `parquet` 或 `csv`。
  - `aggregated/`：按月/年汇总的聚合结果（用于可视化的中间产物）。
  - `output/`：最终转换为 ECharts 格式或其他可视化产物的输出目录。
- `src/`（主要源码）

  - `__init__.py`
  - `config.py`：项目路径与运行配置常量（例如 `MAX_IN_MEMORY_BYTES`、临时目录 manifest 路径等）。
  - `io_utils.py`：核心 I/O 辅助函数
    - 从 ZIP 中读取 `.nc` 成员（支持尽量在内存中打开以减少落地临时文件）
    - 临时目录创建、字符集/ASCII 路径回退、以及 `tmp_dirs_to_cleanup.json` 的记录与清理接口
  - `preprocess.py`：单日/单 zip 的读取、变量提取、物理边界移除、离群值处理（IQR 或 全局分位裁剪）、坐标四舍五入、批量空间映射与按行政区聚合，并保存日级中间文件
  - `aggregate.py`：按月/按行政区的聚合函数（对接 `processed/` 目录中的日文件）
  - `geo_utils.py`：将点映射到行政区的空间函数（基于 GeoPandas），以及 `canonicalize_admin_mapping` 等用于选择中文/英文行政名的规则
  - `remove_outliers.py`：包含物理范围过滤与 IQR 离群值检测/掐头逻辑
  - `visualize.py`：把聚合后的 DataFrame 转换为 ECharts 能消费的 JSON/结构
  - `geocode_amap.py`（可选）：调用高德逆地理服务以回填省市（代码支持缓存以避免重复请求）
  - `io_utils.py`：以及临时目录记录/清理逻辑
- `tmp_dirs_to_cleanup.json`

  - 记录运行过程中需要后续清理的临时目录路径（当 `DEFER_CLEANUP=True` 时会写入）。可通过 `src.io_utils.cleanup_tmp_dirs()` 批量清除。

## 主要功能概览

- 从 ZIP 中读取 .nc（优先在内存中打开以避免落盘，受 `MAX_IN_MEMORY_BYTES` 与 ENV 控制）
- 数据清洗（边界过滤、IQR 离群值移除或全局分位裁剪）
- 将格点数据映射到行政区（市/省）并按行政区聚合
- 保存日级中间结果（`resources/processed/...`），并按月合并到 `resources/aggregated/...`，最后输出 ECharts JSON 到 `resources/output/`。

## 目录结构（重要文件/文件夹）

- `processing/`：处理模块（包含 `processing/src/` 与 `processing/main.py`）。
  - `src/`：主实现代码：
    - `src/config.py`：全局路径与运行参数（`MAX_IN_MEMORY_BYTES`、`TMP_CLEANUP_MANIFEST` 等）
    - `src/io_utils.py`：内存优先读取 ZIP/.nc、临时目录管理与清理接口
    - `src/preprocess.py`、`src/aggregate.py`、`src/visualize.py` 等：核心处理与导出逻辑
- `tmp_dirs_to_cleanup.json`：记录需要手动/延迟清理的临时目录（当 `DEFER_CLEANUP=True` 时写入）
- `resources/`：数据根目录（推荐放在仓库根）。包含：
  - `raw/`（ZIP 原始数据，建议按年分子目录）
  - `processed/`（日级中间结果，按粒度如 `city` 或 `grid`）
  - `aggregated/`（月度/年聚合结果）
  - `output/`（最终可视化产物，例如 ECharts JSON）

## 快速安装（建议使用 conda）

```cmd
conda create -n vis_env python=3.10 -y
conda activate vis_env
conda install -c conda-forge geopandas netcdf4 h5py gdal fiona rtree -y
pip install -r requirements.txt
```

## 常用环境变量与纯内存模式

- `PREPROCESS_DEBUG=1`：启用详细调试输出（推荐在排查 I/O/后端问题时启用）。
- `PREPROCESS_PURE_MEMORY=1`：严格启用纯内存模式，禁止在磁盘写入临时文件（仅在后端支持且内存足够时可用）。
- `PREPROCESS_ALLOW_DISK_FALLBACK=1`：允许在内存打开失败时回退到磁盘解压（默认不允许）。
- `PREPROCESS_FORCE_DISK=1`：强制使用磁盘解压（与历史行为兼容）。
- `MAX_IN_MEMORY_BYTES`：在 `src/config.py` 中配置（默认 300MB），决定是否尝试把 .nc 读入内存。

示例（Windows cmd）：

```cmd
REM 严格纯内存（失败时不回退到磁盘）
set PREPROCESS_PURE_MEMORY=1
set PREPROCESS_DEBUG=1
python main.py --step all --year 2013

REM 允许回退到磁盘（更兼容，但会写入临时文件）
set PREPROCESS_ALLOW_DISK_FALLBACK=1
set PREPROCESS_DEBUG=1
python main.py --step extract --year 2013
```

## 运行 `processing`（示例）

```cmd
REM 使用默认 python
python processing/main.py --step export --year 2013

REM 使用指定 conda env 的 python（你的示例路径）
E:\Anaconda\envs\fgo_downloader\python.exe g:/学习相关/可视化计算/China-VIS2021/processing/main.py --step all --year 2013
```

参数说明快速参考：

- `--step`: `extract` | `aggregate` | `export` | `all`（默认 `all`）
- `--year`: 指定年份（例如 2013）
- `--processed-root`: 指定 day-level 文件位置（覆盖 `src/config.PROCESSED_DIR`）
- `--aggregated-dir`: 指定聚合目录（覆盖 `src/config.AGGREGATED_DIR`）

## 临时目录与清理

- 代码会在出现回退或特殊情况时，把临时目录路径写入 `tmp_dirs_to_cleanup.json`（默认位于仓库根），以便集中清理：

```py
from src.io_utils import cleanup_tmp_dirs
cleanup_tmp_dirs()
```

## 调试常见问题（简要）

1. 内存/后端错误：在 Windows 上优先用 conda 安装 `netcdf4`/`h5py`，或开启 `PREPROCESS_FORCE_DISK=1` 回退解压；减少 `MAX_IN_MEMORY_BYTES` 可降低内存压力。
2. 处理不到数据：确认 `resources/raw/`（或 `processing/resources/raw/`）下是否有 zip，或通过 `--processed-root`/`--aggregated-dir` 指向正确位置。
3. 地理映射（行政区为空）：检查 `gadm` GeoJSON 是否正确放置并与 `src/geo_utils.py` 中的字段候选项匹配。