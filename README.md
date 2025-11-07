# 项目说明文档（中文）

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

## 快速运行与常用环境变量（Windows 命令行示例）

建议使用 Conda 环境并在 `conda-forge` 中安装二进制包：

```cmd
conda create -n demo_env python=3.10 -y
conda activate demo_env
# 推荐先用 conda 安装 geopandas/netCDF4 等以避免编译问题
conda install -c conda-forge geopandas netcdf4 h5py gdal fiona rtree -y
pip install -r requirements.txt
```

重要环境变量：
- `PREPROCESS_DEBUG=1`：启用调试打印（会输出临时目录、内存尝试、映射样例等）。
- `PREPROCESS_FORCE_DISK=1|0`：如果设置为 `1` 强制使用磁盘解压（适合遇到内存/后端兼容问题时）；设置为 `0` 则允许尝试内存打开（若后端与文件大小允许）。
- `PREPROCESS_SKIP_IQR=1`：跳过按组 IQR 运算，改用全局分位数裁剪（更快，行为等价于 `run_single_day_quick.py` 的快速模式）。
- `MAX_IN_MEMORY_BYTES`：在 `src/config.py` 中配置（默认为 300MB），决定何时尝试把 `.nc` 完整读入内存并立即删除临时文件。

## 临时文件与清理策略

- 默认在 Windows 上为稳妥起见，代码倾向于使用磁盘解压（可通过 `PREPROCESS_FORCE_DISK=0` 改变）。
- 当启用内存读取并且文件小于阈值时，函数会 `ds.load()` 把数据完整加载到内存，关闭底层文件句柄并尝试立即删除临时目录，从而减少持久临时文件。
- 若删除失败或采用了 fallback ASCII 目录，会把目录记录到 `tmp_dirs_to_cleanup.json`，你可在任意时间运行：

```py
from src.io_utils import cleanup_tmp_dirs
cleanup_tmp_dirs()
```

## 调试常见问题

1. "提交很多 job 后进程退出"：确认 `PREPROCESS_DEBUG=1`，观察线程池的 `submitted ... jobs` 后是否有异常打印。通常通过查看每个 task 的 start/success/failed 日志可定位出错的 zip 文件。
2. "看到 NA/空行政名": 开启 `PREPROCESS_DEBUG=1`，检查 `mapped_coords` 与 `merged` 的 sample 输出，确认 GeoJSON 的列名（NAME_1/NAME_2 或者 NL_NAME_*）是否与代码匹配，必要时在 `src/geo_utils.py` 中调整候选列顺序。
3. "内存/后端错误": 在 Windows 上优先用 conda 安装 `netcdf4`/`h5py` 并设置 `PREPROCESS_FORCE_DISK=1` 回退到磁盘解压；或减小 `MAX_IN_MEMORY_BYTES`。

## 贡献与扩展点

- 如果需要更激进的临时删除策略（例如即使是大文件也尝试流式读取后删除临时），请在 issue 中说明你能接受的内存上限与可能的风险（误删/OOM）。
- 如果需要对 `canonicalize_admin_mapping` 的回填规则做更严格匹配（如优先使用本地 GADM 的 `NL_NAME` 字段），可以提交带有目标 GeoJSON 的样本以便我调整候选列优先级。