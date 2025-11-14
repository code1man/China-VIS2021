---
title: Markdown编辑环境搭建
author: zkd
date: 2025年11月13日
CJKmainfont: 楷体
   latex 选项
fontsize: 12pt
linkcolor: blue
urlcolor: green
citecolor: cyan
filecolor: magenta
toccolor: red
geometry: margin=0.3in
papersize: A4
documentclass: article

# pandoc设置
output:
   word_document
   # path: 你想保存的路径，默认与.md文档同文件
# 打印背景色
# 保存文件时自动生成
# export_on_save:
#   pandoc: true
---

# China-VIS2021 大气污染时空态势分析

> 引用与参考
> 
> - Liu, X., et al. (2023). High-dimensional spatiotemporal visual analysis of the air quality in China. Scientific Reports. https://www.nature.com/articles/s41598-023-31645-1
>

### 借鉴自 Liu et al. (2023) 的处理与可视化要点

基于 Liu et al. (2023) 中描述的高维时空可视分析思想，在本项目实现中采用并复现了其中若干方法要点（以论文中提出的思路为参考，结合仓库中已有的数据形式与实现）：

- 多视图交互（multi-view, linked views）：实现地图、时序折线、排名/条形、饼图与雷达图的联动，图表之间的交互事件在前端组件中转发并触发数据加载和细化视图（参见 `src/components/Demo.vue`、`src/components/ProvinceView.vue`）。
- 高维污染物谱的归一化与比较：对污染物谱（如 pm25, pm10, so2, no2, co, o3）进行统一命名、数值类型转换与必要的量纲注释后，用雷达/并列条形图展示城市或省级的污染物谱对比，以便于并行坐标或降维前的概览分析。
- 时空分解（temporal decomposition）：对时间按年/月/日分层（year → 12 months, month → days, day → distribution），并在数据管道中保留不同粒度的聚合产物以支持交互切换，此语义与论文中对时序分解与多尺度观察的建议一致。
- 空间汇总策略：将点位/格点数据汇总到行政单元（city/province）进行对比分析，保留样本数以量化聚合置信度（paper 中对单位空间聚合的做法被用作参考，具体实现采用 `geopandas.sjoin` 或基于 city->province 映射表的逻辑）。
- 可视化上的异常/模式提示：在统计摘要中计算每变量的 min/max/mean/median 与缺失数（见 `resources/metadata.yaml` 中的 `preliminary_statistics`），用于在可视化中标注异常区间或筛选高值区域。

以上内容为基于论文方法论的实现梳理，已在项目代码与生成的中间产物中体现（参见第 5 节清单）。

---

## 目录
1. 数据下载
2. 数据理解
   - 2.1 地理空间数据理解
   - 2.2 时间数据理解
3. 数据处理
   - 3.1 地理按省/市统计与分析
   - 3.2 业务时间聚合与分析

---

## 1. 数据下载
目标：[中国高分辨率空气质量再分析数据集](https://www.ncdc.ac.cn/portal/metadata/ce051692-a881-43e8-b416-0ee86db4c1b0)
> PS: 原生比赛空气质量数据集网页已经挂了，遂重新寻找替代数据集

重要操作与示例：

- 下载 FileZilla FTP Client 作为 FTP 传输的客户端，按照网页提供的方法进行：
![下载方式](pictures\ftp下载方式.png)
![下载截图](pictures\下载过程.png)

- 下载后文件格式：.nc


交付物：
- `resources/raw/*`（原始zip文件）

---

## 2. 数据理解
目的：快速评估数据的字段、空间/时间覆盖、单位、缺失与异常，为后续处理与可视化做好可复现的记录。

### 2.1 NetCDF 与处理后 CSV 的结构检查

已执行动作与观察到的结构：

- 使用 xarray 载入 NetCDF 并检视变量、维度与坐标信息。观测到的核心变量包括：u, v, temp, rh, psfc, pm25, pm10, so2, no2, co, o3，以及 lon/lat。具体读取实现参考 `processing/src/util/readNC.py`。
- 处理后供前端使用的 CSV（按市/省或按月聚合）通常包含以下列：

   province,city,pm25,pm10,so2,no2,co,o3,temp,rh,psfc,u,v[,time]

- 对于聚合 CSV（如 `resources/aggregated/2013/201301.csv`），存在 `time` 字段用于表示该条记录所属的日期或时间点；对按日/按月文件，时间格式不完全一致，因此在读取时使用 pandas 的 `pd.to_datetime(..., errors='coerce')` 进行解析并统计解析失败的行。

示例（xarray / pandas 检查）：

```python
import xarray as xr
import pandas as pd

ds = xr.open_dataset('resources/raw/your_reanalysis.nc')
print(ds)

df = pd.read_csv('resources/aggregated/2013/201301.csv', encoding='utf-8')
df['time'] = pd.to_datetime(df['time'], errors='coerce')
print(df['time'].min(), df['time'].max(), 'failed:', df['time'].isna().sum())
```

### 2.2 时间覆盖与统计摘要

已完成的统计与写入：

- 使用 `scripts/compute_metadata.py` 读取 `resources/aggregated/2013/*.csv`，计算每个变量的 min/max/mean/median/missing/rows，并将结果写回 `resources/metadata.yaml`（字段：`preliminary_statistics`、`temporal_coverage`、`sample_counts`）。
- 统计结果（示例，已写入仓库）：时间起止为 `2013-01-01 00:00:00` 到 `2013-12-01 00:00:00`；各月行数以及合并后行数见 `resources/metadata.yaml`。

---

---


## 3. 数据处理（Data processing）
目标：生成可直接用于前端可视化的中间产物（按省/市、按时间粒度的聚合表），并在处理流程中记录质量控制信息与可复现的脚本调用。

总体原则与已执行的实践：

- 小样本先跑、验证后再对全量批处理（在本仓库中多数脚本先在示例月份上测试后再运行全量月文件）。
- 使用 Parquet 提高读写性能与类型一致性，按需导出为 CSV 供前端静态加载。输出位置示例：`data/processed/*.parquet`、`resources/aggregated/{YYYY}/{YYYYMM}.csv`。
- 保留并记录关键转换（日志/脚本），并在中间产物中保留 `count` 等质量列以便审计。

### 3.1 字段标准化与空间对齐

实施细节：

1. 字段标准化：对 CSV/表格使用统一规则——列名小写、去除空白、按固定顺序挑选感兴趣列（`province, city, pm25, pm10, so2, no2, co, o3, temp, rh, psfc, u, v, time`）。示例实现见仓库内的处理脚本片段。

2. 空间对齐（已在项目中采用的两种方式）：
   - 若原始数据包含经纬度点，使用 `geopandas` 的点到行政区面 `sjoin` 将点映射到省/市边界（需预先加载 GADM 或本地行政区面数据）。
   - 若仅有 city/province 字段或字段复合（如 `省|市`），采用映射表（`resources/lookups/` 下可存放 city->province 对照）进行名称标准化与映射。

示例（字段标准化）：

```python
import pandas as pd

df = pd.read_csv('resources/processed/city/2013/01/201301.csv', encoding='utf-8')
df.columns = [c.strip().lower() for c in df.columns]
cols = ['province','city','pm25','pm10','so2','no2','co','o3','temp','rh','psfc','u','v','time']
df = df[[c for c in cols if c in df.columns]]
```

### 3.2 时序聚合与导出（已实施）

实施细节：

1. 对点/站数据按日/月做聚合（groupby + agg），计算 mean/count/std/min/max 等指标并保存为 Parquet 供下游使用。
2. 为支持前端静态加载，按月导出 CSV 到 `resources/aggregated/{YYYY}/{YYYYMM}.csv`；前端逻辑优先读取该路径，若该月文件不存在则枚举并合并 `resources/processed/city/{YYYY}/{MM}/` 下的逐日文件。

示例（按日聚合并导出月 CSV 的流程概要）：

```python
import pandas as pd
import os

df = pd.read_parquet('data/processed/city_daily.parquet')
df['month'] = pd.to_datetime(df['date']).dt.to_period('M')
for period, g in df.groupby('month'):
   y = period.year
   m = f"{period.month:02d}"
   outdir = f"resources/aggregated/{y}/"
   os.makedirs(outdir, exist_ok=True)
   g.to_csv(f"{outdir}/{y}{m}.csv", index=False)
```

3. 统计摘要：运行 `scripts/compute_metadata.py` 对聚合后的 CSV 做列统计并更新 `resources/metadata.yaml`（脚本已提交并运行一次，结果写回 metadata）。

### 3.3 质量控制

已在处理流程中保留的 QC 步骤：

- 在聚合表中保留 `count` 字段说明样本量；对样本量较小的分组在 downstream 可视化中作为注释或降权处理（实现上为记录而非自动丢弃）。
- 对数值列执行简单异常值检测（如基于经验阈值或 z-score），并将被标记的异常样本记录到 QC 日志（日志路径与格式由调用脚本写入）。

---

（下一节为仓库内已生成的文件与脚本清单）

---

## 验收标准（Acceptance Criteria）
- `data/raw/manifest.csv` 完整记录并校验所有原始文件；
- `data/processed/province_daily.parquet` 與 `data/processed/city_daily.parquet` 存在並包含關鍵統計欄位；
- `data/metadata.yaml`（或 docs/data_overview.md）描述字段、时间与空间范围；
- 前端能通过 `resources/processed/...` 路徑按日/月加载文件，或通过 `index.json` 发现文件。

---


