<template>
  <div class="province-view">
    <div class="pv-header">
      <h2 class="pv-title">{{ province }} — 省级视图</h2>
      <div class="pv-sub">污染物: {{ pollutant || '（未选择）' }} · 粒度: {{ granularity }}</div>
      <div class="pv-controls">
        <label>右侧视图: </label>
        <button class="btn-small" :class="{active: rightChartType === 'trend'}" @click.prevent="rightChartType = 'trend'">Trend</button>
        <button class="btn-small" :class="{active: rightChartType === 'pie'}" @click.prevent="rightChartType = 'pie'">Pie</button>
        <button class="btn-small" :class="{active: rightChartType === 'bar'}" @click.prevent="rightChartType = 'bar'">Bar</button>
      </div>
    </div>
    <div class="pv-grid">
      <section class="pv-chart pv-chart-left">
        <!-- left: fixed radar chart -->
        <div id="radarChart" class="chart-canvas"></div>
      </section>
      <section class="pv-chart pv-chart-right">
        <!-- top: trend (for month/year) or distribution (for day) -->
        <div id="trendChart" class="chart-canvas" v-show="rightChartType === 'trend' && granularity !== 'day'"></div>
        <div id="distChart" class="chart-canvas" v-show="rightChartType === 'trend' && granularity === 'day'"></div>
        <!-- city comparison (visible when not showing trend) -->
        <div id="cityChart" class="chart-canvas" v-show="rightChartType !== 'trend'" style="margin-top:12px;"></div>
      </section>
    </div>
  </div>
</template>

<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue';

const props = defineProps({
  province: { type: String, required: true },
  data: { type: Array, required: true },
  pollutant: { type: String, required: false },
  granularity: { type: String, required: false },
  fromMonth: { type: String, required: false },
  fromYear: { type: [String,Number], required: false }
});

// month-directory loaded rows (if we fetch CSVs from resources/processed/city/YYYY/MM/)
const monthlyLoadedRows = ref([]);
const loadingMonthly = ref(false);
const rightChartType = ref('trend'); // 'trend' | 'bar' | 'pie'

let trendChart = null;
let cityChart = null;
let distChart = null;
let radarChart = null;

// helper: robust date parsing similar to Demo
function parseDateFromRow(r) {
  if (!r) return null;
  const keys = Object.keys(r || {});
  const candidates = ['time','date','day','datetime','timestamp'];
  for (const k of candidates) {
    if (keys.includes(k) && r[k]) {
      const s = String(r[k]);
      let d = new Date(s);
      if (!isNaN(d.getTime())) return d;
      const m = s.match(/^(\d{4})(\d{2})(\d{2})/);
      if (m) return new Date(`${m[1]}-${m[2]}-${m[3]}T00:00:00`);
    }
  }
  return null;
}

const rows = computed(() => {
  // if month granularity and we have monthlyLoadedRows, prefer those (they contain all days from the folder)
  const fallback = Array.isArray(props.data) ? props.data : [];
  const all = (props.granularity === 'month' && monthlyLoadedRows.value && monthlyLoadedRows.value.length) ? monthlyLoadedRows.value : fallback;
  const sp = (props.province || '').toString().toLowerCase();
  if (!sp) return all;
  return all.filter(r => {
    if (!r) return false;
    const p = (r.province || r.state || r.region || '').toString().toLowerCase();
    const c = (r.city || '').toString().toLowerCase();
    return p === sp || c === sp || p.includes(sp) || c.includes(sp);
  });
});

const rowsCount = computed(() => (Array.isArray(rows.value) ? rows.value.length : 0));

const columnsList = computed(() => {
  const sample = (rows.value && rows.value.length && rows.value[0]) || (props.data && props.data.length && props.data[0]);
  if (!sample) return '';
  try { return Object.keys(sample).join(', '); } catch (e) { return ''; }
});

function aggregateTrend(data, pollutant, granularity) {
  const map = new Map();
  for (const r of data) {
    const d = parseDateFromRow(r);
    if (!d) continue;
    let key = '';
    // year -> aggregate by month (show 12 months)
    if (granularity === 'year') key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`;
    // month -> aggregate by day within the month
    else if (granularity === 'month') key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    else if (granularity === 'hour') key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')} ${String(d.getHours()).padStart(2,'0')}:00`;
    else key = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;
    const v = pollutant ? Number(r[pollutant]) || 0 : 0;
    if (!map.has(key)) map.set(key, { sum: v, n: 1 });
    else { const cur = map.get(key); cur.sum += v; cur.n += 1; map.set(key, cur); }
  }
  const arr = Array.from(map.entries()).map(([k,v]) => ({ key: k, val: v.n ? v.sum / v.n : 0 }));
  // sort by actual date value (handles YYYY-MM and YYYY-MM-DD keys)
  arr.sort((a,b) => {
    const da = new Date(a.key);
    const db = new Date(b.key);
    return da - db;
  });
  return arr;
}

function aggregateCity(data, pollutant) {
  const map = new Map();
  for (const r of data) {
    const city = (r.city || r.cityname || r.city_name || 'unknown').toString();
    const v = pollutant ? Number(r[pollutant]) || 0 : 0;
    if (!map.has(city)) map.set(city, { sum: v, n: 1 });
    else { const cur = map.get(city); cur.sum += v; cur.n += 1; map.set(city, cur); }
  }
  const arr = Array.from(map.entries()).map(([k,v]) => ({ city: k, val: v.n ? v.sum / v.n : 0 }));
  arr.sort((a,b) => b.val - a.val);
  return arr.slice(0,20);
}

// -------------------
// CSV directory helpers (small copies of Demo helpers)
// -------------------
const fetchText = async (url) => {
  try { const res = await fetch(url); if (!res.ok) return null; return await res.text(); } catch (e) { return null; }
};

const parseDirListing = (html, baseUrl) => {
  try {
    const doc = new DOMParser().parseFromString(html, 'text/html');
    const anchors = Array.from(doc.querySelectorAll('a'));
    const links = anchors.map(a => a.getAttribute('href')).filter(Boolean).map(h => new URL(h, baseUrl).toString());
    return links;
  } catch (e) { return []; }
};

const gatherCsvLinksFromDir = async (dirUrl, seen = new Set()) => {
  if (!dirUrl.endsWith('/')) dirUrl = dirUrl + '/';
  if (seen.has(dirUrl)) return [];
  seen.add(dirUrl);
  const html = await fetchText(dirUrl);
  if (!html) return [];
  const links = parseDirListing(html, dirUrl);
  const csvs = [];
  for (const l of links) {
    if (l.toLowerCase().endsWith('.csv')) csvs.push(l);
    else if (l.endsWith('/')) {
      const sub = await gatherCsvLinksFromDir(l, seen);
      csvs.push(...sub);
    }
  }
  return csvs;
};

const parseCsv = (text) => {
  const lines = text.split(/\r?\n/).filter(l => l.trim() !== '');
  if (!lines.length) return [];
  const header = lines[0].split(',').map(h => h.trim().toLowerCase());
  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length < 1) continue;
    const obj = {};
    for (let j = 0; j < header.length; j++) {
      const val = cols[j] !== undefined ? cols[j].trim() : '';
      obj[header[j]] = val === '' ? undefined : val;
    }
    out.push(obj);
  }
  return out;
};

const fetchAndParseCsv = async (url) => {
  const t = await fetchText(url);
  if (!t) return [];
  return parseCsv(t);
};

// try aggregated monthly CSV first (resources/aggregated/{year}/{yyyymm}.csv)
const tryLoadAggregatedMonth = async (yearMonth) => {
  if (!yearMonth) return [];
  let Y = yearMonth.slice(0,4);
  let M = '';
  if (yearMonth.includes('-')) M = yearMonth.split('-')[1]; else M = yearMonth.slice(4,6);
  if (!Y || !M) return [];
  M = String(M).padStart(2,'0');
  const yyyymm = `${Y}${M}`;
  const candidates = [
    `/resources/aggregated/${Y}/${yyyymm}.csv`,
    `/resources/aggregated/${Y}/${yyyymm}.CSV`,
    `/resources/aggregated/${Y}/${yyyymm}`
  ];
  for (const c of candidates) {
    try {
      const rows = await fetchAndParseCsv(c);
      if (rows && rows.length) return rows;
    } catch (e) {}
  }
  return [];
};

// unified month loader: try aggregated files first, then fall back to folder enumeration
async function loadMonthData(yearMonth) {
  try {
    const agg = await tryLoadAggregatedMonth(yearMonth);
    if (agg && agg.length) {
      monthlyLoadedRows.value = agg;
      return agg;
    }
    // fallback to per-day directory under resources/processed/city/Y/M/
    const bydir = await loadMonthlyDir(yearMonth);
    monthlyLoadedRows.value = bydir || [];
    return monthlyLoadedRows.value;
  } catch (e) { monthlyLoadedRows.value = []; return []; }
}

// try to load a whole year by aggregating available monthly files/directories
async function loadYearData(year) {
  if (!year) return [];
  const combined = [];
  for (let m = 1; m <= 12; m++) {
    const mm = String(m).padStart(2, '0');
    const ym = `${String(year)}-${mm}`;
    try {
      const agg = await tryLoadAggregatedMonth(ym);
      if (agg && agg.length) { combined.push(...agg); continue; }
      const bydir = await loadMonthlyDir(ym);
      if (bydir && bydir.length) { combined.push(...bydir); continue; }
    } catch (e) {}
  }
  monthlyLoadedRows.value = combined;
  return combined;
}

async function loadMonthlyDir(yearMonth) {
  // yearMonth format: 'YYYY-MM' or 'YYYYMM'
  try {
    if (!yearMonth) return [];
    let Y = yearMonth.slice(0,4);
    let M = '';
    if (yearMonth.includes('-')) M = yearMonth.split('-')[1]; else M = yearMonth.slice(4,6);
    if (!Y || !M) return [];
    M = String(M).padStart(2,'0');
    const dir = `/resources/processed/city/${Y}/${M}/`;
    loadingMonthly.value = true;
    const csvs = await gatherCsvLinksFromDir(dir);
    const combined = [];
    for (const c of csvs) {
      try {
        const rows = await fetchAndParseCsv(c);
        if (rows && rows.length) combined.push(...rows);
      } catch (e) {}
    }
    loadingMonthly.value = false;
    monthlyLoadedRows.value = combined;
    return combined;
  } catch (e) { loadingMonthly.value = false; return []; }
}

function renderTrend() {
  if (!trendChart) return;
  const arr = aggregateTrend(rows.value, props.pollutant, props.granularity || 'day');
  if (!arr.length) {
    trendChart.setOption({ title: { text: `${props.province} — 无数据`, left:'center' } });
    return;
  }
  const x = arr.map(d => d.key);
  const y = arr.map(d => Number(d.val.toFixed(2)));
  const option = {
    title: { text: `${props.province} 趋势`, left: 'center' },
    tooltip: { trigger: 'axis', formatter: params => `${params[0].axisValue}<br/>${props.pollutant || 'value'}: ${params[0].data}` },
    toolbox: { feature: { saveAsImage: {} } },
    xAxis: { type: 'category', data: x, boundaryGap: false, axisLabel: { rotate: 0 } },
    yAxis: { type: 'value' },
    series: [{ type: 'line', data: y, smooth: true, symbol: 'circle', symbolSize: 6, areaStyle: { color: { type: 'linear', x:0, y:0, x2:0, y2:1, colorStops:[{offset:0,color:'rgba(43,138,239,0.4)'},{offset:1,color:'rgba(43,138,239,0.05)'}]} } }]
  };
  trendChart.setOption(option, { notMerge: true });
}

function renderDist() {
  if (!distChart) return;
  // 构建跨行污染物值的直方图
  const vals = rows.value.map(r => Number(r[props.pollutant]) ).filter(v => !isNaN(v));
  if (!vals.length) { distChart.setOption({ title: { text: `${props.province} 值分布 — 无数据`, left:'center' } }); return; }
  // 计算桶 (10)
  const min = Math.min(...vals); const max = Math.max(...vals);
  const buckets = 10; const step = (max - min) / buckets || 1;
  const counts = new Array(buckets).fill(0);
  for (const v of vals) {
    let idx = Math.floor((v - min) / step);
    if (idx < 0) idx = 0; if (idx >= buckets) idx = buckets - 1;
    counts[idx]++;
  }
  const x = Array.from({length: buckets}, (_,i) => `${(min + i*step).toFixed(1)}-${(min + (i+1)*step).toFixed(1)}`);
  const option = {
    title: { text: `${props.province} 值分布`, left: 'center' },
    tooltip: { trigger: 'axis' },
    xAxis: { type: 'category', data: x, axisLabel: { rotate: 30 } },
    yAxis: { type: 'value' },
    series: [{ type: 'bar', data: counts, itemStyle: { color: '#f59e0b' } }]
  };
  distChart.setOption(option, { notMerge: true });
}

function renderCity() {
  if (!cityChart) return;
  const type = rightChartType.value || 'bar';
  const arr = aggregateCity(rows.value, props.pollutant);
  if (!arr.length) { cityChart.setOption({ title: { text: `${props.province} 城市对比 — 无数据`, left:'center' } }); return; }
  if (type === 'bar') {
    const x = arr.map(d => d.city);
    const y = arr.map(d => Number(d.val.toFixed(2)));
    const option = {
      title: { text: `${props.province} 城市对比`, left: 'center' },
      tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' } },
      toolbox: { feature: { saveAsImage: {} } },
      xAxis: { type: 'category', data: x, axisLabel: { interval:0, rotate: 30 } },
      yAxis: { type: 'value' },
      series: [{ type: 'bar', data: y, itemStyle: { color: new echarts.graphic.LinearGradient(0,0,0,1,[{offset:0,color:'#000'},{offset:1,color:'#444'}]) }, label: { show: true, position: 'top', color: '#000' } }]
    };
    cityChart.setOption(option, { notMerge: true });
    return;
  }
  if (type === 'pie') {
    const items = arr.map(d => ({ name: d.city, value: Number(d.val) }));
    const option = {
      title: { text: `${props.province} 城市分布（${props.pollutant || 'value'}）`, left: 'center' },
      tooltip: { trigger: 'item', formatter: params => `${params.data.name}<br/>${props.pollutant || 'value'}: ${params.data.value}` },
      legend: { orient: 'vertical', right: 10, top: 20, bottom: 20, data: items.map(d=>d.name), type: 'scroll' },
      series: [{ type: 'pie', radius: ['30%','55%'], center: ['50%','50%'], data: items, label: { show:false }, emphasis: { itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0,0,0,0.5)' } } }]
    };
    cityChart.setOption(option, { notMerge: true });
    return;
  }
  // radar: compare multiple pollutants across top cities (top 5)
  if (type === 'radar') {
    // select candidate pollutant fields
    const sample = rows.value && rows.value.length ? rows.value[0] : {};
    const candidatePollutants = ['pm25','pm10','so2','no2','co','o3'];
    const pollutants = candidatePollutants.filter(p => Object.prototype.hasOwnProperty.call(sample, p));
    if (!pollutants.length) {
      cityChart.setOption({ title: { text: '缺少可比较的污染物字段', left:'center' } });
      return;
    }
    const topCities = arr.slice(0,5);
    const indicator = pollutants.map(p => ({ name: p, max: 0 }));
    // compute values and maxima
    const seriesData = [];
    for (const c of topCities) {
      const cityRows = rows.value.filter(r => (r.city || '').toString() === c.city);
      const vals = pollutants.map(p => {
        const vs = cityRows.map(r => Number(r[p]) ).filter(v => !isNaN(v));
        const avg = vs.length ? vs.reduce((a,b)=>a+b,0)/vs.length : 0;
        return Number(avg.toFixed(2));
      });
      vals.forEach((v,i) => { if (v > indicator[i].max) indicator[i].max = Math.ceil(v*1.2) || 1; });
      seriesData.push({ name: c.city, value: vals });
    }
    const option = {
      title: { text: `${props.province} 城市污染物雷达对比（前 ${topCities.length}）`, left: 'center' },
      tooltip: {},
      legend: { data: seriesData.map(s=>s.name), bottom: 0 },
      radar: { indicator },
      series: [{ type: 'radar', data: seriesData }]
    };
    cityChart.setOption(option, { notMerge: true });
    return;
  }
}

function renderRadar() {
  if (!radarChart) return;
  const sample = rows.value && rows.value.length ? rows.value[0] : {};
  const candidatePollutants = ['pm25','pm10','so2','no2','co','o3'];
  const pollutants = candidatePollutants.filter(p => Object.prototype.hasOwnProperty.call(sample, p));
  if (!pollutants.length) {
    radarChart.setOption({ title: { text: '缺少可比较的污染物字段', left:'center' } });
    return;
  }
  const arr = aggregateCity(rows.value, props.pollutant);
  const topCities = arr.slice(0,5);
  if (!topCities.length) { radarChart.setOption({ title: { text: '无城市数据可比较', left:'center' } }); return; }
  const indicator = pollutants.map(p => ({ name: p, max: 0 }));
  const seriesData = [];
  for (const c of topCities) {
    const cityRows = rows.value.filter(r => (r.city || '').toString() === c.city);
    const vals = pollutants.map(p => {
      const vs = cityRows.map(r => Number(r[p]) ).filter(v => !isNaN(v));
      const avg = vs.length ? vs.reduce((a,b)=>a+b,0)/vs.length : 0;
      return Number(avg.toFixed(2));
    });
    vals.forEach((v,i) => { if (v > indicator[i].max) indicator[i].max = Math.ceil(v*1.2) || 1; });
    seriesData.push({ name: c.city, value: vals });
  }
  const option = {
    title: { text: `${props.province} 城市污染物雷达对比（前 ${topCities.length}）`, left: 'center' },
    tooltip: {},
    legend: { data: seriesData.map(s=>s.name), bottom: 0 },
    radar: { indicator },
    series: [{ type: 'radar', data: seriesData }]
  };
  radarChart.setOption(option, { notMerge: true });
}

onMounted(() => {
  // initialize radar and city charts
  try { if (document.getElementById('radarChart')) radarChart = echarts.init(document.getElementById('radarChart')); } catch (e) {}
  try { if (document.getElementById('trendChart')) trendChart = echarts.init(document.getElementById('trendChart')); } catch (e) {}
  try { if (document.getElementById('distChart')) distChart = echarts.init(document.getElementById('distChart')); } catch (e) {}
  try { cityChart = echarts.init(document.getElementById('cityChart')); } catch (e) {}
  // render according to granularity; radar is always rendered on left
  try {
    if (props.granularity === 'year' && props.fromYear) {
      loadYearData(props.fromYear).then(() => {
        try { renderTrend(); renderRadar(); } catch (e) {}
      }).catch(() => { try { renderTrend(); renderRadar(); } catch (e) {} });
    } else if (props.granularity === 'month' && props.fromMonth) {
      loadMonthData(props.fromMonth).then(() => {
        try { if (props.granularity === 'day') renderDist(); else renderTrend(); renderRadar(); } catch (e) {}
      }).catch(() => { try { if (props.granularity === 'day') renderDist(); else renderTrend(); renderRadar(); } catch (e) {} });
    } else {
      if (props.granularity === 'day') renderDist(); else renderTrend();
      // always render radar
      renderRadar();
    }
  } catch (e) {}
  // render right-side according to selected mode
  try {
    if (rightChartType.value === 'trend') {
      if (props.granularity === 'day') renderDist(); else renderTrend();
    } else {
      renderCity();
    }
  } catch (e) {}
  // resize handler so charts adapt when container width changes
  try {
    window.__provinceViewResize = () => {
      try { radarChart && radarChart.resize(); } catch (e) {}
      try { trendChart && trendChart.resize(); } catch (e) {}
      try { distChart && distChart.resize(); } catch (e) {}
      try { cityChart && cityChart.resize(); } catch (e) {}
    };
    window.addEventListener('resize', window.__provinceViewResize);
  } catch (e) {}
});

onBeforeUnmount(() => {
  try { if (trendChart && trendChart.dispose) { trendChart.dispose(); trendChart = null; } } catch (e) {}
  try { if (distChart && distChart.dispose) { distChart.dispose(); distChart = null; } } catch (e) {}
  try { if (cityChart && cityChart.dispose) { cityChart.dispose(); cityChart = null; } } catch (e) {}
  try { if (radarChart && radarChart.dispose) { radarChart.dispose(); radarChart = null; } } catch (e) {}
  try { if (window.__provinceViewResize) { window.removeEventListener('resize', window.__provinceViewResize); delete window.__provinceViewResize; } } catch (e) {}
});

watch([() => props.data, () => props.pollutant, () => props.granularity, () => props.fromMonth, () => props.fromYear], () => {
  try {
    if (props.granularity === 'year' && props.fromYear) {
      loadYearData(props.fromYear).then(() => { try { if (rightChartType.value === 'trend') { renderTrend(); } else { renderCity(); } renderRadar(); } catch (e) {} });
    } else if (props.granularity === 'month' && props.fromMonth) {
      // reload monthly aggregated file or monthly directory then render
      loadMonthData(props.fromMonth).then(() => { try { if (rightChartType.value === 'trend') { if (props.granularity === 'day') renderDist(); else renderTrend(); } else { renderCity(); } renderRadar(); } catch (e) {} });
    } else {
      if (props.granularity === 'day') renderDist(); else renderTrend();
      renderRadar();
      if (rightChartType.value === 'trend') { if (props.granularity === 'day') renderDist(); else renderTrend(); } else { renderCity(); }
    }
  } catch (e) {}
}, { deep: true });

watch(() => rightChartType.value, () => {
  try {
    if (rightChartType.value === 'trend') {
      if (props.granularity === 'day') renderDist(); else renderTrend();
    } else {
      renderCity();
    }
  } catch (e) {}
});
</script>

<style scoped>
.province-view { max-width: 1200px; margin: 12px auto; padding: 8px; }
.pv-header { background: rgba(0,0,0,0.04); padding: 8px 12px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); margin-bottom: 8px }
.pv-title { color: #000; margin: 0 0 4px 0; }
.pv-sub { color: #000; font-size: 0.95rem; margin-bottom: 6px }
.pv-meta { color: #000; font-size: 0.85rem; opacity: 0.95 }
.pv-controls { display:flex; align-items:center; gap:8px; margin-top:6px; color: #000}
.btn-small { background:#e6e6e6; border:1px solid #ccc; padding:4px 8px; border-radius:4px; cursor:pointer; color: #000; }
.btn-small.active { background:#222; color:#fff; border-color:#111 }
.pv-grid { display:flex; gap:12px; align-items:start; }
.pv-chart { flex: 1; min-width: 560px }
.pv-chart-left { flex: 1.2 }
.pv-chart-right { flex: 1 }
.chart-canvas { height: 480px; width: 560px; background: #fff; border-radius: 6px; }

@media (max-width: 900px) {
  .pv-grid { flex-direction: column; }
  .chart-canvas { height: 360px; }
}
</style>
