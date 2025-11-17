<template>
  <div class="demo-root">
    <header class="demo-header">
      <h1>污染可视化</h1>
      <div class="controls">
        <div class="controls-left">
          <label>污染物:
            <select v-model="selectedPollutant">
              <option v-for="p in numericColumns" :key="p" :value="p">{{ p }}</option>
            </select>
          </label>

          <label>时间粒度:
            <select v-model="granularity">
              <option value="year">年</option>
              <option value="month">月</option>
              <option value="day">日</option>
              <option value="hour">小时</option>
            </select>
          </label>

          <div class="time-controls">
            <template v-if="granularity === 'year'">
              <label>年份: <input type="number" v-model.number="fromYear" min="1900" max="2100" /></label>
            </template>
            <template v-else-if="granularity === 'month'">
              <label>月份: <input type="month" v-model="fromMonth" /></label>
            </template>
            <template v-else-if="granularity === 'day'">
              <label>日期: <input type="date" v-model="fromDate" /></label>
            </template>
            <template v-else-if="granularity === 'hour'">
              <label>小时: <input type="datetime-local" v-model="fromDateTime" /></label>
            </template>
          </div>

          <button class="btn" @click="applyFilters">应用过滤</button>
          <button class="btn" @click="resetFilters">重置</button>
        </div>

  <div class="controls-right">
          <label class="uploader">本地加载:
            <input id="fileInput" type="file" accept=".json,.csv" @change="onFileInput">
          </label>
          <button class="btn" @click="toggleTable">{{ showTable ? '隐藏表格' : '显示表格' }}</button>
          <button class="btn" @click="exportCsv">导出 CSV</button>
          <input id="pathInput" type="text" v-model="loadPath" placeholder="输入资源路径或 URL" class="path-input" />
          <button class="btn" @click="loadFromPath">加载路径</button>
          <button class="btn" @click="loadTestFile">加载测试文件</button>
        </div>
      </div>
    </header>

    <div class="header-selection" v-if="selectedProvince">
      <div class="sel-inner">
        <strong style="color:#fff">已选省份：</strong>
        <span style="color:#fff;margin:0 8px">{{ selectedProvince }}</span>
        <button class="btn" style="padding:4px 8px;background:#666" @click="clearSelection">清除选择</button>
      </div>
    </div>

    <main>
      <div id="diag" class="diag"></div>
      <div class="debug-box">
        <div><strong>Rows loaded:</strong> {{ filtered.length }}</div>
        <div><strong>Columns:</strong> {{ filtered.length ? Object.keys(filtered[0]).join(', ') : (raw.length ? Object.keys(raw[0]).join(', ') : '(none)') }}</div>
      </div>

      <div class="grid">
        <div class="left">
          <template v-if="!selectedProvince">
            <section class="chart large">
              <div id="previewChart" class="chart-canvas"></div>
            </section>
            <section class="chart large">
              <div id="rankChart" class="chart-canvas"></div>
            </section>
            <!-- Parallel coordinates below the preview and ranking charts -->
            <section class="chart large">
              <div style="display:flex; align-items:center; gap:12px; padding:8px 12px;">
                <strong style="color:#000">并行坐标 — 选择污染物：</strong>
                <label v-for="p in candidatePollutants" :key="p" style="display:inline-flex;align-items:center;gap:6px;margin-right:8px;color:#000">
                  <input type="checkbox" :value="p" v-model="selectedPollutants" /> {{ p }}
                </label>
                <button class="btn" @click="resetParallelSelection">重置</button>
              </div>
              <div id="parallelChart" class="chart-canvas"></div>
            </section>
          </template>
          <template v-else>
            <!-- when a province is selected, show province-level component -->
            <ProvinceView :province="selectedProvince" :data="filtered" :pollutant="selectedPollutant" :granularity="granularity" :from-month="fromMonth" :from-year="fromYear" />
          </template>
        </div>
      </div>

      <!-- 数据示例（前 50 条） -->
      <div id="sampleTableContainer" style="max-width:1200px;margin:12px auto;padding:10px;border-radius:6px;">
        <div style="background:#fff;padding:8px;border-radius:6px;box-shadow:0 1px 3px rgba(0,0,0,0.04)">
          <h3 style="color:#000;margin:0 0 8px 0;">数据示例（前 50 条，可滚动）</h3>
          <div style="max-height:360px; overflow:auto;">
            <table>
              <thead>
                <tr>
                  <th v-for="c in displayColumns" :key="c">{{ c }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="(r, idx) in sampleRows" :key="r.id || idx">
                  <td v-for="c in displayColumns" :key="c">{{ displayCell(r, c) }}</td>
                </tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </main>
  </div>
</template>

<script setup>
import { computed, nextTick, onMounted, ref, watch } from 'vue';
import ProvinceView from './ProvinceView.vue';

// dynamic columns and controls
const selectedPollutant = ref('');
const raw = ref([]);
const fromDate = ref('2013-01-01');
const toDate = ref('');
const fromMonth = ref('');
const toMonth = ref('');
const fromYear = ref(new Date().getFullYear());
const toYear = ref(new Date().getFullYear());
const fromDateTime = ref('');
const toDateTime = ref('');
const granularity = ref('day'); // 'year'|'month'|'day'|'hour'
const loadedFiles = ref([]);
const showTable = ref(true);
const loadPath = ref('');
const selectedProvince = ref(null);
const attemptedCandidates = ref([]);

// parallel chart state
const candidatePollutants = ['pm25','pm10','so2','no2','co','o3'];
const selectedPollutants = ref(candidatePollutants.slice(0, 4));
let parallelChart = null;

// remember last auto-loaded key to avoid duplicate loads
let lastAutoLoadedKey = null;

// try candidate urls sequentially and return first that yields rows
async function tryLoadCandidates(candidates) {
  attemptedCandidates.value = [];
  for (const url of candidates) {
    try {
      attemptedCandidates.value.push(url);
    } catch (e) {}
    updateDiag('尝试加载: ' + url);
    const rows = await fetchAndParseCsv(url);
    if (rows && rows.length) {
      updateDiag(`自动加载成功：${rows.length} 行 来自 ${url}`);
      raw.value = rows;
      try { addLoadedFile(url); } catch (e) {}
      initCharts();
      return { rows, url };
    }
  }
  return null;
}

function addLoadedFile(url) {
  if (!url) return;
  try {
    const u = String(url);
    if (!u.toLowerCase().endsWith('.csv')) return; // only track csv files
    if (!loadedFiles.value.includes(u)) loadedFiles.value.push(u);
  } catch (e) {}
}

// construct likely CSV paths for a given selection according to granularity
function buildCandidatePathsForSelection() {
  const g = granularity.value;
  const candidates = [];
  try {
    if (g === 'day' && fromDate.value) {
      const d = new Date(fromDate.value);
      if (!isNaN(d.getTime())) {
        const Y = String(d.getFullYear());
        const M = String(d.getMonth() + 1).padStart(2, '0');
        const D = String(d.getDate()).padStart(2, '0');
        const fname = `${Y}${M}${D}.csv`;
        // common processed path
        candidates.push(`/resources/processed/city/${Y}/${M}/${D}/${fname}`);
        candidates.push(`/resources/processed/city/${Y}/${M}/${fname}`);
        candidates.push(`/resources/processed/city/${Y}/${fname}`);
        // aggregated or fallback location
        candidates.push(`/resources/aggregated/${Y}/${M}/${fname}`);
        candidates.push(`/resources/aggregated/${fname}`);
      }
    } else if (g === 'month' && fromMonth.value) {
        const [Y, M] = fromMonth.value.split('-');
        const ym = `${Y}${M}`; // e.g. 201301
        // try the canonical aggregated-month filename first (e.g. /resources/aggregated/2013/201301.csv)
        candidates.push(`/resources/aggregated/${Y}/${ym}.csv`);      // /aggregated/2013/201301.csv
        // then try a month-number filename and json variant as secondary options
        candidates.push(`/resources/aggregated/${Y}/${M}.csv`);       // /aggregated/2013/01.csv (less common)
        candidates.push(`/resources/aggregated/${Y}/${ym}.json`);     // json variant
        // try processed city month directory and year directory
        candidates.push(`/resources/processed/city/${Y}/${M}/`);
        candidates.push(`/resources/processed/city/${Y}/`);
        // also allow aggregated month directory
        candidates.push(`/resources/aggregated/${Y}/${M}/`);
    } else if (g === 'year' && fromYear.value) {
      const Y = String(fromYear.value);
      candidates.push(`/resources/processed/city/${Y}/`);
      candidates.push(`/resources/aggregated/${Y}/`);
    } else if (g === 'hour' && fromDateTime.value) {
      const d = new Date(fromDateTime.value);
      if (!isNaN(d.getTime())) {
        const Y = String(d.getFullYear());
        const M = String(d.getMonth() + 1).padStart(2, '0');
        const D = String(d.getDate()).padStart(2, '0');
        const hh = String(d.getHours()).padStart(2, '0');
        const fname = `${Y}${M}${D}${hh}.csv`;
        candidates.push(`/resources/processed/city/${Y}/${M}/${D}/${fname}`);
        candidates.push(`/resources/aggregated/${Y}/${M}/${fname}`);
      }
    }
  } catch (e) {}
  return candidates;
}

// Auto load when selection changes
async function autoLoadForSelection() {
  const key = `${granularity.value}::${granularity.value==='day'?fromDate.value:(granularity.value==='month'?fromMonth.value:(granularity.value==='year'?fromYear.value:fromDateTime.value))}`;
  if (!key || key === `${granularity.value}::null`) return;
  if (key === lastAutoLoadedKey) return; // avoid duplicate reload
  lastAutoLoadedKey = key;
  const candidates = buildCandidatePathsForSelection();
  if (!candidates || !candidates.length) return;
  // if candidate is a directory (ends with '/'), try to gather csv links in it
  const expanded = [];
  for (const c of candidates) {
    if (c.endsWith('/')) {
      try {
        const csvs = await gatherCsvLinksFromDir(c);
        for (const s of csvs) expanded.push(s);
      } catch (e) {}
    } else expanded.push(c);
  }
  await tryLoadCandidates(expanded);
}

let mapChart = null;
let rankChart = null;

function updateDiag(msg) {
  try { console.log('[demo diag]', msg); } catch (e) {}
  const d = document.getElementById('diag');
  if (d) {
    d.innerText = String(msg);
    d.classList.remove('error');
  }
}

function displayCell(row, col) {
  const v = row[col];
  if (v === undefined || v === null) return '';
  if (['lat','lon'].includes(col)) {
    const n = Number(v);
    if (isNaN(n)) return '';
    return n.toFixed(3);
  }
  return String(v);
}

const tryFetchJson = async (url) => {
  try {
    const res = await fetch(url);
    if (!res.ok) return null;
    const j = await res.json();
    if (Array.isArray(j)) return j;
    if (j && Array.isArray(j.data)) return j.data;
    if (j && Array.isArray(j.rows)) return j.rows;
    for (const k of Object.keys(j || {})) if (Array.isArray(j[k])) return j[k];
    return null;
  } catch (e) { return null; }
};

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
  // normalize header names to lower-case, trimmed (helps mapping)
  const header = lines[0].split(',').map(h => h.trim().toLowerCase());
  const out = [];
  for (let i = 1; i < lines.length; i++) {
    const cols = lines[i].split(',');
    if (cols.length < 1) continue;
    const obj = {};
    for (let j = 0; j < header.length; j++) {
      // coerce empty strings to undefined for easier checks
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
  const rows = parseCsv(t);
  try {
    const u = new URL(url, window.location.href);
    const parts = u.pathname.split('/');
    const fname = parts[parts.length - 1] || '';
    const m = fname.match(/(\d{4})(\d{2})(\d{2})/);
    let day = null;
    if (m) day = `${m[1]}-${m[2]}-${m[3]}`;
    if (day) {
      for (const r of rows) {
        if (!r.time) r.time = day;
      }
    }
    if (rows && rows.length) {
      try { addLoadedFile(url); } catch (e) {}
      updateDiag(`Loaded ${rows.length} rows from ${url}`);
      console.log('loaded csv', url, rows.length);
    }
  } catch (e) {}
  return rows;
};

// helper: detect a parsable date string in a row and return a Date or null
function parseDateFromRow(r) {
  if (!r || typeof r !== 'object') return null;
  const keys = Object.keys(r);
  const candidates = ['time','date','day','datetime','timestamp'];
  for (const k of candidates) {
    if (keys.includes(k) && r[k]) {
      const s = String(r[k]);
      // try ISO
      let d = new Date(s);
      if (!isNaN(d.getTime())) return d;
      // try YYYYMMDD
      const m = s.match(/^(\d{4})(\d{2})(\d{2})/);
      if (m) return new Date(`${m[1]}-${m[2]}-${m[3]}T00:00:00`);
    }
  }
  return null;
}

function formatKeyByGranularity(r) {
  const d = parseDateFromRow(r);
  if (!d) return 'unknown';
  const y = d.getFullYear();
  const m = (d.getMonth()+1).toString().padStart(2,'0');
  const day = d.getDate().toString().padStart(2,'0');
  const hh = d.getHours().toString().padStart(2,'0');
  if (granularity.value === 'year') return String(y);
  if (granularity.value === 'month') return `${y}-${m}`;
  if (granularity.value === 'hour') return `${y}-${m}-${day}T${hh}`;
  return `${y}-${m}-${day}`;
}

const loadData = async () => {
  const params = new URLSearchParams(window.location.search);
  const userPath = params.get('data');
  if (userPath) {
    const maybeJson = await tryFetchJson(userPath);
    if (maybeJson && maybeJson.length) { raw.value = maybeJson; initCharts(); return; }
    const csvRows = await fetchAndParseCsv(userPath);
    if (csvRows && csvRows.length) { raw.value = csvRows; initCharts(); return; }
  }

  const outDir = '/resources/output/echarts/';
  const outListHtml = await fetchText(outDir);
  if (outListHtml) {
    const links = parseDirListing(outListHtml, outDir).filter(u => u.toLowerCase().endsWith('.json'));
    for (const jurl of links) {
      const j = await tryFetchJson(jurl);
      if (j && j.length) { raw.value = j; console.log('loaded echarts json from', jurl); initCharts(); return; }
    }
  }

  const aggDir = '/resources/aggregated/2013/';
  const csvs = await gatherCsvLinksFromDir(aggDir);
  if (csvs && csvs.length) {
    let combined = [];
    for (const c of csvs) {
      const rows = await fetchAndParseCsv(c);
      combined = combined.concat(rows);
    }
    if (combined.length) { raw.value = combined; console.log('loaded aggregated csvs from', aggDir); initCharts(); return; }
  }

  const procDir = '/resources/processed/city/2013/';
  const procCsvs = await gatherCsvLinksFromDir(procDir);
  if (procCsvs && procCsvs.length) {
    let combined = [];
    for (const c of procCsvs) {
      const rows = await fetchAndParseCsv(c);
      combined = combined.concat(rows);
    }
    if (combined.length) { raw.value = combined; console.log('loaded processed city csvs from', procDir); initCharts(); return; }
  }

  const sample = await tryFetchJson('./data/sample_data.json');
  if (sample && sample.length) { raw.value = sample; initCharts(); return; }

  console.warn('no dataset found; frontend will be empty');
  raw.value = [];
  initCharts();
};

function handleParsed(rows) {
  if (!rows || !rows.length) return;
  raw.value = rows.map((r, i) => {
    const obj = {};
    // normalize incoming keys to lower-case trimmed names
    for (const k of Object.keys(r)) {
      const lk = String(k).trim().toLowerCase();
      const v = r[k];
      obj[lk] = v === '' ? undefined : v;
    }
    if (!obj.id) obj.id = i + 1;
    return obj;
  });
  initCharts();

  // if pollutant not set yet, pick first numeric column
  try {
    const nums = numericColumns.value;
    if ((!selectedPollutant.value || selectedPollutant.value === '') && nums && nums.length) selectedPollutant.value = nums[0];
  } catch (e) {}
}

function handleFile(file) {
  if (!file) return;
  const name = file.name.toLowerCase();
  if (name.endsWith('.json')) {
    const reader = new FileReader();
    reader.onload = (ev) => {
      try {
        const j = JSON.parse(ev.target.result);
        if (Array.isArray(j)) handleParsed(j);
        else if (j && Array.isArray(j.data)) handleParsed(j.data);
        else console.warn('JSON file does not contain array');
      } catch (e) { console.error('failed parsing json', e); }
    };
    reader.readAsText(file);
  } else if (name.endsWith('.csv')) {
    if (window.Papa && Papa.parse) {
      Papa.parse(file, { header: true, dynamicTyping: true, skipEmptyLines: true, complete: (res) => { handleParsed(res.data); } });
    } else {
      const reader = new FileReader();
      reader.onload = (ev) => { handleParsed(parseCsv(ev.target.result)); };
      reader.readAsText(file);
    }
  } else {
    console.warn('unsupported file type', file.name);
  }
}

function onFileInput(e) { if (e && e.target && e.target.files) handleFiles(e.target.files); }
function handleFiles(fileList) { if (!fileList || fileList.length === 0) return; handleFile(fileList[0]); }

async function loadFromPath() {
  if (!loadPath.value) { updateDiag('请输入要加载的路径或 URL'); return; }
  updateDiag('尝试从路径加载: ' + loadPath.value);
  // try JSON first then CSV
  const maybeJson = await tryFetchJson(loadPath.value);
  if (maybeJson && maybeJson.length) { raw.value = maybeJson; updateDiag('Loaded ' + maybeJson.length + ' rows from ' + loadPath.value); initCharts(); return; }
  const csvRows = await fetchAndParseCsv(loadPath.value);
  if (csvRows && csvRows.length) { raw.value = csvRows; updateDiag('Loaded ' + csvRows.length + ' rows from ' + loadPath.value); initCharts(); return; }
  updateDiag('无法从指定路径加载数据: ' + loadPath.value);
}

async function loadTestFile() {
  const test = '/resources/processed/city/2013/01/01/20130101.csv';
  updateDiag('尝试加载测试文件: ' + test);
  const rows = await fetchAndParseCsv(test);
  if (rows && rows.length) { raw.value = rows; updateDiag('Loaded ' + rows.length + ' rows from ' + test); initCharts(); return; }
  updateDiag('测试文件未能加载或为空: ' + test);
}

const numericColumns = computed(() => {
  const rows = raw.value || [];
  if (!rows.length) return [];
  const sample = rows.slice(0, Math.min(50, rows.length));
  const keys = Object.keys(sample[0] || {});
  const nums = [];
  for (const k of keys) {
    let numericCount = 0, total = 0;
    for (const r of sample) {
      const v = r[k];
      if (v === undefined || v === null || v === '') { total++; continue; }
      total++;
      if (!isNaN(Number(v))) numericCount++;
    }
    if (total > 0 && numericCount / total >= 0.6) nums.push(k);
  }
  return nums;
});

const columns = computed(() => {
  if (!raw.value || !raw.value.length) return [];
  return Object.keys(raw.value[0] || {});
});

// columns to display in the sample table and to export
const displayColumns = computed(() => {
  const keys = columns.value || [];
  if (!selectedPollutant.value) return keys;
  // preferred context columns to keep for location/time
  const preferred = ['time','province','city','county','district'];
  const keep = [];
  for (const p of preferred) if (keys.includes(p)) keep.push(p);
  // ensure pollutant column is included
  if (keys.includes(selectedPollutant.value)) keep.push(selectedPollutant.value);
  // if pollutant not in keys (unexpected), fallback to keys
  if (!keep.length) return keys;
  return keep;
});

const filtered = computed(() => {
  let rows = (raw.value || []).slice();
  // apply granularity-aware range filtering; rows without parseable date are kept
  if (granularity.value === 'year') {
    const fy = fromYear.value || null;
    if (fy) {
      rows = rows.filter(r => { const d = parseDateFromRow(r); if (!d) return true; const y = d.getFullYear(); return y === Number(fy); });
    }
  } else if (granularity.value === 'month') {
    const fm = fromMonth.value || null;
    if (fm) {
      rows = rows.filter(r => { const d = parseDateFromRow(r); if (!d) return true; const ym = `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}`; return ym === fm; });
    }
  } else if (granularity.value === 'hour') {
    const fdt = fromDateTime.value ? new Date(fromDateTime.value) : null;
    if (fdt) {
      rows = rows.filter(r => { const d = parseDateFromRow(r); if (!d) return true; return d.getFullYear()===fdt.getFullYear() && d.getMonth()===fdt.getMonth() && d.getDate()===fdt.getDate() && d.getHours()===fdt.getHours(); });
    }
  } else { // day
    const fd = fromDate.value ? new Date(fromDate.value) : null;
    if (fd) {
      rows = rows.filter(r => { const d = parseDateFromRow(r); if (!d) return true; return d.getFullYear()===fd.getFullYear() && d.getMonth()===fd.getMonth() && d.getDate()===fd.getDate(); });
    }
  }
  // if a province is selected by clicking a pie slice, filter rows to that province
  if (selectedProvince.value) {
    const sp = String(selectedProvince.value).toLowerCase();
    rows = rows.filter(r => {
      if (!r) return false;
      const p = (r.province || r.state || r.region || '').toString().toLowerCase();
      const c = (r.city || '').toString().toLowerCase();
      return p === sp || c === sp || p.includes(sp) || c.includes(sp);
    });
  }
  return rows;
});

// apply province selection filter: if a province is selected by clicking pie, filter to it
// when selectedProvince changes, manage charts lifecycle so clearing returns to previous view
watch(selectedProvince, async () => {
  try {
    if (selectedProvince.value) {
      // a province was selected: dispose global charts to avoid DOM/instance conflicts
      try { if (mapChart && mapChart.dispose) { mapChart.dispose(); mapChart = null; } } catch (e) {}
      try { if (rankChart && rankChart.dispose) { rankChart.dispose(); rankChart = null; } } catch (e) {}
      // no need to render global charts while province view is active
    } else {
      // selection cleared: re-init the global charts after DOM updates
      await nextTick();
      try { initCharts(); renderAll(); } catch (e) {}
    }
  } catch (e) {}
});

const sampleRows = computed(() => filtered.value.slice(0, 50));

// when raw data changes, pick a default pollutant if none selected
watch(raw, () => {
  try {
    const nums = numericColumns.value;
    if ((!selectedPollutant.value || selectedPollutant.value === '') && nums && nums.length) selectedPollutant.value = nums[0];
  } catch (e) {}
});

// when pollutant selection changes, re-render charts so preview/ranking only show that pollutant
watch(selectedPollutant, () => {
  try { renderAll(); } catch (e) {}
});

// trigger auto load when granularity or the selected time value changes
watch([granularity, fromDate, fromMonth, fromYear, fromDateTime], () => {
  try { autoLoadForSelection(); } catch (e) {}
});

function initCharts() {
  function showDiag(msg) { updateDiag(msg); }
  if (typeof echarts === 'undefined') { showDiag('ECharts is not available (echarts is undefined).'); return; }
  const mapDom = document.getElementById('previewChart');
  if (!mapDom) { showDiag('previewChart DOM element not found (id=previewChart)'); return; }
  try { mapChart = echarts.init(mapDom); } catch (e) { showDiag('Failed to init preview chart: '+e.message); return; }
  const rankDom = document.getElementById('rankChart');
  if (!rankDom) { showDiag('rankChart DOM element not found (id=rankChart)'); return; }
  try { rankChart = echarts.init(rankDom); } catch (e) { showDiag('Failed to init rank chart: '+e.message); return; }
  // init parallel chart if present
  try {
    const parDom = document.getElementById('parallelChart');
    if (parDom) parallelChart = echarts.init(parDom);
  } catch (e) { parallelChart = null; }
  showDiag('ECharts initialized — rendering charts');
  renderAll();
  // ensure map click handlers are attached after charts are initialized
  try { attachMapClickHandlers(); } catch (e) {}
  // listen to parallel chart city selection events
  try {
    window.__onProvinceViewCitySelected = (ev) => {
      try {
        const city = ev && ev.detail && ev.detail.city;
        if (city) {
          selectedProvince.value = city;
          updateDiag('已选(来自并行坐标): ' + city);
        }
      } catch (e) {}
    };
    window.addEventListener('provinceview-city-selected', window.__onProvinceViewCitySelected);
  } catch (e) {}
  window.addEventListener('resize', () => { mapChart && mapChart.resize(); rankChart && rankChart.resize(); parallelChart && parallelChart.resize(); });
}

// centralize attachment of click handlers for the preview map chart
function attachMapClickHandlers() {
  try {
    if (!mapChart && !rankChart) return;
    if (mapChart) {
      mapChart.off('click');
      mapChart.on('click', (params) => {
      try {
        if (!params) return;
        if (params.componentType === 'series' && params.seriesType === 'pie') {
          selectedProvince.value = params.name;
          updateDiag('已选: ' + params.name);
          return;
        }
        if (params.componentType === 'series' && params.seriesType === 'scatter') {
          const rawdata = params.data && params.data.raw;
          if (rawdata) {
            if (rawdata.province) selectedProvince.value = rawdata.province;
            else if (rawdata.city) selectedProvince.value = rawdata.city;
            updateDiag('已选: ' + (selectedProvince.value || 'N/A'));
          }
        }
      } catch (e) {}
      });
    }
    if (rankChart) {
      rankChart.off('click');
      rankChart.on('click', (params) => {
        try {
          if (!params) return;
          // bar chart click => use axis value or name
          const name = params.name || (params.data && params.data.name) || (params.value && params.value[0]);
          if (name) {
            selectedProvince.value = name;
            updateDiag('已选: ' + name);
          }
        } catch (e) {}
      });
    }
  } catch (e) {}
}

// renderAll is declared later (includes parallel). remove this earlier duplicate.

function renderMap() {
  const hasCoords = filtered.value.some(r => r.lon !== undefined && r.lat !== undefined && !isNaN(Number(r.lon)) && !isNaN(Number(r.lat)));
  if (hasCoords) {
    const data = filtered.value.map((r, idx) => ({ name: r.city || `p${idx}`, value: [Number(r.lon) || 0, Number(r.lat) || 0, Number(selectedPollutant.value ? r[selectedPollutant.value] : 0) || 0], raw: r }));
    // try to load GeoJSON for nicer heatmap (GADM provided in resources/GADM)
    (async () => {
      const geoPath = '/resources/GADM/gadm41_CHN_2.json';
      let gj = null;
      try {
        const res = await fetch(geoPath);
        if (res && res.ok) gj = await res.json();
      } catch (e) { gj = null; }
      if (gj) {
        try {
          echarts.registerMap('CHN', gj);
        } catch (e) {}
        const option = {
          title: { text: `污染热力图` + (selectedPollutant.value ? ` — ${selectedPollutant.value.toUpperCase()}` : ''), left: 'center' },
          tooltip: { trigger: 'item', formatter: params => { if (params.seriesType === 'heatmap' || params.seriesType === 'scatter') { const d = params.data; return `${d && d[3] ? d[3].city || '' : ''}<br/>${selectedPollutant.value}: ${d && d[2]}`; } return params.name; } },
          visualMap: { min: 0, max: Math.max(...data.map(d=>d.value[2]||0)), realtime: false, calculable: true, inRange: { color: ['#50a3ba','#eac736','#d94e5d'] } },
          geo: { map: 'CHN', roam: true, silent: true },
          series: [{ type: 'heatmap', coordinateSystem: 'geo', data: data.map(d => [d.value[0], d.value[1], d.value[2], d.raw]) }]
        };
        mapChart.setOption(option);
        return;
      }
      // fallback: scatter
      const option = {
        title: { text: `散点地图` + (selectedPollutant.value ? ` — ${selectedPollutant.value.toUpperCase()}` : ''), left: 'center' },
        tooltip: { trigger: 'item', formatter: params => {
          const d = params.data.raw; const parts = [];
          for (const k of Object.keys(d || {})) parts.push(`${k}: ${d[k]}`);
          return parts.join('<br/>');
        } },
        xAxis: { name: 'lon', type: 'value' }, yAxis: { name: 'lat', type: 'value' }, grid: { left: 40, right: 20, top: 60, bottom: 40 },
        series: [{ type: 'scatter', symbolSize: val => Math.max(4, Math.sqrt(val[2] || 0) * 1.8), data: data, encode: { x: 0, y: 1, value: 2 } }]
      };
      mapChart.setOption(option);
    })();
  } else {
    // no coords: show a preview pie of counts by a categorical key (top categories)
    const firstRow = filtered.value[0] || {};
    const keys = Object.keys(firstRow || {});
    let groupKey = keys.find(k => ['city','province','county','district'].includes(k));
    if (!groupKey) {
      groupKey = keys.find(k => { const v = firstRow[k]; return v !== undefined && isNaN(Number(v)); });
    }
    if (!groupKey) groupKey = 'all';
    const counts = {};
    for (const r of filtered.value) {
      const c = groupKey === 'all' ? 'all' : (r[groupKey] || 'unknown');
      counts[c] = (counts[c] || 0) + 1;
    }
    // if a pollutant is selected, show its aggregated distribution (sum) by groupKey
    if (selectedPollutant.value) {
      const sums = {};
      for (const r of filtered.value) {
        const c = groupKey === 'all' ? 'all' : (r[groupKey] || 'unknown');
        const v = Number(r[selectedPollutant.value]) || 0;
        sums[c] = (sums[c] || 0) + v;
      }
  const items = Object.keys(sums).map(k => ({ name: k, value: sums[k] }));
  items.sort((a,b) => b.value - a.value);
  const top = items.slice(0,50);
        const option = {
          title: { text: `预览：按 ${groupKey} 的 ${selectedPollutant.value} 总和分布`, left: 'center' },
          tooltip: { trigger: 'item', formatter: params => `${params.data.name}<br/>${selectedPollutant.value}: ${params.data.value}` },
          legend: { orient: 'vertical', right: 10, top: 20, bottom: 20, data: top.map(d=>d.name), type: 'scroll' },
          series: [{ type: 'pie', radius: ['25%','45%'], center: ['52%','50%'], data: top, avoidLabelOverlap: false, emphasis: { itemStyle: { shadowBlur: 10, shadowOffsetX: 0, shadowColor: 'rgba(0, 0, 0, 0.5)' } }, label: { show: false }, labelLine: { show: false } }]
        };
        mapChart.setOption(option);
    } else {
  const items = Object.keys(counts).map(k => ({ name: k, value: counts[k] }));
  items.sort((a,b) => b.value - a.value);
  const top = items.slice(0,50);
      const option = {
        title: { text: `预览：按 ${groupKey} 分布（计数）`, left: 'center' },
        tooltip: { trigger: 'item' },
        legend: { orient: 'vertical', right: 10, top: 20, bottom: 20, data: top.map(d=>d.name), type: 'scroll' },
        series: [{ type: 'pie', radius: ['25%','45%'], center: ['52%','50%'], data: top, avoidLabelOverlap: false, label: { show: false }, labelLine: { show: false } }]
      };
      mapChart.setOption(option);
      // click handler: if scatter, use data.raw; if pie, params.name is the group (province/city)
      try {
        mapChart.off('click');
        mapChart.on('click', (params) => {
          try {
            if (!params) return;
            if (params.componentType === 'series' && params.seriesType === 'pie') {
              // clicking a pie slice selects that group (e.g., province)
              selectedProvince.value = params.name;
              updateDiag('已选: ' + params.name);
              return;
            }
            if (params.componentType === 'series' && params.seriesType === 'scatter') {
              const rawdata = params.data && params.data.raw;
              if (rawdata) {
                if (rawdata.province) selectedProvince.value = rawdata.province;
                else if (rawdata.city) selectedProvince.value = rawdata.city;
                updateDiag('已选: ' + (selectedProvince.value || 'N/A'));
              }
            }
          } catch (e) {}
        });
      } catch (e) {}
        try {
          mapChart.off('click');
          mapChart.on('click', (params) => {
            try {
                if (params && params.componentType === 'series' && params.seriesType === 'pie') {
                selectedProvince.value = params.name;
                updateDiag('已选: ' + params.name);
              }
            } catch (e) {}
          });
        } catch (e) {}
    }
  }
}

function renderRanking() {
  // ranking: aggregate by chosen categorical key and compute average of selected pollutant
  const firstRow = filtered.value[0] || {};
  const keys = Object.keys(firstRow || {});
  let groupKey = keys.find(k => ['city','province','county','district'].includes(k));
  if (!groupKey) {
    groupKey = keys.find(k => { const v = firstRow[k]; return v !== undefined && isNaN(Number(v)); });
  }
  if (!groupKey) groupKey = 'all';
  const byGroup = {};
  for (const r of filtered.value) {
    const c = groupKey === 'all' ? 'all' : (r[groupKey] || 'unknown');
    const v = selectedPollutant.value ? Number(r[selectedPollutant.value]) || 0 : 0;
    if (!byGroup[c]) byGroup[c] = { sum: 0, n: 0 };
    byGroup[c].sum += v; byGroup[c].n += 1;
  }
  const rows = Object.keys(byGroup).map(k => ({ key: k, val: byGroup[k].n ? byGroup[k].sum / byGroup[k].n : 0 }));
  rows.sort((a,b) => b.val - a.val);
  const top = rows.slice(0,50);
  const option = { title: { text: `按 ${groupKey} 平均值排名` + (selectedPollutant.value ? `（${selectedPollutant.value.toUpperCase()}）` : ''), left: 'center' }, tooltip: { trigger: 'axis' }, xAxis: { type: 'value' }, yAxis: { type: 'category', data: top.map(d => d.key).reverse() }, series: [{ type: 'bar', data: top.map(d => d.val).reverse() }] };
  rankChart.setOption(option);
}
function renderParallel() {
  try {
    if (!parallelChart) return;
    const rows = filtered.value || [];
    if (!rows.length) {
      parallelChart.setOption({ title: { text: '并行坐标：无数据', left: 'center' } });
      return;
    }
    const pollutants = (selectedPollutants.value && selectedPollutants.value.length) ? selectedPollutants.value : candidatePollutants;
    const stats = {};
    for (const p of pollutants) {
      const vals = rows.map(r => Number(r[p])).filter(v => !isNaN(v));
      stats[p] = { min: vals.length ? Math.min(...vals) : 0, max: vals.length ? Math.max(...vals) : 1 };
      if (stats[p].min === stats[p].max) stats[p].max = stats[p].min + 1;
    }
    const seriesData = rows.map(r => {
      const vals = pollutants.map(p => {
        const v = Number(r[p]);
        if (isNaN(v)) return 0;
        const s = stats[p];
        return (v - s.min) / (s.max - s.min);
      });
      return { value: vals.concat([r.city || r.province || '']), raw: r };
    }).slice(0, 2000);

    const parallelAxis = pollutants.map((p, i) => ({ dim: i, name: p, min: 0, max: 1, axisLabel: { color: '#000', formatter: v => { const real = (v * (stats[p].max - stats[p].min) + stats[p].min); return Number(real).toFixed(1); } }, nameTextStyle: { color: '#000' } }));
    const option = {
      title: { text: `并行坐标（城市）`, left: 'center', textStyle: { color: '#000' } },
      tooltip: { trigger: 'item', formatter: params => {
        const raw = params.data && params.data.raw;
        if (!raw) return params.name || '';
        const parts = [`城市: ${raw.city || raw.province || 'N/A'}`];
        for (const p of pollutants) parts.push(`${p}: ${raw[p]}`);
        return parts.join('<br/>');
      }},
      parallel: { left: '5%', right: '8%', top: 60, bottom: 30 },
      parallelAxis,
      visualMap: { show: true, min: 0, max: 1, dimension: 0, inRange: { color: ['#50a3ba','#eac736','#d94e5d'] } },
      series: [{ name: 'parallel', type: 'parallel', lineStyle: { width: 1, opacity: 0.6 }, data: seriesData, progressive: 1000, large: true }]
    };
    parallelChart.setOption(option, { notMerge: true });
    parallelChart.off('click');
    parallelChart.on('click', params => {
      try {
        const city = params && params.data && params.data.raw && (params.data.raw.city || params.data.raw.province);
        if (city) {
          const ev = new CustomEvent('provinceview-city-selected', { detail: { city } });
          window.dispatchEvent(ev);
        }
      } catch (e) {}
    });
  } catch (e) {
    try { parallelChart.setOption({ title: { text: '并行坐标渲染失败' } }); } catch (e) {}
  }
}
function renderAll() { renderMap(); renderRanking(); try { renderParallel(); } catch (e) {} }

function resetParallelSelection() {
  selectedPollutants.value = candidatePollutants.slice(0, 4);
  try { renderParallel(); } catch (e) {}
}

watch(selectedPollutants, () => { try { renderParallel(); } catch (e) {} });

function resetFilters() { fromDate.value = ''; toDate.value = ''; fromMonth.value = ''; toMonth.value = ''; fromYear.value = new Date().getFullYear(); toYear.value = new Date().getFullYear(); fromDateTime.value = ''; toDateTime.value = ''; selectedPollutant.value = (numericColumns.value && numericColumns.value.length) ? numericColumns.value[0] : ''; renderAll(); }

function applyFilters() { renderAll(); updateDiag('应用了过滤条件'); }

function toggleTable() { showTable.value = !showTable.value; updateDiag(showTable.value ? '表格显示' : '表格隐藏'); }

function exportCsv() {
  try {
    const rows = filtered.value;
    if (!rows || !rows.length) { updateDiag('没有数据可导出'); return; }
    const cols = (displayColumns.value && displayColumns.value.length) ? displayColumns.value : ((columns.value && columns.value.length) ? columns.value : Object.keys(rows[0] || {}));
    const csv = [cols.join(',')].concat(rows.map(r => cols.map(c => {
      const v = r[c] === undefined || r[c] === null ? '' : String(r[c]);
      return '"' + v.replace(/"/g,'""') + '"';
    }).join(',')) ).join('\n');
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a'); a.href = url; a.download = 'export.csv'; document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
    updateDiag('已导出 CSV（export.csv）');
  } catch (e) { updateDiag('导出失败：' + (e && e.message)); }
}

async function ensureScript(url) {
  return new Promise((resolve, reject) => {
    const exists = Array.from(document.getElementsByTagName('script')).some(s => s.src && s.src.indexOf(url) !== -1);
    if (exists) { resolve(); return; }
    const s = document.createElement('script'); s.src = url; s.onload = () => resolve(); s.onerror = (e) => reject(e); document.head.appendChild(s);
  });
}

onMounted(async () => {
  if (typeof echarts === 'undefined') {
    updateDiag('ECharts not found locally; loading from CDN...');
    try { await ensureScript('https://cdn.jsdelivr.net/npm/echarts@5.4.3/dist/echarts.min.js'); updateDiag('ECharts loaded from CDN'); } catch (e) { updateDiag('ECharts failed to load from CDN.'); }
  } else updateDiag('ECharts available');

  if (typeof Papa === 'undefined') {
    try { await ensureScript('https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js'); } catch (e) { /* ignore */ }
  }

  // default month selection (user expectation: default 2013-01 for month granularity)
  try { if (!fromMonth.value) fromMonth.value = '2013-01'; } catch (e) {}

  // try to load an optional city->centroid mapping to enrich data with lon/lat
  try {
    const centroids = await tryFetchJson('/resources/city_centroids.json');
    if (centroids && typeof centroids === 'object') {
      // convert to map by city name (and also support province|city keys)
      const cmap = new Map();
      for (const item of Array.isArray(centroids) ? centroids : Object.values(centroids)) {
        // expected item: { city: 'CityName', lon: 116.3, lat: 39.9 }
        if (item.city) cmap.set(item.city, item);
        if (item.key) cmap.set(item.key, item);
      }
      // store on window for debug and later enrichment
      window.__cityCentroids = cmap;
      updateDiag('Loaded city_centroids mapping (' + cmap.size + ' entries). Will attempt to enrich data with lat/lon when loading datasets.');
    }
  } catch (e) { /* ignore mapping load errors */ }

  await loadData();

  // if a centroid map was loaded, enrich raw rows after loading
  try {
    const cmap = window.__cityCentroids;
  if (cmap && raw.value && raw.value.length) {
      for (const r of raw.value) {
        if ((r.lon === undefined || r.lat === undefined || r.lon === '' || r.lat === '')) {
          const candidates = [];
          if (r.city) candidates.push(r.city);
          if (r.province && r.city) candidates.push(r.province + '|' + r.city);
          if (r.city && r.city.includes('|')) candidates.push(r.city.split('|').slice(-1)[0]);
          for (const c of candidates) {
            const hit = cmap.get(c);
            if (hit) { r.lon = hit.lon; r.lat = hit.lat; break; }
          }
        }
      }
      updateDiag('Enriched rows with centroids where available.');
      if (mapChart || rankChart) renderAll();
    }
  } catch (e) { /* ignore */ }

  try { const fi = document.getElementById('fileInput'); if (fi) fi.addEventListener('change', (e) => handleFiles(e.target.files)); } catch (e) {}
  window.addEventListener('dragover', (e) => { e.preventDefault(); });
  window.addEventListener('drop', (e) => { e.preventDefault(); if (e.dataTransfer && e.dataTransfer.files) handleFiles(e.dataTransfer.files); });
  // attempt auto-load for the current selection (useful if default date is set)
  try { autoLoadForSelection(); } catch (e) {}
  setInterval(() => { try { renderAll(); } catch (e) {} }, 800);
});

function clearSelection() { selectedProvince.value = null; updateDiag('已清除选择'); }
</script>

<style scoped>
.demo-root { background: #f6f8fa; padding-bottom: 28px; }
.demo-root header h1 { color: #111; margin: 8px 0; }
.demo-header { background: #000; padding: 14px 12px; border-radius: 6px; margin: 12px auto; max-width:1200px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }
.demo-header { position: relative; }
.demo-header h1 { color: #fff; margin: 0 0 8px 0; font-size: 1.25rem; }
.controls { display:flex; justify-content:space-between; align-items:center; gap:12px; max-width:1200px; margin:8px auto; padding:6px; position:sticky; top:8px; background:transparent; z-index:80; }
.controls-left { display:flex; align-items:center; gap:8px; }
.controls-right { display:flex; align-items:center; gap:8px; }
.selection-indicator { display:flex; align-items:center; gap:6px; }
.header-selection { position: absolute; right: 18px; top: 14px; background: transparent; }
.header-selection .sel-inner { display:flex; align-items:center; gap:8px }
.btn { background:#2b8aef; color:#fff; border:none; padding:6px 10px; border-radius:4px; cursor:pointer; }
.btn:hover { background:#196fd1 }
.uploader input { margin-left:6px }
.diag { padding:8px 12px;color:#a00;max-width:1100px;margin:8px auto;font-size:13px }
.diag.error { background:#3b0b0b; color:#fff; padding:10px; border-radius:4px }
.debug-box { max-width:1200px; margin:8px auto; background:#fff; padding:8px; border-radius:6px; box-shadow:0 1px 3px rgba(0,0,0,0.04); color: #000; }
.debug-box strong { color: #000; }
.sample-json { max-height:160px; overflow:auto; background:#f8f9fb; padding:8px; border-radius:4px; }
.controls label, .controls select, .controls input { color: #fff; }
.files strong { color: #111; }
.path-input { margin-left:8px; width:300px; padding:6px; border-radius:4px; border:1px solid #333; background:#000; color:#fff }
.controls select, .controls input[type="date"], .controls input[type="month"], .controls input[type="number"], .controls input[type="datetime-local"] { background:#000; color:#fff; border:1px solid #333; padding:6px 8px; border-radius:4px }
.grid { display:grid; grid-template-columns: 1fr; gap: 16px; max-width:1200px; margin:12px auto; align-items:start; }
.left { display:flex; flex-direction:column; gap:12px; }
.right { display:flex; flex-direction:column; gap:12px; }
.chart-canvas { width: 100%; height: 420px; background: #ffffff; border-radius: 6px; box-shadow: 0 2px 6px rgba(0,0,0,0.04); z-index:10 }
.chart.small .chart-canvas { height: 220px; }
.data-table { background: #ffffff; padding: 10px; border-radius:6px; box-shadow: 0 2px 6px rgba(0,0,0,0.04); }
table { width: 100%; border-collapse: collapse; }
th, td { border: 1px solid #eee; padding: 6px; text-align: left; color: #111; }
th { background: #f3f6f9; color: #111; }

@media (max-width: 900px) {
  .grid { grid-template-columns: 1fr; padding: 0 12px; }
  .controls { max-width: calc(100% - 24px); }
}
</style>
