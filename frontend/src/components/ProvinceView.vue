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
        <!-- Parallel view is shown separately below; no toggle button here -->
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
          <div id="pv-trend-source" style="font-size:0.85rem;color:#666;margin-top:6px;"></div>
          <div id="pv-trend-debug" style="font-size:0.8rem;color:#999;margin-top:4px;"></div>
          <div id="pv-trend-sample" style="font-size:0.8rem;color:#666;margin-top:4px;white-space:pre-wrap;word-break:break-all"></div>
        <div id="distChart" class="chart-canvas" v-show="rightChartType === 'trend' && granularity === 'day'"></div>
        
        <!-- city comparison (visible when not showing trend) -->
        <div id="cityChart" class="chart-canvas" v-show="rightChartType !== 'trend'" style="margin-top:12px;"></div>
      </section>

    </div>

    <!-- parallel coordinates row: placed outside pv-grid so it always occupies its own full-width line -->
    <section class="pv-parallel-row">
      <div class="pv-parallel-controls">
        <label style="margin-right:8px;color:#000">并行坐标显示污染物：</label>
        <label v-for="p in ['pm25','pm10','so2','no2','co','o3']" :key="p" style="margin-right:6px;color:#000"><input type="checkbox" :value="p" v-model="selectedPollutants" /> {{ p }}</label>
      </div>
      <div class="pv-parallel-area">
        <div id="parallelChart" class="chart-canvas pv-parallel-chart" style="height:360px;"></div>
        <div class="pv-parallel-table">
          <h4 style="margin:0 0 6px 0;">城市概览（按选中污染物平均值排序）</h4>
          <table style="width:100%;font-size:0.9rem;">
            <thead><tr><th>城市</th><th style="text-align:right">值</th></tr></thead>
            <tbody>
              <tr v-for="row in parallelSummary" :key="row.city">
                <td>{{ row.city }}</td>
                <td style="text-align:right">{{ row.val.toFixed(2) }}</td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>
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
let parallelChart = null;

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

// selected pollutants for parallel coordinates (default: all)
const selectedPollutants = ref(['pm25','pm10','so2','no2','co','o3']);

// pollutant to use when none provided by parent (defaults to pm25)
const pollutantUsed = computed(() => (props.pollutant && String(props.pollutant).trim()) ? String(props.pollutant).trim() : 'pm25');

// debug / info: which precomputed source (URL) was used
const precomputedSource = ref('');
const precomputedType = ref(''); // 'province'|'city-aggregated'|'none'
const precomputedKeyField = ref('');
const precomputedValField = ref('');

const parallelSummary = computed(() => {
  const sel = (selectedPollutants.value && selectedPollutants.value.length) ? selectedPollutants.value : ['pm25','pm10','so2','no2','co','o3'];
  const map = new Map();
  for (const r of rows.value) {
    const city = (r.city || r.cityname || r.city_name || r.province || 'unknown').toString();
    if (!map.has(city)) map.set(city, { sum: 0, n: 0 });
    const entry = map.get(city);
    // compute average across selected pollutants for this row
    let rowSum = 0; let rowCount = 0;
    for (const p of sel) {
      const v = Number(r[p]); if (!isNaN(v)) { rowSum += v; rowCount++; }
    }
    if (rowCount>0) { entry.sum += rowSum / rowCount; entry.n += 1; }
    map.set(city, entry);
  }
  const out = Array.from(map.entries()).map(([city, e]) => ({ city, val: e.n ? e.sum / e.n : 0 }));
  out.sort((a,b) => b.val - a.val);
  return out.slice(0,50);
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
  const tt = String(t).trim().toLowerCase();
  // quick detection: if server returned HTML (page) instead of raw CSV, try to extract CSV links
  if (tt.startsWith('<!doctype') || tt.startsWith('<html') || tt.indexOf('<html') !== -1) {
    console.debug('fetchAndParseCsv: received HTML instead of CSV for', url);
    // try to parse directory listing / page for CSV anchors and attempt those
    try {
      const links = parseDirListing(t, url).filter(u => u.toLowerCase().endsWith('.csv'));
      if (links && links.length) {
        console.debug('fetchAndParseCsv: found csv links in html, trying', links);
        for (const l of links) {
          const rows = await fetchAndParseCsv(l);
          if (rows && rows.length) return rows;
        }
      }
    } catch (e) { console.debug('fetchAndParseCsv: html parse failed', e); }
    return [];
  }
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

// Try loading a precomputed trend CSV for this province (monthly/daily).
async function tryLoadPrecomputedTrend() {
  try {
    const base = '/resources/trends/province/';
    // decide frequency: use monthly for year/month, daily for day
    const freq = (props.granularity === 'day') ? 'daily' : 'monthly';
    const sanitize = s => String(s || '').trim();
    const rawName = sanitize(props.province);
    // helper to normalize names: strip common suffixes and split pipe-separated
    const normalizeName = (n) => {
      if (!n) return '';
      let s = String(n).trim();
      // if contains pipe like '北京|北京', take last token
      if (s.indexOf('|') !== -1) s = s.split('|').slice(-1)[0];
      // remove common administrative suffixes
      s = s.replace(/省|市|自治区|自治州|自治县|特别行政区|回族自治州|壮族自治州|藏族|羌族自治州/g, '');
      return s.trim();
    };
    const p = normalizeName(rawName);
    const enc = encodeURIComponent(p);
    const candidates = [];
    // direct filename attempts (try several common patterns and encoded variants)
    const patterns = [
      `${p}_${freq}.csv`,
      `${p}_${p}_${freq}.csv`,
      `${p}.csv`,
      `${p}_${freq}.CSV`,
      `${enc}_${freq}.csv`,
      `${enc}_${enc}_${freq}.csv`,
      `${enc}.csv`
    ];
    for (const pat of patterns) candidates.push(base + pat);
    // also try variants using rawName (in case normalize changed it)
    if (rawName && rawName !== p) {
      const encRaw = encodeURIComponent(rawName);
      const rawPatterns = [`${rawName}_${freq}.csv`, `${rawName}.csv`, `${encRaw}_${freq}.csv`, `${encRaw}.csv`];
      for (const pat of rawPatterns) candidates.push(base + pat);
    }
    // if those fail, enumerate directory and find a file containing the province string
    try {
      const dirListHtml = await fetchText(base);
      if (dirListHtml) {
        const links = parseDirListing(dirListHtml, base).filter(u => u.toLowerCase().endsWith('.csv'));
        const matched = links.filter(p => p.indexOf(encodeURIComponent(props.province)) !== -1 || p.indexOf(props.province) !== -1 || p.toLowerCase().indexOf(props.province.toLowerCase()) !== -1);
        for (const m of matched) candidates.push(m);
        // if nothing matched by encoding, push all csvs as last resort
        if (!matched.length) candidates.push(...links);
      }
    } catch (e) {}
      console.debug('tryLoadPrecomputedTrend: candidates=', candidates);
      for (const c of candidates) {
        try {
          // try raw and URI-encoded URL (some servers require percent-encoding)
          let rows = await fetchAndParseCsv(c);
          if ((!rows || !rows.length) && typeof encodeURI === 'function') rows = await fetchAndParseCsv(encodeURI(c));
          // If we got a very small number of rows (e.g. 1), inspect raw text to detect whether server returned a preview/html or truncated content
          if (rows && rows.length && rows.length < 3) {
            try {
              const rawText = await fetchText(c);
              if (rawText) {
                const lines = rawText.split(/\r?\n/).filter(l => l.trim() !== '');
                // if rawText contains more CSV lines than our parsed rows, re-parse raw CSV and prefer that
                if (lines.length > rows.length) {
                  try {
                    const reparsed = parseCsv(rawText);
                    if (reparsed && reparsed.length) {
                      console.debug('tryLoadPrecomputedTrend: reparsed raw text and got', reparsed.length, 'rows from', c);
                      precomputedSource.value = c;
                      precomputedType.value = 'province';
                      return reparsed;
                    }
                  } catch (e) { console.debug('tryLoadPrecomputedTrend: reparsing failed', e); }
                }
              }
            } catch (e) { console.debug('tryLoadPrecomputedTrend: rawText check failed', e); }
          }
          if (rows && rows.length) {
            console.debug('tryLoadPrecomputedTrend: success from', c, 'rows=', rows.length);
            precomputedSource.value = c;
            precomputedType.value = 'province';
            return rows;
          }
        } catch (e) { console.debug('tryLoadPrecomputedTrend: fetch error', c, e); }
      }
      // also try to fetch the candidate filenames under city dir (sometimes province files are placed there)
      const cityBase = '/resources/trends/city/';
      for (const pat of patterns) {
        const c = cityBase + pat;
        try {
          let rows = await fetchAndParseCsv(c);
          if ((!rows || !rows.length) && typeof encodeURI === 'function') rows = await fetchAndParseCsv(encodeURI(c));
          if (rows && rows.length) { console.debug('tryLoadPrecomputedTrend: success from city', c, 'rows=', rows.length); precomputedSource.value = c; precomputedType.value = 'province'; return rows; }
        } catch (e) { /* ignore */ }
      }
      // If no province-level precomputed file found, try aggregating city-level trend files
      try {
        const cityBase = '/resources/trends/city/';
        const cityDirHtml = await fetchText(cityBase);
        if (cityDirHtml) {
          const links = parseDirListing(cityDirHtml, cityBase).filter(u => u.toLowerCase().endsWith('.csv'));
          // match filenames that contain the province substring (encoded or raw)
          const matched = links.filter(u => u.indexOf(enc) !== -1 || u.indexOf(p) !== -1 || u.toLowerCase().indexOf(p.toLowerCase()) !== -1);
          console.debug('tryLoadPrecomputedTrend: city-level candidates=', matched.length);
          if (matched.length) {
            // aggregate by date key
            const aggMap = new Map();
            const normalize = s => String(s || '').toLowerCase().replace(/[^a-z0-9]/g,'');
            for (const link of matched) {
              try {
                const crow = await fetchAndParseCsv(link);
                if (!crow || !crow.length) continue;
                // detect key and val field for this file
                const sample = crow[0] || {};
                const keyField = (sample.date && 'date') || (sample.time && 'time') || Object.keys(sample)[0];
                // find header matching pollutant normalized
                const headers = Object.keys(sample || {});
                const targetNorm = normalize(pollutantUsed.value || '');
                let valField = null;
                if (targetNorm) {
                  for (const h of headers) {
                    if (normalize(h) === targetNorm || normalize(h).indexOf(targetNorm) !== -1) { valField = h; break; }
                  }
                }
                // fallback: pick first numeric-like column by sampling
                if (!valField) {
                  for (const h of headers) {
                    if (h === keyField) continue;
                    const sampleVals = crow.slice(0, Math.min(10, crow.length)).map(r => r[h]);
                    const numericCount = sampleVals.filter(v => v !== undefined && v !== null && !isNaN(Number(String(v).trim()))).length;
                    if (numericCount >= Math.max(1, Math.floor(sampleVals.length * 0.6))) { valField = h; break; }
                  }
                }
                if (!valField) continue;
                for (const r of crow) {
                  const key = r[keyField] || r.time || r.date;
                  if (!key) continue;
                  const rawv = r[valField];
                  const v = rawv === undefined || rawv === null ? NaN : Number(String(rawv).trim());
                  if (isNaN(v)) continue;
                  if (!aggMap.has(key)) aggMap.set(key, { sum: v, n: 1 }); else { const cur = aggMap.get(key); cur.sum += v; cur.n += 1; aggMap.set(key, cur); }
                }
              } catch (e) { console.debug('tryLoadPrecomputedTrend: city file fetch/parse error', link, e); }
            }
            if (aggMap.size) {
              const out = Array.from(aggMap.entries()).map(([k, v]) => ({ key: k, val: v.n ? v.sum / v.n : 0 }));
              out.sort((a,b) => new Date(a.key) - new Date(b.key));
              console.debug('tryLoadPrecomputedTrend: aggregated city->province rows=', out.length);
              precomputedSource.value = matched.slice(0,8).join(',');
              precomputedType.value = 'city-aggregated';
              return out; // note: return array of {key,val} (not raw rows)
            }
          }
        }
      } catch (e) { console.debug('tryLoadPrecomputedTrend: city-aggregation error', e); }
  } catch (e) {}
  return null;
}

async function renderTrend() {
  if (!trendChart) return;
  try {
    // prefer precomputed trends (fast) — fall back to aggregating raw rows
    const pre = await tryLoadPrecomputedTrend();
    let arr = [];
    if (pre && pre.length) {
      console.debug('renderTrend: using precomputed rows length=', pre.length, 'sample=', pre[0]);
        // interpret precomputed CSV rows: try to detect date/key and pollutant column
        const keyField = (pre[0].date && 'date') || (pre[0].time && 'time') || Object.keys(pre[0])[0];
        // choose valField: prefer explicit pollutant prop; otherwise pick a numeric column by sampling
        let valField = null;
        const headers = Object.keys(pre[0] || {});
        if (pollutantUsed.value) {
          valField = headers.find(k => k.toLowerCase() === String(pollutantUsed.value).toLowerCase());
        }
        if (!valField) {
          // sample up to first 10 rows and pick first column (excluding keyField) that is numeric in majority
          const sampleRows = pre.slice(0, Math.min(10, pre.length));
          const normalize = s => String(s || '').trim();
          for (const h of headers) {
            if (h === keyField) continue;
            let numericCount = 0;
            for (const r of sampleRows) {
              const v = normalize(r[h]);
              if (v !== '' && !isNaN(Number(v))) numericCount++;
            }
            if (numericCount >= Math.max(1, Math.floor(sampleRows.length * 0.6))) { valField = h; break; }
          }
        }
        // if still not found, prefer pm25 if present
        if (!valField) {
          const found = headers.find(h => String(h).toLowerCase() === 'pm25');
          if (found) valField = found;
        }
        // fallback: pick second column if still not found
        if (!valField) valField = headers.find(k => k !== keyField) || headers[1] || headers[0];
        // record chosen fields for debugging
        precomputedKeyField.value = keyField;
        precomputedValField.value = valField || '';
        arr = pre.map(r => ({ key: r[keyField] || r.time || r.date || '', val: Number(r[valField]) || 0 }));
      // sort by date if possible
      arr.sort((a,b) => new Date(a.key) - new Date(b.key));
    } else {
      console.debug('renderTrend: no precomputed found, falling back to aggregateTrend; rows.value length=', rows.value && rows.value.length);
        arr = aggregateTrend(rows.value, pollutantUsed.value, props.granularity || 'day');
    }

    if (!arr || !arr.length) {
      trendChart.setOption({ title: { text: `${props.province} — 无数据`, left:'center' } });
      return;
    }
    // defensive numeric conversion and diagnostics
    const x = arr.map(d => d.key);
    const y = arr.map(d => { const n = Number(d.val); return isNaN(n) ? 0 : Number(n.toFixed ? Number(n.toFixed(2)) : n); });
    // diagnostics: expose summary when unexpected
    try {
      const sampleEl = document.getElementById('pv-trend-sample');
      if (sampleEl) {
        const keys = Array.from(new Set(x));
        const min = y.length ? Math.min(...y) : 0;
        const max = y.length ? Math.max(...y) : 0;
        sampleEl.innerText = `rows=${arr.length} uniqueKeys=${keys.length} min=${min.toFixed(2)} max=${max.toFixed(2)} sample=${JSON.stringify(arr.slice(0,6))}`;
      }
    } catch (e) { console.debug('pv-trend-sample write failed', e); }
    const option = {
      title: { text: `${props.province} 趋势`, left: 'center' },
      tooltip: { trigger: 'axis', formatter: params => `${params[0].axisValue}<br/>${props.pollutant || 'value'}: ${params[0].data}` },
      toolbox: { feature: { saveAsImage: {} } },
      xAxis: { type: 'category', data: x, boundaryGap: false, axisLabel: { rotate: 0 } },
      yAxis: { type: 'value' },
      series: [{ type: 'line', data: y, smooth: true, symbol: 'circle', symbolSize: 6, areaStyle: { color: { type: 'linear', x:0, y:0, x2:0, y2:1, colorStops:[{offset:0,color:'rgba(43,138,239,0.4)'},{offset:1,color:'rgba(43,138,239,0.05)'}]} } }]
    };
    trendChart.setOption(option, { notMerge: true });
    // show source note when available
    try {
      const noteEl = document.getElementById('pv-trend-source');
      if (noteEl) noteEl.textContent = precomputedSource.value ? `数据来源: ${precomputedType.value} — ${precomputedSource.value}` : '';
      // also show which fields were used
      const debugEl = document.getElementById('pv-trend-debug');
      if (debugEl) debugEl.innerText = precomputedKeyField.value ? `key: ${precomputedKeyField.value}  value: ${precomputedValField.value}` : '';
    } catch (e) {}
  } catch (e) {
    trendChart.setOption({ title: { text: `${props.province} 趋势 — 加载失败`, left:'center' } });
  }
}

function renderDist() {
  if (!distChart) return;
  // 构建跨行污染物值的直方图
  const vals = rows.value.map(r => Number(r[pollutantUsed.value]) ).filter(v => !isNaN(v));
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
  const arr = aggregateCity(rows.value, pollutantUsed.value);
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

function renderParallel() {
  try {
    if (!parallelChart) return;
    const sample = rows.value && rows.value.length ? rows.value[0] : {};
    const candidatePollutants = ['pm25','pm10','so2','no2','co','o3'];
    const pollutants = candidatePollutants.filter(p => Object.prototype.hasOwnProperty.call(sample, p));
    if (!pollutants.length) {
      parallelChart.setOption({ title: { text: '缺少可比较的污染物字段', left:'center' } });
      return;
    }
    // compute min/max per pollutant
    const stats = {};
    for (const p of pollutants) {
      const vals = rows.value.map(r => Number(r[p])).filter(v => !isNaN(v));
      stats[p] = { min: vals.length ? Math.min(...vals) : 0, max: vals.length ? Math.max(...vals) : 1 };
      if (stats[p].min === stats[p].max) stats[p].max = stats[p].min + 1;
    }
    // normalized data for parallel axes [p1_norm, p2_norm, ... , cityName]
    const seriesData = rows.value.map(r => {
      const vals = pollutants.map((p) => {
        const v = Number(r[p]);
        if (isNaN(v)) return 0;
        const s = stats[p];
        return (v - s.min) / (s.max - s.min);
      });
      return { value: vals.concat([r.city || r.province || '']), raw: r };
    }).slice(0, 2000); // cap for performance

    const parallelAxis = pollutants.map((p, i) => ({ dim: i, name: p, min: 0, max: 1, axisLabel: { color: '#000', formatter: v => { const real = (v * (stats[p].max - stats[p].min) + stats[p].min); return Number(real).toFixed(1); } }, nameTextStyle: { color: '#000' } }));

    const option = {
      title: { text: `${props.province} 并行坐标（城市分布）`, left: 'center', textStyle: { color: '#000' } },
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
          // emit selection event to parent if defined
          const ev = new CustomEvent('provinceview-city-selected', { detail: { city } });
          window.dispatchEvent(ev);
        }
      } catch (e) {}
    });
  } catch (e) {
    try { parallelChart.setOption({ title: { text: '并行坐标渲染失败' } }); } catch (e) {}
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
  try { if (document.getElementById('parallelChart')) parallelChart = echarts.init(document.getElementById('parallelChart')); } catch (e) {}
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
  // always render parallel coordinates row by default
  try { renderParallel(); } catch (e) {}
  // resize handler so charts adapt when container width changes
  try {
    window.__provinceViewResize = () => {
      try { radarChart && radarChart.resize(); } catch (e) {}
      try { trendChart && trendChart.resize(); } catch (e) {}
      try { distChart && distChart.resize(); } catch (e) {}
      try { cityChart && cityChart.resize(); } catch (e) {}
      try { parallelChart && parallelChart.resize(); } catch (e) {}
    };
    window.addEventListener('resize', window.__provinceViewResize);
  } catch (e) {}
});

onBeforeUnmount(() => {
  try { if (trendChart && trendChart.dispose) { trendChart.dispose(); trendChart = null; } } catch (e) {}
  try { if (distChart && distChart.dispose) { distChart.dispose(); distChart = null; } } catch (e) {}
  try { if (cityChart && cityChart.dispose) { cityChart.dispose(); cityChart = null; } } catch (e) {}
  try { if (radarChart && radarChart.dispose) { radarChart.dispose(); radarChart = null; } } catch (e) {}
  try { if (parallelChart && parallelChart.dispose) { parallelChart.dispose(); parallelChart = null; } } catch (e) {}
  try { if (window.__provinceViewResize) { window.removeEventListener('resize', window.__provinceViewResize); delete window.__provinceViewResize; } } catch (e) {}
});

watch([() => props.data, () => props.pollutant, () => props.granularity, () => props.fromMonth, () => props.fromYear], () => {
  try {
    if (props.granularity === 'year' && props.fromYear) {
      loadYearData(props.fromYear).then(() => { try { if (rightChartType.value === 'trend') { renderTrend(); } else if (rightChartType.value === 'parallel') { renderParallel(); } else { renderCity(); } renderRadar(); } catch (e) {} });
    } else if (props.granularity === 'month' && props.fromMonth) {
      // reload monthly aggregated file or monthly directory then render
      loadMonthData(props.fromMonth).then(() => { try { if (rightChartType.value === 'trend') { if (props.granularity === 'day') renderDist(); else renderTrend(); } else if (rightChartType.value === 'parallel') { renderParallel(); } else { renderCity(); } renderRadar(); } catch (e) {} });
    } else {
      if (props.granularity === 'day') renderDist(); else renderTrend();
      renderRadar();
      // always update parallel as it is shown by default
      try { renderParallel(); } catch (e) {}
      if (rightChartType.value === 'trend') { if (props.granularity === 'day') renderDist(); else renderTrend(); } else { renderCity(); }
    }
  } catch (e) {}
}, { deep: true });

watch(() => rightChartType.value, () => {
  try {
    if (rightChartType.value === 'trend') {
      if (props.granularity === 'day') renderDist(); else renderTrend();
    } else if (rightChartType.value === 'parallel') {
      renderParallel();
    } else {
      renderCity();
    }
  } catch (e) {}
});

// when selected pollutants change, re-render the parallel chart
watch(() => selectedPollutants.value, () => { try { renderParallel(); } catch (e) {} }, { deep: true });
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

.pv-parallel-row { width: 100%; display: flex; flex-direction: column; gap: 8px; margin-top: 12px; }
.pv-parallel-area { background: #fff; padding: 8px; border-radius: 6px; box-shadow: 0 1px 3px rgba(0,0,0,0.04); display:flex; gap:12px; align-items:flex-start }
.pv-parallel-chart { flex: 1 1 auto; min-width: 0 }
.pv-parallel-table { width: 320px; max-height: 360px; overflow:auto; background:#fff; padding:8px; border-radius:6px }

@media (max-width: 900px) {
  .pv-grid { flex-direction: column; }
  .chart-canvas { height: 360px; }
}
</style>
