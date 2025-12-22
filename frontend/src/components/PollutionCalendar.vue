<template>
  <div class="pollution-calendar">
    <!-- 控制栏 -->
    <div class="controls">
      <div class="control-group">
        <label>城市</label>
        <div class="select-wrapper">
          <select v-model="selectedCity" @change="loadCityData">
            <option v-for="city in cities" :key="city" :value="city">{{ formatCityName(city) }}</option>
          </select>
        </div>
      </div>
      <div class="metric-badge">
        <span class="badge-icon">📊</span>
        <span>AQI 空气质量指数</span>
      </div>
    </div>

    <!-- 加载提示 -->
    <div v-if="loading" class="loading">
      <div class="spinner"></div>
      <span>加载中...</span>
    </div>
    <div v-if="error" class="error">{{ error }}</div>

    <!-- 日历图表 -->
    <div ref="calendarChart" class="calendar-chart"></div>

    <!-- 图例 -->
    <div class="legend">
      <div class="legend-item" v-for="level in aqiLevels" :key="level.label">
        <span class="legend-color" :style="{ backgroundColor: level.color }"></span>
        <span class="legend-label">{{ level.label }} ({{ level.range }})</span>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted, watch, nextTick } from 'vue';
import * as echarts from 'echarts';

// 响应式数据
const selectedCity = ref('');
const selectedMetric = ref('aqi');
const cities = ref([]);
const calendarData = ref([]);
const calendarChart = ref(null);
const loading = ref(false);
const error = ref('');
let chartInstance = null;

// AQI等级配置
const aqiLevels = [
  { label: '优', range: '0-50', color: '#00E400', min: 0, max: 50 },
  { label: '良', range: '51-100', color: '#FFFF00', min: 51, max: 100 },
  { label: '轻度污染', range: '101-150', color: '#FF7E00', min: 101, max: 150 },
  { label: '中度污染', range: '151-200', color: '#FF0000', min: 151, max: 200 },
  { label: '重度污染', range: '201-300', color: '#99004C', min: 201, max: 300 },
  { label: '严重污染', range: '>300', color: '#7E0023', min: 301, max: 500 },
];

// 格式化城市名（处理如 北京_北京 这样的名称）
function formatCityName(cityFileName) {
  if (cityFileName.includes('_')) {
    const parts = cityFileName.split('_');
    return parts[parts.length - 1]; // 返回最后一部分
  }
  return cityFileName;
}

// 加载城市列表
async function loadCities() {
  try {
    loading.value = true;
    // 从索引文件加载城市列表
    const response = await fetch('/resources/output/calendar/2013/_cities.json');
    if (response.ok) {
      const list = await response.json();
      cities.value = list;
      // 默认选择北京或第一个城市
      const beijing = list.find(c => c.includes('北京'));
      selectedCity.value = beijing || list[0];
    } else {
      error.value = '无法加载城市列表';
    }
  } catch (e) {
    console.error('加载城市列表失败:', e);
    error.value = '加载城市列表失败: ' + e.message;
  } finally {
    loading.value = false;
  }
}

// 加载城市数据
async function loadCityData() {
  if (!selectedCity.value) return;
  
  try {
    loading.value = true;
    error.value = '';
    
    const encodedCity = encodeURIComponent(selectedCity.value);
    const url = `/resources/output/calendar/2013/${encodedCity}.json`;
    console.log('Fetching:', url);
    
    const response = await fetch(url);
    if (response.ok) {
      const json = await response.json();
      calendarData.value = json.data || [];
      console.log('Loaded', calendarData.value.length, 'days');
      await nextTick();
      renderChart();
    } else {
      error.value = `加载数据失败: ${response.status}`;
      calendarData.value = [];
    }
  } catch (e) {
    console.error('加载数据异常:', e);
    error.value = '加载数据异常: ' + e.message;
    calendarData.value = [];
  } finally {
    loading.value = false;
  }
}

// 渲染图表
function renderChart() {
  if (!calendarChart.value || calendarData.value.length === 0) return;

  if (!chartInstance) {
    chartInstance = echarts.init(calendarChart.value);
  }

  // 转换数据格式为 ECharts 需要的格式
  const chartData = calendarData.value.map(item => {
    const [date, aqi, level, primary, isWeekend, isHoliday] = item;
    return {
      value: [date, aqi || 0],
      aqi: aqi,
      level: level,
      primary: primary,
      isWeekend: isWeekend,
      isHoliday: isHoliday
    };
  }).filter(item => item.aqi !== null);

  const displayName = formatCityName(selectedCity.value);

  const option = {
    backgroundColor: 'transparent',
    title: {
      text: `${displayName} 2013年空气质量日历`,
      left: 'center',
      textStyle: { 
        fontSize: 18, 
        fontWeight: 'bold',
        color: '#e2e8f0',
        fontFamily: 'Inter, -apple-system, sans-serif'
      }
    },
    tooltip: {
      backgroundColor: 'rgba(15, 23, 42, 0.95)',
      borderColor: 'rgba(255, 255, 255, 0.1)',
      borderWidth: 1,
      textStyle: { color: '#e2e8f0' },
      formatter: function(params) {
        const data = params.data;
        if (!data || data.aqi === null || data.aqi === 0) {
          return `${params.value[0]}<br/>数据缺失`;
        }
        let html = `<strong>${params.value[0]}</strong><br/>`;
        html += `AQI: ${data.aqi}<br/>`;
        html += `等级: ${data.level}<br/>`;
        html += `首要污染物: ${data.primary || '-'}<br/>`;
        if (data.isHoliday) {
          html += `<span style="color:#FF6B6B;">🎉 法定节假日</span><br/>`;
        } else if (data.isWeekend) {
          html += `<span style="color:#6BB3FF;">📅 周末</span><br/>`;
        }
        return html;
      }
    },
    visualMap: {
      min: 0,
      max: 500,
      calculable: true,
      orient: 'horizontal',
      left: 'center',
      bottom: 20,
      textStyle: { color: '#94a3b8' },
      pieces: [
        { min: 0, max: 50, label: '优', color: '#00E400' },
        { min: 51, max: 100, label: '良', color: '#FFFF00' },
        { min: 101, max: 150, label: '轻度', color: '#FF7E00' },
        { min: 151, max: 200, label: '中度', color: '#FF0000' },
        { min: 201, max: 300, label: '重度', color: '#99004C' },
        { min: 301, max: 500, label: '严重', color: '#7E0023' }
      ],
      type: 'piecewise'
    },
    calendar: {
      top: 80,
      left: 60,
      right: 40,
      cellSize: ['auto', 20],
      range: '2013',
      itemStyle: {
        borderWidth: 1,
        borderColor: 'rgba(30, 41, 59, 0.8)'
      },
      yearLabel: { 
        show: true, 
        margin: 40,
        color: '#94a3b8',
        fontFamily: 'Inter, sans-serif'
      },
      dayLabel: {
        firstDay: 1,
        nameMap: ['日', '一', '二', '三', '四', '五', '六'],
        color: '#94a3b8'
      },
      monthLabel: {
        nameMap: 'cn',
        color: '#e2e8f0'
      },
      splitLine: {
        show: true,
        lineStyle: {
          color: 'rgba(148, 163, 184, 0.3)',
          width: 1,
          type: 'solid'
        }
      }
    },
    series: [
      {
        type: 'heatmap',
        coordinateSystem: 'calendar',
        data: chartData.map(item => {
          return {
            value: item.value,
            aqi: item.aqi,
            level: item.level,
            primary: item.primary,
            isWeekend: item.isWeekend,
            isHoliday: item.isHoliday,
            itemStyle: {
              borderWidth: (item.isWeekend || item.isHoliday) ? 2 : 0.5,
              borderColor: item.isHoliday ? '#FF6B6B' : (item.isWeekend ? '#6BB3FF' : 'rgba(30, 41, 59, 0.5)'),
              shadowBlur: (item.isWeekend || item.isHoliday) ? 8 : 0,
              shadowColor: item.isHoliday ? 'rgba(255, 107, 107, 0.4)' : (item.isWeekend ? 'rgba(107, 179, 255, 0.4)' : 'transparent')
            }
          };
        })
      }
    ]
  };

  chartInstance.setOption(option, true);
}

// 监听指标变化
watch(selectedMetric, () => {
  renderChart();
});

// 窗口大小变化时重绘
function handleResize() {
  if (chartInstance) {
    chartInstance.resize();
  }
}

onMounted(async () => {
  await loadCities();
  if (selectedCity.value) {
    await loadCityData();
  }
  window.addEventListener('resize', handleResize);
});
</script>

<style scoped>
.pollution-calendar {
  padding: 28px;
  background: rgba(255, 255, 255, 0.03);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 16px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
}

.controls {
  display: flex;
  gap: 24px;
  margin-bottom: 24px;
  padding: 20px;
  background: rgba(30, 41, 59, 0.5);
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.05);
}

.control-group {
  display: flex;
  align-items: center;
  gap: 12px;
}

.control-group label {
  font-weight: 600;
  color: #94a3b8;
  font-size: 0.9rem;
  letter-spacing: 0.02em;
}

.select-wrapper {
  position: relative;
}

.control-group select {
  padding: 10px 40px 10px 16px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 12px;
  font-size: 14px;
  min-width: 200px;
  background: rgba(15, 23, 42, 0.8);
  color: #e2e8f0;
  appearance: none;
  cursor: pointer;
  transition: all 0.2s ease;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%2394a3b8' d='M2 4l4 4 4-4'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 14px center;
}

.control-group select:hover {
  border-color: rgba(255, 255, 255, 0.2);
  background-color: rgba(30, 41, 59, 0.9);
}

.control-group select:focus {
  outline: none;
  border-color: #4ade80;
  box-shadow: 0 0 0 3px rgba(74, 222, 128, 0.15);
}

.control-group select option {
  background: #1e293b;
  color: #e2e8f0;
  padding: 10px;
}

.metric-badge {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  background: rgba(74, 222, 128, 0.1);
  border: 1px solid rgba(74, 222, 128, 0.2);
  border-radius: 12px;
  color: #4ade80;
  font-size: 0.9rem;
  font-weight: 500;
}

.badge-icon {
  font-size: 1rem;
}

.loading {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  padding: 40px;
  color: #94a3b8;
}

.spinner {
  width: 40px;
  height: 40px;
  border: 3px solid rgba(255, 255, 255, 0.1);
  border-top-color: #4ade80;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.error {
  text-align: center;
  padding: 20px;
  color: #f87171;
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid rgba(239, 68, 68, 0.2);
  border-radius: 12px;
  margin-bottom: 15px;
}

.calendar-chart {
  width: 100%;
  height: 320px;
}

.legend {
  display: flex;
  justify-content: center;
  gap: 20px;
  margin-top: 24px;
  flex-wrap: wrap;
  padding: 16px;
  background: rgba(30, 41, 59, 0.3);
  border-radius: 12px;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 8px;
}

.legend-color {
  width: 18px;
  height: 18px;
  border-radius: 6px;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
}

.legend-label {
  font-size: 0.8rem;
  color: #94a3b8;
  font-weight: 500;
}
</style>
