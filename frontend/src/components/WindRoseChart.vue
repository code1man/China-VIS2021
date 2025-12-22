<script setup>
import { ref, onMounted, watch, computed } from 'vue';
import * as echarts from 'echarts';

// ==================== 常量定义 ====================

// 16个风向方位
const DIRECTIONS = [
  'N', 'NNE', 'NE', 'ENE', 
  'E', 'ESE', 'SE', 'SSE',
  'S', 'SSW', 'SW', 'WSW', 
  'W', 'WNW', 'NW', 'NNW'
];

// 中文风向名称
const DIRECTION_NAMES = {
  'N': '北', 'NNE': '北北东', 'NE': '东北', 'ENE': '东北东',
  'E': '东', 'ESE': '东南东', 'SE': '东南', 'SSE': '南南东',
  'S': '南', 'SSW': '南南西', 'SW': '西南', 'WSW': '西南西',
  'W': '西', 'WNW': '西北西', 'NW': '西北', 'NNW': '北北西'
};

// 季节选项
const SEASON_OPTIONS = [
  { value: 'all', label: '全年' },
  { value: 'heating', label: '采暖季' },
  { value: 'nonHeating', label: '非采暖季' }
];

// ==================== 响应式数据 ====================

const chartRef = ref(null);
let chartInstance = null;

const cities = ref([]);
const selectedCity = ref('北京|北京');
const selectedSeason = ref('all');
const windRoseData = ref(null);
const isLoading = ref(false);
const errorMessage = ref('');

// ==================== 计算属性 ====================

const currentData = computed(() => {
  if (!windRoseData.value) return [];
  return windRoseData.value[selectedSeason.value] || [];
});

// 计算滑块位置
const sliderPosition = computed(() => {
  const index = SEASON_OPTIONS.findIndex(o => o.value === selectedSeason.value);
  return index * 100;
});

// ==================== 方法 ====================

// 格式化城市名称（处理 北京|北京 或 北京_北京 格式）
function formatCityName(cityName) {
  if (!cityName) return '';
  // 处理 | 分隔符
  if (cityName.includes('|')) {
    const parts = cityName.split('|');
    return parts[parts.length - 1];
  }
  // 处理 _ 分隔符
  if (cityName.includes('_')) {
    const parts = cityName.split('_');
    return parts[parts.length - 1];
  }
  return cityName;
}

// 加载城市列表
async function loadCities() {
  try {
    // 从 _cities.json 加载城市列表
    const response = await fetch('/resources/output/calendar/2013/_cities.json');
    if (response.ok) {
      const data = await response.json();
      // _cities.json 是一个简单数组
      cities.value = Array.isArray(data) ? data : (data.cities || []);
      if (cities.value.length > 0 && !cities.value.includes(selectedCity.value)) {
        // 查找北京或使用第一个城市
        const beijing = cities.value.find(c => c.includes('北京'));
        selectedCity.value = beijing || cities.value[0];
      }
    }
  } catch (error) {
    console.warn('无法加载城市列表，使用默认城市');
    cities.value = ['北京_北京', '上海_上海', '广州市', '深圳市'];
  }
}

// 加载风玫瑰图数据
async function loadWindRoseData() {
  if (!selectedCity.value) return;
  
  isLoading.value = true;
  errorMessage.value = '';
  
  try {
    const safeName = selectedCity.value.replace(/\|/g, '_').replace(/\//g, '_');
    const response = await fetch(`/resources/output/wind_rose/2013/${encodeURIComponent(safeName)}.json`);
    
    if (!response.ok) {
      throw new Error(`无法加载 ${selectedCity.value} 的数据`);
    }
    
    windRoseData.value = await response.json();
    updateChart();
  } catch (error) {
    console.error('加载数据失败:', error);
    errorMessage.value = error.message;
    windRoseData.value = null;
  } finally {
    isLoading.value = false;
  }
}

// 初始化图表
function initChart() {
  if (!chartRef.value) return;
  
  chartInstance = echarts.init(chartRef.value);
  updateChart();
  
  window.addEventListener('resize', () => {
    chartInstance?.resize();
  });
}

// 更新图表
function updateChart() {
  if (!chartInstance || !currentData.value.length) {
    if (chartInstance) {
      chartInstance.setOption({
        backgroundColor: 'transparent',
        title: {
          text: '暂无数据',
          left: 'center',
          top: 'center',
          textStyle: { color: '#94a3b8', fontSize: 16 }
        }
      });
    }
    return;
  }
  
  const data = currentData.value;
  
  // 准备数据
  const freqData = data.map(d => d.freq);
  const colorData = data.map(d => d.color);
  const maxFreq = Math.max(...freqData) * 1.2 || 20;
  
  const option = {
    backgroundColor: 'transparent',
    title: {
      text: `${formatCityName(windRoseData.value?.city || selectedCity.value)} 风向玫瑰图`,
      subtext: `${SEASON_OPTIONS.find(o => o.value === selectedSeason.value)?.label || '全年'}`,
      left: 'center',
      top: 10,
      textStyle: { 
        fontSize: 18, 
        fontWeight: 'bold',
        color: '#e2e8f0',
        fontFamily: 'Inter, -apple-system, sans-serif'
      },
      subtextStyle: {
        color: '#94a3b8',
        fontSize: 13
      }
    },
    tooltip: {
      trigger: 'item',
      backgroundColor: 'rgba(15, 23, 42, 0.95)',
      borderColor: 'rgba(255, 255, 255, 0.1)',
      borderWidth: 1,
      textStyle: { color: '#e2e8f0' },
      formatter: function(params) {
        const idx = params.dataIndex;
        const d = data[idx];
        const dirName = DIRECTION_NAMES[d.dir] || d.dir;
        return `
          <div style="font-weight:bold;margin-bottom:4px">${dirName} (${d.dir})</div>
          <div>风频: <b>${d.freq.toFixed(1)}%</b></div>
          <div>PM2.5: <b>${d.value.toFixed(1)} μg/m³</b></div>
          <div style="color:${d.color}">空气质量: ${d.level}</div>
        `;
      }
    },
    legend: {
      data: ['风频'],
      bottom: 10,
      textStyle: { color: '#94a3b8' }
    },
    polar: {
      radius: ['15%', '70%']
    },
    angleAxis: {
      type: 'category',
      data: DIRECTIONS,
      boundaryGap: true,
      startAngle: 90,
      axisLine: { show: true, lineStyle: { color: 'rgba(148, 163, 184, 0.3)' } },
      axisTick: { show: false },
      axisLabel: {
        fontSize: 12,
        color: '#94a3b8',
        formatter: function(value) {
          return DIRECTION_NAMES[value] || value;
        }
      },
      splitLine: { 
        show: true, 
        lineStyle: { 
          color: 'rgba(148, 163, 184, 0.15)',
          type: 'dashed'
        } 
      }
    },
    radiusAxis: {
      min: 0,
      max: maxFreq,
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: {
        fontSize: 10,
        color: '#64748b',
        formatter: '{value}%'
      },
      splitLine: { 
        show: true, 
        lineStyle: { 
          color: 'rgba(148, 163, 184, 0.12)', 
          type: 'dashed' 
        } 
      }
    },
    series: [
      {
        name: '风频',
        type: 'bar',
        coordinateSystem: 'polar',
        data: freqData.map((val, idx) => ({
          value: val,
          itemStyle: {
            color: colorData[idx],
            borderColor: 'rgba(255, 255, 255, 0.2)',
            borderWidth: 1
          }
        })),
        barWidth: '80%',
        emphasis: {
          itemStyle: {
            shadowBlur: 15,
            shadowColor: 'rgba(0, 0, 0, 0.5)'
          }
        }
      }
    ],
    // 添加图例说明
    graphic: [
      {
        type: 'group',
        right: 20,
        top: 60,
        children: [
          { type: 'text', style: { text: 'AQI等级', fontSize: 12, fontWeight: 'bold', fill: '#e2e8f0' }, top: 0 },
          { type: 'rect', shape: { width: 16, height: 16, r: 3 }, style: { fill: '#00E400' }, top: 20, left: 0 },
          { type: 'text', style: { text: '优', fontSize: 11, fill: '#94a3b8' }, top: 22, left: 22 },
          { type: 'rect', shape: { width: 16, height: 16, r: 3 }, style: { fill: '#FFFF00' }, top: 40, left: 0 },
          { type: 'text', style: { text: '良', fontSize: 11, fill: '#94a3b8' }, top: 42, left: 22 },
          { type: 'rect', shape: { width: 16, height: 16, r: 3 }, style: { fill: '#FF7E00' }, top: 60, left: 0 },
          { type: 'text', style: { text: '轻度', fontSize: 11, fill: '#94a3b8' }, top: 62, left: 22 },
          { type: 'rect', shape: { width: 16, height: 16, r: 3 }, style: { fill: '#FF0000' }, top: 80, left: 0 },
          { type: 'text', style: { text: '中度', fontSize: 11, fill: '#94a3b8' }, top: 82, left: 22 },
          { type: 'rect', shape: { width: 16, height: 16, r: 3 }, style: { fill: '#99004C' }, top: 100, left: 0 },
          { type: 'text', style: { text: '重度', fontSize: 11, fill: '#94a3b8' }, top: 102, left: 22 },
          { type: 'rect', shape: { width: 16, height: 16, r: 3 }, style: { fill: '#7E0023' }, top: 120, left: 0 },
          { type: 'text', style: { text: '严重', fontSize: 11, fill: '#94a3b8' }, top: 122, left: 22 }
        ]
      }
    ]
  };
  
  chartInstance.setOption(option, true);
}

// ==================== 生命周期 ====================

onMounted(async () => {
  await loadCities();
  await loadWindRoseData();
  initChart();
});

// 监听城市和季节变化
watch([selectedCity], () => {
  loadWindRoseData();
});

watch([selectedSeason], () => {
  updateChart();
});
</script>

<template>
  <div class="wind-rose-container">
    <!-- 控制面板 -->
    <div class="control-panel">
      <div class="control-group">
        <label>选择城市</label>
        <div class="select-wrapper">
          <select v-model="selectedCity" class="city-selector">
            <option v-for="city in cities" :key="city" :value="city">
              {{ city.replace('|', ' / ') }}
            </option>
          </select>
        </div>
      </div>
      
      <div class="control-group">
        <label>季节</label>
        <!-- Segmented Control -->
        <div class="segmented-control">
          <div class="slider" :style="{ transform: `translateX(${sliderPosition}%)` }"></div>
          <button 
            v-for="option in SEASON_OPTIONS" 
            :key="option.value"
            :class="{ active: selectedSeason === option.value }"
            @click="selectedSeason = option.value"
          >
            {{ option.label }}
          </button>
        </div>
      </div>
    </div>
    
    <!-- 图表区域 -->
    <div class="chart-wrapper">
      <div v-if="isLoading" class="loading-overlay">
        <div class="spinner"></div>
        <span>加载中...</span>
      </div>
      
      <div v-if="errorMessage" class="error-message">
        {{ errorMessage }}
      </div>
      
      <div ref="chartRef" class="chart"></div>
    </div>
    
    <!-- 说明区域 -->
    <div class="info-panel">
      <h4>📊 图表说明</h4>
      <ul>
        <li><strong>半径长度</strong>：代表该风向出现的频率（%）</li>
        <li><strong>颜色</strong>：代表该风向下的平均PM2.5浓度对应的AQI等级</li>
        <li><strong>采暖季</strong>：1、2、11、12月</li>
        <li><strong>非采暖季</strong>：3-10月</li>
      </ul>
    </div>
  </div>
</template>

<style scoped>
.wind-rose-container {
  padding: 28px;
  background: rgba(255, 255, 255, 0.03);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  border: 1px solid rgba(255, 255, 255, 0.08);
  border-radius: 16px;
  box-shadow: 0 10px 30px rgba(0, 0, 0, 0.3);
}

.control-panel {
  display: flex;
  flex-wrap: wrap;
  gap: 32px;
  margin-bottom: 24px;
  padding: 20px;
  background: rgba(30, 41, 59, 0.5);
  border-radius: 12px;
  border: 1px solid rgba(255, 255, 255, 0.05);
}

.control-group {
  display: flex;
  align-items: center;
  gap: 16px;
}

.control-group label {
  font-weight: 600;
  color: #94a3b8;
  font-size: 0.9rem;
  letter-spacing: 0.02em;
  white-space: nowrap;
}

.select-wrapper {
  position: relative;
}

.city-selector {
  padding: 10px 40px 10px 16px;
  font-size: 14px;
  border: 1px solid rgba(255, 255, 255, 0.1);
  border-radius: 12px;
  background: rgba(15, 23, 42, 0.8);
  color: #e2e8f0;
  min-width: 220px;
  cursor: pointer;
  transition: all 0.2s ease;
  appearance: none;
  background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='12' viewBox='0 0 12 12'%3E%3Cpath fill='%2394a3b8' d='M2 4l4 4 4-4'/%3E%3C/svg%3E");
  background-repeat: no-repeat;
  background-position: right 14px center;
}

.city-selector:hover {
  border-color: rgba(255, 255, 255, 0.2);
  background-color: rgba(30, 41, 59, 0.9);
}

.city-selector:focus {
  outline: none;
  border-color: #4ade80;
  box-shadow: 0 0 0 3px rgba(74, 222, 128, 0.15);
}

.city-selector option {
  background: #1e293b;
  color: #e2e8f0;
}

/* Segmented Control */
.segmented-control {
  display: flex;
  position: relative;
  background: rgba(15, 23, 42, 0.6);
  border-radius: 12px;
  padding: 4px;
  border: 1px solid rgba(255, 255, 255, 0.08);
  overflow: hidden;
}

.segmented-control .slider {
  display: none;
}

.segmented-control button {
  position: relative;
  z-index: 1;
  padding: 10px 20px;
  font-size: 14px;
  font-weight: 500;
  border: none;
  background: transparent;
  color: #94a3b8;
  cursor: pointer;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
  min-width: 80px;
  border-radius: 8px;
}

.segmented-control button:hover:not(.active) {
  color: #e2e8f0;
  background: rgba(255, 255, 255, 0.05);
}

.segmented-control button.active {
  color: #0f172a;
  font-weight: 600;
  background: linear-gradient(135deg, #22c55e 0%, #4ade80 100%);
  box-shadow: 0 2px 8px rgba(74, 222, 128, 0.3);
}

.chart-wrapper {
  position: relative;
  width: 100%;
  height: 520px;
  margin-bottom: 24px;
  background: rgba(30, 41, 59, 0.2);
  border-radius: 12px;
  padding: 12px;
}

.chart {
  width: 100%;
  height: 100%;
}

.loading-overlay {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  background: rgba(15, 23, 42, 0.9);
  backdrop-filter: blur(4px);
  z-index: 10;
  gap: 16px;
  border-radius: 12px;
  color: #94a3b8;
}

.spinner {
  width: 44px;
  height: 44px;
  border: 3px solid rgba(255, 255, 255, 0.1);
  border-top-color: #4ade80;
  border-radius: 50%;
  animation: spin 0.8s linear infinite;
}

@keyframes spin {
  to { transform: rotate(360deg); }
}

.error-message {
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  padding: 20px 32px;
  background: rgba(239, 68, 68, 0.15);
  border: 1px solid rgba(239, 68, 68, 0.3);
  color: #f87171;
  border-radius: 12px;
  font-weight: 500;
  z-index: 5;
}

.info-panel {
  padding: 20px 24px;
  background: linear-gradient(135deg, rgba(14, 165, 233, 0.1) 0%, rgba(56, 189, 248, 0.05) 100%);
  border-radius: 12px;
  border-left: 4px solid #0ea5e9;
  border: 1px solid rgba(14, 165, 233, 0.2);
}

.info-panel h4 {
  margin: 0 0 16px 0;
  color: #38bdf8;
  font-size: 1rem;
  letter-spacing: 0.02em;
}

.info-panel ul {
  margin: 0;
  padding-left: 20px;
}

.info-panel li {
  margin-bottom: 8px;
  color: #94a3b8;
  line-height: 1.6;
  font-size: 0.9rem;
}

.info-panel li strong {
  color: #e2e8f0;
}

/* Responsive */
@media (max-width: 768px) {
  .control-panel {
    flex-direction: column;
    gap: 20px;
  }
  
  .control-group {
    width: 100%;
    flex-direction: column;
    align-items: flex-start;
  }
  
  .city-selector,
  .segmented-control {
    width: 100%;
  }
  
  .segmented-control button {
    flex: 1;
    padding: 10px 8px;
  }
}
</style>
