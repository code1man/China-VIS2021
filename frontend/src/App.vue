<script setup>
import { ref } from 'vue';
import Demo from './components/Demo.vue';
import PollutionCalendar from './components/PollutionCalendar.vue';
import WindRoseChart from './components/WindRoseChart.vue';

// 当前视图: 'demo' | 'calendar' | 'windrose'
const currentView = ref('demo');

function setView(view) {
  currentView.value = view;
}
</script>

<template>
  <div class="app-wrapper">
    <!-- Navbar with Glassmorphism -->
    <header class="app-header">
      <div class="brand">
        <div class="logo-glow"></div>
        <h1>China-VIS2021</h1>
        <span class="subtitle">数据可视化平台</span>
      </div>
      
      <nav class="nav-tabs">
        <button 
          :class="{ active: currentView === 'demo' }"
          @click="setView('demo')">
          <span class="nav-icon">📊</span>
          <span class="nav-text">Demo</span>
          <span class="nav-underline"></span>
        </button>
        <button 
          :class="{ active: currentView === 'calendar' }"
          @click="setView('calendar')">
          <span class="nav-icon">🗓️</span>
          <span class="nav-text">污染日历</span>
          <span class="nav-underline"></span>
        </button>
        <button 
          :class="{ active: currentView === 'windrose' }"
          @click="setView('windrose')">
          <span class="nav-icon">🌬️</span>
          <span class="nav-text">风向分析</span>
          <span class="nav-underline"></span>
        </button>
      </nav>
    </header>

    <!-- Main Content with View Transition -->
    <main class="view-container">
      <Transition name="fade" mode="out-in">
        <Demo v-if="currentView === 'demo'" key="demo" />
        <PollutionCalendar v-else-if="currentView === 'calendar'" key="calendar" />
        <WindRoseChart v-else-if="currentView === 'windrose'" key="windrose" />
      </Transition>
    </main>

    <!-- Ambient Background Effects -->
    <div class="ambient-glow ambient-glow-1"></div>
    <div class="ambient-glow ambient-glow-2"></div>
  </div>
</template>

<style scoped>
.app-wrapper {
  min-height: 100vh;
  position: relative;
  overflow-x: hidden;
}

/* === Ambient Background Glows === */
.ambient-glow {
  position: fixed;
  border-radius: 50%;
  filter: blur(100px);
  pointer-events: none;
  z-index: 0;
  opacity: 0.4;
}

.ambient-glow-1 {
  width: 600px;
  height: 600px;
  background: radial-gradient(circle, rgba(74, 222, 128, 0.15) 0%, transparent 70%);
  top: -200px;
  right: -100px;
}

.ambient-glow-2 {
  width: 500px;
  height: 500px;
  background: radial-gradient(circle, rgba(56, 189, 248, 0.12) 0%, transparent 70%);
  bottom: -150px;
  left: -100px;
}

/* === Navbar === */
.app-header {
  position: sticky;
  top: 0;
  z-index: 100;
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 16px 32px;
  background: rgba(15, 23, 42, 0.75);
  backdrop-filter: blur(20px);
  -webkit-backdrop-filter: blur(20px);
  border-bottom: 1px solid rgba(255, 255, 255, 0.06);
  box-shadow: 0 4px 30px rgba(0, 0, 0, 0.3);
}

.brand {
  display: flex;
  align-items: center;
  gap: 12px;
  position: relative;
}

.logo-glow {
  position: absolute;
  left: -20px;
  width: 50px;
  height: 50px;
  background: radial-gradient(circle, var(--accent-glow) 0%, transparent 70%);
  filter: blur(15px);
  opacity: 0.6;
}

.brand h1 {
  font-size: 1.5rem;
  font-weight: 700;
  background: linear-gradient(135deg, #fff 0%, #94a3b8 100%);
  -webkit-background-clip: text;
  -webkit-text-fill-color: transparent;
  background-clip: text;
  letter-spacing: 0.03em;
}

.subtitle {
  font-size: 0.75rem;
  color: var(--text-muted);
  padding: 4px 10px;
  background: rgba(255, 255, 255, 0.05);
  border-radius: 20px;
  letter-spacing: 0.05em;
}

/* === Navigation Tabs === */
.nav-tabs {
  display: flex;
  gap: 8px;
}

.nav-tabs button {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 20px;
  background: transparent;
  border: none;
  color: var(--text-secondary);
  font-size: 0.9rem;
  font-weight: 500;
  border-radius: 10px;
  position: relative;
  transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
}

.nav-tabs button:hover {
  color: var(--text-primary);
  background: rgba(255, 255, 255, 0.05);
  transform: translateY(-1px);
}

.nav-tabs button.active {
  color: var(--accent);
  background: rgba(74, 222, 128, 0.1);
}

.nav-icon {
  font-size: 1rem;
}

.nav-underline {
  position: absolute;
  bottom: 0;
  left: 50%;
  transform: translateX(-50%);
  width: 0;
  height: 2px;
  background: var(--accent-gradient);
  border-radius: 2px;
  transition: all 0.3s ease;
  box-shadow: 0 0 10px var(--accent-glow);
}

.nav-tabs button.active .nav-underline {
  width: 60%;
}

.nav-tabs button:hover .nav-underline {
  width: 40%;
}

/* === View Container === */
.view-container {
  position: relative;
  z-index: 1;
  padding: 32px;
  min-height: calc(100vh - 80px);
}

/* === View Transitions === */
.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.35s ease, transform 0.35s ease;
}

.fade-enter-from {
  opacity: 0;
  transform: translateY(16px);
}

.fade-leave-to {
  opacity: 0;
  transform: translateY(-16px);
}

/* === Responsive === */
@media (max-width: 768px) {
  .app-header {
    flex-direction: column;
    gap: 16px;
    padding: 16px;
  }
  
  .nav-tabs {
    width: 100%;
    justify-content: center;
  }
  
  .nav-tabs button {
    padding: 8px 12px;
    font-size: 0.8rem;
  }
  
  .nav-text {
    display: none;
  }
  
  .view-container {
    padding: 16px;
  }
  
  .subtitle {
    display: none;
  }
}
</style>
