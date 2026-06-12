<script setup>
import { ref, computed } from 'vue'
import { withBase } from 'vitepress'

const logo = withBase('/beacon-logo-small.png')
const mode = ref('cloud') // 'cloud' | 'onprem' | 'local'

const MODES = {
  cloud: {
    regions: [
      { x: 10, w: 228, label: 'Internet', class: '' },
      { x: 298, w: 612, label: 'AWS Cloud', class: 'region-cloud' },
    ],
    jupyterSub: 'Notebook client',
    link1: 'SQL / Flight SQL',
    beaconSub: 'on EC2',
    link2: 'reads via S3 API',
    store: { ico: '🪣', title: 'S3 Bucket', sub: 'object storage' },
    aria: 'A Jupyter notebook on the internet queries Beacon running on EC2, which reads data from an S3 bucket.',
  },
  onprem: {
    regions: [
      { x: 10, w: 228, label: 'Internet', class: '' },
      { x: 298, w: 612, label: 'On-premise server', class: 'region-onprem' },
    ],
    jupyterSub: 'Notebook client',
    link1: 'SQL / Flight SQL',
    beaconSub: 'local process',
    link2: 'reads local files',
    store: { ico: '💾', title: 'Local disk', sub: 'NetCDF · Parquet' },
    aria: 'A Jupyter notebook on the internet queries Beacon running on an on-premise server, which reads data from local disk on that same server.',
  },
  local: {
    regions: [
      { x: 10, w: 900, label: 'Your laptop', class: 'region-local' },
    ],
    jupyterSub: 'Notebook (local)',
    link1: 'localhost',
    beaconSub: 'localhost:5001',
    link2: 'reads local files',
    store: { ico: '💾', title: 'Local files', sub: 'NetCDF · Parquet' },
    aria: 'A Jupyter notebook, Beacon, and the data files all run on a single local machine.',
  },
}

const view = computed(() => MODES[mode.value])
</script>

<template>
  <div class="arch">
    <p class="arch-title">A typical Beacon deployment</p>

    <div class="arch-tabs" role="tablist">
      <button
        :class="['arch-tab', { active: mode === 'cloud' }]"
        role="tab" :aria-selected="mode === 'cloud'"
        @click="mode = 'cloud'"
      >☁️ Cloud (AWS)</button>
      <button
        :class="['arch-tab', { active: mode === 'onprem' }]"
        role="tab" :aria-selected="mode === 'onprem'"
        @click="mode = 'onprem'"
      >🖥️ On-premise</button>
      <button
        :class="['arch-tab', { active: mode === 'local' }]"
        role="tab" :aria-selected="mode === 'local'"
        @click="mode = 'local'"
      >💻 Local</button>
    </div>

    <svg class="arch-svg" viewBox="0 0 920 250" role="img" :aria-label="view.aria">
      <!-- regions -->
      <g v-for="(rg, i) in view.regions" :key="i">
        <rect class="region" :class="rg.class" :x="rg.x" y="30" :width="rg.w" height="200" rx="14" />
        <text class="region-label" :x="rg.x + 16" y="52">{{ rg.label }}</text>
      </g>

      <!-- connectors (behind nodes) -->
      <path class="wire" d="M210 158 H336" />
      <path class="wire-flow" d="M210 158 H336" />
      <circle class="pkt-back back1" cx="336" cy="158" r="3.5" />

      <path class="wire" d="M506 158 H708" />
      <path class="wire-flow" d="M506 158 H708" />
      <circle class="pkt-back back2" cx="708" cy="158" r="3.5" />

      <!-- connector labels -->
      <text class="wire-label" x="273" y="144">{{ view.link1 }}</text>
      <text class="wire-label" x="607" y="144">{{ view.link2 }}</text>

      <!-- Jupyter client -->
      <g class="node">
        <rect x="40" y="112" width="170" height="92" rx="12" />
        <text class="ico" x="74" y="166">💻</text>
        <text class="n-title" x="98" y="152">Jupyter</text>
        <text class="n-sub" x="98" y="172">{{ view.jupyterSub }}</text>
      </g>

      <!-- Beacon -->
      <g class="node">
        <rect x="336" y="112" width="170" height="92" rx="12" />
        <image :href="logo" x="350" y="143" width="30" height="30" />
        <text class="n-title" x="392" y="152">Beacon</text>
        <text class="n-sub" x="392" y="172">{{ view.beaconSub }}</text>
      </g>

      <!-- Storage -->
      <g class="node">
        <rect x="708" y="112" width="170" height="92" rx="12" />
        <text class="ico" x="742" y="166">{{ view.store.ico }}</text>
        <text class="n-title" x="766" y="152">{{ view.store.title }}</text>
        <text class="n-sub" x="766" y="172">{{ view.store.sub }}</text>
      </g>
    </svg>
  </div>
</template>

<style scoped>
.arch {
  margin: 3.5rem auto 1rem;
  max-width: 840px;
  padding: 0 1.5rem;
  text-align: center;
}

.arch-title {
  margin: 0 0 1rem;
  color: var(--vp-c-text-3);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.11em;
  line-height: 1;
  text-transform: uppercase;
}

/* toggle */
.arch-tabs {
  display: inline-flex;
  gap: 4px;
  margin-bottom: 1.5rem;
  padding: 4px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 999px;
  background: var(--vp-c-bg-soft);
}
.arch-tab {
  appearance: none;
  border: none;
  border-radius: 999px;
  padding: 5px 16px;
  background: transparent;
  color: var(--vp-c-text-2);
  font-size: 13px;
  font-weight: 600;
  cursor: pointer;
  transition: color 0.2s, background-color 0.2s;
}
.arch-tab:hover { color: var(--vp-c-text-1); }
.arch-tab.active {
  color: var(--vp-c-brand-1);
  background: var(--vp-c-bg);
  box-shadow: 0 1px 4px rgba(0, 0, 0, 0.08);
}

.arch-svg {
  width: 100%;
  height: auto;
  overflow: visible;
}

/* regions */
.region {
  fill: var(--vp-c-bg-soft);
  stroke: var(--vp-c-divider);
  stroke-width: 1.5;
  stroke-dasharray: 5 5;
}
.region-cloud { fill: color-mix(in srgb, var(--vp-c-brand-soft) 28%, transparent); }
.region-onprem { fill: color-mix(in srgb, var(--vp-c-green-soft, var(--vp-c-bg-soft)) 40%, transparent); }
.region-local { fill: color-mix(in srgb, var(--vp-c-yellow-soft, var(--vp-c-bg-soft)) 36%, transparent); }

.region-label {
  fill: var(--vp-c-text-3);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
}

/* nodes */
.node rect {
  fill: var(--vp-c-bg);
  stroke: var(--vp-c-divider);
  stroke-width: 1.5;
}
.node .n-title {
  fill: var(--vp-c-text-1);
  font-size: 15px;
  font-weight: 700;
}
.node .n-sub {
  fill: var(--vp-c-text-3);
  font-size: 11.5px;
}
.ico {
  font-size: 22px;
  text-anchor: middle;
}

/* connectors */
.wire {
  fill: none;
  stroke: var(--vp-c-divider);
  stroke-width: 2;
}
.wire-flow {
  fill: none;
  stroke: var(--vp-c-brand-1);
  stroke-width: 2;
  stroke-linecap: round;
  stroke-dasharray: 1 7;
  animation: arch-march 0.6s linear infinite;
}
@keyframes arch-march { to { stroke-dashoffset: -8; } }

.wire-label {
  fill: var(--vp-c-text-3);
  font-size: 11px;
  text-anchor: middle;
}

/* return packets (results flowing back) */
.pkt-back { fill: var(--vp-c-green-1); }
.back1 { animation: arch-back1 2.6s ease-in-out infinite; animation-delay: 0.3s; }
.back2 { animation: arch-back2 2.6s ease-in-out infinite; animation-delay: 0.6s; }
@keyframes arch-back1 {
  0%   { transform: translateX(0); opacity: 0; }
  12%  { opacity: 1; }
  88%  { opacity: 1; }
  100% { transform: translateX(-126px); opacity: 0; }
}
@keyframes arch-back2 {
  0%   { transform: translateX(0); opacity: 0; }
  12%  { opacity: 1; }
  88%  { opacity: 1; }
  100% { transform: translateX(-202px); opacity: 0; }
}

@media (prefers-reduced-motion: reduce) {
  .wire-flow { animation: none; }
  .pkt-back { display: none; }
}

@media (max-width: 560px) {
  .arch-svg .n-sub, .arch-svg .wire-label { display: none; }
}
</style>
