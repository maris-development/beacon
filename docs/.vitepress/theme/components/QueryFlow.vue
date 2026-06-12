<script setup>
import { ref, computed, onMounted, onBeforeUnmount } from 'vue'
import { withBase } from 'vitepress'

const logo = withBase('/beacon-logo-small.png')

// Each setup mirrors the homepage deployment cards. The env box wraps Beacon +
// storage for cloud/on-prem, and the whole thing (incl. the client) for local.
const setups = [
  {
    key: 'cloud', label: '☁ Cloud (AWS)', cls: 'env-cloud',
    box: { x: 376, w: 596, lx: 392 },
    client: { ico: '💻', sub: 'remote notebook' },
    beaconSub: 'on EC2',
    store: { ico: '🪣', title: 'S3 Bucket', sub: 'object storage' },
  },
  {
    key: 'onprem', label: '🖥 On-premise', cls: 'env-onprem',
    box: { x: 376, w: 596, lx: 392 },
    client: { ico: '💻', sub: 'remote notebook' },
    beaconSub: 'your server',
    store: { ico: '💾', title: 'Local disk', sub: 'NetCDF · Parquet' },
  },
  {
    key: 'local', label: '💻 Local', cls: 'env-local',
    box: { x: 8, w: 964, lx: 24 },
    client: { ico: '📓', sub: 'same machine' },
    beaconSub: 'localhost:5001',
    store: { ico: '💾', title: 'Local files', sub: 'on disk' },
  },
]

const active = ref(0)
const cur = computed(() => setups[active.value])

let timer = null
onMounted(() => {
  const reduce = window.matchMedia &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  if (reduce) return
  timer = setInterval(() => {
    active.value = (active.value + 1) % setups.length
  }, 3000)
})
onBeforeUnmount(() => { if (timer) clearInterval(timer) })
</script>

<template>
  <div class="qflow">
    <svg class="qflow-svg" viewBox="0 0 980 260" role="img"
         aria-label="A client sends an SQL or JSON query to Beacon along a request lane; Beacon reads from storage and the results (Parquet, NetCDF or Arrow) flow back along a response lane. Beacon can be deployed in the cloud, on-premise, or fully locally.">
      <!-- ===== resizing deployment-environment box ===== -->
      <rect class="env-fill" :class="cur.cls" :x="cur.box.x" y="56" :width="cur.box.w" height="176" rx="18" />
      <rect class="env-border" :x="cur.box.x" y="56" :width="cur.box.w" height="176" rx="18" />

      <!-- ===== request lane (top, flows right) ===== -->
      <path class="wire" d="M220 118 H392" />
      <path class="wire" d="M588 118 H760" />
      <!-- ===== response lane (bottom, flows left) ===== -->
      <path class="wire" d="M220 156 H392" />
      <path class="wire" d="M588 156 H760" />

      <!-- ===== animated packets ===== -->
      <circle class="pkt fwd q"   cx="220" cy="118" r="5" />
      <circle class="pkt fwd rd"  cx="588" cy="118" r="4.5" />
      <circle class="pkt ret dt"  cx="760" cy="156" r="4.5" />
      <circle class="pkt ret res" cx="392" cy="156" r="5" />

      <!-- ===== lane labels ===== -->
      <text class="elabel" x="306" y="106">SQL / JSON</text>
      <text class="elabel" x="674" y="106">reads</text>
      <text class="elabel" x="674" y="176">data</text>
      <text class="elabel res-label" x="306" y="176">Parquet · NetCDF · Arrow</text>

      <!-- ===== node frames + static text ===== -->
      <g class="node">
        <rect x="24" y="84" width="196" height="104" rx="14" />
        <text class="n-title" x="74" y="130">Clients</text>
      </g>
      <g class="node">
        <rect class="beacon-rect" x="392" y="84" width="196" height="104" rx="14" />
        <image :href="logo" x="408" y="118" width="36" height="36" />
        <text class="n-title big" x="454" y="130">Beacon</text>
      </g>
      <g class="node">
        <rect x="760" y="84" width="196" height="104" rx="14" />
      </g>

      <!-- ===== per-setup content (crossfades on change) ===== -->
      <Transition name="setup">
        <g :key="cur.key" class="setup-layer">
          <text class="env-name" :class="cur.cls" :x="cur.box.lx" y="80">{{ cur.label }}</text>

          <text class="ico" x="52" y="144">{{ cur.client.ico }}</text>
          <text class="n-sub tight" x="74" y="150">{{ cur.client.sub }}</text>

          <text class="n-sub" x="454" y="150">{{ cur.beaconSub }}</text>

          <text class="ico" x="788" y="144">{{ cur.store.ico }}</text>
          <text class="n-title" x="810" y="130">{{ cur.store.title }}</text>
          <text class="n-sub" x="810" y="150">{{ cur.store.sub }}</text>
        </g>
      </Transition>
    </svg>
  </div>
</template>

<style scoped>
.qflow { margin: 1.25rem 0 0.5rem; }
.qflow-svg { width: 100%; height: auto; overflow: visible; }

/* environment box — morphs size + colour per setup */
.env-fill {
  stroke: none;
  transition: x 0.5s ease, width 0.5s ease, fill 0.5s ease;
}
.env-border {
  fill: none;
  stroke: var(--vp-c-divider);
  stroke-width: 1.5;
  stroke-dasharray: 6 6;
  transition: x 0.5s ease, width 0.5s ease;
}
.env-cloud  { fill: color-mix(in srgb, var(--vp-c-brand-soft) 26%, transparent); }
.env-onprem { fill: color-mix(in srgb, var(--vp-c-green-soft, var(--vp-c-bg-soft)) 38%, transparent); }
.env-local  { fill: color-mix(in srgb, var(--vp-c-yellow-soft, var(--vp-c-bg-soft)) 36%, transparent); }

.env-name { font-size: 13.5px; font-weight: 700; letter-spacing: 0.02em; }
.env-name.env-cloud  { fill: var(--vp-c-brand-1); }
.env-name.env-onprem { fill: var(--vp-c-green-1); }
.env-name.env-local  { fill: var(--vp-c-yellow-2, var(--vp-c-text-1)); }

/* per-setup crossfade */
.setup-enter-active, .setup-leave-active { transition: opacity 0.45s ease; }
.setup-enter-from, .setup-leave-to { opacity: 0; }

/* connectors */
.wire { fill: none; stroke: var(--vp-c-divider); stroke-width: 2.5; }

/* nodes */
.node rect { fill: var(--vp-c-bg); stroke: var(--vp-c-divider); stroke-width: 1.5; }
.beacon-rect { fill: var(--vp-c-bg-soft); }
.n-title { fill: var(--vp-c-text-1); font-size: 15px; font-weight: 700; }
.n-title.big { font-size: 18px; }
.n-sub { fill: var(--vp-c-text-3); font-size: 11.5px; }
.n-sub.tight { font-size: 11px; }
.ico { font-size: 24px; text-anchor: middle; }

.elabel { fill: var(--vp-c-text-3); font-size: 12px; text-anchor: middle; }
.res-label { opacity: 0.9; }

/* ===== round-trip packet animation (shared 5s loop) ===== */
.pkt { filter: drop-shadow(0 0 2px currentColor); }
.pkt.fwd { fill: var(--vp-c-brand-1); color: var(--vp-c-brand-1); }
.pkt.ret { fill: var(--vp-c-green-1); color: var(--vp-c-green-1); }
.pkt.q   { animation: qf-q   5s ease-in-out infinite; }
.pkt.rd  { animation: qf-rd  5s ease-in-out infinite; }
.pkt.dt  { animation: qf-dt  5s ease-in-out infinite; }
.pkt.res { animation: qf-res 5s ease-in-out infinite; }

/* forward (right) along the top lane: client->beacon, then beacon->storage */
@keyframes qf-q {
  0%   { transform: translateX(0);     opacity: 0; }
  4%   { opacity: 1; }
  20%  { transform: translateX(172px); opacity: 1; }
  23%, 100% { transform: translateX(172px); opacity: 0; }
}
@keyframes qf-rd {
  0%, 30%   { transform: translateX(0);     opacity: 0; }
  34%       { opacity: 1; }
  48%       { transform: translateX(172px); opacity: 1; }
  51%, 100% { transform: translateX(172px); opacity: 0; }
}
/* return (left) along the bottom lane: storage->beacon, then beacon->client */
@keyframes qf-dt {
  0%, 52%   { transform: translateX(0);      opacity: 0; }
  56%       { opacity: 1; }
  68%       { transform: translateX(-172px); opacity: 1; }
  71%, 100% { transform: translateX(-172px); opacity: 0; }
}
@keyframes qf-res {
  0%, 72%   { transform: translateX(0);      opacity: 0; }
  76%       { opacity: 1; }
  92%       { transform: translateX(-172px); opacity: 1; }
  95%, 100% { transform: translateX(-172px); opacity: 0; }
}

/* beacon glow while it's working (between receiving query and sending result) */
.beacon-rect { animation: qf-glow 5s ease-in-out infinite; }
@keyframes qf-glow {
  0%, 18% { stroke: var(--vp-c-divider); }
  24%     { stroke: var(--vp-c-brand-1); }
  72%     { stroke: var(--vp-c-brand-1); }
  78%     { stroke: var(--vp-c-divider); }
  100%    { stroke: var(--vp-c-divider); }
}

@media (prefers-reduced-motion: reduce) {
  .pkt { display: none; }
  .beacon-rect { animation: none; }
  .env-fill, .env-border { transition: none; }
}

@media (max-width: 560px) {
  .qflow-svg .n-sub { display: none; }
}
</style>
