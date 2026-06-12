<script setup>
import { ref, computed, watch, onMounted, onBeforeUnmount } from 'vue'

const tab = ref('sql')
// SSR / no-JS / reduced-motion default: the finished state.
const phase = ref('results') // 'typing' | 'running' | 'results'
const typed = ref(Infinity)  // number of chars revealed (Infinity = all)

const rows = [
  ['2024-01-03', '36.21', '-5.43', '21.8'],
  ['2024-01-03', '35.88', '-6.10', '22.4'],
  ['2024-01-04', '37.02', '-4.77', '20.9'],
  ['2024-01-04', '36.55', '-5.92', '23.1'],
  ['2024-01-05', '35.40', '-6.58', '22.0'],
]

// Pre-tokenized code so we can reveal char-by-char while keeping syntax colors.
const tokens = {
  sql: [
    { t: 'SELECT', c: 'k' },
    { t: ' time, latitude, longitude, temperature\n', c: '' },
    { t: 'FROM', c: 'k' },
    { t: ' ', c: '' },
    { t: 'read_netcdf', c: 'fn' },
    { t: '(', c: '' },
    { t: "'argo/**/*.nc'", c: 's' },
    { t: ')\n', c: '' },
    { t: 'WHERE', c: 'k' },
    { t: ' temperature ', c: '' },
    { t: '>', c: 'o' },
    { t: ' ', c: '' },
    { t: '20', c: 'n' },
    { t: '\n', c: '' },
    { t: 'LIMIT', c: 'k' },
    { t: ' ', c: '' },
    { t: '5', c: 'n' },
    { t: ';', c: '' },
  ],
  python: [
    { t: 'from', c: 'k' },
    { t: ' beacon_api ', c: '' },
    { t: 'import', c: 'k' },
    { t: ' ', c: '' },
    { t: 'Client', c: 'fn' },
    { t: '\n\nclient ', c: '' },
    { t: '=', c: 'o' },
    { t: ' ', c: '' },
    { t: 'Client', c: 'fn' },
    { t: '(', c: '' },
    { t: '"https://beacon.example.com"', c: 's' },
    { t: ')\ndf ', c: '' },
    { t: '=', c: 'o' },
    { t: ' client.', c: '' },
    { t: 'sql_query', c: 'fn' },
    { t: '(query).', c: '' },
    { t: 'to_pandas_dataframe', c: 'fn' },
    { t: '()', c: '' },
  ],
}

const totalLen = computed(() =>
  tokens[tab.value].reduce((a, t) => a + t.t.length, 0)
)

const visible = computed(() => {
  const limit = typed.value
  const toks = tokens[tab.value]
  if (limit === Infinity) return toks
  const out = []
  let used = 0
  for (const tk of toks) {
    if (used >= limit) break
    const remain = limit - used
    if (tk.t.length <= remain) {
      out.push(tk)
      used += tk.t.length
    } else {
      out.push({ t: tk.t.slice(0, remain), c: tk.c })
      break
    }
  }
  return out
})

const showCursor = computed(() => phase.value === 'typing')

let timer = null
const clear = () => {
  if (timer) { clearInterval(timer); clearTimeout(timer); timer = null }
}

function animate() {
  clear()
  const reduce =
    typeof window !== 'undefined' &&
    window.matchMedia &&
    window.matchMedia('(prefers-reduced-motion: reduce)').matches
  if (reduce) {
    phase.value = 'results'
    typed.value = Infinity
    return
  }
  phase.value = 'typing'
  typed.value = 0
  const len = totalLen.value
  timer = setInterval(() => {
    typed.value = Math.min(len, typed.value + 1)
    if (typed.value >= len) {
      clear()
      phase.value = 'running'
      timer = setTimeout(() => { phase.value = 'results' }, 700)
    }
  }, 24)
}

onMounted(animate)
watch(tab, animate)
onBeforeUnmount(clear)
</script>

<template>
  <div class="hero-query" aria-label="Example Beacon query and its results">
    <div class="hq-bar">
      <span class="hq-dot"></span>
      <span class="hq-dot"></span>
      <span class="hq-dot"></span>
      <div class="hq-tabs" role="tablist">
        <button
          :class="['hq-tab', { active: tab === 'sql' }]"
          role="tab"
          :aria-selected="tab === 'sql'"
          @click="tab = 'sql'"
        >SQL</button>
        <button
          :class="['hq-tab', { active: tab === 'python' }]"
          role="tab"
          :aria-selected="tab === 'python'"
          @click="tab = 'python'"
        >Python</button>
      </div>
    </div>

    <pre class="hq-code"><code><span v-for="(tk, i) in visible" :key="i" :class="tk.c">{{ tk.t }}</span><span v-if="showCursor" class="hq-cursor" aria-hidden="true"></span></code></pre>

    <div class="hq-result">
      <table :class="{ 'hq-df': tab === 'python' }">
        <thead>
          <tr>
            <th v-if="tab === 'python'" class="hq-idx"></th>
            <th>time</th>
            <th>latitude</th>
            <th>longitude</th>
            <th>temperature</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="(r, i) in rows"
            :key="i"
            class="hq-row"
            :class="{ show: phase === 'results' }"
            :style="{ transitionDelay: (i * 55) + 'ms' }"
          >
            <td v-if="tab === 'python'" class="hq-idx">{{ i }}</td>
            <td>{{ r[0] }}</td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td>
          </tr>
        </tbody>
      </table>

      <div v-if="phase === 'running'" class="hq-running" aria-hidden="true">
        <span class="hq-spin"></span> running query…
      </div>

      <div class="hq-foot">
        <template v-if="phase === 'results'">
          <span class="hq-ok">●</span>
          {{ tab === 'python' ? 'pandas.DataFrame · 5 rows × 4 columns' : '5 rows · 12 ms · Arrow IPC' }}
        </template>
        <template v-else-if="phase === 'running'">executing…</template>
        <template v-else>&nbsp;</template>
      </div>
    </div>
  </div>
</template>

<style scoped>
.hero-query {
  width: 100%;
  max-width: 620px;
  margin-inline: auto;
  border: 1px solid var(--vp-c-divider);
  border-radius: 12px;
  overflow: hidden;
  background: var(--vp-c-bg);
  box-shadow:
    0 24px 70px rgba(0, 0, 0, 0.16),
    0 8px 24px rgba(0, 0, 0, 0.08);
  font-family: var(--vp-font-family-mono);
  text-align: left;
}

/* window bar + tabs */
.hq-bar {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 14px;
  background: var(--vp-c-bg-soft);
  border-bottom: 1px solid var(--vp-c-divider);
}

.hq-dot {
  width: 11px;
  height: 11px;
  border-radius: 50%;
}
.hq-dot:nth-child(1) { background: #ff5f56; }
.hq-dot:nth-child(2) { background: #ffbd2e; }
.hq-dot:nth-child(3) { background: #27c93f; }

.hq-tabs {
  display: flex;
  gap: 4px;
  margin-left: auto;
}

.hq-tab {
  appearance: none;
  border: 1px solid transparent;
  border-radius: 6px;
  padding: 3px 12px;
  background: transparent;
  color: var(--vp-c-text-3);
  font-family: var(--vp-font-family-base);
  font-size: 12px;
  font-weight: 600;
  cursor: pointer;
  transition: color 0.2s, background-color 0.2s, border-color 0.2s;
}
.hq-tab:hover { color: var(--vp-c-text-1); }
.hq-tab.active {
  color: var(--vp-c-brand-1);
  background: var(--vp-c-brand-soft);
  border-color: color-mix(in srgb, var(--vp-c-brand-1) 30%, transparent);
}

/* code */
.hq-code {
  margin: 0;
  padding: 18px 20px;
  box-sizing: border-box;
  /* Pinned so every phase/tab keeps the (ideal) SQL height — no jump */
  min-height: 124px;
  font-size: 13px;
  line-height: 1.7;
  color: var(--vp-c-text-1);
  background: var(--vp-c-bg);
  white-space: pre;
  overflow-x: auto;
}
.hq-code code { font-family: inherit; }

.hq-code .k  { color: var(--vp-c-brand-1); font-weight: 600; }
.hq-code .fn { color: var(--vp-c-purple-1, var(--vp-c-brand-2)); }
.hq-code .s  { color: var(--vp-c-green-1); }
.hq-code .n  { color: var(--vp-c-yellow-2, var(--vp-c-yellow-1)); }
.hq-code .o  { color: var(--vp-c-text-2); }

.hq-cursor {
  display: inline-block;
  width: 7px;
  height: 1.05em;
  margin-left: 1px;
  transform: translateY(2px);
  background: var(--vp-c-brand-1);
  animation: hq-blink 1s step-end infinite;
}
@keyframes hq-blink { 50% { opacity: 0; } }

/* result table */
.hq-result {
  position: relative;
  border-top: 1px solid var(--vp-c-divider);
  background: var(--vp-c-bg-soft);
}

.hq-result table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12.5px;
}

.hq-result th,
.hq-result td {
  padding: 8px 14px;
  text-align: right;
  border-bottom: 1px solid var(--vp-c-divider);
  font-variant-numeric: tabular-nums;
}

.hq-result th:first-child,
.hq-result td:first-child {
  text-align: left;
}

.hq-result thead th {
  color: var(--vp-c-text-3);
  font-weight: 600;
  text-transform: uppercase;
  font-size: 11px;
  letter-spacing: 0.05em;
  background: var(--vp-c-bg);
}

.hq-result tbody td { color: var(--vp-c-text-2); }
.hq-result tbody tr:last-child td { border-bottom: none; }

/* rows are laid out always (height stays constant); they fade in on results */
.hq-row {
  opacity: 0;
  transform: translateY(3px);
  transition: opacity 0.35s ease, transform 0.35s ease;
}
.hq-row.show { opacity: 1; transform: none; }

/* pandas-style index column */
.hq-df .hq-idx {
  width: 1%;
  color: var(--vp-c-text-3);
  font-weight: 700;
  text-transform: none;
  letter-spacing: 0;
}

/* running overlay */
.hq-running {
  position: absolute;
  inset: 34px 0 30px;
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 8px;
  color: var(--vp-c-text-2);
  font-size: 12.5px;
  pointer-events: none;
}
.hq-spin {
  width: 13px;
  height: 13px;
  border: 2px solid var(--vp-c-divider);
  border-top-color: var(--vp-c-brand-1);
  border-radius: 50%;
  animation: hq-spin 0.7s linear infinite;
}
@keyframes hq-spin { to { transform: rotate(360deg); } }

.hq-foot {
  padding: 9px 14px;
  color: var(--vp-c-text-3);
  font-size: 11.5px;
  border-top: 1px solid var(--vp-c-divider);
}
.hq-ok { color: #27c93f; font-size: 9px; vertical-align: middle; }

@media (prefers-reduced-motion: reduce) {
  .hq-row { opacity: 1; transform: none; transition: none; }
  .hq-cursor, .hq-spin { animation: none; }
}
</style>
