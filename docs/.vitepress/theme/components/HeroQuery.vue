<script setup>
import { ref } from 'vue'

const tab = ref('sql')

const rows = [
  ['2024-01-03', '36.21', '-5.43', '21.8'],
  ['2024-01-03', '35.88', '-6.10', '22.4'],
  ['2024-01-04', '37.02', '-4.77', '20.9'],
  ['2024-01-04', '36.55', '-5.92', '23.1'],
  ['2024-01-05', '35.40', '-6.58', '22.0'],
]
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

    <!-- SQL -->
    <template v-if="tab === 'sql'">
      <pre class="hq-code"><code><span class="k">SELECT</span> time, latitude, longitude, temperature
<span class="k">FROM</span> <span class="fn">read_netcdf</span>(<span class="s">'argo/**/*.nc'</span>)
<span class="k">WHERE</span> temperature <span class="o">&gt;</span> <span class="n">20</span>
<span class="k">LIMIT</span> <span class="n">5</span>;</code></pre>

      <div class="hq-result">
        <table>
          <thead>
            <tr>
              <th>time</th>
              <th>latitude</th>
              <th>longitude</th>
              <th>temperature</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(r, i) in rows" :key="i">
              <td>{{ r[0] }}</td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td>
            </tr>
          </tbody>
        </table>
        <div class="hq-foot">
          <span class="hq-ok">●</span> 5 rows · 12&nbsp;ms · Arrow IPC
        </div>
      </div>
    </template>

    <!-- Python -->
    <template v-else>
      <pre class="hq-code"><code><span class="k">from</span> beacon_api <span class="k">import</span> <span class="fn">Client</span>

client <span class="o">=</span> <span class="fn">Client</span>(<span class="s">"https://beacon.example.com"</span>)
df <span class="o">=</span> client.<span class="fn">sql_query</span>(query).<span class="fn">to_pandas_dataframe</span>()</code></pre>

      <div class="hq-result">
        <table class="hq-df">
          <thead>
            <tr>
              <th class="hq-idx"></th>
              <th>time</th>
              <th>latitude</th>
              <th>longitude</th>
              <th>temperature</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(r, i) in rows" :key="i">
              <td class="hq-idx">{{ i }}</td>
              <td>{{ r[0] }}</td><td>{{ r[1] }}</td><td>{{ r[2] }}</td><td>{{ r[3] }}</td>
            </tr>
          </tbody>
        </table>
        <div class="hq-foot">
          <span class="hq-ok">●</span> pandas.DataFrame · 5 rows × 4 columns
        </div>
      </div>
    </template>
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
  /* Pinned so both tabs keep the (ideal) SQL height — no jump on switch */
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

/* result table */
.hq-result {
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

/* pandas-style index column */
.hq-df .hq-idx {
  width: 1%;
  color: var(--vp-c-text-3);
  font-weight: 700;
  text-transform: none;
  letter-spacing: 0;
}

.hq-foot {
  padding: 9px 14px;
  color: var(--vp-c-text-3);
  font-size: 11.5px;
  border-top: 1px solid var(--vp-c-divider);
}
.hq-ok { color: #27c93f; font-size: 9px; vertical-align: middle; }
</style>
