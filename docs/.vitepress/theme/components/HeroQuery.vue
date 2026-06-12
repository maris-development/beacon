<template>
  <div class="hero-query" aria-label="Example Beacon SQL query and its results">
    <div class="hq-bar">
      <span class="hq-dot"></span>
      <span class="hq-dot"></span>
      <span class="hq-dot"></span>
      <span class="hq-file">query.sql</span>
    </div>

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
          <tr><td>2024-01-03</td><td>36.21</td><td>-5.43</td><td>21.8</td></tr>
          <tr><td>2024-01-03</td><td>35.88</td><td>-6.10</td><td>22.4</td></tr>
          <tr><td>2024-01-04</td><td>37.02</td><td>-4.77</td><td>20.9</td></tr>
          <tr><td>2024-01-04</td><td>36.55</td><td>-5.92</td><td>23.1</td></tr>
          <tr><td>2024-01-05</td><td>35.40</td><td>-6.58</td><td>22.0</td></tr>
        </tbody>
      </table>
      <div class="hq-foot">
        <span class="hq-ok">●</span> 5 rows · 12&nbsp;ms · Arrow IPC
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

/* window bar */
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
  background: var(--vp-c-gray-2, var(--vp-c-divider));
}
.hq-dot:nth-child(1) { background: #ff5f56; }
.hq-dot:nth-child(2) { background: #ffbd2e; }
.hq-dot:nth-child(3) { background: #27c93f; }

.hq-file {
  margin-left: 6px;
  color: var(--vp-c-text-3);
  font-size: 12px;
  letter-spacing: 0.02em;
}

/* code */
.hq-code {
  margin: 0;
  padding: 18px 20px;
  font-size: 13.5px;
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

.hq-result tbody td {
  color: var(--vp-c-text-2);
}

.hq-result tbody tr:last-child td {
  border-bottom: none;
}

.hq-foot {
  padding: 9px 14px;
  color: var(--vp-c-text-3);
  font-size: 11.5px;
  border-top: 1px solid var(--vp-c-divider);
}
.hq-ok { color: #27c93f; font-size: 9px; vertical-align: middle; }
</style>
