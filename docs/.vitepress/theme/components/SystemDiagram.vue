<script setup>
import Icon from './Icon.vue'

const sources = [
  { icon: 'files', name: 'Local files', sub: 'NetCDF, Zarr, Parquet…' },
  { icon: 'cloud', name: 'Object storage', sub: 'S3, GCS, Azure' },
  { icon: 'database', name: 'SQL databases', sub: 'Postgres, MySQL' },
  { icon: 'satellite-dish', name: 'Other Beacons', sub: 'remote catalogs' },
]

const consumers = [
  { icon: 'notebook-text', name: 'Python & notebooks', sub: 'pandas, Polars, Arrow' },
  { icon: 'terminal', name: 'SQL clients', sub: 'DataGrip, DBeaver, JDBC' },
  { icon: 'chart-column', name: 'Dashboards & BI', sub: 'over HTTP or Flight SQL' },
  { icon: 'package', name: 'Exports & pipelines', sub: 'Parquet, NetCDF, CSV, ODV' },
]
</script>

<template>
  <figure class="sysd">
    <div class="sysd-grid">
      <!-- sources -->
      <div class="sysd-col">
        <p class="sysd-cap">Your data, in place</p>
        <div v-for="s in sources" :key="s.name" class="sysd-chip">
          <span class="sysd-chip-ico"><Icon :name="s.icon" :size="18" /></span>
          <span class="sysd-chip-txt"><b>{{ s.name }}</b><small>{{ s.sub }}</small></span>
        </div>
      </div>

      <div class="sysd-lane in" aria-hidden="true">
        <span class="dot" style="--d: 0s"></span>
        <span class="dot" style="--d: 0.8s"></span>
        <span class="dot" style="--d: 1.6s"></span>
      </div>

      <!-- core: BeaconDB is the engine, nested inside the Beacon Data Lake service -->
      <div class="sysd-core">
        <div class="sysd-lake">
          <div class="sysd-lake-head">
            <span class="sysd-lake-ico"><Icon name="server" :size="17" /></span>
            <span class="sysd-lake-name">Beacon Data Lake</span>
            <span class="sysd-tag lake">the server</span>
          </div>
          <div class="sysd-services">
            <span>HTTP + Flight SQL</span>
            <span>Access control</span>
            <span>Admin UI</span>
            <span>Crawlers</span>
            <span>Exports</span>
          </div>

          <div class="sysd-db">
            <div class="sysd-db-head">
              <span class="sysd-db-ico"><Icon name="database" :size="17" /></span>
              <span class="sysd-db-name">BeaconDB</span>
              <span class="sysd-tag db">the engine</span>
            </div>
            <p class="sysd-db-sub">
              Rust, on Apache Arrow and DataFusion. One SQL dialect, the format readers, and the
              portable <code>beacon.db</code> file.
            </p>
          </div>

          <p class="sysd-nest-note">Beacon Data Lake runs BeaconDB inside it</p>
        </div>
      </div>

      <div class="sysd-lane out" aria-hidden="true">
        <span class="dot" style="--d: 0.4s"></span>
        <span class="dot" style="--d: 1.2s"></span>
        <span class="dot" style="--d: 2s"></span>
      </div>

      <!-- consumers -->
      <div class="sysd-col">
        <p class="sysd-cap">Query from anywhere</p>
        <div v-for="c in consumers" :key="c.name" class="sysd-chip">
          <span class="sysd-chip-ico"><Icon :name="c.icon" :size="18" /></span>
          <span class="sysd-chip-txt"><b>{{ c.name }}</b><small>{{ c.sub }}</small></span>
        </div>
      </div>
    </div>
    <figcaption class="sysd-cap-foot">
      BeaconDB is the engine. Embed it on its own, or run Beacon Data Lake, which wraps that same
      engine in a service.
    </figcaption>
  </figure>
</template>

<style scoped>
.sysd {
  margin: 1.5rem 0 0.5rem;
}
.sysd-grid {
  display: grid;
  grid-template-columns: 0.95fr 44px 1.9fr 44px 0.95fr;
  align-items: stretch;
  gap: 0;
}

.sysd-cap {
  margin: 0 0 0.7rem;
  color: var(--vp-c-text-3);
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.09em;
  text-transform: uppercase;
}

.sysd-col { display: flex; flex-direction: column; gap: 0.55rem; justify-content: center; }

.sysd-chip {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 9px 11px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
  background: var(--vp-c-bg-soft);
  transition: border-color 0.2s ease, transform 0.2s ease;
}
.sysd-chip:hover { border-color: var(--vp-c-brand-1); transform: translateX(2px); }
.sysd-chip-ico {
  display: inline-flex;
  color: var(--vp-c-brand-1);
  flex: none;
}
.sysd-chip-txt { display: flex; flex-direction: column; line-height: 1.25; min-width: 0; }
.sysd-chip-txt b { font-size: 13.5px; color: var(--vp-c-text-1); }
.sysd-chip-txt small { font-size: 11.5px; color: var(--vp-c-text-3); }

/* animated flow lanes */
.sysd-lane { position: relative; align-self: stretch; min-height: 64px; }
.sysd-lane::before {
  content: "";
  position: absolute;
  top: 50%;
  left: 4px;
  right: 4px;
  height: 2px;
  transform: translateY(-1px);
  background: repeating-linear-gradient(90deg, var(--vp-c-divider) 0 6px, transparent 6px 12px);
}
.sysd-lane .dot {
  position: absolute;
  top: 50%;
  width: 7px;
  height: 7px;
  margin-top: -3.5px;
  border-radius: 50%;
  opacity: 0;
  animation: sysd-flow 2.6s linear infinite;
  animation-delay: var(--d, 0s);
}
.sysd-lane.in .dot { background: var(--vp-c-brand-1); box-shadow: 0 0 7px var(--vp-c-brand-1); }
.sysd-lane.out .dot { background: var(--vp-c-green-1); box-shadow: 0 0 7px var(--vp-c-green-1); }
@keyframes sysd-flow {
  0% { left: 0; opacity: 0; }
  15% { opacity: 1; }
  85% { opacity: 1; }
  100% { left: calc(100% - 7px); opacity: 0; }
}

/* core: outer service box (Beacon Data Lake) containing the engine (BeaconDB) */
.sysd-core { align-self: center; }

.sysd-lake {
  position: relative;
  border: 1px solid color-mix(in srgb, var(--vp-c-green-1) 45%, var(--vp-c-divider));
  border-radius: 16px;
  background: color-mix(in srgb, var(--vp-c-green-1) 7%, var(--vp-c-bg-soft));
  padding: 14px 14px 12px;
}

.sysd-lake-head,
.sysd-db-head {
  display: flex;
  align-items: center;
  gap: 7px;
  margin-bottom: 9px;
}
.sysd-lake-ico { display: inline-flex; color: var(--vp-c-green-1); }
.sysd-db-ico { display: inline-flex; color: var(--vp-c-brand-1); }
.sysd-lake-name,
.sysd-db-name {
  font-size: 15px;
  font-weight: 800;
  letter-spacing: -0.01em;
  color: var(--vp-c-text-1);
  white-space: nowrap;
}
.sysd-tag {
  margin-left: auto;
  padding: 2px 7px;
  border-radius: 999px;
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}
.sysd-tag.lake {
  color: var(--vp-c-green-1);
  background: color-mix(in srgb, var(--vp-c-green-1) 14%, transparent);
}
.sysd-tag.db {
  color: var(--vp-c-brand-1);
  background: color-mix(in srgb, var(--vp-c-brand-1) 14%, transparent);
}

/* service capabilities the server layer adds */
.sysd-services {
  display: flex;
  flex-wrap: wrap;
  gap: 4px;
  margin-bottom: 11px;
}
.sysd-services span {
  padding: 3px 8px;
  border: 1px solid color-mix(in srgb, var(--vp-c-green-1) 25%, var(--vp-c-divider));
  border-radius: 999px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-2);
  font-size: 10.5px;
  font-weight: 600;
  line-height: 1.4;
}

/* the nested engine */
.sysd-db {
  border: 1px solid color-mix(in srgb, var(--vp-c-brand-1) 45%, var(--vp-c-divider));
  border-radius: 12px;
  background: color-mix(in srgb, var(--vp-c-brand-soft) 55%, var(--vp-c-bg));
  padding: 12px 13px 11px;
  animation: sysd-glow 3.2s ease-in-out infinite;
}
@keyframes sysd-glow {
  0%, 100% { box-shadow: 0 0 0 0 color-mix(in srgb, var(--vp-c-brand-1) 22%, transparent); }
  50% { box-shadow: 0 0 0 5px color-mix(in srgb, var(--vp-c-brand-1) 10%, transparent); }
}
.sysd-db-sub {
  margin: 0;
  font-size: 11.5px;
  line-height: 1.5;
  color: var(--vp-c-text-2);
}
.sysd-db-sub code {
  padding: 0 3px;
  border-radius: 4px;
  background: var(--vp-c-bg-soft);
  font-size: 11px;
}

.sysd-nest-note {
  margin: 9px 0 0;
  text-align: center;
  font-size: 10.5px;
  font-style: italic;
  color: var(--vp-c-text-3);
}

.sysd-cap-foot {
  margin: 1.1rem 0 0;
  text-align: center;
  font-size: 12.5px;
  color: var(--vp-c-text-3);
}

@media (prefers-reduced-motion: reduce) {
  .sysd-lane .dot { display: none; }
  .sysd-core { animation: none; }
}

@media (max-width: 820px) {
  .sysd-grid { grid-template-columns: 1fr; gap: 1rem; }
  .sysd-lane { min-height: 34px; }
  .sysd-lane::before {
    left: 50%;
    right: auto;
    top: 4px;
    bottom: 4px;
    height: auto;
    width: 2px;
    transform: translateX(-1px);
    background: repeating-linear-gradient(180deg, var(--vp-c-divider) 0 6px, transparent 6px 12px);
  }
  .sysd-lane .dot { left: 50% !important; margin-left: -3.5px; animation-name: sysd-flow-v; }
  @keyframes sysd-flow-v {
    0% { top: 0; opacity: 0; }
    15% { opacity: 1; }
    85% { opacity: 1; }
    100% { top: calc(100% - 7px); opacity: 0; }
  }
}
</style>
