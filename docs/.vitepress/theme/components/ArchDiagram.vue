<script setup>
import { withBase } from 'vitepress'

const logo = withBase('/beacon-logo-small.png')

const cards = [
  {
    key: 'cloud',
    icon: '☁️',
    title: 'Cloud (AWS)',
    nodes: [
      { ico: '💻', name: 'Jupyter', sub: 'remote notebook' },
      { logo: true, name: 'Beacon', sub: 'on EC2' },
      { ico: '🪣', name: 'S3 Bucket', sub: 'object storage' },
    ],
    links: ['SQL / Flight SQL', 'reads via S3 API'],
    foot: 'Managed cloud — Beacon on EC2, data in S3.',
  },
  {
    key: 'onprem',
    icon: '🖥️',
    title: 'On-premise',
    nodes: [
      { ico: '💻', name: 'Jupyter', sub: 'remote notebook' },
      { logo: true, name: 'Beacon', sub: 'your server' },
      { ico: '💾', name: 'Local disk', sub: 'NetCDF · Parquet' },
    ],
    links: ['SQL / Flight SQL', 'reads local files'],
    foot: 'Self-hosted — Beacon and data on one server.',
  },
  {
    key: 'local',
    icon: '💻',
    title: 'Local',
    nodes: [
      { ico: '📓', name: 'Jupyter', sub: 'same machine' },
      { logo: true, name: 'Beacon', sub: 'localhost:5001' },
      { ico: '💾', name: 'Local files', sub: 'on disk' },
    ],
    links: ['localhost', 'reads local files'],
    foot: 'All on one machine — ideal for development.',
  },
]
</script>

<template>
  <div class="arch">
    <p class="arch-title">Deploy Beacon anywhere</p>

    <div class="depcards">
      <article v-for="c in cards" :key="c.key" :class="['depcard', 'accent-' + c.key]">
        <div class="depcard-head"><span class="dh-ico">{{ c.icon }}</span>{{ c.title }}</div>

        <div class="depflow">
          <template v-for="(n, i) in c.nodes" :key="i">
            <div class="dep-node">
              <img v-if="n.logo" class="dn-logo" :src="logo" alt="" />
              <span v-else class="dn-ico">{{ n.ico }}</span>
              <span class="dn-text"><b>{{ n.name }}</b><small>{{ n.sub }}</small></span>
            </div>
            <div v-if="i < c.nodes.length - 1" class="dep-link">
              <span class="dl-line"><i class="dl-dot"></i></span>
              <em>{{ c.links[i] }}</em>
            </div>
          </template>
        </div>

        <p class="depcard-foot">{{ c.foot }}</p>
      </article>
    </div>
  </div>
</template>

<style scoped>
.arch {
  margin: 3.75rem auto 1rem;
  max-width: 1000px;
  padding: 0 1.5rem;
  text-align: center;
}

.arch-title {
  margin: 0 0 1.5rem;
  color: var(--vp-c-text-3);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.11em;
  line-height: 1;
  text-transform: uppercase;
}

.depcards {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 1rem;
}

.depcard {
  border: 1px solid var(--vp-c-divider);
  border-top: 4px solid var(--vp-c-divider);
  border-radius: 14px;
  background: var(--vp-c-bg-soft);
  padding: 22px 20px 20px;
  text-align: left;
}
.accent-cloud  { border-top-color: var(--vp-c-brand-1); }
.accent-onprem { border-top-color: var(--vp-c-green-1); }
.accent-local  { border-top-color: var(--vp-c-yellow-1, var(--vp-c-brand-1)); }

.depcard-head {
  display: flex;
  align-items: center;
  gap: 10px;
  margin-bottom: 18px;
  font-size: 16px;
  font-weight: 700;
  color: var(--vp-c-text-1);
}
.dh-ico { font-size: 18px; }

.depflow {
  display: flex;
  flex-direction: column;
}

.dep-node {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 11px 13px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 10px;
  background: var(--vp-c-bg);
}
.dn-ico {
  width: 28px;
  text-align: center;
  font-size: 20px;
  flex: none;
}
.dn-logo {
  width: 26px;
  height: 26px;
  flex: none;
}
.dn-text { display: flex; flex-direction: column; line-height: 1.3; min-width: 0; }
.dn-text b { font-size: 14.5px; color: var(--vp-c-text-1); }
.dn-text small { font-size: 12px; color: var(--vp-c-text-3); }

/* vertical connector + label, aligned under the node icon column */
.dep-link {
  display: flex;
  align-items: center;
  gap: 10px;
  padding: 3px 0 3px 26px;
}
.dl-line {
  position: relative;
  width: 2px;
  height: 28px;
  background: var(--vp-c-divider);
  flex: none;
}
.dl-dot {
  position: absolute;
  left: -1.5px;
  top: -5px;
  width: 5px;
  height: 5px;
  border-radius: 50%;
  background: var(--vp-c-brand-1);
  animation: dep-down 2.4s linear infinite;
}
@keyframes dep-down {
  0%   { top: -5px; opacity: 0; }
  15%  { opacity: 1; }
  85%  { opacity: 1; }
  100% { top: 28px; opacity: 0; }
}
.dep-link em {
  font-style: normal;
  font-size: 11.5px;
  color: var(--vp-c-text-3);
}

.depcard-foot {
  margin: 18px 0 0;
  font-size: 12.5px;
  line-height: 1.5;
  color: var(--vp-c-text-2);
}

@media (prefers-reduced-motion: reduce) {
  .dl-dot { display: none; }
}

@media (max-width: 720px) {
  .depcards { grid-template-columns: 1fr; max-width: 380px; margin-inline: auto; }
}
</style>
