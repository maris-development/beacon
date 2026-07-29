<script setup>
import { withBase } from 'vitepress'
import Icon from './Icon.vue'

const paths = [
  {
    key: 'db',
    eyebrow: 'Embed it',
    icon: 'database',
    name: 'BeaconDB',
    blurb: 'Run the engine in-process, straight from Python.',
    cmd: 'pip install beacondb',
    cta: 'Get started with BeaconDB',
    // BeaconDB only exists in 2.0.0, so this necessarily lands on the
    // pre-release docs (which carry the pre-release banner).
    link: withBase('/docs/2.0.0-rc1/beacondb/python/getting-started'),
  },
  {
    key: 'lake',
    eyebrow: 'Serve it',
    icon: 'server',
    name: 'Beacon Data Lake',
    blurb: 'Run the server and share one lakehouse with your team.',
    cmd: 'docker pull ghcr.io/maris-development/beacon:latest',
    cta: 'Run the server',
    // Data Lake ships in the stable release, so point at it rather than the RC.
    link: withBase('/docs/1.8.0/getting-started'),
  },
]
</script>

<template>
  <section class="cta">
    <h2 class="cta-title">Get started in minutes</h2>
    <p class="cta-sub">Two ways to run it. Same SQL either way, so you can switch later.</p>

    <div class="cta-grid">
      <article
        v-for="p in paths"
        :key="p.key"
        :class="['cta-card', 'accent-' + p.key]"
      >
        <div class="cta-card-head">
          <span class="cta-icon"><Icon :name="p.icon" :size="22" /></span>
          <span class="cta-eyebrow">{{ p.eyebrow }}</span>
        </div>

        <h3 class="cta-name">{{ p.name }}</h3>
        <p class="cta-blurb">{{ p.blurb }}</p>

        <code class="cta-code">{{ p.cmd }}</code>

        <a class="cta-btn" :href="p.link">{{ p.cta }} <span aria-hidden="true">→</span></a>
      </article>
    </div>

    <p class="cta-foot">
      Free and open source under AGPL-3.0.
      <a href="https://github.com/maris-development/beacon" target="_blank" rel="noopener">View on GitHub</a>
    </p>
  </section>
</template>

<style scoped>
.cta {
  margin: 4rem auto 1rem;
  max-width: 1000px;
  padding: 0 1.5rem;
  text-align: center;
}

.cta-title {
  margin: 0 0 0.5rem;
  font-size: 1.9rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  line-height: 1.2;
  color: var(--vp-c-text-1);
  border: 0;
}

.cta-sub {
  margin: 0 auto 2.25rem;
  max-width: 540px;
  color: var(--vp-c-text-2);
  font-size: 1.05rem;
  line-height: 1.5;
}

.cta-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1.25rem;
}

.cta-card {
  display: flex;
  flex-direction: column;
  border: 1px solid var(--vp-c-divider);
  border-top: 4px solid var(--vp-c-divider);
  border-radius: 16px;
  background: var(--vp-c-bg-soft);
  padding: 26px 26px 24px;
  text-align: left;
  transition: transform 0.25s ease, box-shadow 0.25s ease, border-color 0.25s ease;
}
.cta-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 16px 36px rgba(0, 0, 0, 0.12);
}
.accent-db { border-top-color: var(--vp-c-brand-1); }
.accent-db:hover {
  border-color: color-mix(in srgb, var(--vp-c-brand-1) 55%, var(--vp-c-divider));
  border-top-color: var(--vp-c-brand-1);
}
.accent-lake { border-top-color: var(--vp-c-green-1); }
.accent-lake:hover {
  border-color: color-mix(in srgb, var(--vp-c-green-1) 55%, var(--vp-c-divider));
  border-top-color: var(--vp-c-green-1);
}

.cta-card-head {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
}
.cta-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 42px;
  height: 42px;
  border-radius: 11px;
  border: 1px solid var(--vp-c-divider);
}
.accent-db .cta-icon {
  color: var(--vp-c-brand-1);
  background: color-mix(in srgb, var(--vp-c-brand-1) 12%, transparent);
  border-color: color-mix(in srgb, var(--vp-c-brand-1) 30%, var(--vp-c-divider));
}
.accent-lake .cta-icon {
  color: var(--vp-c-green-1);
  background: color-mix(in srgb, var(--vp-c-green-1) 12%, transparent);
  border-color: color-mix(in srgb, var(--vp-c-green-1) 30%, var(--vp-c-divider));
}
.cta-eyebrow {
  color: var(--vp-c-text-3);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

.cta-name {
  margin: 0 0 0.3rem;
  font-size: 1.4rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  border: 0;
}
.accent-db .cta-name { color: var(--vp-c-brand-1); }
.accent-lake .cta-name { color: var(--vp-c-green-1); }

.cta-blurb {
  margin: 0 0 1.15rem;
  color: var(--vp-c-text-2);
  font-size: 0.95rem;
  line-height: 1.55;
}

.cta-code {
  display: block;
  margin-bottom: 1.4rem;
  padding: 0.65rem 0.9rem;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-size: 0.8rem;
  overflow-x: auto;
  white-space: nowrap;
}

.cta-btn {
  margin-top: auto;
  align-self: flex-start;
  display: inline-block;
  border-radius: 20px;
  padding: 0 20px;
  line-height: 40px;
  font-size: 14px;
  font-weight: 600;
  color: #fff;
  transition: filter 0.2s, transform 0.2s;
}
.cta-btn span {
  display: inline-block;
  transition: transform 0.2s ease;
}
.cta-btn:hover span { transform: translateX(3px); }
.accent-db .cta-btn { background: var(--vp-c-brand-3); }
.accent-db .cta-btn:hover { filter: brightness(1.08); }
.accent-lake .cta-btn { background: var(--vp-c-green-2, var(--vp-c-green-1)); }
.accent-lake .cta-btn:hover { filter: brightness(1.08); }

.cta-foot {
  margin: 1.75rem 0 0;
  color: var(--vp-c-text-3);
  font-size: 0.9rem;
}
.cta-foot a {
  color: var(--vp-c-brand-1);
  font-weight: 600;
}
.cta-foot a:hover { text-decoration: underline; }

@media (max-width: 720px) {
  .cta-grid {
    grid-template-columns: 1fr;
    max-width: 420px;
    margin-inline: auto;
  }
  .cta-title { font-size: 1.6rem; }
}
</style>
