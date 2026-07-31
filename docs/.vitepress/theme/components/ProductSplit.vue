<script setup>
import { withBase } from 'vitepress'
import Icon from './Icon.vue'

const logo = withBase('/beacon-logo-small.png')

const products = [
  {
    key: 'db',
    eyebrow: 'Embed it',
    icon: 'database',
    name: 'BeaconDB',
    lede: 'The engine, in-process.',
    body: 'An analytical query engine that runs inside your Python process, with no server to stand up. Everything it owns lives in one portable beacon.db file you can query from a notebook or ship inside an app.',
    cmd: 'pip install beacondb',
    points: [
      'In-process, no server to run',
      'One portable beacon.db file',
      'Python binding today, more coming',
      'Arrow-native, streaming results',
    ],
    // BeaconDB only exists in 2.0.0, so this necessarily lands on the
    // pre-release docs (which carry the pre-release banner).
    link: withBase('/docs/2.0.0-rc1/beacondb/'),
    linkText: 'Explore BeaconDB',
  },
  {
    key: 'lake',
    eyebrow: 'Serve it',
    icon: 'server',
    name: 'Beacon Data Lake',
    lede: 'The same engine, as a service.',
    body: 'The exact same engine behind an HTTP + Arrow Flight SQL service. Add a managed dataset store, crawlers, role-based access control, a web admin UI, and client SDKs. One lakehouse for your whole team.',
    cmd: 'docker pull ghcr.io/maris-development/beacon',
    points: [
      'HTTP + Arrow Flight SQL',
      'Role-based access control & admin UI',
      'Crawlers & managed dataset store',
      'Export to Parquet, NetCDF, CSV & ODV',
    ],
    // Data Lake ships in the stable release, so point at it rather than the RC.
    link: withBase('/docs/1.8.0/getting-started'),
    linkText: 'Run the server',
  },
]
</script>

<template>
  <section class="psplit">
    <p class="psplit-kicker">Two ways to run Beacon</p>
    <h2 class="psplit-title">Embed the engine, or serve it</h2>
    <p class="psplit-sub">
      One engine, two ways to run it. Pick the one that fits, and switch without rewriting a query.
    </p>

    <div class="psplit-grid">
      <article
        v-for="p in products"
        :key="p.key"
        :class="['pcard', 'accent-' + p.key]"
      >
        <div class="pcard-head">
          <span class="pcard-icon"><Icon :name="p.icon" :size="22" /></span>
          <span class="pcard-eyebrow">{{ p.eyebrow }}</span>
        </div>

        <h3 class="pcard-name">{{ p.name }}</h3>
        <p class="pcard-lede">{{ p.lede }}</p>
        <p class="pcard-body">{{ p.body }}</p>

        <code class="pcard-cmd">{{ p.cmd }}</code>

        <ul class="pcard-points">
          <li v-for="pt in p.points" :key="pt">{{ pt }}</li>
        </ul>

        <a class="pcard-link" :href="p.link">{{ p.linkText }} <span aria-hidden="true">→</span></a>
      </article>
    </div>

    <p class="psplit-foot">
      <img class="psplit-logo" :src="logo" alt="" />
      Same SQL dialect, same formats, same <code>beacon.db</code> file, whether embedded or served.
    </p>
  </section>
</template>

<style scoped>
.psplit {
  margin: 3.5rem auto 1rem;
  max-width: 1000px;
  padding: 0 1.5rem;
  text-align: center;
}

.psplit-kicker {
  margin: 0 0 0.6rem;
  color: var(--vp-c-text-3);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.11em;
  line-height: 1;
  text-transform: uppercase;
}

.psplit-title {
  margin: 0 0 0.6rem;
  font-size: 1.9rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  line-height: 1.2;
  color: var(--vp-c-text-1);
  border: 0;
}

.psplit-sub {
  margin: 0 auto 2.25rem;
  max-width: 560px;
  color: var(--vp-c-text-2);
  font-size: 1.05rem;
  line-height: 1.5;
}

.psplit-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 1.25rem;
}

.pcard {
  position: relative;
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
.pcard:hover {
  transform: translateY(-4px);
  box-shadow: 0 16px 36px rgba(0, 0, 0, 0.12);
}
.accent-db {
  border-top-color: var(--vp-c-brand-1);
}
.accent-db:hover {
  border-color: color-mix(in srgb, var(--vp-c-brand-1) 55%, var(--vp-c-divider));
  border-top-color: var(--vp-c-brand-1);
}
.accent-lake {
  border-top-color: var(--vp-c-green-1);
}
.accent-lake:hover {
  border-color: color-mix(in srgb, var(--vp-c-green-1) 55%, var(--vp-c-divider));
  border-top-color: var(--vp-c-green-1);
}

.pcard-head {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 14px;
}
.pcard-icon {
  display: inline-flex;
  align-items: center;
  justify-content: center;
  width: 42px;
  height: 42px;
  border-radius: 11px;
  border: 1px solid var(--vp-c-divider);
}
.accent-db .pcard-icon {
  color: var(--vp-c-brand-1);
  background: color-mix(in srgb, var(--vp-c-brand-1) 12%, transparent);
  border-color: color-mix(in srgb, var(--vp-c-brand-1) 30%, var(--vp-c-divider));
}
.accent-lake .pcard-icon {
  color: var(--vp-c-green-1);
  background: color-mix(in srgb, var(--vp-c-green-1) 12%, transparent);
  border-color: color-mix(in srgb, var(--vp-c-green-1) 30%, var(--vp-c-divider));
}
.pcard-eyebrow {
  color: var(--vp-c-text-3);
  font-size: 0.72rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}

.pcard-name {
  margin: 0 0 0.2rem;
  font-size: 1.5rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: var(--vp-c-text-1);
  border: 0;
}
.accent-db .pcard-name { color: var(--vp-c-brand-1); }
.accent-lake .pcard-name { color: var(--vp-c-green-1); }

.pcard-lede {
  margin: 0 0 0.85rem;
  font-size: 1rem;
  font-weight: 600;
  color: var(--vp-c-text-1);
}

/* The two cards stretch to equal height, and every row except this one is the
   same height in both. Letting the body absorb the slack keeps the install
   command (and everything below it) on the same baseline across both cards. */
.pcard-body {
  flex: 1 1 auto;
  margin: 0 0 1.1rem;
  color: var(--vp-c-text-2);
  font-size: 0.92rem;
  line-height: 1.6;
}

.pcard-cmd {
  display: block;
  margin-bottom: 1.15rem;
  padding: 0.6rem 0.85rem;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-size: 0.8rem;
  overflow-x: auto;
  white-space: nowrap;
}

.pcard-points {
  list-style: none;
  margin: 0 0 1.4rem;
  padding: 0;
  display: grid;
  gap: 0.5rem;
}
.pcard-points li {
  position: relative;
  padding-left: 1.4rem;
  color: var(--vp-c-text-2);
  font-size: 0.88rem;
  line-height: 1.4;
}
.pcard-points li::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0.35rem;
  width: 0.6rem;
  height: 0.6rem;
  border-radius: 50%;
}
.accent-db .pcard-points li::before {
  background: color-mix(in srgb, var(--vp-c-brand-1) 30%, transparent);
  border: 1.5px solid var(--vp-c-brand-1);
}
.accent-lake .pcard-points li::before {
  background: color-mix(in srgb, var(--vp-c-green-1) 30%, transparent);
  border: 1.5px solid var(--vp-c-green-1);
}

/* No `margin-top: auto` here: an auto margin would absorb the card's free space
   before flex-grow is applied, cancelling the body's alignment above. */
.pcard-link {
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--vp-c-brand-1);
  transition: gap 0.2s;
}
.accent-lake .pcard-link { color: var(--vp-c-green-1); }
.pcard-link span {
  display: inline-block;
  transition: transform 0.2s ease;
}
.pcard-link:hover span { transform: translateX(3px); }

.psplit-foot {
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 0.5rem;
  margin: 1.75rem 0 0;
  color: var(--vp-c-text-3);
  font-size: 0.85rem;
}
.psplit-foot code {
  padding: 0.1rem 0.4rem;
  border-radius: 5px;
  background: var(--vp-c-bg-soft);
  font-size: 0.8rem;
}
.psplit-logo {
  width: 18px;
  height: 18px;
}

@media (max-width: 720px) {
  .psplit-grid {
    grid-template-columns: 1fr;
    max-width: 420px;
    margin-inline: auto;
  }
  .psplit-title { font-size: 1.6rem; }
}
</style>
