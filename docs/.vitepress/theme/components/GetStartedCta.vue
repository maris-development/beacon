<script setup>
import { withBase } from 'vitepress'
const start = withBase('/docs/1.7.3/getting-started')
</script>

<template>
  <div class="cta">
    <div class="cta-inner">
      <h2 class="cta-title">Get started in minutes</h2>
      <p class="cta-sub">
        Run Beacon with Docker, point it at your files, and query over SQL or JSON.
      </p>
      <code class="cta-code">docker pull ghcr.io/maris-development/beacon:latest</code>
      <div class="cta-actions">
        <a class="cta-btn brand" :href="start">Get started</a>
        <a class="cta-btn alt" href="https://github.com/maris-development/beacon"
           target="_blank" rel="noopener">View on GitHub</a>
      </div>
    </div>
  </div>
</template>

<style scoped>
.cta {
  margin: 4rem auto 1rem;
  padding: 0 1.5rem;
}
.cta-inner {
  max-width: 720px;
  margin: 0 auto;
  padding: 2.5rem 1.5rem;
  text-align: center;
  border: 1px solid var(--vp-c-divider);
  border-radius: 16px;
  background: color-mix(in srgb, var(--vp-c-brand-soft) 50%, transparent);
}
.cta-title {
  margin: 0 0 0.5rem;
  font-size: 1.7rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  line-height: 1.2;
  color: var(--vp-c-text-1);
}
.cta-sub {
  margin: 0 auto 1.4rem;
  max-width: 460px;
  color: var(--vp-c-text-2);
  font-size: 1rem;
  line-height: 1.5;
}
.cta-code {
  display: inline-block;
  margin-bottom: 1.6rem;
  padding: 0.6rem 1rem;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  font-family: var(--vp-font-family-mono);
  font-size: 0.85rem;
}
.cta-actions {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  justify-content: center;
}
.cta-btn {
  display: inline-block;
  border-radius: 20px;
  padding: 0 22px;
  line-height: 38px;
  font-size: 14px;
  font-weight: 600;
  transition: background-color 0.2s, border-color 0.2s, color 0.2s;
}
.cta-btn.brand {
  background: var(--vp-c-brand-3);
  color: #fff;
  border: 1px solid var(--vp-c-brand-3);
}
.cta-btn.brand:hover {
  background: var(--vp-c-brand-2);
  border-color: var(--vp-c-brand-2);
}
.cta-btn.alt {
  background: var(--vp-c-bg);
  color: var(--vp-c-text-1);
  border: 1px solid var(--vp-c-divider);
}
.cta-btn.alt:hover { border-color: var(--vp-c-brand-1); color: var(--vp-c-brand-1); }

@media (max-width: 640px) {
  .cta-code { font-size: 0.74rem; word-break: break-all; }
}
</style>
