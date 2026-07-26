<script setup>
import { onMounted } from 'vue'
import { withBase } from 'vitepress'
import { LATEST_VERSION, latestPath } from '../version.js'

const target = withBase(latestPath())

// Client-side hop. A <meta http-equiv="refresh"> is injected for this page in
// config.mts as the no-JS fallback, so both paths lead to the same place.
onMounted(() => {
  window.location.replace(target)
})
</script>

<template>
  <div class="lredirect">
    <p class="lredirect-lead">Redirecting to the Beacon {{ LATEST_VERSION }} documentation…</p>
    <p>
      <a class="lredirect-link" :href="target">Continue to the {{ LATEST_VERSION }} docs</a>
    </p>
  </div>
</template>

<style scoped>
.lredirect {
  max-width: 640px;
  margin: 0 auto;
  padding: 6rem 1.5rem;
  text-align: center;
}
.lredirect-lead {
  margin: 0 0 0.75rem;
  color: var(--vp-c-text-2);
  font-size: 1.05rem;
}
.lredirect-link {
  color: var(--vp-c-brand-1);
  font-weight: 600;
}
.lredirect-link:hover { text-decoration: underline; }
</style>
