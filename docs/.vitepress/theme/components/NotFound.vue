<script setup>
import { ref, onMounted } from 'vue'
import { withBase } from 'vitepress'
import { LATEST_VERSION, LATEST_ENTRY } from '../version.js'

// GitHub Pages serves 404.html for any unknown path, which lets this component
// act as a catch-all rewrite for the `/docs/latest/...` alias:
//   /docs/latest/data-lake  ->  /docs/1.8.0/data-lake
// Anything else renders the normal 404.
const redirecting = ref(false)

onMounted(() => {
  const base = withBase('/')                       // e.g. "/beacon/"
  const prefix = `${base}docs/latest`
  const path = window.location.pathname

  if (path !== prefix && !path.startsWith(`${prefix}/`)) return

  const sub = path.slice(prefix.length).replace(/^\//, '')
  const target =
    `${base}docs/${LATEST_VERSION}/${sub || LATEST_ENTRY}` +
    window.location.search +
    window.location.hash

  redirecting.value = true
  window.location.replace(target)
})

const home = withBase('/')
</script>

<template>
  <div class="nf">
    <template v-if="redirecting">
      <p class="nf-lead">Redirecting to the Beacon {{ LATEST_VERSION }} documentation…</p>
    </template>
    <template v-else>
      <p class="nf-code">404</p>
      <h1 class="nf-title">Page not found</h1>
      <p class="nf-lead">
        The page you are looking for does not exist, or it may have moved in a newer version of the
        documentation.
      </p>
      <p>
        <a class="nf-link" :href="home">Go to the homepage</a>
        <span class="nf-sep">·</span>
        <a class="nf-link" :href="withBase(`/docs/${LATEST_VERSION}/${LATEST_ENTRY}`)">
          Latest docs
        </a>
      </p>
    </template>
  </div>
</template>

<style scoped>
.nf {
  max-width: 640px;
  margin: 0 auto;
  padding: 6rem 1.5rem;
  text-align: center;
}
.nf-code {
  margin: 0;
  font-size: 3rem;
  font-weight: 800;
  line-height: 1;
  color: var(--vp-c-text-3);
}
.nf-title {
  margin: 0.5rem 0 0.75rem;
  font-size: 1.6rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  color: var(--vp-c-text-1);
  border: 0;
}
.nf-lead {
  margin: 0 0 1.25rem;
  color: var(--vp-c-text-2);
  line-height: 1.6;
}
.nf-link {
  color: var(--vp-c-brand-1);
  font-weight: 600;
}
.nf-link:hover { text-decoration: underline; }
.nf-sep { margin: 0 0.6rem; color: var(--vp-c-divider); }
</style>
