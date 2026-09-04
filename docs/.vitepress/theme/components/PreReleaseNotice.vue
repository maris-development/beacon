<script setup>
import { computed } from 'vue'
import { useRoute, withBase } from 'vitepress'
import { LATEST_VERSION, LATEST_ENTRY } from '../version.js'

// Versions whose docs are published but not released. Keep in sync with the
// version dropdown label in config.mts. `route.path` carries the site base, so
// match on the version segment rather than a full path.
const PRE_RELEASE = ['2.0.0-rc5']

const route = useRoute()

const version = computed(() =>
  PRE_RELEASE.find((v) => route.path.includes(`/docs/${v}/`))
)

const stableHref = withBase(`/docs/${LATEST_VERSION}/${LATEST_ENTRY}`)
</script>

<template>
  <div v-if="version" class="custom-block warning prerelease-notice">
    <p class="custom-block-title">Pre-release documentation</p>
    <p>
      This describes Beacon <strong>{{ version }}</strong>, a release candidate.
      Behavior documented here may still change before 2.0.0 ships, and some of
      it is not in any released build yet. For the current stable release, see
      the <a :href="stableHref">{{ LATEST_VERSION }} documentation</a>.
    </p>
  </div>
</template>

<style scoped>
/* This renders in the `doc-before` slot, so the page <h1> follows immediately
   and VitePress zeroes that heading's margin-top. Without an explicit gap here
   the only separation is .custom-block's default 16px, which reads as cramped. */
.prerelease-notice {
  margin: 0 0 2rem;
}
</style>
