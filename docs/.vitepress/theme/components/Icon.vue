<script setup>
import { computed } from 'vue'
import { iconPaths } from '../icons.js'

const props = defineProps({
  name: { type: String, required: true },
  size: { type: [Number, String], default: 24 },
})

const inner = computed(() => iconPaths[props.name] || '')
</script>

<template>
  <svg
    class="licon"
    xmlns="http://www.w3.org/2000/svg"
    :width="size"
    :height="size"
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    stroke-width="2"
    stroke-linecap="round"
    stroke-linejoin="round"
    aria-hidden="true"
    v-html="inner"
  />
</template>

<style scoped>
.licon {
  display: inline-block;
  vertical-align: middle;
  flex: none;
}
</style>
