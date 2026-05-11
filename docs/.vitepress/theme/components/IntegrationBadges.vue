<template>
  <div class="integration-strip">
    <span class="integration-strip-label">Works with</span>

    <span
      v-for="integration in integrations"
      :key="integration"
      class="integration-strip-item"
    >
      {{ integration }}
    </span>
  </div>
</template>

<script setup>
const integrations = [
  'DataGrip',
  'DBeaver',
  'JDBC',
  'Arrow Flight SQL',
  'Python',
  'Dask',
  'Jupyter Notebooks'
]
</script>