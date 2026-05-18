<template>
  <div class="format-badges-wrap">
    <p class="format-badges-label">Supported formats</p>

    <div class="format-badges">
      <span v-for="format in formats" :key="format">{{ format }}</span>
    </div>
  </div>
</template>

<script setup>
const formats = ['Zarr', 'Parquet', 'TIFF', 'NetCDF' , 'CSV', 'ODV', 'Arrow']
</script>