<template>
  <div class="feat-intro">
    <h2 class="feat-intro-title">Built for scientific data</h2>
    <p class="feat-intro-sub">
      Serve your existing NetCDF, Zarr, Parquet and more as-is — fast,
      standards-based SQL, fully open source.
    </p>
  </div>
</template>

<style scoped>
.feat-intro {
  max-width: 720px;
  margin: 1rem auto -1rem;
  padding: 0 1.5rem;
  text-align: center;
}
.feat-intro-title {
  margin: 0 0 0.6rem;
  font-size: 1.9rem;
  font-weight: 700;
  letter-spacing: -0.02em;
  line-height: 1.2;
  color: var(--vp-c-text-1);
}
.feat-intro-sub {
  margin: 0;
  color: var(--vp-c-text-2);
  font-size: 1.05rem;
  line-height: 1.5;
}
@media (max-width: 640px) {
  .feat-intro-title { font-size: 1.6rem; }
}
</style>
