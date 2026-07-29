import { defineConfig } from "tsup";

export default defineConfig({
  entry: ["src/index.ts"],
  format: ["esm", "cjs"],
  dts: true,
  clean: true,
  sourcemap: true,
  treeshake: true,
  // Loaded lazily at runtime; keep them out of the bundle.
  external: ["apache-arrow", "fzstd"],
});
