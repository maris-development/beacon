import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import { fileURLToPath, URL } from "node:url";

// https://vite.dev/config/
export default defineConfig(({ command }) => ({
  // Production builds are served by the Beacon server at `{base_path}/admin`.
  // `base_path` is a server setting, so the build cannot write the prefix into
  // the asset URLs. Relative URLs plus the `<base>` tag that `index.html` writes
  // at run time resolve to the right prefix instead. The dev server keeps the
  // SPA at the root, where absolute URLs are correct.
  base: command === "build" ? "./" : "/",
  plugins: [react()],
  resolve: {
    alias: {
      "@": fileURLToPath(new URL("./src", import.meta.url)),
    },
  },
  // Monaco is imported as many deep modules that share one registry of editor
  // contributions. Pre-bundling them into separate optimized chunks gives each
  // chunk its own copy of that registry, so features register into one instance
  // while the editor reads another — the suggest widget simply never appears.
  // Serving them unbundled keeps a single instance in dev; the production build
  // shares the graph anyway.
  optimizeDeps: {
    exclude: ["monaco-editor"],
  },
  server: {
    port: 5173,
  },
}));
