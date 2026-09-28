import { defineConfig } from "vite";
import { resolve } from "node:path";

export default defineConfig({
  base: "./",
  clearScreen: false,
  server: {
    strictPort: true,
    port: 1420,
  },
  build: {
    rollupOptions: {
      input: {
        app: resolve(process.cwd(), "index.html"),
        landing: resolve(process.cwd(), "landing.html"),
      },
    },
  },
});
