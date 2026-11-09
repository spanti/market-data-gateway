import path from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

const dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, dirname, "");
  const apiTarget = env.VITE_DEV_API_TARGET || "http://127.0.0.1:8080";
  const collectorOpsTarget =
    env.VITE_DEV_COLLECTOR_OPS_TARGET || "http://127.0.0.1:8081";
  const replayOpsTarget =
    env.VITE_DEV_REPLAY_OPS_TARGET || "http://127.0.0.1:8082";
  const prometheusTarget =
    env.VITE_DEV_PROMETHEUS_TARGET || "http://127.0.0.1:9090";

  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        "@": path.resolve(dirname, "./src"),
      },
    },
    server: {
      host: "127.0.0.1",
      port: 5173,
      proxy: {
        // Current Contract REST + WebSocket + ops endpoints (same origin).
        "/api": { target: apiTarget, changeOrigin: true },
        "/healthz": { target: apiTarget, changeOrigin: true },
        "/readyz": { target: apiTarget, changeOrigin: true },
        "/status": { target: apiTarget, changeOrigin: true },
        "/metrics": { target: apiTarget, changeOrigin: true },
        "/openapi.json": { target: apiTarget, changeOrigin: true },
        "/docs": { target: apiTarget, changeOrigin: true },
        "/ws": {
          target: apiTarget.replace(/^http/, "ws"),
          ws: true,
          changeOrigin: true,
        },
        // Per-Service-Role ops ports. Prefixes are frontend dev-proxy
        // conventions; the backend itself serves these at "/" on 8081/8082.
        "/ops/collector": {
          target: collectorOpsTarget,
          changeOrigin: true,
          rewrite: (requestPath) =>
            requestPath.replace(/^\/ops\/collector/, ""),
        },
        "/ops/replay": {
          target: replayOpsTarget,
          changeOrigin: true,
          rewrite: (requestPath) => requestPath.replace(/^\/ops\/replay/, ""),
        },
        // Current Contract intranet-development Prometheus access only.
        // Admin API mode uses the authenticated monitoring proxy instead.
        "/prometheus": {
          target: prometheusTarget,
          changeOrigin: true,
          rewrite: (requestPath) => requestPath.replace(/^\/prometheus/, ""),
        },
      },
    },
    build: {
      target: "es2022",
      sourcemap: false,
      rollupOptions: {
        output: {
          manualChunks: {
            "vendor-react": [
              "react",
              "react-dom",
              "react-router-dom",
              "@tanstack/react-query",
            ],
            "vendor-radix": [
              "@radix-ui/react-dialog",
              "@radix-ui/react-label",
              "@radix-ui/react-slot",
              "@radix-ui/react-tabs",
              "@radix-ui/react-tooltip",
            ],
            "vendor-http": ["axios"],
            "vendor-schema": ["zod"],
            "vendor-echarts": [
              "echarts/core",
              "echarts/charts",
              "echarts/components",
              "echarts/renderers",
            ],
          },
        },
      },
    },
    test: {
      environment: "jsdom",
      setupFiles: ["./src/test/setup.ts"],
      css: false,
      restoreMocks: true,
      clearMocks: true,
      unstubEnvs: true,
      unstubGlobals: true,
    },
  };
});
