import { defineConfig, type UserConfig } from "vite";
import { qwikVite } from "@builder.io/qwik/optimizer";
import { qwikCity } from "@builder.io/qwik-city/vite";
import tsconfigPaths from "vite-tsconfig-paths";

// The tracker serves the REST API and the WebSocket feed; in dev, Vite proxies
// both to it so the UI talks to a real backend on the same origin.
const tracker = process.env.TRACKER_URL ?? "http://localhost:8080";

export default defineConfig((): UserConfig => {
  return {
    plugins: [
      qwikCity(),
      qwikVite({ client: { outDir: "../api/internal/webui/dist" } }),
      tsconfigPaths({ root: "." }),
    ],
    server: {
      headers: { "Cache-Control": "public, max-age=0" },
      proxy: {
        "/api": tracker,
        "/ws": { target: tracker.replace(/^http/, "ws"), ws: true },
      },
    },
  };
});
