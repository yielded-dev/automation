import { globSync } from "node:fs";
import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    // Stable entry order keeps committed output identical across filesystems.
    entry: globSync("src/**/*.ts").sort(),
    unbundle: true,
    root: "src",
    platform: "node",
    dts: false,
    publint: true,
  },
  fmt: { ignorePatterns: ["**/dist/**"] },
  lint: { ignorePatterns: ["**/dist/**"] },
});
