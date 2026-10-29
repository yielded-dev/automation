/**
 * Source identity of the shared sandbox-agent container image. The Dockerfile
 * and package manifests are checked against these values by repository static
 * tooling; hosts import them instead of carrying private copies.
 */
export const SANDBOX_PACKAGE_VERSION = "1.0.3" as const;
export const CODEX_VERSION = "0.157.1" as const;
export const GITHUB_CLI_VERSION = "2.99.0" as const;
export const PLAYWRIGHT_CLI_VERSION = "0.1.18" as const;
export const VITE_PLUS_VERSION = "1.1.0" as const;

export const SANDBOX_RUNTIME_IMAGE_IDENTITY =
  `sandbox=${SANDBOX_PACKAGE_VERSION};codex=${CODEX_VERSION};gh=${GITHUB_CLI_VERSION};playwright-cli=${PLAYWRIGHT_CLI_VERSION};vite-plus=${VITE_PLUS_VERSION}` as const;
