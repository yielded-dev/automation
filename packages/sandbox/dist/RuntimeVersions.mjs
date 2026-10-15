//#region src/RuntimeVersions.ts
/**
* Source identity of the shared sandbox-agent container image. The Dockerfile
* and package manifests are checked against these values by repository static
* tooling; hosts import them instead of carrying private copies.
*/
const SANDBOX_PACKAGE_VERSION = "1.0.0";
const CODEX_VERSION = "0.157.1";
const GITHUB_CLI_VERSION = "2.99.0";
const PLAYWRIGHT_CLI_VERSION = "0.1.18";
const VITE_PLUS_VERSION = "1.1.0";
const SANDBOX_RUNTIME_IMAGE_IDENTITY = `sandbox=${SANDBOX_PACKAGE_VERSION};codex=${CODEX_VERSION};gh=${GITHUB_CLI_VERSION};playwright-cli=${PLAYWRIGHT_CLI_VERSION};vite-plus=${VITE_PLUS_VERSION}`;
//#endregion
export { CODEX_VERSION, GITHUB_CLI_VERSION, PLAYWRIGHT_CLI_VERSION, SANDBOX_PACKAGE_VERSION, SANDBOX_RUNTIME_IMAGE_IDENTITY, VITE_PLUS_VERSION };
