/** Retain accepted execution receipts until their boot closes; reject new work at capacity. */
export const NATIVE_OPERATION_LIMIT = 128;

/** Maximum retained bytes for each output stream of an accepted operation. */
export const NATIVE_OUTPUT_LIMIT_BYTES = 16 * 1024 * 1024;

/** Codex and preparation listeners use separately bounded recovery capacity. */
export const NATIVE_LISTENER_PORTS = [4500, 4502] as const;

/** The native Codex websocket is reachable only through its authenticated gateway. */
export const CODEX_NATIVE_PORT = 4501;

/** Authenticated process supervision is never an application preview. */
export const PROCESS_SUPERVISOR_PORT = 4503;
