//#region src/NativeExecutionLimits.ts
/** Retain accepted execution receipts until their boot closes; reject new work at capacity. */
const NATIVE_OPERATION_LIMIT = 128;
/** Maximum retained bytes for each output stream of an accepted operation. */
const NATIVE_OUTPUT_LIMIT_BYTES = 16777216;
/** Codex and preparation listeners use separately bounded recovery capacity. */
const NATIVE_LISTENER_PORTS = [4500, 4502];
//#endregion
export { NATIVE_LISTENER_PORTS, NATIVE_OPERATION_LIMIT, NATIVE_OUTPUT_LIMIT_BYTES };
