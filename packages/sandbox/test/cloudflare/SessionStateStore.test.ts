import { Schema } from "effect";
import { expect, it, vi } from "vite-plus/test";

import { StoredPipelineTurn } from "../../src/cloudflare/SessionStateStore";

// The persisted schema is independent of Cloudflare storage and namespace bindings.
vi.mock("effect-cf", () => ({ DurableObjectNamespace: {}, DurableObjectState: {} }));

it("accepts finite retained start times and rejects non-finite persisted timestamps", () => {
  const turn = {
    id: "turn",
    materializationId: "materialization",
    head: null,
    baseHead: null,
    resumeToken: null,
    harnessVersion: null,
    startedAtMillis: 1_000,
    persisted: null,
  };
  const decode = Schema.decodeUnknownSync(StoredPipelineTurn);
  expect(decode(turn)).toEqual(turn);
  for (const startedAtMillis of [NaN, Infinity, -Infinity]) {
    expect(() => decode({ ...turn, startedAtMillis })).toThrow();
  }
});
