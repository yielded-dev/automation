import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { Effect, Layer, Schema } from "effect";
import { expect, it, vi } from "vite-plus/test";

import { remoteRepairSandbox } from "../../src/server/RemoteSandbox";
import {
  RepairSandboxTaskClient,
  RepairSandboxTaskTransportError,
} from "../../src/server/RepairSandboxTaskClient";

// The remote adapter uses the host port; native Cloudflare bindings are not exercised here.
vi.mock("effect-cf", () => ({ DurableObjectNamespace: {} }));

it("retains one native receipt identity when an operation is retried or reused", async () => {
  const requests: string[] = [];
  const client = Layer.mock(RepairSandboxTaskClient, {
    native: (_id, request, schema) =>
      Effect.gen(function* () {
        requests.push(request.operationId);
        if (requests.length === 1)
          return yield* RepairSandboxTaskTransportError.make({ operation: "lost-reply" });
        const response: unknown = null;
        return yield* Schema.decodeUnknownEffect(schema)(response).pipe(Effect.orDie);
      }),
  });
  await Effect.runPromise(
    Effect.gen(function* () {
      const sandbox = yield* remoteRepairSandbox("task", "source", 1_000);
      const mkdir = sandbox.mkdir("/workspace/proof");
      yield* mkdir.pipe(Effect.retry({ times: 1 }));
      yield* mkdir;
      yield* Effect.all([mkdir, mkdir], { concurrency: "unbounded" });
      yield* sandbox.mkdir("/workspace/other");
    }).pipe(Effect.provide(Layer.merge(client, NodeCrypto.layer))),
  );
  expect(requests).toHaveLength(6);
  expect(requests[0]).toBe(requests[1]);
  expect(requests[1]).toBe(requests[2]);
  expect(requests[3]).toBe(requests[0]);
  expect(requests[4]).toBe(requests[0]);
  expect(requests[5]).not.toBe(requests[0]);
});
