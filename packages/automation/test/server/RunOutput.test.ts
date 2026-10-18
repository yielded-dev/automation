import { Effect, Schema } from "effect";
import { expect, it } from "vite-plus/test";
import { RepairExecutionContext } from "../../src/server/RepairExecutionContext";
import { RepairSandboxTaskClient } from "../../src/server/RepairSandboxTaskClient";
import { readRunEvents } from "../../src/server/RunOutput";
import { SandboxTaskId } from "../../src/server/sandbox-contracts/ids";
import { SandboxTaskEvent } from "../../src/server/sandbox-contracts/sandbox-task-event";

const id = "11111111-1111-4111-8111-111111111111";
const marker = SandboxTaskEvent.make({
  sequence: 132,
  taskId: SandboxTaskId.make(id),
  turnId: null,
  kind: "runtime",
  method: "repair.output_truncated",
  payload: null,
  createdAt: "2026-10-17T00:00:00.000Z",
});
const unused = () => Effect.die("Unexpected operation while projecting run events");

// Supply the application port's page; exercise the real projection without emulating storage.
const project = (events: ReadonlyArray<SandboxTaskEvent>) =>
  readRunEvents(131, 5).pipe(
    Effect.provideService(RepairExecutionContext, id),
    Effect.provideService(RepairSandboxTaskClient, {
      events: (runId, after, limit) =>
        Effect.sync(() => {
          expect([runId, after, limit]).toEqual([id, 131, 5]);
          return events;
        }),
      create: unused,
      inspect: unused,
      snapshot: unused,
      cancel: unused,
      native: unused,
      forward: unused,
    }),
  );

it("returns a null truncation marker and the following Codex event in the same page", async () => {
  const following = SandboxTaskEvent.make({
    ...marker,
    sequence: 133,
    kind: "codex",
    method: "turn.completed",
    payload: { turn: { id: "turn-1", status: "completed" } },
  });
  const events = await Effect.runPromise(project([marker, following]));

  expect(events.map(({ sequence, body }) => ({ sequence, body: JSON.parse(body) }))).toEqual([
    { sequence: 132, body: { type: "repair.output_truncated", value: null } },
    {
      sequence: 133,
      body: {
        type: "codex.event",
        event: { type: "turn.completed", turn: { id: "turn-1", status: "completed" } },
      },
    },
  ]);
});

it("preserves runtime JSON scalars, arrays and objects", async () => {
  for (const payload of [true, 3, "ready", [null, { ready: true }], { ready: true }]) {
    const events = await Effect.runPromise(
      project([SandboxTaskEvent.make({ ...marker, method: "runtime.status", payload })]),
    );
    expect(JSON.parse(events[0]!.body)).toEqual({ type: "runtime.status", value: payload });
  }
});

it("rejects Codex payloads that are not JSON object records through the typed error channel", async () => {
  for (const payload of [null, true, 3, "invalid", [], { value: undefined }]) {
    const error = await Effect.runPromise(
      project([
        SandboxTaskEvent.make({ ...marker, kind: "codex", method: "turn.completed", payload }),
      ]).pipe(Effect.flip),
    );
    expect(Schema.isSchemaError(error)).toBe(true);
  }
});
