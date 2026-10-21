import { Effect, Schema } from "effect";
import { expect, it } from "vite-plus/test";

import * as Sources from "../src/Sources";

const occurrence = {
  id: "occurrence-1",
  timestamp: 1792531200000,
  worker: {
    scriptName: "checkout-api",
    entrypoint: "Checkout",
    scriptVersion: { id: "version-1" },
  },
  invocation: { id: "invocation-1", traceId: "trace-1" },
};
const selected = { ...occurrence, error: { name: "TypeError" } };
const capture = {
  cloudflareAccountId: "a".repeat(32),
  service: "checkout-api",
  issueId: "issue-1",
  occurrence,
  evidence: {
    records: [{ ...occurrence, id: "other-occurrence", error: { name: "OtherError" } }, selected],
    truncated: false,
  },
};

it("keys Cloudflare issues independently of notifications and occurrences, selecting native evidence", async () => {
  const source = Sources.cloudflareIssues();
  const first = await Effect.runPromise(
    source.issue({ ...capture, alert_correlation_id: "delivery-1" }),
  );
  const nextOccurrence = { ...occurrence, id: "occurrence-2" };
  const repeated = await Effect.runPromise(
    source.issue({
      ...capture,
      alert_correlation_id: "delivery-2",
      occurrence: nextOccurrence,
      evidence: { records: [nextOccurrence] },
    }),
  );

  expect(first.key).toBe('["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","checkout-api","issue-1"]');
  expect(repeated.key).toBe(first.key);
  expect(first.title).toBe("checkout-api: TypeError in Checkout");
  expect(first.evidence.records).toEqual([selected]);
  expect(first.evidence).not.toHaveProperty("alert_correlation_id");
  expect(first.evidence.truncated).toBe(false);
});

it("rejects raw Cloudflare notifications and mismatched service captures", async () => {
  const source = Sources.cloudflareIssues();
  for (const input of [
    {
      account_id: capture.cloudflareAccountId,
      alert_correlation_id: "delivery-1",
      alert_type: "issues",
      ts: 1792531200,
      data: { issueId: "issue-1" },
    },
    { ...capture, service: "another-api" },
  ]) {
    const error = await Effect.runPromise(source.issue(input).pipe(Effect.flip));
    expect(Schema.isSchemaError(error)).toBe(true);
  }
});

const sentryEvent = {
  action: "created",
  installation: { uuid: "24b397fc-a86e-43ef-9297-949e21b82480" },
  data: {
    issue: {
      id: "1234567890",
      title: "TypeError: checkout failed",
      project: { id: "112313123123134" },
      culprit: "checkout",
      level: "error",
    },
  },
};

it("scopes Sentry issue identities to provider and project, not installation or title", async () => {
  const source = Sources.sentry({ providerInstance: "https://sentry.io" });
  const first = await Effect.runPromise(source.issue(sentryEvent));
  const reinstall = await Effect.runPromise(
    source.issue({
      ...sentryEvent,
      installation: { uuid: "another-installation" },
      data: { issue: { ...sentryEvent.data.issue, title: "Updated title" } },
    }),
  );
  const otherProject = await Effect.runPromise(
    source.issue({
      ...sentryEvent,
      data: { issue: { ...sentryEvent.data.issue, project: { id: "another-project" } } },
    }),
  );
  const selfHosted = await Effect.runPromise(
    Sources.sentry({ providerInstance: "https://sentry.example.com" }).issue(sentryEvent),
  );

  expect(first.key).toBe('["https://sentry.io","112313123123134","1234567890"]');
  expect(reinstall.key).toBe(first.key);
  expect(otherProject.key).not.toBe(first.key);
  expect(selfHosted.key).not.toBe(first.key);
  expect(first.title).toBe(sentryEvent.data.issue.title);
  expect(first.evidence).toEqual({ issue: sentryEvent.data.issue });
});

it("rejects Sentry alerts, other lifecycle actions, and an unconfigured instance", async () => {
  const source = Sources.sentry({ providerInstance: "https://sentry.io" });
  for (const input of [
    { ...sentryEvent, action: "resolved" },
    {
      action: "triggered",
      installation: sentryEvent.installation,
      data: { event: { issue_id: "1234567890" } },
    },
  ]) {
    const error = await Effect.runPromise(source.issue(input).pipe(Effect.flip));
    expect(Schema.isSchemaError(error)).toBe(true);
  }
  const error = await Effect.runPromise(
    Sources.sentry({ providerInstance: "" }).issue(sentryEvent).pipe(Effect.flip),
  );
  expect(Schema.isSchemaError(error)).toBe(true);
});
