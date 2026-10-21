import { type Effect, Option, Schema } from "effect";

import * as Source from "./Source";

const Identifier = Schema.NonEmptyString.check(Schema.isMaxLength(128));

const CloudflareOccurrence = Schema.Struct({
  id: Identifier,
  timestamp: Schema.Union([Schema.Finite, Schema.String]),
  worker: Schema.Struct({
    scriptName: Identifier,
    entrypoint: Schema.optionalKey(Identifier),
    executionModel: Schema.optionalKey(Identifier),
    scriptVersion: Schema.Struct({ id: Identifier }),
  }),
  invocation: Schema.Struct({
    id: Identifier,
    traceId: Identifier,
    type: Schema.optionalKey(Identifier),
  }),
});

/** A host-verified native capture, not a Cloudflare notification payload. */
export const CloudflareIssueCapture = Schema.Struct({
  cloudflareAccountId: Schema.String.check(Schema.isPattern(/^[a-fA-F0-9]{32}$/u)),
  service: Identifier,
  issueId: Identifier,
  occurrence: CloudflareOccurrence,
  evidence: Schema.Struct({
    records: Schema.Array(Schema.JsonObject),
    truncated: Schema.optionalKey(Schema.Boolean),
  }),
}).check(
  Schema.makeFilter((event) => event.service === event.occurrence.worker.scriptName, {
    expected: "an occurrence from the captured issue's service",
  }),
);
export type CloudflareIssueCapture = typeof CloudflareIssueCapture.Type;

const NativeError = Schema.Struct({
  error: Schema.Struct({
    name: Schema.String.check(Schema.isPattern(/^[a-zA-Z][a-zA-Z0-9_.-]{0,95}$/u)),
  }),
});

interface Options<E, R> {
  readonly dedupeIssue?: (input: Source.Comparison) => Effect.Effect<string | null, E, R>;
}

/**
 * The host verifies the account, issue and occurrence through Cloudflare's native
 * Issues API, authorizes the capture, and sanitizes its evidence before calling
 * this source. Decoding establishes shape, not authenticity or repair authority.
 */
export const cloudflareIssues = <E = never, R = never>(options: Options<E, R> = {}) =>
  Source.make({
    _tag: "CloudflareIssues",
    schema: CloudflareIssueCapture,
    issue: (event) => {
      const record = event.evidence.records.find((row) => row.id === event.occurrence.id);
      const error = Schema.decodeUnknownOption(NativeError)(record);
      const name = Option.isSome(error) ? error.value.error.name : "Runtime failure";
      const entrypoint = event.occurrence.worker.entrypoint;

      return {
        key: JSON.stringify([event.cloudflareAccountId, event.service, event.issueId]),
        title: `${event.service}: ${name}${entrypoint ? ` in ${entrypoint}` : ""}`,
        evidence: {
          cloudflareAccountId: event.cloudflareAccountId,
          service: event.service,
          issueId: event.issueId,
          occurrence: event.occurrence,
          records: record ? [record] : [],
          truncated: event.evidence.truncated === true || record === undefined,
        },
      };
    },
    ...options,
  });

/**
 * The documented issue.created body, after authentication and routing by the
 * Sentry-Hook-Resource: issue header. Other lifecycle actions and event_alert
 * payloads are not supported by this source.
 * https://docs.sentry.io/integrations/integration-platform/webhooks/issues/
 */
export const SentryIssueCreated = Schema.Struct({
  action: Schema.Literal("created"),
  installation: Schema.Struct({ uuid: Schema.NonEmptyString }),
  data: Schema.Struct({
    issue: Schema.Struct({
      id: Identifier,
      title: Source.Issue.fields.title,
      project: Schema.Struct({ id: Identifier }),
      culprit: Schema.optionalKey(Schema.String),
      level: Schema.optionalKey(Schema.String),
      platform: Schema.optionalKey(Schema.NullOr(Schema.String)),
      metadata: Schema.optionalKey(Schema.JsonObject),
    }),
  }),
});
export type SentryIssueCreated = typeof SentryIssueCreated.Type;

const ProviderInstance = Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(128));

/**
 * The host authenticates the webhook and authorizes its installation and project.
 * providerInstance is a stable, host-configured identity for the Sentry instance
 * (for example, its canonical origin), distinguishing SaaS from self-hosted IDs.
 * It must not come from the notification or change when an integration is reinstalled.
 */
export const sentry = <E = never, R = never>(
  options: Options<E, R> & { readonly providerInstance: string },
) =>
  Source.make({
    _tag: "Sentry",
    schema: SentryIssueCreated.check(
      Schema.makeFilter(() => Schema.is(ProviderInstance)(options.providerInstance), {
        expected: "a configured, non-empty Sentry providerInstance",
      }),
    ),
    issue: (event) => ({
      key: JSON.stringify([
        options.providerInstance,
        event.data.issue.project.id,
        event.data.issue.id,
      ]),
      title: event.data.issue.title,
      evidence: { issue: event.data.issue },
    }),
    ...(options.dedupeIssue ? { dedupeIssue: options.dedupeIssue } : {}),
  });
