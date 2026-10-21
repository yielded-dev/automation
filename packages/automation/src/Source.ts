import { Effect, Schema } from "effect";

const Identifier = Schema.Trimmed.check(Schema.isMinLength(1), Schema.isMaxLength(512));

export const Issue = Schema.Struct({
  key: Identifier,
  title: Schema.String.check(Schema.isMaxLength(2_000)),
  evidence: Schema.JsonObject.check(
    Schema.makeFilter(
      (value) => new TextEncoder().encode(JSON.stringify(value)).byteLength <= 32_768,
      {
        expected:
          "source evidence of at most 32 KiB; project the relevant evidence before ingestion",
      },
    ),
  ),
});
export type Issue = typeof Issue.Type;

export const Candidate = Schema.Struct({
  id: Identifier,
  source: Identifier,
  ...Issue.fields,
});
export type Candidate = typeof Candidate.Type;

export interface Comparison {
  readonly incoming: Issue & { readonly source: string };
  readonly candidates: ReadonlyArray<Candidate>;
}

/** A source interprets input; Automation owns canonical identities and persistence. */
export interface Source<Tag extends string, E = never, R = never, DecodeR = never> {
  readonly _tag: Tag;
  readonly issue: (input: unknown) => Effect.Effect<Issue, Schema.SchemaError, DecodeR>;
  readonly dedupeIssue?: (input: Comparison) => Effect.Effect<string | null, E, R>;
}

export const make = <
  const Tag extends string,
  Input extends Schema.Top,
  E = never,
  R = never,
>(options: {
  readonly _tag: Tag;
  readonly schema: Input;
  readonly issue: (input: Input["Type"]) => Issue;
  readonly dedupeIssue?: (input: Comparison) => Effect.Effect<string | null, E, R>;
}): Source<Tag, NoInfer<E>, NoInfer<R>, Input["DecodingServices"]> => ({
  _tag: options._tag,
  issue: Effect.fnUntraced(function* (input: unknown) {
    const event = yield* Schema.decodeUnknownEffect(options.schema)(input);
    return yield* Schema.decodeEffect(Issue)(options.issue(event));
  }),
  ...(options.dedupeIssue ? { dedupeIssue: options.dedupeIssue } : {}),
});
