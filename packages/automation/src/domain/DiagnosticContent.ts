import { Array, Schema } from "effect";

export const DiagnosticContentQuery = {
  offset: Schema.optionalKey(Schema.Natural),
  limit: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 8192 }))),
};

export const DiagnosticContentPage = Schema.Struct({
  content: Schema.String,
  offset: Schema.Natural,
  totalLength: Schema.Natural,
  nextOffset: Schema.NullOr(Schema.Natural),
  hasMore: Schema.Boolean,
  omitted: Schema.Array(Schema.String),
  truncated: Schema.Boolean,
});

const fieldName = (key: string) => key.replace(/[^a-z]/gi, "").toLowerCase();
const credentialField =
  /authorization|cookie|password|secret|apikey|accesstoken|refreshtoken|idtoken|privatekey|credential|^token$/;

const redactQuotedFields = (value: string) => {
  const fields = /"([^"\r\n]+)"\s*:\s*(?=["'])|'([^'\r\n]+)'\s*:\s*(?=["'])/g;
  const quoted = /(?:"(?:\\.|[^"\\\r\n])*"|'(?:\\.|[^'\\\r\n])*')/y;
  let result = "";
  let offset = 0;

  for (let match = fields.exec(value); match !== null; match = fields.exec(value)) {
    const key = match[1] ?? match[2];

    if (key === undefined || !credentialField.test(fieldName(key))) continue;
    quoted.lastIndex = fields.lastIndex;
    const content = quoted.exec(value);

    result += `${value.slice(offset, fields.lastIndex)}"[REDACTED]"`;
    // An unterminated credential consumes the remaining text; do not repeatedly rescan it.
    if (content === null) return result;
    offset = quoted.lastIndex;
    fields.lastIndex = offset;
  }

  return result + value.slice(offset);
};

/** For authorized private readers. Retains user content; unsuitable for logs or public output. */
export const redactPrivateText = (value: string) =>
  redactQuotedFields(value)
    .replace(/(https?:\/\/)[^\s/@]+:[^\s/@]+@/gi, "$1[REDACTED]@")
    .replace(/\b(Bearer|Basic)\s+[A-Za-z0-9_+/=.-]+/gi, "$1 [REDACTED]")
    .replace(
      /\b((?:cookie|set-cookie)[ \t]*[=:][ \t]*)[^\r\n]*(?:(?:\r\n?|\n)[ \t]+[^\r\n]*)*/gi,
      "$1[REDACTED]",
    )
    .replace(
      /\b((?:api[_-]?key|(?:access|refresh|id)[_-]?token|token|password|secret|private[_-]?key|credentials?)\s*[=:]\s*)[^\s,;]+/gi,
      "$1[REDACTED]",
    )
    .replace(
      /\b(?:sk-[A-Za-z0-9_-]{8,}|gh[pousr]_[A-Za-z0-9_]+|xox[baprs]-[A-Za-z0-9-]+|eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+)\b/g,
      "[REDACTED]",
    )
    .replace(
      /-----BEGIN [^-]*PRIVATE KEY-----[\s\S]*?-----END [^-]*PRIVATE KEY-----/g,
      "[REDACTED]",
    )
    .replace(/(https?:\/\/[^\s?#"'<>]+)[?#][^\s"'<>]*/gi, "$1?[REDACTED]");

/** Page a credential-redacted document without losing long prompts to preview limits. */
export const privateEvidencePage = (
  value: Schema.Json,
  query: { readonly offset?: number; readonly limit?: number },
): typeof DiagnosticContentPage.Type => {
  const omitted = new Set<string>();
  let truncated = false;
  let nodes = 200_000;
  let characters = 8 * 1024 * 1024;

  const visit = (input: Schema.Json, depth: number): Schema.Json => {
    if (--nodes < 0 || depth > 48 || characters <= 0) {
      truncated = true;

      return "[Evidence limit reached]";
    }
    if (typeof input === "string") {
      const redacted = redactPrivateText(input);

      if (redacted !== input) omitted.add("credentials");
      const text = redacted.slice(0, characters);

      if (text.length < redacted.length) truncated = true;
      characters -= text.length;

      return text;
    }
    if (Array.isArray<Schema.Json>(input)) {
      const result: Schema.Json[] = [];

      for (const entry of input) {
        if (nodes <= 0 || characters <= 0) {
          truncated = true;
          break;
        }
        result.push(visit(entry, depth + 1));
      }

      return result;
    }
    if (input === null || typeof input !== "object") return input;
    if (
      "type" in input &&
      typeof input.type === "string" &&
      /^(?:input_image|output_image|image|reasoning|reasoning_text)$/u.test(input.type)
    ) {
      omitted.add(input.type.includes("reasoning") ? "reasoning" : "image_bytes");

      return { type: input.type, omitted: true };
    }
    const result = new Map<string, Schema.Json>();
    let renamed = 0;

    for (const key in input) {
      if (!Object.hasOwn(input, key)) continue;
      if (nodes <= 0 || characters <= 0) {
        truncated = true;
        break;
      }
      const redacted = redactPrivateText(key);

      if (redacted !== key) omitted.add("credentials");
      let name = redacted;

      while (result.has(name)) name = `${redacted} (${++renamed})`;
      if (name.length > characters) {
        truncated = true;
        break;
      }
      characters -= name.length;
      const field = fieldName(key);
      let entry = input[key];

      if (entry === undefined) continue;
      if (credentialField.test(field)) {
        omitted.add("credentials");
        entry = "[REDACTED]";
      } else if (
        field === "encryptedcontent" ||
        /^(?:imagedata|images?|attachments?|screenshots?)$/u.test(field)
      ) {
        omitted.add(field === "encryptedcontent" ? "reasoning" : "image_bytes");
        entry = "[OMITTED]";
      }
      result.set(name, visit(entry, depth + 1));
    }

    return Object.fromEntries(result);
  };

  const document = JSON.stringify(visit(value, 0));
  const offset = Math.min(query.offset ?? 0, document.length);
  const end = Math.min(offset + Math.min(query.limit ?? 8192, 8192), document.length);

  return {
    content: document.slice(offset, end),
    offset,
    totalLength: document.length,
    nextOffset: end < document.length ? end : null,
    hasMore: end < document.length,
    omitted: [...omitted],
    truncated,
  };
};
