import { describe, expect, it } from "vite-plus/test";

import { privateEvidencePage } from "../../src/domain/DiagnosticContent";

describe("complete private diagnostic evidence", () => {
  it("redacts credential-bearing keys without losing their separate evidence", () => {
    const page = privateEvidencePage(
      {
        "https://example.test/request?token=first-canary": "first failure",
        "https://example.test/request?token=second-canary": "second failure",
      },
      {},
    );

    expect(page.content).not.toContain("canary");
    expect(Object.values(JSON.parse(page.content))).toEqual(["first failure", "second failure"]);
    expect(page.omitted).toContain("credentials");
  });

  it("redacts the same credential fields in structured and serialized payloads", () => {
    const credentials = {
      token: "token-canary",
      id_token: "id-canary",
      private_key: "key-canary",
      client_secret: "secret-canary",
      "client\\secret": "escaped-key-canary",
      credentials: "credentials-canary",
    };

    const page = privateEvidencePage(
      {
        credentials,
        message: JSON.stringify(credentials),
        raw: "id_token=raw-canary",
        nested: JSON.stringify({ message: "{'password': 'nested-canary'}" }),
        unterminated: "{'password': 'unterminated-canary",
      },
      {},
    );

    expect(page.content).not.toContain("canary");
    expect(page.omitted).toContain("credentials");
  });

  it("counts redacted fields toward the traversal limit", () => {
    const page = privateEvidencePage(
      Object.fromEntries(Array.from({ length: 200_001 }, (_, index) => [`secret${index}`, "x"])),
      {},
    );

    expect(page.truncated).toBe(true);
  });

  it("recovers long original prompts and tool results beyond the history preview", () => {
    const prompt = "Preserve this original instruction. ".repeat(900);

    const value = {
      request: {
        body: {
          model: "original-model",
          instructions: prompt,
          reasoning: { effort: "medium" },
          tools: [{ name: "browser_use" }],
        },
      },
      result: { status: "needs_input", message: "Ask for the missing field." },
    };

    let offset = 0;
    let content = "";

    for (;;) {
      const page = privateEvidencePage(value, { offset, limit: 1024 });

      expect(page.truncated).toBe(false);
      content += page.content;
      if (page.nextOffset === null) break;
      expect(page.nextOffset).toBeGreaterThan(offset);
      offset = page.nextOffset;
    }
    expect(offset).toBeGreaterThan(16_000);
    expect(JSON.parse(content)).toEqual(value);
  });

  it("redacts credentials and image/reasoning bytes while retaining useful private content", () => {
    const page = privateEvidencePage(
      {
        prompt: "Ask which field to use.",
        password: "private-password-canary",
        headers: {
          authorization: "Bearer private-bearer-canary",
          "content-type": "application/json",
        },
        url: "https://example.test/guest?token=private-url-canary",
        message: 'Failed request: {"apiKey":"private-key-canary"}',
        images: [{ data: "private-image-canary" }],
        output: [
          { type: "reasoning", text: "private-reasoning-canary" },
          { type: "text", text: "Please reply in chat." },
        ],
      },
      {},
    );

    expect(page.content).not.toContain("canary");
    expect(page.content).toContain("Ask which field");
    expect(page.content).toContain("Please reply in chat");
    expect(page.omitted).toEqual(
      expect.arrayContaining(["credentials", "image_bytes", "reasoning"]),
    );
  });

  it.each([
    {
      name: "all cookie pairs",
      header: "Cookie: theme=dark; session=opaque-cookie-canary; csrf=second-cookie-canary",
      redacted: "Cookie: [REDACTED]",
    },
    {
      name: "combined set-cookie values",
      header: "Set-Cookie: session=opaque-cookie-canary; Path=/, refresh=second-cookie-canary",
      redacted: "Set-Cookie: [REDACTED]",
    },
    {
      name: "folded cookie values",
      header: "Cookie: theme=dark;\r\n\tsession=opaque-cookie-canary",
      redacted: "Cookie: [REDACTED]",
    },
    {
      name: "escaped quotes in serialized cookie values",
      header: JSON.stringify({ Cookie: 'theme="dark"; session=opaque-cookie-canary' }),
      redacted: JSON.stringify({ Cookie: "[REDACTED]" }),
    },
  ])("omits $name before paging full-record text", ({ header, redacted }) => {
    const value = {
      message: `${header}\r\nBrowser reply: ask for the missing field.`,
      status: "needs_input",
    };

    let offset = 0;
    let content = "";

    for (;;) {
      const page = privateEvidencePage(value, { offset, limit: 19 });

      expect(page.omitted).toContain("credentials");
      expect(page.truncated).toBe(false);
      content += page.content;
      if (page.nextOffset === null) break;
      offset = page.nextOffset;
    }

    expect(content).not.toContain("canary");
    expect(JSON.parse(content)).toEqual({
      ...value,
      message: `${redacted}\r\nBrowser reply: ask for the missing field.`,
    });
  });
});
