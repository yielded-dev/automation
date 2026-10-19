import type { SandboxFileError } from "@cloudflare/sandbox";
import { Schema } from "effect";

const Path = Schema.String.check(Schema.isPattern(/^\/[^\0]*$/));
const Failure = Schema.fromJsonString(
  Schema.Struct({ code: Schema.String, detail: Schema.String }),
);

// Temporary workaround for cloudflare/sandbox-sdk#951. Read stdout immediately
// while draining stderr, instead of waiting for an opening frame on stderr.
// Keep this on native exec so existing containers need no supervisor upgrade.
const source = String.raw`
try {
  const writer = Bun.stdout.writer();
  for await (const chunk of Bun.file(process.argv[1]).stream()) {
    writer.write(chunk);
    await writer.flush();
  }
  await writer.end();
} catch (error) {
  process.stderr.write(JSON.stringify({ code: error.code ?? 'UNKNOWN', detail: error.message }));
  process.exitCode = 1;
}
`;

/** @internal Remove with the upstream SDK and helper's ordered-stream fix. */
export const makeReadFile =
  (container: Pick<Container, "exec">) =>
  async (path: string, signal?: AbortSignal): Promise<Response> => {
    Schema.decodeUnknownSync(Path)(path);
    signal?.throwIfAborted();
    const execution = new AbortController();
    let exited = false;
    let reader: ReadableStreamDefaultReader<Uint8Array> | undefined;
    const abort = () => {
      if (!exited) execution.abort(signal?.reason);
      void reader?.cancel(signal?.reason).catch(() => {});
    };
    const dispose = () => signal?.removeEventListener("abort", abort);

    signal?.addEventListener("abort", abort, { once: true });
    let process: ExecProcess;

    try {
      process = await container.exec(["bun", "-e", source, "--", path], {
        stdout: "pipe",
        stderr: "pipe",
        signal: execution.signal,
      });
    } catch (cause) {
      dispose();
      signal?.throwIfAborted();
      throw cause;
    }
    const exitCode = process.exitCode.finally(() => {
      exited = true;
    });
    const completed = Promise.all([new Response(process.stderr).text(), exitCode]).then(
      ([stderr, code]) => {
        if (code === 0) return;
        if (!stderr) throw new Error(`File reader exited with code ${code}`);
        const failure = Schema.decodeUnknownSync(Failure)(stderr);
        const errorCode: SandboxFileError["code"] = /^E[A-Z0-9]+$/.test(failure.code)
          ? `E${failure.code.slice(1)}`
          : "UNKNOWN";

        throw Object.assign(new Error(failure.detail), {
          name: "SandboxFileError" as const,
          code: errorCode,
          operation: "readFile" as const,
          path,
          detail: failure.detail,
        }) satisfies SandboxFileError;
      },
    );

    // The response owns completion; errors surface after any preceding bytes.
    void completed.catch(() => {});
    const cancel = async (reason?: unknown) => {
      if (!exited) execution.abort(reason);
      await Promise.allSettled([reader?.cancel(reason), completed]);
      reader?.releaseLock();
      dispose();
    };

    try {
      if (!process.stdout) throw new Error("File reader did not provide stdout");
      reader = process.stdout.getReader();
      const output = reader;
      const next = async () => {
        signal?.throwIfAborted();
        const chunk = await output.read();

        signal?.throwIfAborted();
        if (chunk.done) {
          await completed;
          output.releaseLock();
          dispose();
        }

        return chunk;
      };
      // An empty stream must still report an opening failure before a Response.
      const first = await next();

      if (first.done) return new Response(new Uint8Array());

      return new Response(
        new ReadableStream<Uint8Array>({
          start: (controller) => controller.enqueue(first.value),
          async pull(controller) {
            try {
              const chunk = await next();

              if (chunk.done) controller.close();
              else controller.enqueue(chunk.value);
            } catch (cause) {
              await cancel(cause);
              controller.error(cause);
            }
          },
          cancel,
        }),
      );
    } catch (cause) {
      await cancel(cause);
      signal?.throwIfAborted();
      throw cause;
    }
  };
