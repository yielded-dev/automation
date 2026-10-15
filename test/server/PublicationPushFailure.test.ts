import { describe, expect, it } from "vite-plus/test";
import {
  classifyPublicationPush,
  publicationPushMessage,
} from "../../packages/automation/src/server/PublicationPushFailure";

const output = (stderr: string) => ({ stdout: "", stderr, timedOut: false, truncated: false });

describe("publication push failures", () => {
  it.each([
    [
      "! refs/heads/private:refs/heads/private [remote rejected] (missing necessary objects)",
      "protocol",
      "missing-object",
    ],
    [
      "! refs/heads/private:refs/heads/private [remote rejected] (refusing to allow a GitHub App to create or update workflow `.github/workflows/private.yml` without `workflows` permission)",
      "write-access",
      "workflow-permission",
    ],
    [
      "remote: fatal: did not receive expected object aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n! refs/heads/private:refs/heads/private [remote rejected] (missing necessary objects)",
      "protocol",
      "missing-object",
    ],
    [
      "remote: error: GH001: Large files detected. private-file exceeds GitHub's file size limit",
      "rejected",
      "large-file",
    ],
    ["remote: fatal: pack exceeds maximum allowed size (2.00 GiB)", "rejected", "pack-size"],
    [
      "! refs/heads/private:refs/heads/private [remote rejected] (unpacker error)",
      "protocol",
      "unpack-failed",
    ],
    [
      "error: RPC failed; curl 18 transfer closed with outstanding read data remaining",
      "protocol",
      "rpc-failed",
    ],
    ["fatal: the remote end hung up unexpectedly", "protocol", "connection-closed"],
    [
      "fatal: SSL certificate problem: unable to get local issuer certificate",
      "unavailable",
      "tls",
    ],
  ] as const)("classifies %s without exporting private output", (text, reason, diagnostic) => {
    const result = classifyPublicationPush(output(text));
    expect(result).toEqual({ reason, diagnostic });
    expect(publicationPushMessage(result.reason)).toContain("saved");
    expect(JSON.stringify(result)).not.toContain("private");
  });

  it("retains an HTTP failure inside Git's RPC error", () => {
    expect(
      classifyPublicationPush(output("error: RPC failed; HTTP 503 curl 22 private-server")),
    ).toEqual({ reason: "unavailable", status: 503 });
    expect(classifyPublicationPush(output("fatal: The requested URL returned error: 403"))).toEqual(
      { reason: "write-access", status: 403 },
    );
  });

  it("explains the permission needed when GitHub rejects workflow publication", () => {
    expect(publicationPushMessage("write-access", "workflow-permission")).toContain(
      "Workflows: write",
    );
  });

  it("does not trust partial output or invent an unknown reason", () => {
    expect(
      classifyPublicationPush({ ...output("remote: error: GH001:"), truncated: true }),
    ).toEqual({ reason: "Unknown" });
    expect(classifyPublicationPush({ ...output(""), timedOut: true })).toEqual({
      reason: "timeout",
    });
    expect(classifyPublicationPush(output("private-unrecognized-output"))).toEqual({
      reason: "Unknown",
    });
  });
});
