import { Schema } from "effect";

export const PublicationPushFailure = Schema.Struct({
  reason: Schema.Literals([
    "write-access",
    "branch-conflict",
    "rejected",
    "protocol",
    "unavailable",
    "timeout",
    "Unknown",
  ]),
  diagnostic: Schema.optionalKey(
    Schema.Literals([
      "shallow-update",
      "missing-object",
      "unpack-failed",
      "large-file",
      "pack-size",
      "rpc-failed",
      "connection-closed",
      "tls",
      "ref-rejected",
      "workflow-permission",
    ]),
  ),
  status: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 400, maximum: 599 }))),
});

/** Classify private Git output; never return its text, paths, refs or credentials. */
export const classifyPublicationPush = (output: {
  readonly stdout: string;
  readonly stderr: string;
  readonly timedOut: boolean;
  readonly truncated: boolean;
}): typeof PublicationPushFailure.Type => {
  if (output.timedOut) return { reason: "timeout" };
  if (output.truncated) return { reason: "Unknown" };
  const text = `${output.stdout}\n${output.stderr}`;
  const http = /(?:The requested URL returned error: |HTTP )([45][0-9]{2})(?:\s|$)/u.exec(text);
  const status = http === null ? {} : { status: Number(http[1]) };
  if (
    /refusing to allow (?:a GitHub App|a Personal Access Token).*workflow|without [`'"]?workflows?[`'"]? permission/iu.test(
      text,
    )
  )
    return { reason: "write-access", diagnostic: "workflow-permission", ...status };
  if (/\(shallow update not allowed\)/u.test(text))
    return { reason: "protocol", diagnostic: "shallow-update", ...status };
  if (
    /did not receive expected object|missing necessary objects|missing (?:blob|tree|commit)|bad (?:object|tree object)|unable to read [0-9a-f]{40}|failed to read object/u.test(
      text,
    )
  )
    return { reason: "protocol", diagnostic: "missing-object", ...status };
  if (/GH001:|exceeds GitHub's file size limit/u.test(text))
    return { reason: "rejected", diagnostic: "large-file", ...status };
  if (/pack exceeds maximum allowed size|receive-pack request exceeds maximum size/u.test(text))
    return { reason: "rejected", diagnostic: "pack-size", ...status };
  if (/unpacker error|unpack failed|index-pack failed|unresolved delta/u.test(text))
    return { reason: "protocol", diagnostic: "unpack-failed", ...status };
  if (/\[rejected\] \((stale info|fetch first|non-fast-forward)\)/u.test(text))
    return { reason: "branch-conflict", ...status };
  if (status.status === 401 || status.status === 403 || /Authentication failed/u.test(text))
    return { reason: "write-access", ...status };
  if (/\(pre-receive hook declined\)|remote: error: GH006:|remote: error: GH013:/u.test(text))
    return { reason: "rejected", ...status };
  if (
    status.status === 429 ||
    (status.status !== undefined && status.status >= 500) ||
    /Could not resolve host:|Failed to connect to /u.test(text)
  )
    return { reason: "unavailable", ...status };
  if (/SSL certificate problem|server certificate verification failed/u.test(text))
    return { reason: "unavailable", diagnostic: "tls", ...status };
  if (/RPC failed/u.test(text)) return { reason: "protocol", diagnostic: "rpc-failed", ...status };
  if (/remote end hung up unexpectedly|unexpected disconnect|early EOF/u.test(text))
    return { reason: "protocol", diagnostic: "connection-closed", ...status };
  if (/\[remote rejected\]/u.test(text))
    return { reason: "rejected", diagnostic: "ref-rejected", ...status };
  return { reason: "Unknown", ...status };
};

export const publicationPushMessage = (
  reason: (typeof PublicationPushFailure.Type)["reason"],
  diagnostic?: (typeof PublicationPushFailure.Type)["diagnostic"],
) => {
  if (reason === "write-access" && diagnostic === "workflow-permission")
    return "GitHub requires Workflows: write permission to publish this repair. Approve that permission for the GitHub App installation, then retry. The code changes are saved.";
  switch (reason) {
    case "write-access":
      return "GitHub denied access to publish this repair. Check the GitHub App's repository permissions. The code changes are saved.";
    case "branch-conflict":
      return "The repair branch changed on GitHub. Review that branch before retrying publication. The code changes are saved.";
    case "rejected":
      return "GitHub rejected the repair branch. Check repository rules and the publication diagnostics. The code changes are saved.";
    case "protocol":
      return "Git could not transfer the saved repair to GitHub. Check the publication diagnostics before retrying. The code changes are saved.";
    case "unavailable":
      return "GitHub could not be reached to publish this repair. The code changes are saved; retry publication.";
    case "timeout":
      return "Publishing the repair timed out. The code changes are saved; retry publication to check its outcome.";
    case "Unknown":
      return "The repair branch could not be published. The code changes are saved; check the publication diagnostics before retrying.";
  }
};
