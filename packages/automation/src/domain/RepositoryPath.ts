import { Schema } from "effect";

export const RepositoryPath = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(512),
  // Keep path grammar at the host boundary: provider regex constraints can yield empty output.
  Schema.makeFilter(
    (path) =>
      (/^[A-Za-z0-9_.@%+$()[\]-][A-Za-z0-9_.@%+$/()[\]-]*$/.test(path) &&
        !path.split("/").includes("..")) ||
      "Expected a relative repository path without parent segments",
  ),
);
