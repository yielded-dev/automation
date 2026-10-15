import { Schema } from "effect";
//#region src/domain/RepositoryPath.ts
const RepositoryPath = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(512), Schema.makeFilter((path) => /^[A-Za-z0-9_.@%+$()[\]-][A-Za-z0-9_.@%+$/()[\]-]*$/.test(path) && !path.split("/").includes("..") || "Expected a relative repository path without parent segments"));
//#endregion
export { RepositoryPath };
