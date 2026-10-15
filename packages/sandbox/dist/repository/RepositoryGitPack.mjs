import { Context, Stream } from "effect";
//#region src/repository/RepositoryGitPack.ts
/** Destination-specific pack preparation; ordinary Git receivers accept the source encoding. */
const RepositoryGitPack = Context.Reference("kommunikasie/RepositoryGitPack", { defaultValue: () => ({ prepare: (pack) => Stream.succeed({
	_tag: "Direct",
	body: pack
}) }) });
//#endregion
export { RepositoryGitPack };
