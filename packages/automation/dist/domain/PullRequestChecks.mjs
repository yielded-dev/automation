import { RepairSourceRevision } from "./launch.mjs";
import { Schema } from "effect";
//#region src/domain/PullRequestChecks.ts
const PullRequestCheckState = Schema.Literals([
	"pending",
	"passed",
	"failed",
	"skipped"
]);
/** Disposable GitHub observations, independent of saved candidate validation receipts. */
const PullRequestChecks = Schema.Struct({
	headSha: RepairSourceRevision,
	fetchedAt: Schema.DateTimeUtcFromString,
	complete: Schema.Boolean,
	items: Schema.Array(Schema.Struct({
		id: Schema.String,
		name: Schema.String,
		state: PullRequestCheckState,
		description: Schema.String,
		descriptionTruncated: Schema.optionalKey(Schema.Boolean),
		hasReviewFindings: Schema.optionalKey(Schema.Boolean)
	}))
});
//#endregion
export { PullRequestCheckState, PullRequestChecks };
