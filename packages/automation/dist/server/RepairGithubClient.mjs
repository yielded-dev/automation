import { ConflictError, ServiceUnavailableError } from "../domain/errors.mjs";
import { RepairSourceRevision } from "../domain/launch.mjs";
import { GithubPullRequest, GithubWork, boundedGithubEvidence, githubConflictStatus } from "../domain/RepairGithub.mjs";
import { RepairErrorReporter, reportSanitizedCause } from "./RepairErrorReporter.mjs";
import { GithubRateLimited, checkGithubRateLimit } from "./GithubRateLimit.mjs";
import { GithubAppAuth } from "./GithubAppAuth.mjs";
import { RepairGithubConfig } from "./RepairGithubConfig.mjs";
import { Cache, Cause, Context, Data, DateTime, Effect, Exit, Layer, Match, Option, Schedule, Schema } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from "effect/http";
//#region src/server/RepairGithubClient.ts
const Comment = Schema.Struct({
	id: Schema.Int.check(Schema.isGreaterThan(0)),
	body: Schema.String,
	user: Schema.Struct({
		login: Schema.String,
		type: Schema.optionalKey(Schema.String)
	})
});
const PolledComment = Schema.Struct({
	id: Schema.Int.check(Schema.isGreaterThan(0)),
	body: Schema.NullOr(Schema.String),
	user: Schema.Struct({
		login: Schema.String,
		type: Schema.String
	}),
	created_at: Schema.DateTimeUtcFromString,
	commit_id: Schema.optionalKey(RepairSourceRevision),
	in_reply_to_id: Schema.optionalKey(Schema.Int.check(Schema.isGreaterThan(0))),
	path: Schema.optionalKey(Schema.String),
	diff_hunk: Schema.optionalKey(Schema.String)
});
const PolledReview = Schema.Struct({
	id: Schema.Int.check(Schema.isGreaterThan(0)),
	body: Schema.NullOr(Schema.String),
	user: Schema.Struct({
		login: Schema.String,
		type: Schema.String
	}),
	state: Schema.String,
	submitted_at: Schema.optionalKey(Schema.NullOr(Schema.DateTimeUtcFromString)),
	commit_id: RepairSourceRevision
});
const GithubRef = Schema.Struct({ object: Schema.Struct({ sha: RepairSourceRevision }) });
const CheckRuns = Schema.Struct({
	total_count: Schema.Natural,
	check_runs: Schema.Array(Schema.Struct({
		id: Schema.Natural,
		name: Schema.String,
		head_sha: RepairSourceRevision,
		details_url: Schema.optionalKey(Schema.NullOr(Schema.String)),
		output: Schema.optionalKey(Schema.Struct({
			title: Schema.NullOr(Schema.String),
			summary: Schema.NullOr(Schema.String),
			text: Schema.NullOr(Schema.String)
		})),
		status: Schema.Literals([
			"queued",
			"in_progress",
			"completed",
			"waiting",
			"requested",
			"pending"
		]),
		conclusion: Schema.NullOr(Schema.Literals([
			"success",
			"failure",
			"neutral",
			"cancelled",
			"skipped",
			"timed_out",
			"action_required",
			"stale",
			"startup_failure"
		]))
	})).check(Schema.isMaxLength(100))
});
const checkRunState = (run) => run.status !== "completed" ? "pending" : Match.value(run.conclusion).pipe(Match.when("success", () => "passed"), Match.whenOr("neutral", "skipped", () => "skipped"), Match.when(null, () => "pending"), Match.whenOr("failure", "cancelled", "timed_out", "action_required", "stale", "startup_failure", () => "failed"), Match.exhaustive);
const CommitStatus = Schema.Struct({
	sha: RepairSourceRevision,
	total_count: Schema.Natural,
	statuses: Schema.Array(Schema.Struct({
		id: Schema.Natural,
		context: Schema.String,
		state: Schema.Literals([
			"pending",
			"success",
			"failure",
			"error"
		]),
		description: Schema.NullOr(Schema.String)
	})).check(Schema.isMaxLength(100))
});
const Review = Schema.Struct({
	commit_id: RepairSourceRevision,
	body: Schema.NullOr(Schema.String)
});
const ReviewComments = Schema.Array(Schema.Struct({
	path: Schema.String,
	line: Schema.optionalKey(Schema.NullOr(Schema.Natural)),
	body: Schema.String
})).check(Schema.isMaxLength(100));
const ReviewId = Schema.String.check(Schema.isPattern(/^[1-9][0-9]{0,15}$/));
const checkDescriptionLimit = 8e3;
const retainGithubText = (parts) => {
	let text = "";
	for (const part of parts) {
		text += part.slice(0, 8001 - text.length);
		if (text.length > checkDescriptionLimit) break;
	}
	return [...text].join("");
};
const incompleteChecks = () => ServiceUnavailableError.make({ message: "GitHub checks could not be read consistently" });
const replyLocation = (work) => {
	if ("issueNumber" in work) {
		const path = `issues/${work.issueNumber}/comments`;
		return {
			list: path,
			post: path,
			collection: "issues",
			discussion: "issues"
		};
	}
	if (work.comment?.kind === "pull_request_review_comment") return {
		list: `pulls/${work.pullRequestNumber}/comments`,
		post: `pulls/${work.pullRequestNumber}/comments/${work.comment.threadId}/replies`,
		collection: "pulls",
		discussion: "pull_requests"
	};
	const path = `issues/${work.pullRequestNumber}/comments`;
	return {
		list: path,
		post: path,
		collection: "issues",
		discussion: "pull_requests"
	};
};
var ReadAccess = class extends Data.Class {};
var GithubWriteExpired = class extends Schema.TaggedError()("GithubWriteExpired", {}) {};
const make = Effect.gen(function* () {
	const app = yield* GithubAppAuth;
	const config = yield* RepairGithubConfig;
	const report = Effect.provideService(RepairErrorReporter, yield* RepairErrorReporter);
	const http = yield* HttpClient.HttpClient;
	const readTokens = yield* Cache.makeWith(({ repositoryId, access }) => app.repositoryReadToken(repositoryId, access, config.githubInstallationId).pipe(Effect.provideService(HttpClient.HttpClient, http)), {
		capacity: 32,
		timeToLive: (exit) => Exit.isSuccess(exit) ? "5 minutes" : 0
	});
	const request = Effect.fn("RepairGithubClient.request")(function* (work, path, write, accept = "application/vnd.github+json", access = "review") {
		if (write?.deadline !== void 0 && write.deadline <= DateTime.toEpochMillis(yield* DateTime.now)) return yield* new GithubWriteExpired();
		const token = yield* (write === void 0 ? Cache.get(readTokens, new ReadAccess({
			repositoryId: work.repositoryId,
			access
		})) : app.repositoryWriteToken(work.repositoryId, config.githubInstallationId, write.discussion === void 0 ? void 0 : { discussion: write.discussion })).pipe(Effect.provideService(HttpClient.HttpClient, http));
		const remaining = write?.deadline === void 0 ? 1e4 : Math.min(1e4, write.deadline - DateTime.toEpochMillis(yield* DateTime.now));
		if (remaining <= 0) return yield* new GithubWriteExpired();
		const response = yield* http.execute((write === void 0 ? HttpClientRequest.get(`https://api.github.com/repos/${work.repository}/${path}`) : HttpClientRequest.make(write.method)(`https://api.github.com/repos/${work.repository}/${path}`).pipe(HttpClientRequest.bodyJsonUnsafe(write.body))).pipe(HttpClientRequest.bearerToken(token), HttpClientRequest.setHeaders({
			accept,
			"user-agent": "reve-repairs",
			"x-github-api-version": "2022-11-28"
		}))).pipe(Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }), Effect.timeout(remaining));
		yield* checkGithubRateLimit(response);
		return response;
	});
	const botName = Effect.fn("RepairGithubClient.botName")(function* () {
		return yield* app.botLogin().pipe(Effect.provideService(HttpClient.HttpClient, http));
	});
	const readPullRequest = Effect.fn("RepairGithubClient.pullRequest")(function* (work, access = "review") {
		const response = yield* request(work, `pulls/${work.pullRequestNumber}`, void 0, "application/vnd.github+json", access);
		if (response.status === 404) return null;
		yield* HttpClientResponse.filterStatusOk(response);
		const pr = yield* HttpClientResponse.schemaBodyJson(GithubPullRequest)(response);
		if (pr.number !== work.pullRequestNumber || pr.base.repo.id !== work.repositoryId) return yield* ConflictError.make({ message: "The GitHub pull request does not match" });
		return pr;
	});
	const pullRequest = (work, access = "review") => readPullRequest(work, access).pipe(Effect.repeat({
		while: (pr) => pr !== null && pr.state === "open" && !pr.draft && githubConflictStatus(pr) === null,
		times: 2,
		schedule: Schedule.spaced("1 second")
	}));
	const compareDiff = Effect.fn("RepairGithubClient.compareDiff")(function* (work, baseSha, headSha) {
		const response = yield* request(work, `compare/${baseSha}...${headSha}`, void 0, "application/vnd.github.diff");
		if (response.status === 406) return null;
		yield* HttpClientResponse.filterStatusOk(response);
		const patch = yield* response.text;
		return patch.length <= 4e6 ? patch : null;
	});
	const checks = Effect.fn("RepairGithubClient.checks")(function* (work, details) {
		const pr = yield* readPullRequest(work, "checks");
		if (pr === null) return yield* incompleteChecks();
		const headSha = pr.head.sha;
		const read = (path, schema) => request(work, path, void 0, "application/vnd.github+json", "checks").pipe(Effect.flatMap(HttpClientResponse.filterStatusOk), Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)));
		const reviewUnavailable = (cause) => {
			return Cause.hasFails(cause) && cause.reasons.every((reason) => Cause.isFailReason(reason) && !Schema.is(GithubRateLimited)(reason.error) && !Schema.is(ConflictError)(reason.error)) ? reportSanitizedCause(cause, { operation: "RepairGithubClient.review" }).pipe(report, Effect.as(void 0)) : Effect.failCause(cause);
		};
		const readReview = (path, schema) => request(work, path, void 0, "application/vnd.github+json", "checks").pipe(Effect.flatMap((response) => response.status === 404 ? Effect.succeed(void 0) : HttpClientResponse.filterStatusOk(response).pipe(Effect.flatMap(HttpClientResponse.schemaBodyJson(schema)))), Effect.catchCause(reviewUnavailable));
		const [runResult, statusResult] = yield* Effect.all([Effect.gen(function* () {
			const runs = /* @__PURE__ */ new Map();
			let runTotal;
			for (let page = 1; page <= 10; page++) {
				const batch = yield* read(`commits/${headSha}/check-runs?filter=latest&per_page=100&page=${page}`, CheckRuns);
				runTotal ??= batch.total_count;
				if (runTotal !== batch.total_count) return yield* incompleteChecks();
				for (const run of batch.check_runs) {
					if (run.head_sha !== headSha || run.status === "completed" && run.conclusion === null) return yield* incompleteChecks();
					runs.set(run.id, {
						...run,
						name: retainGithubText([run.name]),
						details_url: run.details_url !== void 0 && run.details_url !== null && run.details_url.length <= checkDescriptionLimit ? retainGithubText([run.details_url]) : null,
						...run.output === void 0 ? {} : { output: {
							title: null,
							summary: null,
							text: retainGithubText([
								run.output.title,
								run.output.summary,
								run.output.text
							].filter((part) => part !== null && part.length > 0).flatMap((part, index) => index === 0 ? [part] : ["\n\n", part]))
						} }
					});
				}
				if (runs.size === runTotal) return {
					runs,
					complete: true
				};
				if (batch.check_runs.length < 100) return yield* incompleteChecks();
			}
			return {
				runs,
				complete: false
			};
		}), Effect.gen(function* () {
			const statuses = /* @__PURE__ */ new Map();
			let statusTotal;
			for (let page = 1; page <= 10; page++) {
				const batch = yield* read(`commits/${headSha}/status?per_page=100&page=${page}`, CommitStatus);
				statusTotal ??= batch.total_count;
				if (batch.sha !== headSha || statusTotal !== batch.total_count) return yield* incompleteChecks();
				for (const status of batch.statuses) statuses.set(status.id, {
					...status,
					context: retainGithubText([status.context]),
					description: status.description === null ? null : retainGithubText([status.description])
				});
				if (statuses.size === statusTotal) return {
					statuses,
					complete: true
				};
				if (batch.statuses.length < 100) return yield* incompleteChecks();
			}
			return {
				statuses,
				complete: false
			};
		})], { concurrency: 2 });
		const { runs } = runResult;
		const { statuses } = statusResult;
		const reviewDetails = /* @__PURE__ */ new Map();
		const reviewIds = /* @__PURE__ */ new Set();
		const linkedReviewIds = /* @__PURE__ */ new Map();
		const reviewPrefix = `https://github.com/${work.repository}/pull/${work.pullRequestNumber}#pullrequestreview-`;
		const reviewRuns = details !== void 0 && (details.conflicts || githubConflictStatus(pr) === true || [...runs.values()].some((run) => checkRunState(run) === "failed") || [...statuses.values()].some((status) => ["failure", "error"].includes(status.state))) ? [...runs.values()].sort((left, right) => Number(checkRunState(right) === "failed") - Number(checkRunState(left) === "failed")) : [];
		for (const run of reviewRuns) {
			if (run.status !== "completed") continue;
			const ids = /* @__PURE__ */ new Set();
			const links = `${run.details_url ?? ""}\n${run.output?.text ?? ""}`;
			for (const suffix of links.split(reviewPrefix).slice(1)) {
				const reviewId = Option.getOrUndefined(Schema.decodeOption(ReviewId)(suffix.match(/^\d+(?=$|[^A-Za-z0-9_-])/)?.[0] ?? ""));
				if (reviewId !== void 0) {
					ids.add(reviewId);
					reviewIds.add(reviewId);
				}
			}
			linkedReviewIds.set(run.id, [...ids]);
		}
		const unavailableReviews = new Set(reviewIds);
		const reviewDeadline = details?.reviewDeadline;
		const reviewTime = reviewDeadline === void 0 ? 5e3 : Math.max(0, Math.min(5e3, reviewDeadline - (yield* DateTime.nowAsDate).getTime()));
		if (reviewTime > 0) yield* Effect.forEach(reviewIds, Effect.fnUntraced(function* (reviewId) {
			const path = `pulls/${work.pullRequestNumber}/reviews/${reviewId}`;
			const review = yield* readReview(path, Review);
			if (review === void 0) return;
			if (review.commit_id !== headSha) {
				unavailableReviews.delete(reviewId);
				return;
			}
			const details = {
				body: retainGithubText([review.body ?? ""]),
				inline: ""
			};
			reviewDetails.set(reviewId, details);
			for (let page = 1; details.inline.length <= checkDescriptionLimit; page++) {
				const batch = yield* readReview(`${path}/comments?per_page=100&page=${page}`, ReviewComments);
				if (batch === void 0) return;
				for (const comment of batch) {
					details.inline = retainGithubText([
						details.inline,
						details.inline.length > 0 ? "\n\n" : "",
						comment.path,
						comment.line === null || comment.line === void 0 ? "" : `:${comment.line}`,
						"\n",
						comment.body
					]);
					if (details.inline.length > checkDescriptionLimit) break;
				}
				if (batch.length < 100) break;
			}
			unavailableReviews.delete(reviewId);
		}), {
			concurrency: 8,
			discard: true
		}).pipe(Effect.timeout(reviewTime), Effect.catchCause(reviewUnavailable));
		yield* Effect.annotateCurrentSpan({
			"repair.github.linked_review_count": reviewIds.size,
			"repair.github.current_head_review_count": reviewDetails.size,
			"repair.github.unavailable_review_count": unavailableReviews.size
		});
		if ((yield* readPullRequest(work, "checks"))?.head.sha !== headSha) return yield* incompleteChecks();
		return {
			headSha,
			fetchedAt: yield* DateTime.now,
			complete: runResult.complete && statusResult.complete,
			items: [...[...runs.values()].map((run) => {
				const conclusion = (run.status === "completed" ? run.conclusion ?? "pending" : run.status).replaceAll("_", " ");
				const linked = linkedReviewIds.get(run.id) ?? [];
				const reviews = linked.flatMap((id) => {
					const review = reviewDetails.get(id);
					return review && (review.body.length > 0 || review.inline.length > 0) ? [review] : [];
				});
				const context = [conclusion, linked.some((id) => unavailableReviews.has(id)) ? "Linked review evidence unavailable. Use the available check output and findings." : void 0].filter(Boolean).join("\n\n");
				let findings;
				if (reviews.length > 0) {
					const allocations = reviews.map((review) => ({
						...review,
						size: review.body.length + review.inline.length + (review.body.length > 0 && review.inline.length > 0 ? 2 : 0),
						budget: 0
					}));
					let remaining = checkDescriptionLimit - context.length - 2 - (reviews.length - 1) * 2;
					for (const [index, review] of [...allocations].sort((left, right) => left.size - right.size).entries()) {
						review.budget = Math.min(review.size, Math.floor(remaining / (reviews.length - index)));
						remaining -= review.budget;
					}
					findings = allocations.map(({ body, inline, budget }) => {
						const contentBudget = budget - (body.length > 0 && inline.length > 0 ? 2 : 0);
						const bodyBudget = contentBudget - Math.min(inline.length, Math.floor(contentBudget / 2));
						const inlineBudget = contentBudget - Math.min(body.length, bodyBudget);
						return [boundedGithubEvidence(body, bodyBudget), boundedGithubEvidence(inline, inlineBudget)].filter(Boolean).join("\n\n");
					}).filter(Boolean).join("\n\n");
				}
				const description = [
					context,
					findings,
					run.output?.title,
					run.output?.summary,
					run.output?.text
				].filter(Boolean).join("\n\n");
				return {
					id: `check:${run.id}`,
					name: run.name,
					state: checkRunState(run),
					description: description.slice(0, checkDescriptionLimit),
					descriptionTruncated: description.length > checkDescriptionLimit,
					hasReviewFindings: findings !== void 0 && findings.length > 0
				};
			}), ...[...statuses.values()].map((status) => ({
				id: `status:${status.id}`,
				name: status.context,
				state: Match.value(status.state).pipe(Match.when("pending", () => "pending"), Match.when("success", () => "passed"), Match.whenOr("failure", "error", () => "failed"), Match.exhaustive),
				description: (status.description ?? status.state).slice(0, checkDescriptionLimit),
				descriptionTruncated: (status.description?.length ?? 0) > checkDescriptionLimit
			}))]
		};
	}, Effect.timeout("20 seconds"));
	/** Polling supplies evidence only. Hosts authorize humans and persist processed comment IDs. */
	const comments = Effect.fn("RepairGithubClient.comments")(function* (work, since) {
		const after = yield* Schema.decodeEffect(Schema.DateTimeUtcFromString)(since);
		const pr = yield* readPullRequest(work);
		if (pr === null) return [];
		const installationId = yield* Schema.decodeEffect(Schema.FiniteFromString.check(Schema.isInt(), Schema.isGreaterThan(0)))(config.githubInstallationId);
		const scope = {
			...work,
			installationId
		};
		const results = [];
		const readPages = Effect.fnUntraced(function* (path, schema) {
			const rows = [];
			for (let page = 1; page <= 10; page++) {
				const response = yield* request(work, `${path}${path.includes("?") ? "&" : "?"}per_page=100&page=${page}`);
				yield* HttpClientResponse.filterStatusOk(response);
				const batch = yield* HttpClientResponse.schemaBodyJson(Schema.Array(schema).check(Schema.isMaxLength(100)))(response);
				rows.push(...batch);
				if (batch.length < 100) return rows;
			}
			return yield* ConflictError.make({ message: "GitHub comment history exceeds the readback limit" });
		});
		const query = `since=${encodeURIComponent(DateTime.formatIso(after))}`;
		const { issueComments, inlineComments, reviews } = yield* Effect.all({
			issueComments: readPages(`issues/${work.pullRequestNumber}/comments?${query}`, PolledComment),
			inlineComments: readPages(`pulls/${work.pullRequestNumber}/comments?sort=created&direction=asc&${query}`, PolledComment),
			reviews: readPages(`pulls/${work.pullRequestNumber}/reviews`, PolledReview)
		}, { concurrency: 3 });
		for (const [kind, entries] of [["issue_comment", issueComments], ["pull_request_review_comment", inlineComments]]) for (const entry of entries) {
			if (entry.user.type !== "User" || !entry.body?.trim() || DateTime.toEpochMillis(entry.created_at) < DateTime.toEpochMillis(after)) continue;
			const question = [entry.body, ...kind === "pull_request_review_comment" && entry.path ? [`Review context: ${entry.path}\n${entry.diff_hunk ?? ""}`] : []].join("\n\n");
			const decoded = yield* Schema.decodeEffect(GithubWork)({
				...scope,
				_tag: "comment",
				question: question.length > 4e3 ? `${question.slice(0, 3940)}\n[Truncated; see the GitHub comment.]` : question,
				comment: {
					kind,
					id: entry.id,
					author: entry.user.login,
					url: `https://github.com/${work.repository}/pull/${work.pullRequestNumber}#${kind === "issue_comment" ? "issuecomment-" : "discussion_r"}${entry.id}`,
					threadId: kind === "issue_comment" ? null : entry.in_reply_to_id ?? entry.id,
					createdAt: DateTime.formatIso(entry.created_at),
					...entry.commit_id === void 0 ? {} : { commitId: entry.commit_id }
				}
			});
			if (decoded._tag === "comment") results.push(decoded);
		}
		let bot;
		for (const review of reviews) {
			if (review.state === "PENDING" || review.state === "DISMISSED" || review.submitted_at === void 0 || review.submitted_at === null || DateTime.toEpochMillis(review.submitted_at) < DateTime.toEpochMillis(after) || !review.body?.trim() || !["User", "Bot"].includes(review.user.type)) continue;
			if (review.user.type === "Bot") {
				if (review.commit_id !== pr.head.sha) continue;
				bot ??= yield* botName();
				if (review.user.login !== bot) continue;
			}
			const decoded = yield* Schema.decodeEffect(GithubWork)({
				...scope,
				_tag: "comment",
				question: review.body.length > 4e3 ? `${review.body.slice(0, 3940)}\n[Truncated; see the GitHub review.]` : review.body,
				comment: {
					kind: "pull_request_review",
					id: review.id,
					author: review.user.login,
					url: `https://github.com/${work.repository}/pull/${work.pullRequestNumber}#pullrequestreview-${review.id}`,
					threadId: null,
					createdAt: DateTime.formatIso(review.submitted_at),
					commitId: review.commit_id
				}
			});
			if (decoded._tag === "comment") results.push(decoded);
		}
		if ((yield* readPullRequest(work))?.head.sha !== pr.head.sha) return yield* ServiceUnavailableError.make({ message: "The GitHub pull request changed during comment polling" });
		return results.sort((left, right) => (left.comment.createdAt ?? "").localeCompare(right.comment.createdAt ?? "") || left.comment.kind.localeCompare(right.comment.kind) || left.comment.id - right.comment.id);
	}, Effect.timeout("20 seconds"));
	const merge = Effect.fn("RepairGithubClient.merge")(function* (work, headSha) {
		const response = yield* request(work, `pulls/${work.pullRequestNumber}/merge`, {
			method: "PUT",
			body: {
				sha: headSha,
				merge_method: "squash"
			}
		});
		if ([
			405,
			409,
			422
		].includes(response.status)) return false;
		yield* HttpClientResponse.filterStatusOk(response);
		return (yield* HttpClientResponse.schemaBodyJson(Schema.Struct({ merged: Schema.Boolean }))(response)).merged;
	});
	const branchHead = Effect.fn("RepairGithubClient.branchHead")(function* (work, ref) {
		const response = yield* request(work, `git/ref/heads/${encodeURIComponent(ref)}`);
		yield* HttpClientResponse.filterStatusOk(response);
		return (yield* HttpClientResponse.schemaBodyJson(GithubRef)(response)).object.sha;
	});
	const collaborator = Effect.fn("RepairGithubClient.collaborator")(function* (work) {
		const response = yield* request(work, `collaborators/${work.comment.author}`);
		if (response.status === 404) return false;
		yield* HttpClientResponse.filterStatusOk(response);
		return response.status === 204;
	});
	const findReply = Effect.fn("RepairGithubClient.findReply")(function* (work, marker, author) {
		const location = replyLocation(work);
		for (let page = 1; page <= 50; page++) {
			const response = yield* request(work, `${location.list}?per_page=100&page=${page}`, void 0, void 0, location.discussion === "issues" ? "issues" : "review");
			yield* HttpClientResponse.filterStatusOk(response);
			const comments = yield* HttpClientResponse.schemaBodyJson(Schema.Array(Comment).check(Schema.isMaxLength(100)))(response);
			const found = comments.find((comment) => comment.user.login === author && comment.body.includes(marker));
			if (found) return found;
			if (comments.length < 100) return null;
		}
		return yield* ConflictError.make({ message: "GitHub reply history exceeds the readback limit" });
	});
	const postReply = Effect.fn("RepairGithubClient.postReply")(function* (work, body, deadline) {
		const location = replyLocation(work);
		const response = yield* request(work, location.post, {
			method: "POST",
			body: { body },
			discussion: location.discussion,
			deadline
		});
		if (response.status >= 400 && response.status < 500 && response.status !== 408) {
			yield* HttpClientResponse.filterStatusOk(response).pipe(Effect.catchCause((cause) => reportSanitizedCause(cause, { operation: "RepairGithubClient.postReply" }).pipe(report, Effect.timeout("1 second"), Effect.ignoreCause)));
			return { _tag: "rejected" };
		}
		yield* HttpClientResponse.filterStatusOk(response);
		return {
			_tag: "posted",
			comment: yield* HttpClientResponse.schemaBodyJson(Comment)(response)
		};
	}, Effect.catchTag("GithubWriteExpired", () => Effect.succeed({ _tag: "rejected" })));
	const replyPath = (work, id) => `${replyLocation(work).collection}/comments/${id}`;
	return {
		botName,
		pullRequest,
		merge,
		compareDiff,
		checks,
		comments,
		branchHead,
		collaborator,
		findReply,
		postReply,
		readReply: Effect.fn("RepairGithubClient.readReply")(function* (work, id) {
			const response = yield* request(work, replyPath(work, id), void 0, void 0, replyLocation(work).discussion === "issues" ? "issues" : "review");
			if (response.status === 404) return null;
			yield* HttpClientResponse.filterStatusOk(response);
			return yield* HttpClientResponse.schemaBodyJson(Comment)(response);
		}),
		editReply: Effect.fn("RepairGithubClient.editReply")(function* (work, id, body) {
			yield* request(work, replyPath(work, id), {
				method: "PATCH",
				body: { body },
				discussion: replyLocation(work).discussion
			}).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk));
		}),
		acknowledge: Effect.fn("RepairGithubClient.acknowledge")(function* (work) {
			if (work.comment.kind === "pull_request_review") return;
			yield* request(work, `${replyPath(work, work.comment.id)}/reactions`, {
				method: "POST",
				body: { content: "eyes" },
				discussion: "pull_requests"
			}).pipe(Effect.flatMap(HttpClientResponse.filterStatusOk));
		})
	};
});
var RepairGithubClient = class RepairGithubClient extends Context.Service()("@reve/repairs/RepairGithubClient") {
	static layer = Layer.effect(RepairGithubClient, make);
};
//#endregion
export { GithubRateLimited, RepairGithubClient };
