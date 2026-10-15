import { RepositoryCommitSha } from "../model/RepositorySource.mjs";
import { RepositoryGitPack } from "./RepositoryGitPack.mjs";
import { RepositoryGitSource } from "./RepositoryGitSource.mjs";
import { RepositoryGitTransferFailure } from "./RepositoryGitTransfer.mjs";
import { RepositoryGitPackManifest, repositoryGitPackFiles, repositoryGitPackScript } from "./RepositoryGitPackEncoding.mjs";
import { Cause, Effect, FileSystem, Layer, Path, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
//#region src/repository/RepositoryGitLocal.ts
const preparationCause = (cause) => Cause.map(cause, (error) => Schema.is(RepositoryGitTransferFailure)(error) ? error : RepositoryGitTransferFailure.make({
	reason: "repack",
	message: "Local repository Git pack preparation did not complete",
	cause: error
}));
const cleanupCause = (cause) => Cause.fromReasons(cause.reasons.map((reason) => Cause.isFailReason(reason) ? Cause.makeDieReason(reason.error) : reason));
const failed = () => RepositoryGitTransferFailure.make({
	reason: "repack",
	message: "Local repository Git pack preparation did not complete"
});
const LocalPackLive = Layer.effect(RepositoryGitPack, Effect.gen(function* () {
	const fs = yield* FileSystem.FileSystem;
	const path = yield* Path.Path;
	const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
	return { prepare: (pack) => Stream.unwrap(Effect.gen(function* () {
		const directory = yield* Effect.acquireRelease(fs.makeTempDirectory({ prefix: "repository-git-pack-" }), (directory) => fs.remove(directory, { recursive: true }).pipe(Effect.catchCause((cause) => Effect.failCause(cleanupCause(cause)))));
		let inputBytes = 0;
		yield* pack.pipe(Stream.tap((chunk) => Effect.sync(() => {
			inputBytes += chunk.byteLength;
		})), Stream.run(fs.sink(path.join(directory, "input.pack"))), Effect.ensuring(Effect.suspend(() => Effect.annotateCurrentSpan("repository.git.pack.input_bytes", inputBytes))), Effect.withSpan("RepositoryGitPackLocal.writeInput"));
		if ((yield* spawner.exitCode(ChildProcess.make("bash", [
			"-euo",
			"pipefail",
			"-c",
			repositoryGitPackScript
		], {
			cwd: directory,
			stdin: "ignore",
			stdout: "ignore",
			stderr: "ignore"
		})).pipe(Effect.timeout("2 minutes"), Effect.withSpan("RepositoryGitPackLocal.repack"))) !== 0) return yield* failed();
		const manifest = yield* Schema.decodeEffect(RepositoryGitPackManifest)(yield* fs.readFileString(path.join(directory, "manifest.json")));
		const files = repositoryGitPackFiles(manifest);
		let outputBytes = 0;
		const parts = files.map((file, index) => ({
			_tag: file.mode,
			stagingHead: file.stagingHead,
			body: fs.stream(path.join(directory, "output", file.name)).pipe(Stream.tap((chunk) => Effect.sync(() => {
				outputBytes += chunk.byteLength;
			})), Stream.catchCause((cause) => Stream.failCause(preparationCause(cause))), Stream.withSpan("RepositoryGitPackLocal.readOutput", { attributes: {
				"repository.git.pack.part_index": index,
				"repository.git.pack.part_bytes": file.bytes
			} }))
		}));
		yield* Effect.annotateCurrentSpan({
			"repository.git.pack.part_count": files.length,
			"repository.git.pack.data_part_count": manifest.parts.length,
			"repository.git.pack.max_part_bytes": Math.max(...files.map((file) => file.bytes))
		});
		return Stream.fromIterable(parts).pipe(Stream.ensuring(Effect.suspend(() => Effect.annotateCurrentSpan("repository.git.pack.output_bytes", outputBytes))));
	})).pipe(Stream.catchCause((cause) => Stream.failCause(preparationCause(cause))), Stream.withSpan("RepositoryGitPackLocal.prepare", { attributes: { "repository.git.pack.encoding": "full" } })) };
}));
/** Local Git supplies only the pinned graph; the shared encoder and publisher own remote writes. */
const repositoryGitLocalLayer = (checkout) => Layer.merge(LocalPackLive, Layer.effect(RepositoryGitSource, Effect.gen(function* () {
	const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
	const text = Effect.fn("RepositoryGitLocal.inspect")(function* (args, input) {
		const process = yield* spawner.spawn(ChildProcess.make("git", [
			"-c",
			"core.hooksPath=/dev/null",
			...args
		], {
			cwd: checkout,
			stdin: input === void 0 ? "ignore" : Stream.make(new TextEncoder().encode(input)),
			stdout: "pipe",
			stderr: "ignore"
		}));
		const [output, exitCode] = yield* Effect.all([Stream.mkString(Stream.decodeText(process.stdout)), process.exitCode], { concurrency: "unbounded" });
		if (exitCode !== 0) return yield* failed();
		return output.trim();
	}, Effect.scoped);
	return { pack: ({ commitSha, haves }) => Stream.unwrap(Effect.gen(function* () {
		if ((yield* text(["rev-parse", "--is-shallow-repository"])) !== "false") return yield* failed();
		const known = [];
		if (haves.size > 0) {
			const inspected = yield* text(["cat-file", "--batch-check=%(objectname) %(objecttype)"], `${[...haves].join("\n")}\n`);
			for (const line of inspected.split("\n")) {
				const [sha, type] = line.split(" ");
				if (sha !== void 0 && Schema.is(RepositoryCommitSha)(sha) && haves.has(sha) && type === "commit") known.push(sha);
			}
		}
		const revisions = `${commitSha}\n${known.map((sha) => `^${sha}\n`).join("")}`;
		const process = yield* spawner.spawn(ChildProcess.make("git", [
			"-c",
			"core.hooksPath=/dev/null",
			"pack-objects",
			"--stdout",
			"--revs"
		], {
			cwd: checkout,
			stdin: Stream.make(new TextEncoder().encode(revisions)),
			stdout: "pipe",
			stderr: "ignore"
		}));
		yield* Effect.annotateCurrentSpan("repository.git.local.have_count", known.length);
		return process.stdout.pipe(Stream.concat(Stream.fromEffect(process.exitCode.pipe(Effect.flatMap((code) => code === 0 ? Effect.void : failed()))).pipe(Stream.drain)));
	})).pipe(Stream.catchCause((cause) => Stream.failCause(preparationCause(cause))), Stream.withSpan("RepositoryGitLocal.pack")) };
})));
//#endregion
export { repositoryGitLocalLayer };
