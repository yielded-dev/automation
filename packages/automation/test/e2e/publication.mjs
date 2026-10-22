import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Miniflare, convertV4MiniflareOptions } from "miniflare";
import { build } from "vite-plus/pack";
import { v5 as uuidV5 } from "uuid";
import { localPostgres } from "../../../../test/fixtures/postgres.mjs";

const root = fileURLToPath(new URL("../../", import.meta.url));
const directory = await mkdtemp(join(tmpdir(), "repairs-publication-"));
const quote = (value) => `'${value.replaceAll("'", "''")}'`;
const candidate = "b".repeat(40);
const expected = "a".repeat(40);
const actual = "c".repeat(40);
const content = { kind: "repair", title: "Synthetic repair", body: "Synthetic evidence" };
const intent = JSON.stringify({ sourceHead: candidate, previousHead: expected, content });
const evidence = {
  scope:
    "Native workerd and real local Postgres; actual GithubFollowUps/Publication/RunClient, synthetic host policy, GitHub and read-only saved-snapshot ports. No container, model, provider, consumer or production operation.",
  head: execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim(),
  publicationSha256: createHash("sha256")
    .update(await readFile(join(root, "src/server/Publication.ts")))
    .digest("hex"),
  cases: [],
  passed: false,
};
console.log(`Native publication evidence: ${directory}`);
let pg;
let mf;
try {
  pg = await localPostgres(directory);
  evidence.postgres = pg.version;
  pg.migrate();
  const outDir = join(directory, "bundle");
  await build({
    cwd: root,
    config: false,
    entry: { publication: "test/e2e/publication.worker.ts" },
    outDir,
    outExtensions: () => ({ js: ".mjs" }),
    platform: "neutral",
    target: "es2022",
    dts: false,
    deps: {
      alwaysBundle: [/^(?!cloudflare:|node:)/],
      neverBundle: [/^(cloudflare:|node:)/],
      onlyBundle: false,
    },
  });
  mf = new Miniflare(
    convertV4MiniflareOptions({
      name: "publication-fixture",
      scriptPath: join(outDir, "publication.mjs"),
      modulesRoot: outDir,
      modules: true,
      compatibilityDate: "2026-09-30",
      compatibilityFlags: ["nodejs_compat"],
      hyperdrives: { DATABASE: pg.url },
      cf: false,
      telemetry: { enabled: false },
      outboundService: () => {
        throw new Error("Unexpected external request");
      },
    }),
  );
  const create = (retained = false, retired = false, kind = "rebase") => {
    const attemptId = randomUUID();
    const launch = {
      runId: uuidV5(JSON.stringify(["kommunikasie-repair", "run", attemptId]), uuidV5.URL),
      origin: {
        incidentId: randomUUID(),
        attemptId,
        evidenceManifestId: randomUUID(),
        repository: "fixture/project",
        evidenceHash: "d".repeat(64),
        offendingSha: expected,
        targetSha: expected,
        deadlineAtMs: Date.now() + 600_000,
      },
      launch: {
        seedRepository: "fixture-source",
        githubRepositoryId: 1,
        targetSha: expected,
        targetRef: "main",
        title: "Synthetic repair",
        prompt: "Unused synthetic input",
        modelSettings: { model: "fixture", reasoningEffort: "high", fast: false },
        evidenceHash: "d".repeat(64),
        allowedScopes: ["host_publication"],
        promptPolicy: "fixture",
        ...(kind === "rebase"
          ? { rebase: { branch: "repair/fixture", headSha: expected, pullRequestNumber: 1 } }
          : kind === "continuation" || kind === "unpublished-continuation"
            ? {
                continuation: {
                  runId: randomUUID(),
                  candidateSha: candidate,
                  publication:
                    kind === "continuation"
                      ? {
                          attemptId: randomUUID(),
                          candidateSha: expected,
                          pullRequestNumber: 1,
                        }
                      : null,
                },
              }
            : {}),
      },
    };
    const state = {
      ...launch,
      outcome: "completed",
      retirement: retired ? "retired" : "active",
      stopReason: retired ? "deadline" : null,
      retiredAtMs: retired ? Date.now() : null,
    };
    pg.sql(`INSERT INTO repair_executions(run_id, attempt_id, launch, state, updated_at)
      VALUES (${quote(launch.runId)}, ${quote(attemptId)}, ${quote(JSON.stringify(launch))}, ${quote(JSON.stringify(state))}, now())`);
    if (retained)
      pg.sql(`INSERT INTO repair_publication(run_id, source_head, kind, intent)
      VALUES (${quote(launch.runId)}, ${quote(candidate)}, 'repair', ${quote(intent)})`);
    return launch;
  };
  const request = async (
    launch,
    mode,
    fault = "none",
    remoteHead = actual,
    candidateSha = candidate,
    { draft, pullRequest, verifiedHead } = {},
  ) => {
    const response = await mf.dispatchFetch("http://localhost/", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        launch,
        mode,
        fault,
        remoteHead,
        pullRequest,
        verifiedHead,
        publication: {
          candidateSha,
          draft,
          summary: {
            title: content.title,
            problem: "Synthetic problem",
            fix: "Synthetic fix",
            behaviorChange: null,
            keyChange: null,
            reviewNotes: [],
          },
        },
      }),
      signal: AbortSignal.timeout(30_000),
    });
    const text = await response.text();
    assert.equal(response.status, 200, text);
    return JSON.parse(text);
  };
  const pending = (launch) =>
    pg.sql(`SELECT intent, receipt FROM repair_publication WHERE run_id=${quote(launch.runId)}`);
  const abandonment = (launch) =>
    pg.sql(
      `SELECT count(*) FROM repair_publication_abandonment WHERE run_id=${quote(launch.runId)}`,
    );
  const record = async (name, run) => {
    const item = { name, passed: false };
    evidence.cases.push(item);
    try {
      await run(item);
      item.passed = true;
    } catch (error) {
      item.failure = String(error.stack ?? error);
    }
    console.log(JSON.stringify(item));
  };
  await record("publish-unpublished-continuation-without-new-commit", async (item) => {
    const launch = create(false, false, "unpublished-continuation");
    item.accepted = await request(launch, "publish", "none", null, candidate, {
      pullRequest: "missing",
    });
    assert.deepEqual(item.accepted.reasons, [{ kind: "Fail", tag: "RepairUnavailable" }]);
    const acceptedIntent = JSON.stringify({ sourceHead: candidate, previousHead: null, content });
    assert.equal(pending(launch), `${acceptedIntent}|`);
    // Resume after the exact saved commit reached the remote branch.
    item.published = await request(launch, "publish", "none", candidate, candidate, {
      pullRequest: "missing",
    });
    assert.equal(item.published.exit, "Success", JSON.stringify(item.published));
    assert.equal(item.published.value.sourceHead, launch.launch.continuation.candidateSha);
    assert.equal(item.published.value.number, 1);
    assert.deepEqual(
      item.published.requests.filter((request) => request.method !== "GET"),
      [
        {
          method: "POST",
          path: "/repos/fixture/project/pulls",
          body: {
            title: content.title,
            head: `repair/${launch.origin.incidentId}-${launch.origin.attemptId}`,
            base: "main",
            draft: false,
            body: content.body,
          },
        },
      ],
    );
  });
  for (const kind of ["initial", "continuation", "rebase"]) {
    await record(`${kind}-requires-unpublished-changes`, async (item) => {
      const launch = create(false, false, kind);
      item.result = await request(launch, "publish", "none", expected, expected);
      assert.deepEqual(item.result.reasons, [{ kind: "Fail", tag: "RepairConflict" }]);
      assert.equal(pending(launch), "");
      assert.deepEqual(item.result.requests, []);
    });
  }
  await record("continuation-requires-exact-saved-snapshot", async (item) => {
    const launch = create(false, false, "unpublished-continuation");
    item.result = await request(launch, "publish", "none", null, candidate, {
      pullRequest: "missing",
      verifiedHead: actual,
    });
    assert.deepEqual(item.result.reasons, [{ kind: "Fail", tag: "RepairConflict" }]);
    assert.equal(pending(launch), "");
    assert.deepEqual(item.result.requests, []);
  });
  for (const draft of [undefined, false, true]) {
    await record(`publication-draft-${draft ?? "default"}`, async (item) => {
      const launch = create(false, false, "initial");
      // Stop at the unavailable native port, after the first intent has been accepted.
      item.accepted = await request(launch, "publish", "none", null, candidate, {
        draft,
        pullRequest: "missing",
      });
      assert.deepEqual(item.accepted.reasons, [{ kind: "Fail", tag: "RepairUnavailable" }]);
      const acceptedIntent = JSON.stringify({
        sourceHead: candidate,
        previousHead: null,
        content,
        ...(draft === undefined ? {} : { draft }),
      });
      assert.equal(pending(launch), `${acceptedIntent}|`);
      // Recover a completed push, ignoring a retry's changed publication choice.
      item.created = await request(
        launch,
        "publish",
        draft === true ? "create-response-lost" : "none",
        candidate,
        candidate,
        { draft: !draft, pullRequest: "missing" },
      );
      assert.equal(item.created.exit, "Success", JSON.stringify(item.created));
      assert.deepEqual(
        item.created.requests.filter((request) => request.method !== "GET"),
        [
          {
            method: "POST",
            path: "/repos/fixture/project/pulls",
            body: {
              title: content.title,
              head: `repair/${launch.origin.incidentId}-${launch.origin.attemptId}`,
              base: "main",
              draft: draft ?? false,
              body: content.body,
            },
          },
        ],
      );
      const hash = createHash("sha256").update(acceptedIntent).digest("hex");
      assert.equal(item.created.value.contentHash, hash);
      assert.equal(
        item.created.value.id,
        uuidV5(
          JSON.stringify(["kommunikasie-repair", "publication", launch.runId, candidate, hash]),
          uuidV5.URL,
        ),
      );
      assert.equal(
        pg.sql(`SELECT intent FROM repair_publication WHERE run_id=${quote(launch.runId)}`),
        acceptedIntent,
      );
      item.replay = await request(launch, "publish", "installation", candidate, candidate, {
        draft: !draft,
      });
      assert.equal(item.replay.exit, "Success");
      assert.deepEqual(item.replay.value, item.created.value);
      assert.deepEqual(item.replay.requests, []);
    });
  }
  await record("draft-does-not-convert-existing-pr", async (item) => {
    const launch = create(false, false, "initial");
    item.direct = await request(launch, "direct", "none", expected, candidate, { draft: true });
    item.client = await request(launch, "publish", "none", expected, candidate, { draft: true });
    assert.deepEqual(item.direct.reasons, [{ kind: "Fail", tag: "PublicationError" }]);
    assert.deepEqual(item.client.reasons, [{ kind: "Fail", tag: "RepairUnavailable" }]);
    assert.equal(pending(launch), "");
    assert.ok([...item.direct.requests, ...item.client.requests].every((r) => r.method === "GET"));
  });
  for (const pullRequest of ["draft", "ready"]) {
    await record(`draft-reconciliation-${pullRequest}`, async (item) => {
      const launch = create(true, true, "initial");
      const acceptedIntent = JSON.stringify({
        sourceHead: candidate,
        previousHead: expected,
        content,
        draft: true,
      });
      pg.sql(`UPDATE repair_publication SET intent=${quote(acceptedIntent)}
        WHERE run_id=${quote(launch.runId)}`);
      item.result = await request(launch, "reconcile", "none", candidate, candidate, {
        draft: false,
        pullRequest,
      });
      assert.ok(item.result.requests.every((request) => request.method === "GET"));
      if (pullRequest === "draft") {
        assert.equal(item.result.exit, "Success", JSON.stringify(item.result));
        assert.equal(item.result.value.sourceHead, candidate);
        assert.equal(
          item.result.value.contentHash,
          createHash("sha256").update(acceptedIntent).digest("hex"),
        );
      } else {
        assert.deepEqual(item.result.reasons, [{ kind: "Fail", tag: "RepairUnavailable" }]);
        assert.equal(pending(launch), `${acceptedIntent}|`);
      }
    });
  }
  await record("draft-create-requires-matching-response", async (item) => {
    const launch = create(false, false, "initial");
    const acceptedIntent = JSON.stringify({
      sourceHead: candidate,
      previousHead: null,
      content,
      draft: true,
    });
    pg.sql(`INSERT INTO repair_publication(run_id, source_head, kind, intent)
      VALUES (${quote(launch.runId)}, ${quote(candidate)}, 'repair', ${quote(acceptedIntent)})`);
    item.result = await request(launch, "publish", "create-draft-mismatch", candidate, candidate, {
      pullRequest: "missing",
    });
    assert.deepEqual(item.result.reasons, [{ kind: "Fail", tag: "RepairUnavailable" }]);
    assert.equal(pending(launch), `${acceptedIntent}|`);
    assert.equal(item.result.requests.filter((request) => request.method === "POST").length, 1);
    assert.ok(item.result.requests.every((request) => request.method !== "PATCH"));
  });
  for (const kind of ["continuation", "rebase"]) {
    for (const pullRequest of ["draft", "ready"]) {
      await record(`${kind}-preserves-existing-${pullRequest}`, async (item) => {
        const launch = create(false, false, kind);
        const draft = pullRequest === "ready";
        item.accepted = await request(launch, "publish", "none", expected, candidate, {
          draft,
          pullRequest,
        });
        assert.deepEqual(item.accepted.reasons, [{ kind: "Fail", tag: "RepairUnavailable" }]);
        const acceptedIntent = JSON.stringify({
          sourceHead: candidate,
          previousHead: expected,
          content,
          draft,
        });
        assert.equal(pending(launch), `${acceptedIntent}|`);
        assert.ok(item.accepted.requests.every((request) => request.method === "GET"));
        // Recover the completed push and refresh content without converting the existing PR.
        item.published = await request(launch, "publish", "stale-content", candidate, candidate, {
          draft: !draft,
          pullRequest,
        });
        assert.equal(item.published.exit, "Success", JSON.stringify(item.published));
        assert.equal(item.published.value.number, 1);
        assert.equal(item.published.value.sourceHead, candidate);
        assert.deepEqual(item.published.pullRequest, { number: 1, draft: !draft });
        assert.deepEqual(
          item.published.requests.filter((request) => request.method !== "GET"),
          [
            {
              method: "PATCH",
              path: "/repos/fixture/project/pulls/1",
              body: { title: content.title, body: content.body },
            },
          ],
        );
        assert.equal(
          pg.sql(`SELECT intent FROM repair_publication WHERE run_id=${quote(launch.runId)}`),
          acceptedIntent,
        );
        assert.equal(
          item.published.value.contentHash,
          createHash("sha256").update(acceptedIntent).digest("hex"),
        );
      });
    }
  }
  await record("pre-intent-head-conflict", async (item) => {
    const launch = create();
    item.direct = await request(launch, "direct");
    item.client = await request(launch, "publish");
    item.publication = pending(launch);
    assert.deepEqual(item.direct.reasons, [{ kind: "Fail", tag: "RepairConflict" }]);
    assert.deepEqual(item.client.reasons, [{ kind: "Fail", tag: "RepairConflict" }]);
    assert.deepEqual(item.direct.reports, []);
    assert.deepEqual(item.client.reports, []);
    assert.equal(item.publication, "");
    assert.ok([...item.direct.requests, ...item.client.requests].every((r) => r.method === "GET"));
  });
  await record("retained-intent-head-mismatch", async (item) => {
    const launch = create(true);
    const saved = pending(launch);
    item.direct = await request(launch, "direct");
    item.client = await request(launch, "publish");
    assert.deepEqual(item.direct.reasons, [{ kind: "Fail", tag: "PublicationError" }]);
    assert.deepEqual(item.client.reasons, [{ kind: "Fail", tag: "RepairUnavailable" }]);
    assert.equal(pending(launch), saved);
    assert.equal(abandonment(launch), "0");
  });
  await record("retired-readback-conflict", async (item) => {
    const launch = create(true, true);
    const saved = pending(launch);
    item.result = await request(launch, "abandon", "installation");
    assert.equal(item.result.exit, "Success");
    assert.equal(pending(launch), saved);
    assert.equal(abandonment(launch), "1");
    assert.deepEqual(item.result.requests, []);
  });
  await record("retirement-readback-conflict", async (item) => {
    const launch = create(true);
    const saved = pending(launch);
    item.result = await request(launch, "retire", "installation");
    assert.equal(item.result.exit, "Success");
    assert.equal(item.result.value.retirement, "retired");
    assert.equal(item.result.value.publicationPending, true);
    assert.equal(pending(launch), saved);
    assert.equal(abandonment(launch), "0");
    assert.deepEqual(item.result.requests, []);
  });
  // Regression: https://reve-r6.sentry.io/issues/7769571544/
  // A terminal cancellation may still retain native compute for evidence delivery.
  for (const fault of ["cancel-pending", "cancel-mixed"]) {
    await record(fault, async (item) => {
      const launch = create();
      const admission = {
        id: launch.runId,
        commandId: randomUUID(),
        title: "Retained cancellation",
        prompt: "Unused",
        modelSettings: launch.launch.modelSettings,
        execution: "commands",
        source: { _tag: "None" },
        deadlineAtMillis: launch.origin.deadlineAtMs,
        outputSchema: {},
        requiredCommits: [],
      };
      pg.sql(`INSERT INTO repair_execution_receipts(run_id, kind, key, fingerprint, intent, created_at)
        VALUES (${quote(launch.runId)}, 'sandbox-admission', 'launch', 'fixture', ${quote(JSON.stringify(admission))}, now())`);
      const saved = () =>
        JSON.parse(
          pg.sql(`SELECT state FROM repair_executions WHERE run_id=${quote(launch.runId)}`),
        );
      item.first = await request(launch, "retire", fault);
      assert.equal(saved().retirement, "draining");
      assert.equal(saved().retiredAtMs, null);
      assert.equal(saved().origin.deadlineAtMs, launch.origin.deadlineAtMs);
      if (fault === "cancel-mixed") {
        assert.equal(item.first.exit, "Failure");
        assert.deepEqual(item.first.reasons, [
          { kind: "Fail", tag: "RepairUnavailable" },
          { kind: "Die", value: "fixture-cancel-defect" },
          { kind: "Interrupt", fiberId: 43 },
        ]);
        assert.equal(item.first.reports.length, 1);
        return;
      }
      assert.equal(item.first.exit, "Success");
      assert.equal(item.first.value.retirement, "draining");
      assert.deepEqual(item.first.reports, []);
      item.replay = await request(launch, "retire", fault);
      assert.equal(item.replay.value.retirement, "draining");
      assert.deepEqual(item.replay.cancellations, item.first.cancellations);
      item.complete = await request(launch, "retire");
      assert.equal(item.complete.exit, "Success");
      assert.equal(item.complete.value.retirement, "retired");
      assert.deepEqual(item.complete.reports, []);
      assert.deepEqual(item.complete.cancellations, item.first.cancellations);
      assert.deepEqual(item.complete.cancellations[0].admission, admission);
      assert.deepEqual(item.complete.cancellations[0].options, { allowDraining: true });
      assert.equal(saved().origin.deadlineAtMs, launch.origin.deadlineAtMs);
    });
  }
  await record("mixed-publication-cause", async (item) => {
    const launch = create();
    item.result = await request(launch, "direct", "mixed");
    assert.deepEqual(item.result.reasons, [
      { kind: "Fail", tag: "PublicationError" },
      { kind: "Die", value: "fixture-defect" },
      { kind: "Interrupt", fiberId: 42 },
    ]);
    assert.equal(pending(launch), "");
  });
  await record("mixed-readback-cause", async (item) => {
    const launch = create(true, true);
    const saved = pending(launch);
    item.result = await request(launch, "abandon", "mixed");
    item.abandonments = abandonment(launch);
    assert.equal(item.result.exit, "Failure");
    assert.deepEqual(item.result.reasons, [
      { kind: "Fail", tag: "RepairUnavailable" },
      { kind: "Die", value: "fixture-defect" },
      { kind: "Interrupt", fiberId: 42 },
    ]);
    assert.equal(pending(launch), saved);
    assert.equal(item.abandonments, "0");
  });
  for (const mode of ["follow-up-checks", "follow-up-review"]) {
    await record(mode, async (item) => {
      const source = create();
      if (mode === "follow-up-checks") {
        source.origin.proofPolicy = { retained: "legacy-policy" };
        source.launch.proofPolicy = { retained: "legacy-policy" };
      }
      const retainedSource = JSON.stringify(source);
      const state = {
        ...source,
        outcome: "completed",
        retirement: "retired",
        stopReason: null,
        retiredAtMs: Date.now(),
      };
      const receipt = {
        id: randomUUID(),
        kind: "repair",
        sourceHead: candidate,
        contentHash: "e".repeat(64),
        branch: "repair/fixture",
        number: 1,
        url: "https://github.com/fixture/project/pull/1",
      };
      pg.sql(`UPDATE repair_executions SET launch = ${quote(retainedSource)},
        state = ${quote(JSON.stringify(state))}, candidate_sha = ${quote(candidate)}
        WHERE run_id = ${quote(source.runId)};
        INSERT INTO repair_publication(run_id, source_head, kind, intent, receipt)
        VALUES (${quote(source.runId)}, ${quote(candidate)}, 'repair', ${quote(intent)}, ${quote(JSON.stringify(receipt))})`);
      item.prepared = await request(source, mode, "none", candidate);
      assert.equal(item.prepared.exit, "Success", JSON.stringify(item.prepared));
      const followUp = item.prepared.value;
      assert.ok(followUp);
      assert.equal(followUp.origin.proofPolicy, undefined);
      assert.equal(followUp.launch.proofPolicy, undefined);
      assert.equal(
        pg.sql(`SELECT launch FROM repair_executions WHERE run_id = ${quote(source.runId)}`),
        retainedSource,
      );
      // Recover the accepted publication after GitHub received the new head but its receipt was lost.
      pg.sql(
        `UPDATE repair_executions SET state = ${quote(
          JSON.stringify({
            ...followUp,
            outcome: "completed",
            retirement: "active",
            stopReason: null,
            retiredAtMs: null,
          }),
        )} WHERE run_id = ${quote(followUp.runId)};
        INSERT INTO repair_publication(run_id, source_head, kind, intent)
        VALUES (${quote(followUp.runId)}, ${quote(actual)}, 'repair', ${quote(
          JSON.stringify({
            sourceHead: actual,
            previousHead: candidate,
            content,
          }),
        )})`,
      );
      item.published = await request(followUp, "publish", "none", actual, actual);
      assert.equal(item.published.exit, "Success", JSON.stringify(item.published));
      assert.equal(item.published.value.branch, receipt.branch);
      assert.equal(item.published.value.number, receipt.number);
      assert.equal(item.published.value.sourceHead, actual);
      assert.ok(item.published.requests.every((request) => request.method === "GET"));
      assert.ok(pending(followUp).includes('"branch":"repair/fixture"'));
    });
  }
  evidence.passed = evidence.cases.every((item) => item.passed);
  assert.ok(evidence.passed, "Publication regression failed; see retained native evidence");
} finally {
  await mf?.dispose();
  await pg?.close();
  await writeFile(join(directory, "results.json"), JSON.stringify(evidence, null, 2));
}
