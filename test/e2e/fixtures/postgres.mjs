import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readFile, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { join } from "node:path";

/** Disposable local cluster; no consumer database, environment credentials or shared resources. */
export async function localPostgres(directory) {
  const listener = createServer();
  await new Promise((resolve, reject) =>
    listener.once("error", reject).listen(0, "127.0.0.1", resolve),
  );
  const port = listener.address().port;
  await new Promise((resolve) => listener.close(resolve));
  const data = join(directory, "pg");
  const log = join(directory, "postgres.log");
  const run = (file, args, input) =>
    execFileSync(file, args, {
      encoding: "utf8",
      input,
      timeout: 30_000,
      env: { PATH: process.env.PATH, LC_ALL: "C", TZ: "UTC" },
      stdio: ["pipe", "pipe", "pipe"],
    }).trim();
  run("initdb", ["-D", data, "-A", "trust", "-U", "postgres", "--no-locale", "-E", "UTF8"]);
  run("pg_ctl", [
    "-D",
    data,
    "-l",
    log,
    "-o",
    `-h 127.0.0.1 -k ${directory} -p ${port} -c log_statement=all -c log_line_prefix='%a '`,
    "-w",
    "start",
  ]);
  const sql = (input) =>
    run(
      "psql",
      [
        "-X",
        "-h",
        directory,
        "-p",
        String(port),
        "-U",
        "postgres",
        "-d",
        "postgres",
        "-v",
        "ON_ERROR_STOP=1",
        "-Atq",
      ],
      input,
    );
  assert.equal(sql("SELECT 1"), "1");
  return {
    sql,
    version: run("postgres", ["--version"]),
    url: `postgresql://postgres:synthetic@127.0.0.1:${port}/postgres`,
    statements: async (application) => {
      const lines = (await readFile(log, "utf8")).split("\n");
      return lines.flatMap((line, index) => {
        if (!line.startsWith(`${application} LOG:  `)) return [];
        const match = /(?:execute [^:]+:|statement:) (.*)$/.exec(line);
        if (!match) return [];
        // Multiline SQL templates may start on the next log line.
        return [match[1].trim() || lines[index + 1].trim()];
      });
    },
    async close() {
      run("pg_ctl", ["-D", data, "-m", "immediate", "-w", "stop"]);
      await rm(data, { recursive: true });
    },
  };
}
