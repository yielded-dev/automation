const fs = require("node:fs");
const cp = require("node:child_process");

const main = async () => {
  const names = JSON.parse(process.argv[2]);
  const secrets = names
    .map((name) => Buffer.from(process.env[name] || ""))
    .filter((value) => value.length > 0);

  if (secrets.length !== names.length) process.exit(2);
  const overlap = Math.max(...secrets.map((value) => value.length)) - 1;
  const git = (...args) => {
    const result = cp.spawnSync("git", ["--no-replace-objects", ...args], {
      maxBuffer: 8 * 1024 * 1024,
      timeout: 10000,
    });

    if (result.error || result.status !== 0) process.exit(2);

    return result.stdout;
  };
  const base = process.argv[3];
  const requestedHead = process.argv[4];

  if (!/^[0-9a-f]{40,64}$/.test(base) || !/^(?:HEAD|[0-9a-f]{40,64})$/.test(requestedHead))
    process.exit(2);
  const head = git("rev-parse", "--verify", `${requestedHead}^{commit}`).toString().trim();

  if (requestedHead !== "HEAD" && head !== requestedHead) process.exit(2);
  git("merge-base", "--is-ancestor", base, head);
  const listing = git("ls-files", "--cached", "--others", "--exclude-standard", "-z");
  let scanned = 0;
  const contains = (bytes) => secrets.some((secret) => bytes.includes(secret));
  let tail = Buffer.alloc(0);
  const inspect = (chunk) => {
    scanned += chunk.length;
    if (scanned > 512 * 1024 * 1024) process.exit(2);
    const bytes = Buffer.concat([tail, chunk]);

    if (contains(bytes)) process.exit(1);
    tail = overlap === 0 ? Buffer.alloc(0) : bytes.subarray(Math.max(0, bytes.length - overlap));
  };
  let offset = 0;

  while (offset < listing.length) {
    const end = listing.indexOf(0, offset);

    if (end < 0) process.exit(2);
    const name = listing.subarray(offset, end);

    offset = end + 1;
    let stat;

    try {
      stat = fs.lstatSync(name);
    } catch (error) {
      if (error.code === "ENOENT") continue;
      throw error;
    }
    if (stat.isSymbolicLink()) {
      if (contains(fs.readlinkSync(name, { encoding: "buffer" }))) process.exit(1);
      continue;
    }
    if (stat.isDirectory()) continue;
    if (!stat.isFile()) process.exit(2);
    const fd = fs.openSync(name, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);

    try {
      tail = Buffer.alloc(0);
      const chunk = Buffer.alloc(65536);

      for (;;) {
        const size = fs.readSync(fd, chunk, 0, chunk.length, null);

        if (size === 0) break;
        inspect(chunk.subarray(0, size));
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  // The final tree can be clean while an intermediate commit still contains a
  // credential. Inspect every outgoing object, including commit text and paths.
  const objects = git("rev-list", "--objects", "--no-object-names", head, `^${base}`);
  const ids = objects.toString().trim().split("\n").filter(Boolean);

  if (ids.length === 0) return;
  if (ids.some((id) => !/^[0-9a-f]{40,64}$/.test(id))) process.exit(2);
  const child = cp.spawn("git", ["--no-replace-objects", "cat-file", "--batch=%(objectsize)"], {
    stdio: ["pipe", "pipe", "ignore"],
    timeout: 40000,
    killSignal: "SIGKILL",
  });
  const succeeded = new Promise((resolve) => {
    child.once("error", () => resolve(false));
    child.once("close", (code) => resolve(code === 0));
  });

  child.stdin.on("error", () => {});
  child.stdin.end(objects);
  let header = Buffer.alloc(0);
  let remaining = 0;
  let separator = false;
  let count = 0;

  for await (const chunk of child.stdout) {
    let position = 0;

    while (position < chunk.length) {
      if (separator) {
        if (chunk[position++] !== 10) process.exit(2);
        separator = false;
      } else if (remaining === 0) {
        const newline = chunk.indexOf(10, position);
        const end = newline < 0 ? chunk.length : newline;

        header = Buffer.concat([header, chunk.subarray(position, end)]);
        if (header.length > 20) process.exit(2);
        position = end;
        if (newline < 0) continue;
        if (!/^\d+$/.test(header.toString())) process.exit(2);
        remaining = Number(header.toString());
        if (!Number.isSafeInteger(remaining) || remaining > 512 * 1024 * 1024 - scanned)
          process.exit(2);
        header = Buffer.alloc(0);
        tail = Buffer.alloc(0);
        count++;
        position++;
        separator = remaining === 0;
      } else {
        const size = Math.min(remaining, chunk.length - position);

        inspect(chunk.subarray(position, position + size));
        remaining -= size;
        position += size;
        separator = remaining === 0;
      }
    }
  }
  if (
    !(await succeeded) ||
    count !== ids.length ||
    remaining !== 0 ||
    separator ||
    header.length > 0
  )
    process.exit(2);
};

main().catch(() => process.exit(2));
