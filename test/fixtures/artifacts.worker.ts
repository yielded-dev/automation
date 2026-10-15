import type { Artifacts } from "effect-cf";

/** External Artifacts control plane only; Git transport and Worker storage remain native. */
export const fixtureArtifacts = (binding: Fetcher): Artifacts.ArtifactsBinding => {
  const call = async <A>(operation: string, name: string, args: unknown[] = []): Promise<A> => {
    const response = await binding.fetch("http://fixture.invalid/control", {
      method: "POST",
      body: JSON.stringify({ operation, name, args }),
    });
    if (!response.ok) {
      // oxlint-disable-next-line no-restricted-syntax -- Foreign Artifacts binding rejects native errors; its adapter retains the typed failure.
      throw Object.assign(new Error("Local Git fixture operation failed"), {
        code:
          response.status === 404
            ? "NOT_FOUND"
            : response.status === 409
              ? "ALREADY_EXISTS"
              : "INTERNAL_ERROR",
      });
    }
    return response.json<A>();
  };
  return {
    get: async (name) => ({
      info: () => call("info", name),
      createToken: (...args) => call("createToken", name, args),
      listTokens: () => call("listTokens", name),
      revokeToken: (...args) => call("revokeToken", name, args),
      fork: (...args) => call("fork", name, args),
      log: (...args) => call("log", name, args),
      readCommit: (...args) => call("readCommit", name, args),
      readTree: (...args) => call("readTree", name, args),
    }),
    create: (name, ...args) => call("create", name, args),
    import: (...args) => call("import", "", args),
    list: (...args) => call("list", "", args),
    delete: (name) => call("delete", name),
  };
};
