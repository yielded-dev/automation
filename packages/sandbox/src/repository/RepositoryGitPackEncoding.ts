import { Schema } from "effect";

const MAX_PACK_BYTES = 48 * 1024 * 1024;
const PackBytes = Schema.Int.check(Schema.isBetween({ minimum: 32, maximum: MAX_PACK_BYTES }));

export const RepositoryGitPackManifest = Schema.fromJsonString(
  Schema.Struct({
    stagingHead: Schema.String.check(Schema.isPattern(/^[a-f0-9]{40}$/)),
    bootstrapBytes: PackBytes,
    parts: Schema.NonEmptyArray(
      Schema.Struct({
        name: Schema.String.check(Schema.isPattern(/^data-[a-f0-9]{40}\.pack$/)),
        bytes: PackBytes,
      }),
    ),
  }),
);

export const repositoryGitPackScript = String.raw`
git init --bare --object-format=sha1 objects.git
git -C objects.git index-pack --stdin < input.pack
mkdir output
for pack in objects.git/objects/pack/*.pack; do basename "$pack"; done > received-packs
git -C objects.git pack-objects --stdin-packs --max-pack-size=48m --window=0 --depth=0 --no-reuse-delta --no-reuse-object ../output/data < received-packs
tree=$(git -C objects.git hash-object -t tree -w --stdin </dev/null)
staging_head=$(printf 'tree %s\nauthor Kommunikasie <git-transfer@kommunikasie.invalid> 0 +0000\ncommitter Kommunikasie <git-transfer@kommunikasie.invalid> 0 +0000\n\nRepository transfer staging\n' "$tree" | git -C objects.git hash-object -t commit -w --stdin)
printf '%s\n%s\n' "$tree" "$staging_head" | git -C objects.git pack-objects --stdout --window=0 --depth=0 --no-reuse-delta --no-reuse-object > output/bootstrap.pack
printf '{"stagingHead":"%s","bootstrapBytes":%s,"parts":[' "$staging_head" "$(wc -c < output/bootstrap.pack)" > manifest.json
separator=
for part in output/data-*.pack; do
  printf '%s{"name":"%s","bytes":%s}' "$separator" "$(basename "$part")" "$(wc -c < "$part")" >> manifest.json
  separator=,
done
printf ']}\n' >> manifest.json
`;

/** Pure file plan shared by the sandbox and local process adapters. */
export const repositoryGitPackFiles = (manifest: typeof RepositoryGitPackManifest.Type) => {
  const mode = manifest.parts.length > 1 ? "Staged" : "Direct";
  const files: Array<{
    readonly mode: "Staged" | "Direct";
    readonly name: string;
    readonly bytes: number;
    readonly stagingHead: string;
  }> = [];

  if (mode === "Staged")
    files.push({
      mode,
      name: "bootstrap.pack",
      bytes: manifest.bootstrapBytes,
      stagingHead: manifest.stagingHead,
    });
  for (const part of manifest.parts)
    files.push({ mode, name: part.name, bytes: part.bytes, stagingHead: manifest.stagingHead });

  return files;
};
