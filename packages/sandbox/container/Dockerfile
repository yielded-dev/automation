# Shared sandbox-agent runtime image. The base and tool pins are validated
# against the package's exported runtime identity by check-browser-tooling.
FROM docker.io/library/node:24-bookworm-slim@sha256:0e0ff40c39bc087845bfb27465a0df4ea419520094bc35842ff83dd8cbe6f9b6

COPY --from=docker.io/cloudflare/sandbox:1.0.0@sha256:5bc187515bad0d9d486a4f1476f7d8ad39fe97c57965f044e4b0a0dcc93eac06 /usr/local/bin/sandbox-shim /usr/local/bin/sandbox-shim

ARG CODEX_VERSION=0.157.1
ARG GITHUB_CLI_VERSION=2.99.0
ARG PLAYWRIGHT_CLI_VERSION=0.1.18
ARG VITE_PLUS_VERSION=1.1.0

# The host connects privately to Codex app-server; wrangler needs the port declared locally.
EXPOSE 4500 4502 5173

USER root

ENV VP_HOME=/opt/vite-plus

COPY --from=docker.io/oven/bun:1.4.2@sha256:9114c058aeae42162ee16dd5084b95fe9473970bb6bcb5b232ab1630f0546895 /usr/local/bin/bun /usr/local/bin/bun

# The pinned Codex CLI is the only reasoning agent. Postgres exists for
# workspace dev servers (image-owned, disposable, no persistence lifecycle).
RUN apt-get update \
  && apt-get install --yes --no-install-recommends bash ca-certificates curl git postgresql procps util-linux \
  && rm -rf /var/lib/apt/lists/* \
  && npm install --global \
      "@openai/codex@${CODEX_VERSION}" \
      "@playwright/cli@${PLAYWRIGHT_CLI_VERSION}" \
  && codex --version \
  && playwright-cli --version \
  && test "$(bun --version)" = "1.4.2" \
  && psql --version \
  && test -n "$(pg_lsclusters --no-header)" \
  # The Cloudflare container runtime has no POSIX shared memory; Postgres
  # fails at startup with "could not open shared memory segment" unless its
  # dynamic shared memory falls back to mmap (verified live in a session).
  && pg_version="$(ls /etc/postgresql | head -1)" \
  && mkdir -p "/etc/postgresql/${pg_version}/main/conf.d" \
  && printf 'dynamic_shared_memory_type = mmap\n' \
      > "/etc/postgresql/${pg_version}/main/conf.d/sandbox-shm.conf" \
  && npm cache clean --force

# The global CLI delegates to the project's installed toolchain. Installing the
# project package globally splits Vitest's runner from vite-plus/test imports.
RUN curl --fail --silent --show-error --location https://vite.plus --output /tmp/vp-install.sh \
  && VP_VERSION="${VITE_PLUS_VERSION}" VP_NODE_MANAGER=no CI=true bash /tmp/vp-install.sh \
  && ln -s /opt/vite-plus/bin/vp /usr/local/bin/vp \
  && vp --version \
  && rm /tmp/vp-install.sh

# Browser payload is image-owned; a missing browser at runtime is an image
# defect, never something to install on demand. The config pins navigation to
# IPv4 because dev servers bind 0.0.0.0 while localhost resolves IPv6 first.
ENV PLAYWRIGHT_BROWSERS_PATH=/opt/ms-playwright
ENV PWTEST_CLI_GLOBAL_CONFIG=/opt/sandbox-agent
COPY playwright-cli.config.json /opt/sandbox-agent/.playwright/cli.config.json

RUN mkdir -p /etc/codex/skills \
  && cp -R "$(npm root -g)/@playwright/cli/skills/playwright-cli" /etc/codex/skills/playwright-cli \
  && test -f /etc/codex/skills/playwright-cli/SKILL.md \
  && playwright-cli install-browser chromium --with-deps \
  && rm -rf /var/lib/apt/lists/*

# Installation alone cannot prove Chromium launches in the final image; fail
# the build here instead of discovering an image defect inside a session.
RUN playwright-cli -s=browser-build-smoke open about:blank \
  && playwright-cli -s=browser-build-smoke snapshot \
  && playwright-cli -s=browser-build-smoke close \
  && playwright-cli close-all \
  && playwright-cli kill-all \
  && rm -rf /root/.cache/ms-playwright/daemon \
  && rm -rf /workspace/.runtime

COPY scripts/task-preview.sh /usr/local/bin/task-preview
COPY scripts/repository-secret-scan.cjs /opt/sandbox-agent/repository-secret-scan.cjs
RUN chmod 0755 /usr/local/bin/task-preview

# Native PR attachments need gh 2.99+. Keep credentials at the host boundary.
RUN gh_arch="$(dpkg --print-architecture)" \
  && case "$gh_arch" in \
      amd64) gh_sha256=471feb449cc98d527fc9a67601b9ea04296c100b666d970a784a07dc17a59a8f ;; \
      arm64) gh_sha256=20ccc660b06aef27e2164ae0de5085108e1a3d1e7ba4440e7be10bd9b4b5d0ab ;; \
      *) exit 1 ;; \
     esac \
  && curl --fail --silent --show-error --location \
      "https://github.com/cli/cli/releases/download/v${GITHUB_CLI_VERSION}/gh_${GITHUB_CLI_VERSION}_linux_${gh_arch}.deb" \
      --output /tmp/gh.deb \
  && echo "${gh_sha256}  /tmp/gh.deb" | sha256sum --check --strict \
  && dpkg --install /tmp/gh.deb \
  && rm /tmp/gh.deb \
  && test "$(gh --version | awk 'NR == 1 { print $3 }')" = "$GITHUB_CLI_VERSION" \
  && gh pr create --help | grep -F -- '--attach' \
  && gh pr edit --help | grep -F -- '--attach'

# Reap orphaned command descendants without adding another supervisor or restart path.
RUN apt-get update \
  && apt-get install --yes --no-install-recommends tini \
  && rm -rf /var/lib/apt/lists/* \
  && tini --version

WORKDIR /workspace
# Native start selects the boot command; Docker must not prepend sleep to it.
ENTRYPOINT []
CMD ["sleep", "infinity"]
