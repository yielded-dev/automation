#!/bin/sh
set -eu

if [ "$#" -lt 3 ] || [ "$#" -gt 4 ]; then
  echo "Usage: task-preview <generation> <materialization-id> <site|repository> [command]" >&2
  exit 64
fi

case "$1" in
  '' | *[!0-9]*)
    echo "Usage: task-preview <generation> <materialization-id> <site|repository> [command]" >&2
    exit 64
    ;;
esac

materialization_bytes="$(printf '%s' "$2" | wc -c | tr -d ' ')"
if [ "$materialization_bytes" -eq 0 ] || [ "$materialization_bytes" -gt 127 ]; then
  echo "Usage: task-preview <generation> <materialization-id> <site|repository> [command]" >&2
  exit 64
fi

source_kind="$3"
if [ "$source_kind" != "repository" ] && [ "$source_kind" != "site" ]; then
  echo "Usage: task-preview <generation> <materialization-id> <site|repository> [command]" >&2
  exit 64
fi

if [ "$source_kind" = "repository" ] && { [ "$#" -ne 4 ] || [ -z "$4" ]; }; then
  echo "A repository preview requires a committed setup command" >&2
  exit 64
fi

materialization_marker="/workspace/.sandbox-agent-materialization"
if [ ! -f "$materialization_marker" ]; then
  echo "Sandbox workspace materialization is unavailable" >&2
  exit 75
fi

current_materialization="$(dd if="$materialization_marker" bs=128 count=1 2>/dev/null | od -An -tx1 | tr -d '[:space:]')"
expected_materialization="$(printf '%s' "$2" | od -An -tx1 | tr -d '[:space:]')"
if [ "$current_materialization" != "$expected_materialization" ]; then
  echo "Sandbox workspace materialization changed before preview startup" >&2
  exit 75
fi

cd /workspace/repo

if [ "$source_kind" = "site" ]; then
  if [ ! -x /workspace/repo/node_modules/.bin/vp ]; then
    vp install
  fi
  exec /workspace/repo/node_modules/.bin/vp run preview
fi

exec sh -c "$4"
