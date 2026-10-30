#!/usr/bin/env bash
# Stage an independent module whose dependencies are selected by the chosen
# Xray checkout, rather than pinning transitive packages from the baseline.
set -euo pipefail
if [[ $# -ne 2 ]]; then
  printf 'Usage: %s /path/to/Xray-core /new/adapter/directory\n' "$0" >&2
  exit 2
fi
xray_dir="$(cd "$1" && pwd)"
adapter_dir="$(cd "$(dirname "$0")/.." && pwd)"
shine_dir="$(cd "$adapter_dir/../shine" && pwd)"
mkdir "$2"
stage="$(cd "$2" && pwd)"
cp -R "$adapter_dir/." "$stage/"
(
  cd "$stage"
  awk '/\/\/ indirect[[:space:]]*$/ { if ($1 == "require") print $2; else print $1 }' go.mod |
    while IFS= read -r module; do GOWORK=off go mod edit -droprequire="$module"; done
  GOWORK=off go mod edit \
    -require=github.com/xtls/xray-core@v0.0.0 \
    -replace=github.com/xtls/xray-core="$xray_dir" \
    -replace=github.com/hhhflow2020/shine/go/shine="$shine_dir"
)
printf 'Prepared adapter module in %s for Xray %s\n' "$stage" "$(git -C "$xray_dir" rev-parse HEAD)"
