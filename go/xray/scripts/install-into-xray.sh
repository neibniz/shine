#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
  printf 'Usage: %s /path/to/Xray-core\n' "$0" >&2
  exit 2
fi
adapter_dir="$(cd "$(dirname "$0")/.." && pwd)"
shine_dir="$(cd "$adapter_dir/../shine" && pwd)"
if [[ -n "${SHINE_XRAY_ADAPTER_DIR:-}" ]]; then
  local_adapter_dir="$(cd "$SHINE_XRAY_ADAPTER_DIR" && pwd)"
else
  local_adapter_dir="$adapter_dir"
fi
xray_dir="$(cd "$1" && pwd)"
actual="$(git -C "$xray_dir" rev-parse HEAD)"
if ! git -C "$xray_dir" diff --quiet HEAD -- infra/conf/xray.go go.mod; then
  printf 'Xray checkout has local changes in infra/conf/xray.go or go.mod; refusing to overwrite them.\n' >&2
  exit 1
fi
patch="$adapter_dir/patches/xray-core.patch"
if ! git -C "$xray_dir" apply --check "$patch"; then
  printf 'Shine registration patch is incompatible with Xray revision %s.\n' "$actual" >&2
  exit 1
fi
git -C "$xray_dir" apply "$patch"
(
  cd "$xray_dir"
  GOWORK=off go mod edit -require=github.com/hhhflow2020/shine/go/xray@v0.0.0 \
    -replace=github.com/hhhflow2020/shine/go/xray="$local_adapter_dir" \
    -require=github.com/hhhflow2020/shine/go/shine@v0.0.0 \
    -replace=github.com/hhhflow2020/shine/go/shine="$shine_dir"
)
printf 'Shine registration added to Xray %s. Build with: cd %s && GOWORK=off go build -mod=mod -o xray ./main\n' "$actual" "$xray_dir"
