#!/usr/bin/env bash
set -euo pipefail
if [[ $# -ne 1 ]]; then
  printf 'Usage: %s /path/to/Xray-core\n' "$0" >&2
  exit 2
fi
adapter_dir="$(cd "$(dirname "$0")/.." && pwd)"
shine_dir="$(cd "$adapter_dir/../shine" && pwd)"
xray_dir="$(cd "$1" && pwd)"
expected=0afec66085a85191e9dd2a4a6646c7c9a94688d1
actual="$(git -C "$xray_dir" rev-parse HEAD)"
if [[ "$actual" != "$expected" ]]; then
  printf 'Unsupported Xray revision: %s\nExpected: %s\n' "$actual" "$expected" >&2
  exit 1
fi
patch="$adapter_dir/patches/xray-core.patch"
git -C "$xray_dir" apply --check "$patch"
git -C "$xray_dir" apply "$patch"
(
  cd "$xray_dir"
  go mod edit -require=github.com/hhhflow2020/shine/go/xray@v0.0.0 \
    -replace=github.com/hhhflow2020/shine/go/xray="$adapter_dir" \
    -require=github.com/hhhflow2020/shine/go/shine@v0.0.0 \
    -replace=github.com/hhhflow2020/shine/go/shine="$shine_dir"
)
printf 'Shine registration added. Build with: cd %s && GOWORK=off go build -mod=mod -o xray ./main\n' "$xray_dir"
