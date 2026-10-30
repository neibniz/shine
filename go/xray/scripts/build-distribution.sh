#!/usr/bin/env bash
set -euo pipefail
if [[ $# -lt 4 || $# -gt 5 ]]; then
  printf 'Usage: %s /path/to/Xray-core GOOS GOARCH /output/directory [GOARM]\n' "$0" >&2
  exit 2
fi
xray_dir="$(cd "$1" && pwd)"
os="$2"
arch="$3"
mkdir -p "$4"
out_dir="$(cd "$4" && pwd)"
arm="${5:-}"
script_dir="$(cd "$(dirname "$0")" && pwd)"
repo_dir="$(cd "$script_dir/../../.." && pwd)"
xray_sha="$(git -C "$xray_dir" rev-parse HEAD)"
shine_sha="$(git -C "$repo_dir" rev-parse HEAD)"
if ! grep -Fq 'shineconf.InboundConfig' "$xray_dir/infra/conf/xray.go" ||
   ! grep -Fq 'shineconf.OutboundConfig' "$xray_dir/infra/conf/xray.go"; then
  printf 'Shine is not registered in the Xray checkout. Run install-into-xray.sh first.\n' >&2
  exit 1
fi
name="${os}-${arch}"
if [[ -n "$arm" ]]; then
  if [[ "$arch" != arm || ! "$arm" =~ ^[567]$ ]]; then
    printf 'GOARM is only valid for arm targets and must be 5, 6, or 7.\n' >&2
    exit 2
  fi
  name="${name}v${arm}"
fi
stage="$out_dir/xray-shine-$name"
mkdir -p "$stage"
binary=xray
if [[ "$os" == windows ]]; then binary=xray.exe; fi
(
  cd "$xray_dir"
  export GOWORK=off CGO_ENABLED=0 GOOS="$os" GOARCH="$arch"
  if [[ -n "$arm" ]]; then export GOARM="$arm"; fi
  go build -mod=mod -trimpath -buildvcs=false -ldflags "-s -w -buildid= -X github.com/xtls/xray-core/core.build=$xray_sha" -o "$stage/$binary" ./main
)
cp "$xray_dir/LICENSE" "$stage/LICENSE-Xray-core"
cp "$repo_dir/LICENSE" "$stage/LICENSE-Shine"
printf 'Xray-core commit: %s\nShine commit: %s\nPlatform: %s\nGo toolchain: %s\n' \
  "$xray_sha" "$shine_sha" "$name" "$(cd "$xray_dir" && GOWORK=off go version)" > "$stage/BUILD-INFO.txt"
if [[ "$os" == windows ]]; then
  (cd "$out_dir" && zip -q -r "xray-shine-$name.zip" "xray-shine-$name")
else
  (cd "$out_dir" && tar -czf "xray-shine-$name.tar.gz" "xray-shine-$name")
fi
