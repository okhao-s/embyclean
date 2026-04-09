#!/usr/bin/env bash
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

SHA="$(git rev-parse --short HEAD)"
BASE_IMAGE="${IMAGE_REPO:-okhao/emby_cleaner}"
PRIMARY_TAG="${IMAGE_TAG:-dev}"
IMAGE_PRIMARY="${BASE_IMAGE}:${PRIMARY_TAG}"
IMAGE_SHA="${BASE_IMAGE}:dev-${SHA}"

printf '==> building %s and %s\n' "$IMAGE_PRIMARY" "$IMAGE_SHA"
docker build -t "$IMAGE_PRIMARY" -t "$IMAGE_SHA" .

printf '==> pushing %s\n' "$IMAGE_PRIMARY"
docker push "$IMAGE_PRIMARY"

printf '==> pushing %s\n' "$IMAGE_SHA"
docker push "$IMAGE_SHA"

printf '==> done: %s , %s\n' "$IMAGE_PRIMARY" "$IMAGE_SHA"
