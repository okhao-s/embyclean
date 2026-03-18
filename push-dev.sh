#!/usr/bin/env bash
set -euo pipefail

IMAGE="okhao/emby_cleaner:dev"
DIR="$(cd "$(dirname "$0")" && pwd)"

cd "$DIR"
echo "==> building $IMAGE"
docker build -t "$IMAGE" .

echo "==> pushing $IMAGE"
docker push "$IMAGE"

echo "==> done: $IMAGE"
