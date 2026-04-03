#!/bin/sh
set -eu

mkdir -p /app/data
chown -R app:app /app /app/data 2>/dev/null || true

exec gosu app "$@"
