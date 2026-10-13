#!/usr/bin/env bash
# Runs a command against a throwaway Postgres cluster, then deletes it.
#   bash scripts/with-postgres.sh npm test
# Needs initdb and pg_ctl on PATH (brew install postgresql).
set -euo pipefail

DIR=$(mktemp -d)
PORT=${PGDURABLE_TEST_PORT:-54329}

initdb -D "$DIR/data" -U postgres -A trust >/dev/null
pg_ctl -D "$DIR/data" -l "$DIR/log" -w \
  -o "-p $PORT -c listen_addresses=127.0.0.1 -k $DIR" start >/dev/null

cleanup() {
  pg_ctl -D "$DIR/data" -m immediate stop >/dev/null 2>&1 || true
  rm -rf "$DIR"
}
trap cleanup EXIT

DATABASE_URL="postgres://postgres@127.0.0.1:$PORT/postgres" "$@"
