#!/usr/bin/env bash
#
# Stop the local JCache cluster started by start.sh.
#
# Usage: ./distribution/scripts/stop.sh [--clean]
#
#   --clean     also delete the data volumes
#   -h, --help  show this help

set -euo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$SELF")/../.."
COMPOSE_FILE=distribution/docker-compose.yml

case "${1:-}" in
  "") docker compose -f "$COMPOSE_FILE" down --timeout 15 ;;
  --clean) docker compose -f "$COMPOSE_FILE" down --timeout 15 --volumes ;;
  -h|--help) sed -n '3,8p' "$SELF" | sed 's/^# \{0,1\}//' ;;
  *) echo "error: unknown option $1" >&2; exit 1 ;;
esac
