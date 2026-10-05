#!/usr/bin/env bash
#
# Start a local JCache cluster (up to three nodes on ports 6379-6381).
#
# Usage: ./distribution/scripts/start.sh [options]
#
#   --nodes N          number of nodes, 1-3 (default 3)
#   --policy NAME      LRU, LFU or ARC (default LRU)
#   --capacity N       maximum keys per node (default 10000)
#   --no-persistence   keep data in memory only
#   --build            build the image from source first
#   --pull             pull the published image first
#   -h, --help         show this help

set -euo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$SELF")/../.."
COMPOSE_FILE=distribution/docker-compose.yml

nodes=3
build=false
pull=false
export JCACHE_POLICY=LRU JCACHE_CAPACITY=10000 JCACHE_PERSISTENCE_ENABLED=true

usage() { sed -n '3,14p' "$SELF" | sed 's/^# \{0,1\}//'; }
die() { echo "error: $*" >&2; exit 1; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes) nodes="$2"; shift 2 ;;
    --policy) JCACHE_POLICY="$(tr '[:lower:]' '[:upper:]' <<< "$2")"; shift 2 ;;
    --capacity) JCACHE_CAPACITY="$2"; shift 2 ;;
    --no-persistence) JCACHE_PERSISTENCE_ENABLED=false; shift ;;
    --build) build=true; shift ;;
    --pull) pull=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown option $1" ;;
  esac
done

[[ "$nodes" =~ ^[1-3]$ ]] || die "--nodes must be 1, 2 or 3"
[[ "$JCACHE_POLICY" =~ ^(LRU|LFU|ARC)$ ]] || die "--policy must be LRU, LFU or ARC"
[[ "$JCACHE_CAPACITY" =~ ^[1-9][0-9]*$ ]] || die "--capacity must be a positive integer"
docker info >/dev/null 2>&1 || die "Docker is not running"

services=()
for i in $(seq 1 "$nodes"); do services+=("jcache-$i"); done

if $pull; then docker compose -f "$COMPOSE_FILE" pull "${services[@]}"; fi
if $build; then docker compose -f "$COMPOSE_FILE" build "${services[@]}"; fi
docker compose -f "$COMPOSE_FILE" up -d --wait "${services[@]}"

echo
for i in $(seq 1 "$nodes"); do
  port=$((6378 + i))
  reply=$(printf 'PING\r\n' | nc -w 2 localhost "$port" 2>/dev/null | head -1 | tr -d '\r' || true)
  printf '  jcache-node-%d  localhost:%d  %s\n' "$i" "$port" "${reply:-no reply}"
done
echo
echo "Policy $JCACHE_POLICY, capacity $JCACHE_CAPACITY per node, persistence $JCACHE_PERSISTENCE_ENABLED."
echo "Stop with ./distribution/scripts/stop.sh (add --clean to delete the data volumes)."
