#!/usr/bin/env bash
#
# Scripted terminal demo for screen recording. Commands are typed out and run
# against real containers; nothing is faked.
#
# Usage: ./distribution/scripts/demo.sh [--fast]
#
# Uses ports 6379-6381, a container named jcache-demo and the cluster from
# start.sh. The cluster's keys are flushed before the cluster part starts.

set -euo pipefail

SELF="$(cd "$(dirname "$0")" && pwd)/$(basename "$0")"
cd "$(dirname "$SELF")/../.."

IMAGE=himanshmunjal/jcache
CLIENT_JAR=cache-client/target/cache-client-1.0.3.jar
NODES=(node-1:localhost:6379 node-2:localhost:6380 node-3:localhost:6381)

type_delay=0.04
pause=1.6
if [[ "${1:-}" == "--fast" ]]; then type_delay=0; pause=0.2; fi

bold=$'\e[1m'; dim=$'\e[2m'; cyan=$'\e[36m'; green=$'\e[32m'; yellow=$'\e[33m'; red=$'\e[31m'; reset=$'\e[0m'

die() { echo "error: $*" >&2; exit 1; }

caption() { echo; echo "${yellow}# $*${reset}"; sleep "$pause"; }

# Print a prompt and type the command out character by character.
typed() {
  local prompt=$1 text=$2 i
  printf '%s' "$prompt"
  for (( i = 0; i < ${#text}; i++ )); do
    printf '%s' "${text:i:1}"
    sleep "$type_delay"
  done
  echo
}

# Show a shell command, then run it.
run() {
  typed "${green}\$${reset} " "$*"
  eval "$@"
  sleep "$pause"
}

# Open a raw TCP connection to the server, as `nc` would.
connect() {
  exec 3<>"/dev/tcp/127.0.0.1/$1"
  typed "${green}\$${reset} " "nc localhost $1"
}

send() {
  local reply
  typed "${dim}>${reset} " "$1"
  printf '%s\r\n' "$1" >&3
  IFS= read -r reply <&3
  reply=${reply%$'\r'}
  if [[ $reply == -* ]]; then echo "${red}${reply}${reset}"; else echo "${cyan}${reply}${reset}"; fi
  sleep "$pause"
}

disconnect() { exec 3>&-; }

# Run one command in the cluster CLI and print only its output.
cluster() {
  typed "${bold}jcache[cluster:3]>${reset} " "$1"
  printf '%s\n' "$1" | java -jar "$CLIENT_JAR" --cluster "${NODES[@]}" 2>&1 \
    | sed '1d; $d; s/^jcache\[cluster:3\]> //'
  sleep "$pause"
}

wait_for_ping() {
  local i
  for i in {1..30}; do
    if (exec 4<>"/dev/tcp/127.0.0.1/$1" && printf 'PING\r\n' >&4 && read -r -t 1 r <&4 && [[ $r == +PONG* ]]) 2>/dev/null; then
      return
    fi
    sleep 0.5
  done
  die "server on port $1 did not start"
}

docker info >/dev/null 2>&1 || die "Docker is not running"
[[ -f "$CLIENT_JAR" ]] || die "build first: mvn package -DskipTests"
docker rm -f jcache-demo >/dev/null 2>&1 || true
docker volume rm jcache-demo-data >/dev/null 2>&1 || true
./distribution/scripts/stop.sh >/dev/null 2>&1 || true
for port in 6379 6380 6381; do
  if (exec 4<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    die "port $port is in use; stop whatever is listening there first"
  fi
done
clear

caption "JCache: an in-memory cache written from scratch in Java"
run "docker run -d --name jcache-demo -p 6379:6379 -v jcache-demo-data:/data -e JCACHE_PERSISTENCE_ENABLED=true -e JCACHE_POLICY=ARC $IMAGE"
wait_for_ping 6379

caption "Plain-text protocol, so nc is enough"
connect 6379
send "PUT user:1 Alice Smith"
send "GET user:1"

caption "Keys can expire"
send "PUT session:42 abc123 3"
send "TTL session:42"
sleep 2
send "TTL session:42"
sleep 2
send "GET session:42"
disconnect

caption "Restart the server: writes go to an append-only log and snapshots"
run "docker restart jcache-demo >/dev/null"
wait_for_ping 6379
connect 6379
send "GET user:1"
disconnect

docker rm -f jcache-demo >/dev/null
docker volume rm jcache-demo-data >/dev/null

caption "Three nodes; the client shards keys with a consistent hash ring"
run "./distribution/scripts/start.sh --nodes 3 2>/dev/null | grep jcache-node"
printf 'flush\n' | java -jar "$CLIENT_JAR" --cluster "${NODES[@]}" >/dev/null 2>&1

cluster "put user:1 Alice"
cluster "put user:2 Bob"
cluster "put order:8 shipped"
cluster "route user:1"
cluster "route user:2"
cluster "route order:8"
cluster "distribution"

caption "github.com/himanshmunjal/JCache"
sleep 2
