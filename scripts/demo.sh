#!/usr/bin/env bash
# Start a 3-node raftkv cluster on localhost for a hands-on demo.
# Nodes log to /tmp/raftkv-demo/nodeN.log. Ctrl-C stops everything.
set -euo pipefail

BASE=/tmp/raftkv-demo
rm -rf "$BASE"
mkdir -p "$BASE"

cd "$(dirname "$0")/.."

start_node() {
  local id=$1 port=$2 peers=$3
  python3 -m raftkv.server --id "$id" --port "$port" \
    --peers "$peers" --data-dir "$BASE/$id" > "$BASE/$id.log" 2>&1 &
  echo $! > "$BASE/$id.pid"
}

start_node node1 5001 "node2=127.0.0.1:5002,node3=127.0.0.1:5003"
start_node node2 5002 "node1=127.0.0.1:5001,node3=127.0.0.1:5003"
start_node node3 5003 "node1=127.0.0.1:5001,node2=127.0.0.1:5002"

cleanup() {
  kill $(cat "$BASE"/*.pid) 2>/dev/null || true
  wait 2>/dev/null || true
}
trap cleanup EXIT

sleep 2
echo "raftkv demo cluster is up (node1:5001, node2:5002, node3:5003)"
echo
echo "Try:"
echo "  python3 -m raftkv.client --node 127.0.0.1:5001 set hello world"
echo "  python3 -m raftkv.client --node 127.0.0.1:5002 get hello"
echo
echo "Kill the leader to watch re-election:"
echo "  kill \$(cat $BASE/node1.pid)   # if node1 is the leader; check the logs"
echo "  tail -f $BASE/node2.log"
echo
echo "Logs: $BASE/nodeN.log   (Ctrl-C stops the cluster)"
wait
