#!/usr/bin/env bash
# Start an N-node local cluster (default 3) on ports 5001..500N.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
N="${1:-3}"
DATA="${ROOT}/.data"
BIN="${ROOT}/bin"

mkdir -p "$BIN" "$DATA"
cd "$ROOT"
go build -o "$BIN/server" ./cmd/server
go build -o "$BIN/client" ./cmd/client
go build -o "$BIN/bench" ./cmd/bench
go build -o "$BIN/chaos" ./cmd/chaos

# Kill previous cluster from this script, if any.
if [[ -f "$DATA/pids" ]]; then
  while read -r pid; do kill "$pid" 2>/dev/null || true; done < "$DATA/pids"
  rm -f "$DATA/pids"
fi

peers_for() {
  local self=$1
  local out=()
  for ((i = 1; i <= N; i++)); do
    if [[ "$i" -ne "$self" ]]; then
      out+=("n${i}=127.0.0.1:$((5000 + i))")
    fi
  done
  local IFS=,
  echo "${out[*]}"
}

: >"$DATA/pids"
for ((i = 1; i <= N; i++)); do
  id="n${i}"
  addr="127.0.0.1:$((5000 + i))"
  dir="$DATA/$id"
  mkdir -p "$dir"
  "$BIN/server" \
    -id="$id" \
    -addr="$addr" \
    -peers="$(peers_for "$i")" \
    -data-dir="$dir" \
    >"$DATA/$id.log" 2>&1 &
  echo $! >>"$DATA/pids"
  echo "started $id on $addr (pid $!)"
done

echo
echo "Client example:"
echo "  $BIN/client -addrs 127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003 put hello world"
echo "Logs: $DATA/n*.log   Stop: kill \$(cat $DATA/pids)"
