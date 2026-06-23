#!/usr/bin/env bash
# test-cluster-chaos.sh - Automated 3-node etcd cluster chaos testing for conch
set -euo pipefail

export GOCOVERDIR="./tmp-coverage"
rm -rf "$GOCOVERDIR"
mkdir -p "$GOCOVERDIR"


# Colors for log output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log() {
    echo -e "${BLUE}[$(date +'%H:%M:%S')]${NC} $1"
}

warn() {
    echo -e "${YELLOW}[$(date +'%H:%M:%S')] WARNING:${NC} $1"
}

success() {
    echo -e "${GREEN}[$(date +'%H:%M:%S')] SUCCESS:${NC} $1"
}

error() {
    echo -e "${RED}[$(date +'%H:%M:%S')] ERROR:${NC} $1" >&2
}

# Auto-discovery function for etcd
find_etcd() {
    if command -v etcd &>/dev/null; then
        echo "etcd"
        return
    fi
    # Check common Homebrew / local paths
    for bp in "/home/linuxbrew/.linuxbrew/bin/etcd" "/opt/homebrew/bin/etcd" "/usr/local/bin/etcd"; do
        if [ -f "$bp" ]; then
            echo "$bp"
            return
        fi
    done
    local matches
    matches=($(ls -d /nix/store/*-etcd*/bin/etcd 2>/dev/null))
    if [ ${#matches[@]} -gt 0 ]; then
        echo "${matches[0]}"
        return
    fi
    echo ""
}

# Auto-discovery function for etcdctl
find_etcdctl() {
    if command -v etcdctl &>/dev/null; then
        echo "etcdctl"
        return
    fi
    # Check common Homebrew / local paths
    for bp in "/home/linuxbrew/.linuxbrew/bin/etcdctl" "/opt/homebrew/bin/etcdctl" "/usr/local/bin/etcdctl"; do
        if [ -f "$bp" ]; then
            echo "$bp"
            return
        fi
    done
    local matches
    matches=($(ls -d /nix/store/*-etcdctl*/bin/etcdctl 2>/dev/null || ls -d /nix/store/*-etcd*/bin/etcdctl 2>/dev/null))
    if [ ${#matches[@]} -gt 0 ]; then
        echo "${matches[0]}"
        return
    fi
    echo ""
}

# 1. Discovery
ETCD_BIN=$(find_etcd)
ETCDCTL_BIN=$(find_etcdctl)

if [ -z "$ETCD_BIN" ]; then
    error "etcd binary not found!"
    exit 1
fi
if [ -z "$ETCDCTL_BIN" ]; then
    error "etcdctl binary not found!"
    exit 1
fi

log "Found etcd binary: $ETCD_BIN"
log "Found etcdctl binary: $ETCDCTL_BIN"

# Directories and files
DATA_DIR1="./tmp-node1.etcd"
DATA_DIR2="./tmp-node2.etcd"
DATA_DIR3="./tmp-node3.etcd"
CHILD_LOG="child-heartbeat.log"

# Clean up function
cleanup() {
    log "Cleaning up chaos test environment..."
    
    # Kill conch wrapper and child if running
    if [ -f conch.pid ]; then
        local pid
        pid=$(cat conch.pid 2>/dev/null)
        if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
            log "Killing conch process group ($pid)..."
            kill -TERM -"$pid" 2>/dev/null || kill -9 "$pid" 2>/dev/null
        fi
        rm -f conch.pid
    fi

    # Kill etcd nodes
    for node in node1 node2 node3; do
        if [ -f "$node.pid" ]; then
            local pid
            pid=$(cat "$node.pid" 2>/dev/null)
            if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
                kill -9 "$pid" 2>/dev/null
            fi
            rm -f "$node.pid"
        fi
    done

    # Clean port listeners
    for port in 2379 2380 2381 2382 2383 2384; do
        fuser -k -n tcp "$port" &>/dev/null || true
    done

    # Extract coverage data if any exists
    if [ -d "$GOCOVERDIR" ] && [ "$(ls -A "$GOCOVERDIR")" ]; then
        log "Generating binary coverage report from chaos tests..."
        go tool covdata textfmt -i="$GOCOVERDIR" -o=coverage-chaos.txt
    fi
}

# Delete data dirs and log files strictly on a fresh start
rm_logs_and_dirs() {
    log "Removing existing data directories and old log files..."
    rm -rf "$DATA_DIR1" "$DATA_DIR2" "$DATA_DIR3"
    rm -rf node1.log node2.log node3.log "$CHILD_LOG"
}

# Trap cleanup on exit (keeps logs intact for user review on failure)
trap cleanup EXIT

# Clean and remove old dirs/logs on fresh start
cleanup
rm_logs_and_dirs

log "Starting 3-node etcd cluster locally..."

# Start Node 1 (Client: 2379, Peer: 2380)
"$ETCD_BIN" --name node1 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-chaos-cluster \
  --initial-cluster-state new \
  --log-level warn \
  --data-dir "$DATA_DIR1" > node1.log 2>&1 &
echo $! > node1.pid

# Start Node 2 (Client: 2381, Peer: 2382)
"$ETCD_BIN" --name node2 \
  --initial-advertise-peer-urls http://127.0.0.1:2382 \
  --listen-peer-urls http://127.0.0.1:2382 \
  --listen-client-urls http://127.0.0.1:2381 \
  --advertise-client-urls http://127.0.0.1:2381 \
  --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-chaos-cluster \
  --initial-cluster-state new \
  --log-level warn \
  --data-dir "$DATA_DIR2" > node2.log 2>&1 &
echo $! > node2.pid

# Start Node 3 (Client: 2383, Peer: 2384)
"$ETCD_BIN" --name node3 \
  --initial-advertise-peer-urls http://127.0.0.1:2384 \
  --listen-peer-urls http://127.0.0.1:2384 \
  --listen-client-urls http://127.0.0.1:2383 \
  --advertise-client-urls http://127.0.0.1:2383 \
  --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-chaos-cluster \
  --initial-cluster-state new \
  --log-level warn \
  --data-dir "$DATA_DIR3" > node3.log 2>&1 &
echo $! > node3.pid

log "Waiting for cluster to become healthy..."
CLUSTER_HEALTHY=false
for i in {1..20}; do
    if "$ETCDCTL_BIN" --endpoints=127.0.0.1:2379,127.0.0.1:2381,127.0.0.1:2383 endpoint health &>/dev/null; then
        CLUSTER_HEALTHY=true
        break
    fi
    sleep 0.5
done

if [ "$CLUSTER_HEALTHY" = false ]; then
    error "Cluster failed to become healthy in time!"
    exit 1
fi

success "3-node etcd cluster is HEALTHY and clustered!"
"$ETCDCTL_BIN" --endpoints=127.0.0.1:2379 member list

# Re-compile to ensure conch binary is up to date
log "Rebuilding conch binary with coverage instrumentation..."
go build -cover -o conch cmd/conch/main.go

log "Running smoke test suite on the cluster..."
export CONCH_ENDPOINTS="127.0.0.1:2379"
./conch-smoke.sh

# ---------------------------------------------------------
# Phase 1: Leadership Election with 3-node Consensus
# ---------------------------------------------------------
log "========================================================="
log "PHASE 1: Running Leader Election with 3-node consensus"
log "========================================================="

# Run conch elect with a child process logging heartbeat
# We use standard outputs redirect to child log
./conch elect chaos-office --restart --endpoints=127.0.0.1:2379,127.0.0.1:2381,127.0.0.1:2383 --ttl=6s --kill-after=2s -- sh -c '
  while true; do
    echo "HEARTBEAT: $(date +%s)"
    sleep 0.5
  done
' > "$CHILD_LOG" 2>&1 &
echo $! > conch.pid

log "Waiting for conch to acquire leadership..."
sleep 3

# Verify child is running and generating heartbeat
if ! grep -q "HEARTBEAT" "$CHILD_LOG" 2>/dev/null; then
    error "Child process is not running or heartbeat was not recorded!"
    exit 1
fi

log "Current Leader info:"
./conch elect chaos-office --who --endpoints=127.0.0.1:2379

success "Phase 1 passed: Leadership successfully established and child running!"

# ---------------------------------------------------------
# Phase 2: Single Node Failure (Maintain Quorum)
# ---------------------------------------------------------
log "========================================================="
log "PHASE 2: Killing Node 3 (Quorum should be maintained: 2/3)"
log "========================================================="

NODE3_PID=$(cat node3.pid)
log "Killing etcd Node 3 (PID: $NODE3_PID)..."
kill -9 "$NODE3_PID"
rm -f node3.pid

# Check cluster health (Node 3 should be dead, Node 1 & 2 healthy)
log "Checking cluster health with remaining endpoints..."
"$ETCDCTL_BIN" --endpoints=127.0.0.1:2379,127.0.0.1:2381 endpoint health || true

# Assert conch is still healthy and child process heartbeat continues
sleep 3
log "Asserting child process is still running..."
HEARTBEATS_BEFORE=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")
sleep 2
HEARTBEATS_AFTER=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")

if [ "$HEARTBEATS_AFTER" -le "$HEARTBEATS_BEFORE" ]; then
    error "Heartbeat stopped after Node 3 death! Conch should have maintained execution."
    exit 1
fi

success "Phase 2 passed: Conch execution continued uninterrupted after losing Node 3."

# ---------------------------------------------------------
# Phase 2.5: Recover Node 3 (Quorum restoration for next phase)
# ---------------------------------------------------------
log "========================================================="
log "PHASE 2.5: Restarting Node 3 (Restore 3-node cluster)"
log "========================================================="

log "Starting Node 3..."
"$ETCD_BIN" --name node3 \
  --initial-advertise-peer-urls http://127.0.0.1:2384 \
  --listen-peer-urls http://127.0.0.1:2384 \
  --listen-client-urls http://127.0.0.1:2383 \
  --advertise-client-urls http://127.0.0.1:2383 \
  --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-chaos-cluster \
  --initial-cluster-state existing \
  --log-level warn \
  --data-dir "$DATA_DIR3" > node3.log 2>&1 &
echo $! > node3.pid

log "Waiting for Node 3 to rejoin and cluster health to fully recover..."
CLUSTER_RECOVERED=false
for i in {1..20}; do
    if "$ETCDCTL_BIN" --endpoints=127.0.0.1:2379,127.0.0.1:2381,127.0.0.1:2383 endpoint health &>/dev/null; then
        CLUSTER_RECOVERED=true
        break
    fi
    sleep 0.5
done

if [ "$CLUSTER_RECOVERED" = false ]; then
    error "Node 3 failed to rejoin and cluster did not recover health!"
    exit 1
fi

success "Phase 2.5 passed: 3-node cluster fully restored and healthy!"

# ---------------------------------------------------------
# Phase 3: Connected Endpoint Loss (Re-campaign & Re-acquire)
# ---------------------------------------------------------
log "========================================================="
log "PHASE 3: Killing Node 1 (Conch connection lost, must re-campaign)"
log "========================================================="

NODE1_PID=$(cat node1.pid)
log "Killing etcd Node 1 (PID: $NODE1_PID)..."
kill -9 "$NODE1_PID"
rm -f node1.pid

# Client should lose its lease session with Node 1, SIGTERM the child, resign, 
# and re-campaign/re-connect to Node 2 or Node 3 (the healthy quorum survivors).
log "Waiting for conch to handle connected endpoint loss and re-campaign..."
sleep 6

# Assert conch is still running (on surviving Nodes 2 and 3)
if ! kill -0 "$(cat conch.pid)" 2>/dev/null; then
    error "Conch wrapper process died instead of re-campaigning!"
    exit 1
fi

# Verify new heartbeat resumes
HEARTBEATS_BEFORE=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")
sleep 2.5
HEARTBEATS_AFTER=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")

if [ "$HEARTBEATS_AFTER" -le "$HEARTBEATS_BEFORE" ]; then
    error "Heartbeat did not resume on Node 2/3 after Node 1 connection loss!"
    exit 1
fi

log "Current Leader info on surviving Node 2/3:"
./conch elect chaos-office --who --endpoints=127.0.0.1:2381

success "Phase 3 passed: Conch gracefully re-campaigned and resumed child execution on surviving endpoints!"

# ---------------------------------------------------------
# Phase 4: Quorum Loss (Fail Closed & Kill Child)
# ---------------------------------------------------------
log "========================================================="
log "PHASE 4: Killing Node 2 (Quorum lost: only Node 3 alive, must fail closed)"
log "========================================================="

NODE2_PID=$(cat node2.pid)
log "Killing etcd Node 2 (PID: $NODE2_PID)..."
kill -9 "$NODE2_PID"
rm -f node2.pid

# Only Node 3 is now alive. 1/3 is below the 2-node quorum threshold. Quorum is completely lost.
# The conch wrapper should detect the keepalive loss and SIGTERM/SIGKILL the child process.
log "Waiting for conch to detect quorum loss and kill child..."
sleep 5

# Let's count heartbeats. It should NOT be increasing anymore.
HEARTBEATS_BEFORE=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")
sleep 2
HEARTBEATS_AFTER=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")

if [ "$HEARTBEATS_AFTER" -gt "$HEARTBEATS_BEFORE" ]; then
    error "Child process is still running after complete cluster quorum loss! Safety violated!"
    exit 1
fi

success "Phase 4 passed: Child process was safely terminated upon complete quorum loss."

# ---------------------------------------------------------
# Phase 5: Cluster Recovery (Re-acquire leadership)
# ---------------------------------------------------------
log "========================================================="
log "PHASE 5: Restarting Node 1 and Node 2 (Recovering quorum)"
log "========================================================="

# Start Node 1 back up
log "Starting Node 1..."
"$ETCD_BIN" --name node1 \
  --initial-advertise-peer-urls http://127.0.0.1:2380 \
  --listen-peer-urls http://127.0.0.1:2380 \
  --listen-client-urls http://127.0.0.1:2379 \
  --advertise-client-urls http://127.0.0.1:2379 \
  --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-chaos-cluster \
  --initial-cluster-state existing \
  --log-level warn \
  --data-dir "$DATA_DIR1" > node1.log 2>&1 &
echo $! > node1.pid

# Start Node 2 back up
log "Starting Node 2..."
"$ETCD_BIN" --name node2 \
  --initial-advertise-peer-urls http://127.0.0.1:2382 \
  --listen-peer-urls http://127.0.0.1:2382 \
  --listen-client-urls http://127.0.0.1:2381 \
  --advertise-client-urls http://127.0.0.1:2381 \
  --initial-cluster node1=http://127.0.0.1:2380,node2=http://127.0.0.1:2382,node3=http://127.0.0.1:2384 \
  --initial-cluster-token etcd-chaos-cluster \
  --initial-cluster-state existing \
  --log-level warn \
  --data-dir "$DATA_DIR2" > node2.log 2>&1 &
echo $! > node2.pid

log "Waiting for cluster quorum recovery..."
for i in {1..20}; do
    if "$ETCDCTL_BIN" --endpoints=127.0.0.1:2379,127.0.0.1:2381 endpoint health &>/dev/null; then
        break
    fi
    sleep 0.5
done

log "Quorum recovered. Waiting for conch to automatically re-campaign and resume child..."
sleep 6

# Verify child process restarted and heartbeat is increasing again
HEARTBEATS_BEFORE=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")
sleep 2.5
HEARTBEATS_AFTER=$(awk '/^HEARTBEAT/ {count++} END {print count+0}' "$CHILD_LOG")

if [ "$HEARTBEATS_AFTER" -le "$HEARTBEATS_BEFORE" ]; then
    error "Child process did not resume execution after cluster recovered quorum!"
    exit 1
fi

log "Current Leader info after recovery:"
./conch elect chaos-office --who --endpoints=127.0.0.1:2379

success "Phase 5 passed: Conch automatically recovered and resumed child when cluster quorum returned!"

log "========================================================="
success "ALL LOCAL CLUSTER CHAOS AND CONSENSUS TESTS PASSED!"
log "========================================================="
