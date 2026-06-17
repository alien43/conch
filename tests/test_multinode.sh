#!/usr/bin/env bash
#
# Multi-Node Integration Test for Conch Coordination Suite
#
# This script executes a multi-node distributed coordination test using:
# - NODE_A (asus-server: 10.0.1.3)
# - NODE_B (dell-server: 10.0.1.6)
# - NODE_C (rpi-server:  10.0.1.8)
#
# Primitives tested:
# 1. Leader Election (conch elect) - Mutual Exclusion & Failover
# 2. Distributed Semaphore (conch sema) - Concurrency Control & Queueing
#

set -uo pipefail

# Node Configuration
NODE_A="10.0.1.3"  # asus-server
NODE_B="10.0.1.6"  # dell-server
NODE_C="10.0.1.8"  # rpi-server
NODES=("$NODE_A" "$NODE_B" "$NODE_C")

ENDPOINTS="10.0.1.3:2379,10.0.1.6:2379,10.0.1.8:2379"

# Output Formatting
GREEN='\033[0;32m'
RED='\033[0;31m'
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

# Unique ID for this test run to prevent any collision
TEST_ID="multinode-$$"
ELECT_OFFICE="test-elect-$TEST_ID"
SEMA_NAME="test-sema-$TEST_ID"

log "================================================="
log "   Starting Conch Multi-Node Integration Test     "
log "   Test ID: $TEST_ID"
log "================================================="

# 0. Clean up helper
cleanup() {
    log "Performing remote cleanup of test processes..."
    for node in "${NODES[@]}"; do
        ssh -o BatchMode=yes -o ConnectTimeout=2 "andrey@$node" "
            pkill -f '$ELECT_OFFICE' 2>/dev/null || true
            pkill -f '$SEMA_NAME' 2>/dev/null || true
            pkill -f 'sleep 1234' 2>/dev/null || true
        " &>/dev/null || true
    done
    log "Cleanup complete."
}

# Set trap to clean up processes on exit
trap cleanup EXIT

# 1. Check Node & etcd Reachability
log "Verifying node and etcd reachability..."
for node in "${NODES[@]}"; do
    if ! ping -c 1 -W 2 "$node" > /dev/null 2>&1; then
        error "Node $node is unreachable. Aborting test."
        exit 1
    fi
    # Test conch version on each node to ensure binary is ready and functional
    if ! ssh -o BatchMode=yes -o ConnectTimeout=2 "andrey@$node" "conch version" &>/dev/null; then
        error "Conch binary is not functional on $node. Aborting test."
        exit 1
    fi
done
success "All 3 target nodes are ONLINE and conch is ready."

# 2. test Leader Election (conch elect)
log "-------------------------------------------------"
log "TEST 1: Leader Election (Mutual Exclusion & Failover)"
log "-------------------------------------------------"

log "Launching Campaign 1 on NODE_A ($NODE_A: asus-server)..."
ssh -o BatchMode=yes "andrey@$NODE_A" "conch elect $ELECT_OFFICE --endpoints=$ENDPOINTS --ttl=6s -- sleep 1234" >/dev/null 2>&1 &

log "Waiting for Campaign 1 to establish leadership..."
sleep 3

# Verify that NODE_A holds the leader lock
LEADER_INFO=$(ssh -o BatchMode=yes "andrey@$NODE_A" "conch elect $ELECT_OFFICE --endpoints=$ENDPOINTS --who" 2>/dev/null || true)
log "Current Leader: $LEADER_INFO"

if [[ "$LEADER_INFO" == *"asus-server"* ]]; then
    success "NODE_A (asus-server) successfully acquired leadership."
else
    error "NODE_A failed to acquire leadership! Info: '$LEADER_INFO'"
    exit 1
fi

log "Launching contending Campaign 2 on NODE_B ($NODE_B: dell-server)..."
ssh -o BatchMode=yes "andrey@$NODE_B" "conch elect $ELECT_OFFICE --endpoints=$ENDPOINTS --ttl=6s -- sleep 1234" >/dev/null 2>&1 &

log "Waiting 3 seconds to verify mutual exclusion..."
sleep 3

LEADER_INFO=$(ssh -o BatchMode=yes "andrey@$NODE_A" "conch elect $ELECT_OFFICE --endpoints=$ENDPOINTS --who" 2>/dev/null || true)
log "Current Leader: $LEADER_INFO"

if [[ "$LEADER_INFO" == *"asus-server"* ]] && [[ "$LEADER_INFO" != *"dell-server"* ]]; then
    success "Mutual exclusion verified. NODE_B is waiting and NODE_A is still the sole leader."
else
    error "Mutual exclusion violated! Both or wrong node got leadership. Info: '$LEADER_INFO'"
    exit 1
fi

log "Simulating Failover: Terminating Campaign 1 on NODE_A..."
ssh -o BatchMode=yes "andrey@$NODE_A" "pkill -f 'sleep 1234'" &>/dev/null || true

log "Waiting for Campaign 2 on NODE_B to detect vacancy and take over..."
sleep 3

LEADER_INFO=$(ssh -o BatchMode=yes "andrey@$NODE_B" "conch elect $ELECT_OFFICE --endpoints=$ENDPOINTS --who" 2>/dev/null || true)
log "Current Leader after failover: $LEADER_INFO"

if [[ "$LEADER_INFO" == *"dell-server"* ]]; then
    success "Failover verified. NODE_B (dell-server) successfully took over leadership."
else
    error "Failover failed! Leadership was not acquired by NODE_B. Info: '$LEADER_INFO'"
    exit 1
fi

# Clean up elect test processes before next test
ssh -o BatchMode=yes "andrey@$NODE_B" "pkill -f 'sleep 1234'" &>/dev/null || true
sleep 1

# 3. Test Distributed Semaphore (conch sema)
log "-------------------------------------------------"
log "TEST 2: Distributed Semaphore (Concurrency & Queueing)"
log "-------------------------------------------------"

log "Acquiring slot 1 on NODE_A ($NODE_A)..."
ssh -o BatchMode=yes "andrey@$NODE_A" "conch sema $SEMA_NAME --endpoints=$ENDPOINTS --max=2 -- sleep 1234" >/dev/null 2>&1 &

log "Acquiring slot 2 on NODE_B ($NODE_B)..."
ssh -o BatchMode=yes "andrey@$NODE_B" "conch sema $SEMA_NAME --endpoints=$ENDPOINTS --max=2 -- sleep 1234" >/dev/null 2>&1 &

log "Attempting to acquire slot 3 (over-limit) on NODE_C ($NODE_C)..."
ssh -o BatchMode=yes "andrey@$NODE_C" "conch sema $SEMA_NAME --endpoints=$ENDPOINTS --max=2 -- sleep 1234" >/dev/null 2>&1 &

log "Waiting for semaphore state to settle..."
sleep 3

SEMA_WHO=$(ssh -o BatchMode=yes "andrey@$NODE_A" "conch sema $SEMA_NAME --endpoints=$ENDPOINTS --max=2 --who" 2>/dev/null || true)
echo -e "$SEMA_WHO"

# Count the number of HELD and WAIT statuses
HELD_COUNT=$(echo "$SEMA_WHO" | grep -c "HELD" || true)
WAIT_COUNT=$(echo "$SEMA_WHO" | grep -c "WAIT" || true)

log "Held Slots: $HELD_COUNT, Waiting Contenders: $WAIT_COUNT"

if [ "$HELD_COUNT" -eq 2 ] && [ "$WAIT_COUNT" -eq 1 ]; then
    success "Distributed semaphore concurrency control and queueing verified perfectly!"
else
    error "Semaphore assertion failed! Expected 2 HELD, 1 WAIT. Got: $HELD_COUNT HELD, $WAIT_COUNT WAIT."
    exit 1
fi

log "Releasing a slot on NODE_A..."
ssh -o BatchMode=yes "andrey@$NODE_A" "pkill -f 'sleep 1234'" &>/dev/null || true

log "Waiting for NODE_C to acquire the newly available slot..."
sleep 3

SEMA_WHO=$(ssh -o BatchMode=yes "andrey@$NODE_B" "conch sema $SEMA_NAME --endpoints=$ENDPOINTS --max=2 --who" 2>/dev/null || true)
echo -e "$SEMA_WHO"

HELD_COUNT=$(echo "$SEMA_WHO" | grep -c "HELD" || true)
WAIT_COUNT=$(echo "$SEMA_WHO" | grep -c "WAIT" || true)

log "After release - Held Slots: $HELD_COUNT, Waiting Contenders: $WAIT_COUNT"

if [ "$HELD_COUNT" -eq 2 ] && [ "$WAIT_COUNT" -eq 0 ]; then
    success "Semaphore promotion verified. Contender on NODE_C was successfully promoted to HELD."
else
    error "Semaphore promotion assertion failed! Expected 2 HELD, 0 WAIT. Got: $HELD_COUNT HELD, $WAIT_COUNT WAIT."
    exit 1
fi

log "================================================="
success "ALL MULTI-NODE CONCH INTEGRATION TESTS PASSED!"
log "================================================="
exit 0
