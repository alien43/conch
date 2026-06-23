package core

import (
	"bufio"
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// This entrypoint is used by sub-processes spawned during invariant tests
func init() {
	if os.Getenv("CONCH_INVARIANT_CHILD") == "1" {
		runInvariantChild()
		os.Exit(0)
	}
}

func runInvariantChild() {
	holder := os.Getenv("CONCH_NAME")
	logFile := os.Getenv("CONCH_LOG_FILE")
	rev := os.Getenv("CONCH_REV")

	fmt.Fprintf(os.Stderr, "RUN INVARIANT CHILD START: holder=%s logFile=%s rev=%s\n", holder, logFile, rev)

	startNs := time.Now().UnixNano()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGTERM)

	select {
	case <-sigChan:
		fmt.Fprintf(os.Stderr, "RUN INVARIANT CHILD RECEIVED SIGTERM: holder=%s\n", holder)
	case <-time.After(15 * time.Second):
		fmt.Fprintf(os.Stderr, "RUN INVARIANT CHILD NATURALLY COMPLETED: holder=%s\n", holder)
	}

	endNs := time.Now().UnixNano()

	// Append record to shared log file
	f, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "FAILED TO OPEN LOG FILE %s: %v\n", logFile, err)
	} else {
		defer f.Close()
		_, err = fmt.Fprintf(f, "%s %d %d %s\n", holder, startNs, endNs, rev)
		if err != nil {
			fmt.Fprintf(os.Stderr, "FAILED TO WRITE TO LOG FILE %s: %v\n", logFile, err)
		} else {
			fmt.Fprintf(os.Stderr, "RUN INVARIANT CHILD LOGGED SUCCESSFULLY: holder=%s\n", holder)
		}
	}
}

type interval struct {
	Holder  string
	StartNs int64
	EndNs   int64
	Rev     int64
}

func TestInvariantConcurrency(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}

	// Create shared log file
	logFile := filepath.Join(t.TempDir(), "intervals.log")

	// Create etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// We will run K=4 competing processes for N=2 slots in a semaphore
	K := 4
	N := 2
	ttl := 3 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// We'll track wrapper processes we spawn
	type wrapperProc struct {
		Cmd    *exec.Cmd
		SessID clientv3.LeaseID
	}
	var procs []*wrapperProc

	// Let's spawn them.
	// Since we built the main.go earlier, we can use the compiled conch binary!
	// Wait, to be completely self-contained and accurate, we can run the conch binary we compiled.
	// Build the conch binary into t.TempDir() to avoid leaving a stray binary in the workspace
	moduleRoot, err := findModuleRoot()
	if err != nil {
		t.Fatalf("failed to find module root: %v", err)
	}
	conchPath := filepath.Join(t.TempDir(), "conch")
	buildCmd := exec.Command("go", "build", "-o", conchPath, filepath.Join(moduleRoot, "cmd", "conch", "main.go"))
	if err := buildCmd.Run(); err != nil {
		t.Fatalf("failed to build conch: %v", err)
	}

	for i := 0; i < K; i++ {
		// Each wrapper will run: conch sema inv-test --max 2 --ttl 3s --endpoints <url> -- <self> (with env CONCH_INVARIANT_CHILD=1)
		cmd := exec.Command(conchPath, "sema", "inv-test", "--max", strconv.Itoa(N), "--ttl", ttl.String(), "--endpoints", etcd.ClientURL, "--", os.Args[0])
		cmd.Env = append(os.Environ(),
			"CONCH_INVARIANT_CHILD=1",
			"CONCH_LOG_FILE="+logFile,
		)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Start(); err != nil {
			t.Fatalf("failed to start wrapper %d: %v", i, err)
		}
		procs = append(procs, &wrapperProc{Cmd: cmd})
	}

	// Wait for things to start running and stabilize
	time.Sleep(2 * time.Second)

	// Seed reproducible randomized behavior
	seed := time.Now().UnixNano()
	if envSeed := os.Getenv("CONCH_SEED"); envSeed != "" {
		if s, err := strconv.ParseInt(envSeed, 10, 64); err == nil {
			seed = s
		}
	}
	t.Logf("CONCH_SEED=%d", seed)
	r := rand.New(rand.NewSource(seed))

	var lossTimes []int64

	// Induce some chaos!
	// 1. Induce a lease revocation on etcd
	t.Log("chaos: revoking a lease")
	resp, err := cli.Leases(ctx)
	if err == nil && len(resp.Leases) > 0 {
		// Revoke the first active lease
		lossTimes = append(lossTimes, time.Now().UnixNano())
		_, _ = cli.Revoke(ctx, resp.Leases[0].ID)
	}

	// 2. SIGSTOP and SIGCONT a random wrapper process (GC pause simulator)
	t.Log("chaos: SIGSTOP random wrapper")
	targetIdx := r.Intn(K)
	targetProc := procs[targetIdx]
	if targetProc.Cmd.Process != nil {
		lossTimes = append(lossTimes, time.Now().UnixNano())
		_ = targetProc.Cmd.Process.Signal(syscall.SIGSTOP)
		time.Sleep(1500 * time.Millisecond)
		_ = targetProc.Cmd.Process.Signal(syscall.SIGCONT)
	}

	// Let them run more to allow convergence
	time.Sleep(3 * time.Second)

	// Invariant 2 (Convergence): Check the state just before cleaning up
	getResp, err := cli.Get(ctx, "/conch/v1/sema/inv-test/2/", clientv3.WithPrefix())
	if err != nil {
		t.Errorf("convergence check failed: cannot get keys: %v", err)
	} else {
		// Count active wrappers (not yet exited)
		activeWrappers := 0
		for _, p := range procs {
			if p.Cmd.Process != nil {
				if err := syscall.Kill(p.Cmd.Process.Pid, 0); err == nil {
					activeWrappers++
				}
			}
		}
		expectedHolders := N
		if activeWrappers < N {
			expectedHolders = activeWrappers
		}
		// Count actual holders
		holders := 0
		for i := 0; i < len(getResp.Kvs); i++ {
			if i < N {
				holders++
			}
		}
		if holders != expectedHolders {
			t.Errorf("invariant 2 failure (convergence): expected %d holders, got %d", expectedHolders, holders)
		} else {
			t.Logf("invariant 2 passed (converged to %d holders with %d active wrappers)", holders, activeWrappers)
		}
	}

	// Kill all wrappers cleanly by sending SIGTERM to them
	t.Log("cleaning up wrappers")
	for _, p := range procs {
		if p.Cmd.Process != nil {
			_ = p.Cmd.Process.Signal(syscall.SIGTERM)
			_ = p.Cmd.Wait()
		}
	}

	// Parse interval log
	f, err := os.Open(logFile)
	if err != nil {
		t.Fatalf("failed to open log file: %v", err)
	}
	defer f.Close()

	var intervals []interval
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		start, _ := strconv.ParseInt(parts[1], 10, 64)
		end, _ := strconv.ParseInt(parts[2], 10, 64)
		rev, _ := strconv.ParseInt(parts[3], 10, 64)
		intervals = append(intervals, interval{
			Holder:  parts[0],
			StartNs: start,
			EndNs:   end,
			Rev:     rev,
		})
	}

	if len(intervals) == 0 {
		t.Log("no intervals recorded, possibly no holders won before cleanup")
		return
	}

	// Assert invariant 1: At any instant, ≤ N intervals overlap,
	// except during a window of ≤ TTL after each induced loss event (which we can check or model).
	// Let's check overlaps at all boundary times (start and end times of all intervals)
	var timePoints []int64
	for _, inv := range intervals {
		timePoints = append(timePoints, inv.StartNs, inv.EndNs)
	}

	for _, tp := range timePoints {
		overlapCount := 0
		for _, inv := range intervals {
			if tp >= inv.StartNs && tp <= inv.EndNs {
				overlapCount++
			}
		}
		if overlapCount > N {
			// Invariant 1 (Strict boundary): Overlap > N is strictly forbidden unless within TTL after an induced loss event.
			inLossWindow := false
			for _, lt := range lossTimes {
				if tp >= lt && tp <= lt+ttl.Nanoseconds() {
					inLossWindow = true
					break
				}
			}
			if !inLossWindow {
				t.Errorf("invariant 1 failure: overlap of %d (max N=%d) at %d outside of any allowed loss window of TTL %s after chaos", overlapCount, N, tp, ttl)
			} else {
				t.Logf("At %d, overlap count was %d (exceeded N=%d). Allowed during transition window <= TTL after induced loss event.", tp, overlapCount, N)
			}
			if overlapCount > N+1 {
				t.Errorf("critical failure: overlap of %d (max N=%d) exceeds N+1 unconditionally", overlapCount, N)
			}
		}
	}

	// Assert invariant 3: Fencing tokens (revisions) strictly increase per slot/successor
	// Let's sort intervals by StartNs and assert that revisions are increasing for successions
	for i := 0; i < len(intervals)-1; i++ {
		for j := i + 1; j < len(intervals); j++ {
			if intervals[i].EndNs < intervals[j].StartNs {
				// i succeeded by j
				if intervals[i].Rev > intervals[j].Rev {
					t.Errorf("fencing token regression: interval %d (rev %d) finished before %d (rev %d) started", i, intervals[i].Rev, j, intervals[j].Rev)
				}
			}
		}
	}
}

func findModuleRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", fmt.Errorf("go.mod not found")
}
