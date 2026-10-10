package elect

import (
	"context"
	"encoding/json"
	"errors"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/alien43/conch/internal/core"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// leaderCmd is the holder's child command ("" when vacant).
func leaderCmd(t *testing.T, cli *clientv3.Client, office string) string {
	t.Helper()
	r, err := cli.Get(context.Background(), core.ElectPrefix(office), clientv3.WithPrefix(),
		clientv3.WithSort(clientv3.SortByCreateRevision, clientv3.SortAscend), clientv3.WithLimit(1))
	if err != nil {
		t.Error(err)
		return ""
	}
	if len(r.Kvs) == 0 {
		return ""
	}
	var h core.HolderJSON
	if json.Unmarshal(r.Kvs[0].Value, &h) != nil {
		return string(r.Kvs[0].Value) // a raw election.Campaign value
	}
	return h.Cmd
}

// leaders samples the office's holder every 20 ms until stop, recording
// each holder seen and when it was first seen.
type leaders struct {
	mu    sync.Mutex
	first map[string]time.Time
}

func sampleLeaders(t *testing.T, cli *clientv3.Client, office string, stop <-chan struct{}) *leaders {
	l := &leaders{first: map[string]time.Time{}}
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			if c := leaderCmd(t, cli, office); c != "" {
				l.mu.Lock()
				if _, ok := l.first[c]; !ok {
					l.first[c] = time.Now()
				}
				l.mu.Unlock()
			}
		}
	}()
	return l
}

func (l *leaders) seen(c string) (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	at, ok := l.first[c]
	return at, ok
}

func campaignAfter(ctx context.Context, endpoints []string, office string, d, waitLimit time.Duration, restart bool, child []string) int {
	code, _ := RunElectFenced(ctx, quietLogger(), endpoints, time.Second, 10*time.Second, 2*time.Second, restart, office,
		waitLimit, 500*time.Millisecond, "", "", time.Second, Fence{CampaignAfter: d}, child)
	return code
}

// TestCampaignAfterPreferredKeepsOfficeAcrossBlips: the preferred member (no
// delay) keeps coming back within the other's delay, so the other never
// holds. Once the preferred one stops for good, the other takes over after its
// delay, and not before.
func TestCampaignAfterPreferredKeepsOfficeAcrossBlips(t *testing.T) {
	etcd, cli := fenceEtcd(t)
	endpoints := []string{etcd.ClientURL}
	const office = "pref"
	preferred := []string{"sleep", "1"} // a 1 s term, then re-campaign after a 1 s backoff
	other := []string{"sleep", "600"}

	stop := make(chan struct{})
	defer close(stop)
	seen := sampleLeaders(t, cli, office, stop)

	pctx, pcancel := context.WithCancel(context.Background())
	defer pcancel()
	pdone := make(chan int, 1)
	go func() { pdone <- campaignAfter(pctx, endpoints, office, 0, 0, true, preferred) }()
	waitKey(t, cli, office)

	octx, ocancel := context.WithCancel(context.Background())
	defer ocancel()
	odone := make(chan int, 1)
	go func() { odone <- campaignAfter(octx, endpoints, office, 2500*time.Millisecond, 0, true, other) }()

	time.Sleep(7 * time.Second) // several 1 s vacancies, each shorter than the other's delay
	if at, ok := seen.seen("sleep 600"); ok {
		t.Fatalf("the delayed member took the office at %v during a blip shorter than its delay", at)
	}

	pcancel()
	<-pdone
	stopped := time.Now()
	for deadline := stopped.Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if at, ok := seen.seen("sleep 600"); ok {
			if took := at.Sub(stopped); took < 2500*time.Millisecond {
				t.Fatalf("the delayed member took over %v after the preferred one stopped, before its 2.5 s delay", took)
			}
			ocancel()
			<-odone
			return
		}
	}
	t.Fatal("the delayed member never took over once the preferred one had stopped")
}

// TestCampaignAfterTakesOverAfterDelay: a holder's lease is revoked (its
// session died). A delayed candidate holds no key while waiting, and takes
// the office only once it has been vacant for its delay.
func TestCampaignAfterTakesOverAfterDelay(t *testing.T) {
	etcd, cli := fenceEtcd(t)
	const office = "delay"
	sess, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if err := concurrency.NewElection(sess, core.ElectElectionKey(office)).Campaign(context.Background(), "A"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() {
		done <- campaignAfter(ctx, []string{etcd.ClientURL}, office, time.Second, 0, false, []string{"sleep", "600"})
	}()

	time.Sleep(500 * time.Millisecond)
	if r, err := cli.Get(context.Background(), core.ElectPrefix(office), clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil || r.Count != 1 {
		t.Fatalf("while delaying the candidate must hold no key: %d keys (%v)", r.Count, err)
	}

	if _, err := cli.Revoke(context.Background(), sess.Lease()); err != nil {
		t.Fatal(err)
	}
	revoked := time.Now()
	waitKey(t, cli, office)
	took := time.Since(revoked)
	if took < time.Second || took > 3*time.Second {
		t.Fatalf("took the office %v after the vacancy, want about the 1 s delay", took)
	}
	if c := leaderCmd(t, cli, office); c != "sleep 600" {
		t.Fatalf("holder %q", c)
	}
	cancel()
	<-done
}

// TestCampaignAfterWaitCoversTheDelay: --wait bounds the whole acquisition,
// the delay included.
func TestCampaignAfterWaitCoversTheDelay(t *testing.T) {
	etcd, cli := fenceEtcd(t)
	start := time.Now()
	code := campaignAfter(context.Background(), []string{etcd.ClientURL}, "wait", 2*time.Second, 500*time.Millisecond, false, []string{"true"})
	if code != 75 {
		t.Fatalf("exit %d, want 75 (acquire timed out)", code)
	}
	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Fatalf("gave up after %v, want about --wait (500ms)", took)
	}
	if r, err := cli.Get(context.Background(), core.ElectPrefix("wait"), clientv3.WithPrefix(), clientv3.WithCountOnly()); err != nil || r.Count != 0 {
		t.Fatalf("left %d keys (%v)", r.Count, err)
	}
}

func TestCampaignAfterUsage(t *testing.T) {
	conch := buildConch(t)
	for _, args := range [][]string{
		{"elect", "x", "--nonblock", "--campaign-after", "1s", "--", "true"},
		{"elect", "x", "--campaign-after", "soon", "--", "true"},
		{"elect", "x", "--campaign-after", "-1s", "--", "true"},
	} {
		err := exec.Command(conch, args...).Run()
		var ee *exec.ExitError
		if !errors.As(err, &ee) || ee.ExitCode() != 64 {
			t.Errorf("%v: %v, want exit 64", args, err)
		}
	}
}
