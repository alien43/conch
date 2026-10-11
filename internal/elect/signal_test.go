package elect

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/alien43/conch/internal/core"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// TestSignalWhileCampaigningDropsTheKey: a conch stopped (SIGTERM) while
// queued must exit and take its candidate key with it, not leave it to block
// the queue until its lease expires.
func TestSignalWhileCampaigningDropsTheKey(t *testing.T) {
	conch := buildConch(t)
	etcd, cli := fenceEtcd(t)
	const office = "sigq"
	sess, err := concurrency.NewSession(cli, concurrency.WithTTL(30))
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()
	if err := concurrency.NewElection(sess, core.ElectElectionKey(office)).Campaign(context.Background(), "A"); err != nil {
		t.Fatal(err)
	}

	for _, extra := range [][]string{nil, {"--campaign-after", "5s"}} {
		args := append([]string{"elect", office, "--endpoints", etcd.ClientURL, "--ttl", "30s"}, extra...)
		c := exec.Command(conch, append(args, "--", "sleep", "600")...)
		c.Stderr = os.Stderr
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		if extra == nil { // queued: wait for its key behind A's
			for i := 0; ; i++ {
				r, err := cli.Get(context.Background(), core.ElectPrefix(office), clientv3.WithPrefix(), clientv3.WithCountOnly())
				if err == nil && r.Count == 2 {
					break
				}
				if i > 100 {
					t.Fatal("never queued")
				}
				time.Sleep(50 * time.Millisecond)
			}
		} else { // delaying: holds no key; just let it start watching
			time.Sleep(time.Second)
		}
		sent := time.Now()
		_ = c.Process.Signal(syscall.SIGTERM)
		done := make(chan error, 1)
		go func() { done <- c.Wait() }()
		select {
		case err := <-done:
			var ee *exec.ExitError
			if !errors.As(err, &ee) || ee.ExitCode() != core.ExitWithdrawn {
				t.Fatalf("%v: exit %v after SIGTERM while campaigning, want %d (withdrawn)", extra, err, core.ExitWithdrawn)
			}
		case <-time.After(5 * time.Second):
			_ = c.Process.Kill()
			t.Fatalf("%v: conch did not exit within 5s of SIGTERM while campaigning", extra)
		}
		r, err := cli.Get(context.Background(), core.ElectPrefix(office), clientv3.WithPrefix(), clientv3.WithCountOnly())
		if err != nil || r.Count != 1 {
			t.Fatalf("%v: %d keys after SIGTERM (want only the holder's): the candidate key was left behind", extra, r.Count)
		}
		t.Logf("%v: exited and dropped its key %v after SIGTERM", extra, time.Since(sent).Round(time.Millisecond))
	}
}
