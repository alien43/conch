package cron

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"

	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestCronDistributedScheduling(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize 3 conchd instances
	var conchds []*Conchd
	for i := 0; i < 3; i++ {
		c, err := NewConchd(endpoints, 1*time.Second, 10*time.Second, logger)
		if err != nil {
			t.Fatalf("failed to create conchd %d: %v", i, err)
		}
		conchds = append(conchds, c)
		go func(instance *Conchd) {
			_ = instance.Run(ctx)
		}(c)
	}

	// 2. Connect client to add the job
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// Add an @every 1s job
	jobName := "every1s"
	_, err = CmdAdd(ctx, cli, jobName, "@every 1s", "5s", []string{"true"})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Wait for several ticks to execute (e.g., 4 seconds)
	time.Sleep(4 * time.Second)

	// Fetch results
	getResp, err := cli.Get(ctx, "/conch/v1/cron/result/"+jobName+"/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("failed to get results: %v", err)
	}

	if len(getResp.Kvs) == 0 {
		t.Errorf("expected some results, got 0")
	}

	for _, kv := range getResp.Kvs {
		t.Logf("Result: key=%s value=%s", string(kv.Key), string(kv.Value))
		// Check that the node is set
		if !strings.Contains(string(kv.Value), `"node"`) {
			t.Errorf("result does not contain 'node' field: %s", string(kv.Value))
		}
		// Check that exit is 0
		if !strings.Contains(string(kv.Value), `"exit":0`) {
			t.Errorf("expected exit: 0, got different result: %s", string(kv.Value))
		}
	}
}

func TestCronKilledWinnerClaimedNoRerun(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// Add @every 1s job running a sleep command
	jobName := "killed-winner"
	_, err = CmdAdd(ctx, cli, jobName, "@every 1s", "5s", []string{"sleep", "5"})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Start conchd instance
	c1, err := NewConchd(endpoints, 1*time.Second, 10*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create conchd: %v", err)
	}
	c1Ctx, c1Cancel := context.WithCancel(ctx)

	go func() {
		_ = c1.Run(c1Ctx)
	}()

	// Wait for a tick to fire and claim the key
	time.Sleep(1500 * time.Millisecond)

	// Verify that a fire key exists under the fire prefix
	firePrefix := "/conch/v1/cron/fire/" + jobName + "/"
	resp, err := cli.Get(ctx, firePrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("failed to fetch fire keys: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatalf("expected a fire key to be created, but none found")
	}

	// Get the claimed tick time from the key
	claimedKey := string(resp.Kvs[0].Key)
	t.Logf("conchd 1 claimed tick: %s", claimedKey)

	// Now kill conchd 1 (winner) mid-run by cancelling its context
	c1Cancel()
	time.Sleep(500 * time.Millisecond)

	// Ensure NO result key exists for this tick
	resultPrefix := "/conch/v1/cron/result/" + jobName + "/"
	respResult, err := cli.Get(ctx, resultPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("failed to fetch result keys: %v", err)
	}
	if len(respResult.Kvs) > 0 {
		t.Errorf("expected no result key for killed winner, but found %d results", len(respResult.Kvs))
	}

	// Start a second conchd instance c2
	c2, err := NewConchd(endpoints, 1*time.Second, 10*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create conchd 2: %v", err)
	}
	go func() {
		_ = c2.Run(ctx)
	}()

	// Wait for another 2 seconds
	time.Sleep(2 * time.Second)

	// Verify that the claimed tick was NOT rerun by c2, and is still absent in results
	// Let's check results for that specific claimedKey tick suffix
	tickSuffix := claimedKey[len(firePrefix):]
	specificResultKey := "/conch/v1/cron/result/" + jobName + "/" + tickSuffix
	respResultSpecific, err := cli.Get(ctx, specificResultKey)
	if err != nil {
		t.Fatalf("failed to query specific result key: %v", err)
	}
	if len(respResultSpecific.Kvs) > 0 {
		t.Errorf("critical failure: claimed tick %s was rerun after winner was killed!", tickSuffix)
	} else {
		t.Logf("success: claimed tick %s was claimed but never rerun", tickSuffix)
	}
}

func TestCronRmDuringRunFinish(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// Add an @every 1s job running a sleep 2 command
	jobName := "rm-during-run"
	_, err = CmdAdd(ctx, cli, jobName, "@every 1s", "5s", []string{"sleep", "2"})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Start conchd instance
	c, err := NewConchd(endpoints, 1*time.Second, 10*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create conchd: %v", err)
	}
	go func() {
		_ = c.Run(ctx)
	}()

	// Wait for a tick to fire and start running
	time.Sleep(1500 * time.Millisecond)

	// Ensure it has claimed the tick
	firePrefix := "/conch/v1/cron/fire/" + jobName + "/"
	resp, err := cli.Get(ctx, firePrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("failed to fetch fire keys: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatalf("expected job to be running, but fire key is missing")
	}

	// Now remove the job from conchd (cron rm)
	t.Logf("removing job %s while in-flight", jobName)
	_, err = CmdRm(ctx, cli, jobName)
	if err != nil {
		t.Fatalf("failed to remove job: %v", err)
	}

	// Wait 2.5 seconds to let the in-flight run complete
	time.Sleep(2500 * time.Millisecond)

	// Verify that the result key exists with exit 0, meaning it was allowed to finish
	resultPrefix := "/conch/v1/cron/result/" + jobName + "/"
	respResult, err := cli.Get(ctx, resultPrefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("failed to query result: %v", err)
	}
	if len(respResult.Kvs) == 0 {
		t.Errorf("critical failure: job was killed/interrupted or failed to log result after cron rm!")
	} else {
		t.Logf("success: job finished and result was written: %s", string(respResult.Kvs[0].Value))
		if !strings.Contains(string(respResult.Kvs[0].Value), `"exit":0`) {
			t.Errorf("expected exit 0, got: %s", string(respResult.Kvs[0].Value))
		}
	}
}

func TestCronStatusServer(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Connect etcd client
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// Add @every 1s job
	jobName := "status-test-job"
	scheduleExpr := "*/5 * * * *"
	_, err = CmdAdd(ctx, cli, jobName, scheduleExpr, "5s", []string{"true"})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// Add fake candidate key for test-office to verify elect status server
	officeKey := "/conch/v1/elect/test-office/cand1"
	fakeHolder := `{"host":"test-host","pid":1234,"started":"2026-06-14T12:00:00Z","cmd":"test-cmd"}`
	_, err = cli.Put(ctx, officeKey, fakeHolder)
	if err != nil {
		t.Fatalf("failed to put fake elect candidate key: %v", err)
	}

	// Find free TCP port for status server
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to find free port: %v", err)
	}
	statusAddr := l.Addr().String()
	l.Close()

	// Start conchd with status server enabled
	c, err := NewConchd(endpoints, 1*time.Second, 10*time.Second, logger)
	if err != nil {
		t.Fatalf("failed to create conchd: %v", err)
	}
	c.StatusAddr = statusAddr

	go func() {
		_ = c.Run(ctx)
	}()

	// Wait for status server to start
	time.Sleep(500 * time.Millisecond)

	// 1. Fetch cron status over HTTP
	resp, err := http.Get("http://" + statusAddr + "/cron")
	if err != nil {
		t.Fatalf("failed to GET cron status: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for /cron, got: %s", resp.Status)
	}

	var items []CronStatusItem
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatalf("failed to decode cron JSON response: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 cron status item, got %d", len(items))
	}

	item := items[0]
	if item.Name != jobName {
		t.Errorf("expected name %q, got %q", jobName, item.Name)
	}
	if item.Schedule != scheduleExpr {
		t.Errorf("expected schedule %q, got %q", scheduleExpr, item.Schedule)
	}
	if item.NextTick == "" {
		t.Errorf("expected computed NextTick, got empty string")
	}
	if item.LastTick != "—" {
		t.Errorf("expected LastTick to be '—', got %q", item.LastTick)
	}
	if item.Node != "—" {
		t.Errorf("expected Node to be '—', got %q", item.Node)
	}
	if item.Exit != nil {
		t.Errorf("expected Exit to be nil, got %v", *item.Exit)
	}

	// 2. Fetch elect status over HTTP
	respElect, err := http.Get("http://" + statusAddr + "/elect")
	if err != nil {
		t.Fatalf("failed to GET elect status: %v", err)
	}
	defer respElect.Body.Close()

	if respElect.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for /elect, got: %s", respElect.Status)
	}

	var electItems []ElectStatusItem
	if err := json.NewDecoder(respElect.Body).Decode(&electItems); err != nil {
		t.Fatalf("failed to decode elect JSON response: %v", err)
	}

	if len(electItems) != 1 {
		t.Fatalf("expected 1 elect status item, got %d", len(electItems))
	}

	electItem := electItems[0]
	if electItem.Office != "test-office" {
		t.Errorf("expected office 'test-office', got %q", electItem.Office)
	}
	if electItem.Leader != "test-host" {
		t.Errorf("expected leader 'test-host', got %q", electItem.Leader)
	}
	if electItem.Pid != 1234 {
		t.Errorf("expected pid 1234, got %d", electItem.Pid)
	}
	if electItem.Started != "2026-06-14T12:00:00Z" {
		t.Errorf("expected started '2026-06-14T12:00:00Z', got %q", electItem.Started)
	}
	if electItem.Cmd != "test-cmd" {
		t.Errorf("expected cmd 'test-cmd', got %q", electItem.Cmd)
	}

	// Add fake semaphore keys to verify sema status server
	semaKey1 := "/conch/v1/sema/test-sema/2/holder1"
	semaKey2 := "/conch/v1/sema/test-sema/2/holder2"
	semaKey3 := "/conch/v1/sema/test-sema/2/holder3"
	fakeHolder1 := `{"host":"sema-host1","pid":1001,"started":"2026-06-14T12:01:00Z","cmd":"sema-cmd1"}`
	fakeHolder2 := `{"host":"sema-host2","pid":1002,"started":"2026-06-14T12:02:00Z","cmd":"sema-cmd2"}`
	fakeHolder3 := `{"host":"sema-host3","pid":1003,"started":"2026-06-14T12:03:00Z","cmd":"sema-cmd3"}`

	if _, err := cli.Put(ctx, semaKey1, fakeHolder1); err != nil {
		t.Fatalf("failed to put fake sema key 1: %v", err)
	}
	if _, err := cli.Put(ctx, semaKey2, fakeHolder2); err != nil {
		t.Fatalf("failed to put fake sema key 2: %v", err)
	}
	if _, err := cli.Put(ctx, semaKey3, fakeHolder3); err != nil {
		t.Fatalf("failed to put fake sema key 3: %v", err)
	}

	// 3. Fetch sema status over HTTP
	respSema, err := http.Get("http://" + statusAddr + "/sema")
	if err != nil {
		t.Fatalf("failed to GET sema status: %v", err)
	}
	defer respSema.Body.Close()

	if respSema.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK for /sema, got: %s", respSema.Status)
	}

	var semaItems []SemaStatusItem
	if err := json.NewDecoder(respSema.Body).Decode(&semaItems); err != nil {
		t.Fatalf("failed to decode sema JSON response: %v", err)
	}

	if len(semaItems) != 1 {
		t.Fatalf("expected 1 sema status item, got %d", len(semaItems))
	}

	semaItem := semaItems[0]
	if semaItem.Name != "test-sema" {
		t.Errorf("expected sema name 'test-sema', got %q", semaItem.Name)
	}
	if semaItem.Max != 2 {
		t.Errorf("expected sema max 2, got %d", semaItem.Max)
	}
	if len(semaItem.Holders) != 2 {
		t.Errorf("expected 2 holders, got %d", len(semaItem.Holders))
	} else {
		if semaItem.Holders[0].Host != "sema-host1" {
			t.Errorf("expected holder 0 host 'sema-host1', got %q", semaItem.Holders[0].Host)
		}
		if semaItem.Holders[1].Host != "sema-host2" {
			t.Errorf("expected holder 1 host 'sema-host2', got %q", semaItem.Holders[1].Host)
		}
	}
	if len(semaItem.Waitlist) != 1 {
		t.Errorf("expected 1 waiter in waitlist, got %d", len(semaItem.Waitlist))
	} else {
		if semaItem.Waitlist[0].Host != "sema-host3" {
			t.Errorf("expected waitlist 0 host 'sema-host3', got %q", semaItem.Waitlist[0].Host)
		}
	}
}

func captureStdout(f func()) string {
	r, w, _ := os.Pipe()
	oldStdout := os.Stdout
	os.Stdout = w

	defer func() {
		os.Stdout = oldStdout
	}()

	f()

	w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func TestCronLs(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatalf("failed to start etcd: %v", err)
	}
	defer etcd.Stop()

	endpoints := []string{etcd.ClientURL}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 1 * time.Second,
	})
	if err != nil {
		t.Fatalf("failed to connect to etcd: %v", err)
	}
	defer cli.Close()

	// 1. Add cron job
	_, err = CmdAdd(ctx, cli, "job1", "*/5 * * * *", "10m", []string{"true"})
	if err != nil {
		t.Fatalf("failed to add job: %v", err)
	}

	// 2. Run CmdLs text mode without last run status
	outText1 := captureStdout(func() {
		_, _ = CmdLs(ctx, cli, false, false)
	})
	if !strings.Contains(outText1, "NAME") || !strings.Contains(outText1, "SCHEDULE") || !strings.Contains(outText1, "job1") {
		t.Errorf("unexpected text output: %q", outText1)
	}

	// 3. Run CmdLs text mode with last run status
	outText2 := captureStdout(func() {
		_, _ = CmdLs(ctx, cli, true, false)
	})
	if !strings.Contains(outText2, "NAME") || !strings.Contains(outText2, "LAST-TICK") || !strings.Contains(outText2, "DURATION") {
		t.Errorf("unexpected text output with last: %q", outText2)
	}

	// 4. Run CmdLs JSON mode
	outJSON := captureStdout(func() {
		_, _ = CmdLs(ctx, cli, false, true)
	})
	var parsed []map[string]interface{}
	if err := json.Unmarshal([]byte(strings.TrimSpace(outJSON)), &parsed); err != nil {
		t.Fatalf("failed to unmarshal JSON output %q: %v", outJSON, err)
	}
	if len(parsed) != 1 || parsed[0]["name"] != "job1" || parsed[0]["schedule"] != "*/5 * * * *" {
		t.Errorf("unexpected JSON payload: %v", parsed)
	}
}

