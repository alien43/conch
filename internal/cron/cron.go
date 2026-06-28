package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/alien43/conch/internal/core"

	cron "github.com/robfig/cron/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type JobSpec struct {
	Schedule string   `json:"schedule"`
	Cmd      []string `json:"cmd"`
	RunTTL   string   `json:"run_ttl"`
	AddedBy  string   `json:"added_by"`
	AddedAt  string   `json:"added_at"`
}

type ResultJSON struct {
	Node     string `json:"node"`
	Exit     int    `json:"exit"`
	Started  string `json:"started"`
	Duration string `json:"duration"`
}

type CronHolder struct {
	name string
	rev  int64
}

func (ch *CronHolder) Acquire(ctx context.Context) (int64, error) {
	return ch.rev, nil
}

func (ch *CronHolder) Release(ctx context.Context) error {
	return nil // Do not delete the fire key on release
}

func (ch *CronHolder) Name() string {
	return ch.name
}

func (ch *CronHolder) Key() string {
	return core.CronFirePrefix(ch.name)
}

func CmdAdd(ctx context.Context, client *clientv3.Client, name, scheduleExpr, runTTL string, cmdArgs []string) (int, error) {
	// Validate schedule expression
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	_, err := parser.Parse(scheduleExpr)
	if err != nil {
		return 64, fmt.Errorf("invalid schedule: %w", err)
	}

	// Validate run_ttl if provided
	if runTTL != "" {
		if _, err := time.ParseDuration(runTTL); err != nil {
			return 64, fmt.Errorf("invalid run-ttl: %w", err)
		}
	} else {
		runTTL = "10m"
	}

	username := "unknown"
	if u, err := user.Current(); err == nil {
		username = u.Username
	}
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}

	spec := JobSpec{
		Schedule: scheduleExpr,
		Cmd:      cmdArgs,
		RunTTL:   runTTL,
		AddedBy:  fmt.Sprintf("%s@%s", username, host),
		AddedAt:  time.Now().UTC().Format(time.RFC3339),
	}

	bytes, err := json.Marshal(spec)
	if err != nil {
		return 1, err
	}

	key := core.CronJobKey(name)
	_, err = client.Put(ctx, key, string(bytes))
	if err != nil {
		return 1, err
	}

	return 0, nil
}

func CmdRm(ctx context.Context, client *clientv3.Client, name string) (int, error) {
	key := core.CronJobKey(name)
	resp, err := client.Delete(ctx, key)
	if err != nil {
		return 1, err
	}
	if resp.Deleted == 0 {
		return 1, fmt.Errorf("job not found: %s", name)
	}
	return 0, nil
}

type CronStatusItem struct {
	Name     string `json:"name"`
	Schedule string `json:"schedule"`
	NextTick string `json:"next_tick"`
	LastTick string `json:"last_tick"`
	Node     string `json:"node"`
	Exit     *int   `json:"exit"`
	Duration string `json:"duration"`
}

func GetCronStatus(ctx context.Context, client *clientv3.Client) ([]CronStatusItem, error) {
	resp, err := client.Get(ctx, core.CronJobPrefix(), clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	items := []CronStatusItem{}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

	for _, kv := range resp.Kvs {
		name := strings.TrimPrefix(string(kv.Key), core.CronJobPrefix())
		var spec JobSpec
		if err := json.Unmarshal(kv.Value, &spec); err != nil {
			continue
		}

		var nextTick string
		if sched, err := parser.Parse(spec.Schedule); err == nil {
			nextTick = sched.Next(time.Now()).UTC().Format(time.RFC3339)
		}

		item := CronStatusItem{
			Name:     name,
			Schedule: spec.Schedule,
			NextTick: nextTick,
			LastTick: "—",
			Node:     "—",
			Exit:     nil,
			Duration: "—",
		}

		var rawTime int64

		// Fetch last fire
		fireResp, err := client.Get(ctx, core.CronFirePrefix(name),
			clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
			clientv3.WithLimit(1),
		)
		if err == nil && len(fireResp.Kvs) > 0 {
			fireKv := fireResp.Kvs[0]
			tickStr := strings.TrimPrefix(string(fireKv.Key), core.CronFirePrefix(name))
			if ts, err := strconv.ParseInt(tickStr, 10, 64); err == nil {
				rawTime = ts
				item.LastTick = time.Unix(ts, 0).UTC().Format(time.RFC3339)
				var h core.HolderJSON
				if err := json.Unmarshal(fireKv.Value, &h); err == nil {
					item.Node = h.Host
				}
			}
		}

		// Fetch last result
		resResp, err := client.Get(ctx, core.CronResultPrefix(name),
			clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend),
			clientv3.WithLimit(1),
		)
		if err == nil && len(resResp.Kvs) > 0 {
			resKv := resResp.Kvs[0]
			resTickStr := strings.TrimPrefix(string(resKv.Key), core.CronResultPrefix(name))
			if rts, err := strconv.ParseInt(resTickStr, 10, 64); err == nil {
				// Only use result if it's for the same or newer tick as fire
				if rts >= rawTime {
					rawTime = rts
					item.LastTick = time.Unix(rts, 0).UTC().Format(time.RFC3339)
					var res ResultJSON
					if err := json.Unmarshal(resKv.Value, &res); err == nil {
						item.Node = res.Node
						exitVal := res.Exit
						item.Exit = &exitVal
						item.Duration = res.Duration
					}
				}
			}
		}

		items = append(items, item)
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Name < items[j].Name
	})

	return items, nil
}

type ElectStatusItem struct {
	Office         string `json:"office"`
	Leader         string `json:"leader"`
	Pid            int    `json:"pid"`
	Started        string `json:"started"`
	Cmd            string `json:"cmd"`
	CreateRevision int64  `json:"create_revision"`
}

type electCandidate struct {
	Key            []byte
	Value          []byte
	CreateRevision int64
}

func GetElectStatus(ctx context.Context, client *clientv3.Client) ([]ElectStatusItem, error) {
	resp, err := client.Get(ctx, "/conch/v1/elect/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	candidatesByOffice := make(map[string][]electCandidate)
	for _, kv := range resp.Kvs {
		keyStr := string(kv.Key)
		stripped := strings.TrimPrefix(keyStr, "/conch/v1/elect/")
		parts := strings.Split(stripped, "/")
		if len(parts) >= 1 && parts[0] != "" {
			office := parts[0]
			candidatesByOffice[office] = append(candidatesByOffice[office], electCandidate{
				Key:            kv.Key,
				Value:          kv.Value,
				CreateRevision: kv.CreateRevision,
			})
		}
	}

	items := []ElectStatusItem{}
	for office, kvs := range candidatesByOffice {
		if len(kvs) == 0 {
			continue
		}
		sort.Slice(kvs, func(i, j int) bool {
			return kvs[i].CreateRevision < kvs[j].CreateRevision
		})

		leaderKv := kvs[0]
		var h core.HolderJSON
		_ = json.Unmarshal(leaderKv.Value, &h)

		items = append(items, ElectStatusItem{
			Office:         office,
			Leader:         h.Host,
			Pid:            h.Pid,
			Started:        h.Started,
			Cmd:            h.Cmd,
			CreateRevision: leaderKv.CreateRevision,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Office < items[j].Office
	})

	return items, nil
}

type SemaHolderStatus struct {
	Host           string `json:"host"`
	Pid            int    `json:"pid"`
	Started        string `json:"started"`
	Cmd            string `json:"cmd"`
	CreateRevision int64  `json:"create_revision"`
}

type SemaStatusItem struct {
	Name     string             `json:"name"`
	Max      int                `json:"max"`
	Holders  []SemaHolderStatus `json:"holders"`
	Waitlist []SemaHolderStatus `json:"waitlist"`
}

func parseSemaKey(keyStr string) (string, int, string, error) {
	stripped := strings.TrimPrefix(keyStr, "/conch/v1/sema/")
	parts := strings.Split(stripped, "/")
	if len(parts) < 3 {
		return "", 0, "", fmt.Errorf("invalid sema key structure: %s", keyStr)
	}
	identifier := parts[len(parts)-1]
	maxStr := parts[len(parts)-2]
	name := strings.Join(parts[:len(parts)-2], "/")
	max, err := strconv.Atoi(maxStr)
	if err != nil {
		return "", 0, "", fmt.Errorf("invalid max capacity: %s in key %s", maxStr, keyStr)
	}
	return name, max, identifier, nil
}

func GetSemaStatus(ctx context.Context, client *clientv3.Client) ([]SemaStatusItem, error) {
	resp, err := client.Get(ctx, "/conch/v1/sema/", clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	type rawCandidate struct {
		value          []byte
		key            string
		createRevision int64
	}

	type semaGroupKey struct {
		name string
		max  int
	}

	groups := make(map[semaGroupKey][]rawCandidate)
	for _, kv := range resp.Kvs {
		name, max, _, err := parseSemaKey(string(kv.Key))
		if err != nil {
			continue
		}
		gKey := semaGroupKey{name: name, max: max}
		groups[gKey] = append(groups[gKey], rawCandidate{
			value:          kv.Value,
			key:            string(kv.Key),
			createRevision: kv.CreateRevision,
		})
	}

	items := []SemaStatusItem{}
	for gKey, candidates := range groups {
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].createRevision < candidates[j].createRevision
		})

		var holders []SemaHolderStatus
		var waitlist []SemaHolderStatus

		for idx, cand := range candidates {
			var h core.HolderJSON
			if err := json.Unmarshal(cand.value, &h); err != nil {
				h = core.HolderJSON{
					Host:    "unknown",
					Started: "unknown",
				}
			}

			holderStatus := SemaHolderStatus{
				Host:           h.Host,
				Pid:            h.Pid,
				Started:        h.Started,
				Cmd:            h.Cmd,
				CreateRevision: cand.createRevision,
			}

			if idx < gKey.max {
				holders = append(holders, holderStatus)
			} else {
				waitlist = append(waitlist, holderStatus)
			}
		}

		if holders == nil {
			holders = []SemaHolderStatus{}
		}
		if waitlist == nil {
			waitlist = []SemaHolderStatus{}
		}

		items = append(items, SemaStatusItem{
			Name:     gKey.name,
			Max:      gKey.max,
			Holders:  holders,
			Waitlist: waitlist,
		})
	}

	sort.Slice(items, func(i, j int) bool {
		if items[i].Name == items[j].Name {
			return items[i].Max < items[j].Max
		}
		return items[i].Name < items[j].Name
	})

	return items, nil
}

func CmdLs(ctx context.Context, client *clientv3.Client, showLast, useJSON bool) (int, error) {
	items, err := GetCronStatus(ctx, client)
	if err != nil {
		return 1, err
	}

	if useJSON {
		var output []map[string]interface{}
		for _, item := range items {
			m := map[string]interface{}{
				"name":     item.Name,
				"schedule": item.Schedule,
			}
			if showLast {
				m["last_tick"] = item.LastTick
				m["node"] = item.Node
				m["exit"] = item.Exit
				m["duration"] = item.Duration
			}
			output = append(output, m)
		}
		bytes, _ := json.Marshal(output)
		fmt.Println(string(bytes))
	} else {
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		if showLast {
			fmt.Fprintln(w, "NAME\tSCHEDULE\tLAST-TICK\tNODE\tEXIT\tDURATION")
			for _, item := range items {
				exitStr := "—"
				if item.Exit != nil {
					exitStr = strconv.Itoa(*item.Exit)
				} else if item.LastTick != "—" {
					exitStr = "?"
				}
				fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s\n", item.Name, item.Schedule, item.LastTick, item.Node, exitStr, item.Duration)
			}
		} else {
			fmt.Fprintln(w, "NAME\tSCHEDULE")
			for _, item := range items {
				fmt.Fprintf(w, "%s\t%s\n", item.Name, item.Schedule)
			}
		}
		w.Flush()
	}

	return 0, nil
}

type Conchd struct {
	client      *clientv3.Client
	endpoints   []string
	dialTimeout time.Duration
	ttl         time.Duration
	logger      *slog.Logger
	StatusAddr  string
}

func NewConchd(endpoints []string, dialTimeout, ttl time.Duration, logger *slog.Logger) (*Conchd, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		return nil, err
	}
	return &Conchd{
		client:      cli,
		endpoints:   endpoints,
		dialTimeout: dialTimeout,
		ttl:         ttl,
		logger:      logger,
	}, nil
}

func (cd *Conchd) startStatusServer(ctx context.Context) {
	if cd.StatusAddr == "" {
		return
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/cron", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		items, err := GetCronStatus(r.Context(), cd.client)
		if err != nil {
			cd.logger.Error("failed to get cron status", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(items)
	})

	mux.HandleFunc("/elect", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		items, err := GetElectStatus(r.Context(), cd.client)
		if err != nil {
			cd.logger.Error("failed to get elect status", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(items)
	})

	mux.HandleFunc("/sema", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		items, err := GetSemaStatus(r.Context(), cd.client)
		if err != nil {
			cd.logger.Error("failed to get sema status", "err", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(items)
	})

	server := &http.Server{
		Addr:    cd.StatusAddr,
		Handler: mux,
	}

	go func() {
		cd.logger.Info("starting cron status server", "addr", cd.StatusAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			cd.logger.Error("status server error", "err", err)
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
}

func (cd *Conchd) Run(ctx context.Context) error {
	sess, err := core.NewCoreSession(ctx, cd.endpoints, cd.dialTimeout, cd.ttl, cd.logger)
	if err != nil {
		return err
	}
	defer sess.Close()

	cd.startStatusServer(ctx)

	jobs := make(map[string]JobSpec)
	cancels := make(map[string]context.CancelFunc)

	defer func() {
		for _, cancel := range cancels {
			cancel()
		}
	}()

	// 1. Get initial jobs
	resp, err := cd.client.Get(ctx, core.CronJobPrefix(), clientv3.WithPrefix())
	if err != nil {
		return err
	}

	for _, kv := range resp.Kvs {
		var spec JobSpec
		if err := json.Unmarshal(kv.Value, &spec); err == nil {
			name := strings.TrimPrefix(string(kv.Key), core.CronJobPrefix())
			jobs[name] = spec

			jobCtx, cancel := context.WithCancel(ctx)
			cancels[name] = cancel
			go runJobScheduler(ctx, jobCtx, cd.logger, cd.client, sess, name, spec)
		}
	}

	// 2. Watch for job changes
	watchChan := cd.client.Watch(ctx, core.CronJobPrefix(), clientv3.WithPrefix(), clientv3.WithRev(resp.Header.Revision+1))

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-sess.DoneCh:
			return fmt.Errorf("conchd session lost")
		case wresp, ok := <-watchChan:
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			for _, ev := range wresp.Events {
				cd.handleWatchEvent(ctx, ev, jobs, cancels, sess)
			}
		}
	}
}

func (cd *Conchd) handleWatchEvent(ctx context.Context, ev *clientv3.Event, jobs map[string]JobSpec, cancels map[string]context.CancelFunc, sess *core.CoreSession) {
	name := strings.TrimPrefix(string(ev.Kv.Key), core.CronJobPrefix())
	if ev.Type == clientv3.EventTypeDelete {
		if cancel, ok := cancels[name]; ok {
			cancel()
			delete(cancels, name)
		}
		delete(jobs, name)
		cd.logger.Info("job removed", "name", name)
		return
	}

	var spec JobSpec
	if err := json.Unmarshal(ev.Kv.Value, &spec); err != nil {
		return
	}

	oldSpec, exists := jobs[name]
	if exists && oldSpec.Schedule == spec.Schedule && oldSpec.RunTTL == spec.RunTTL && len(oldSpec.Cmd) == len(spec.Cmd) {
		cmdChanged := false
		for i := range spec.Cmd {
			if spec.Cmd[i] != oldSpec.Cmd[i] {
				cmdChanged = true
				break
			}
		}
		if !cmdChanged {
			// Execution spec is unchanged. Update jobs map for metadata, but do NOT restart scheduler.
			jobs[name] = spec
			cd.logger.Debug("job touched but execution spec unchanged; not restarting scheduler", "name", name)
			return
		}
	}

	if cancel, ok := cancels[name]; ok {
		cancel()
	}
	jobs[name] = spec
	jobCtx, cancel := context.WithCancel(ctx)
	cancels[name] = cancel
	go runJobScheduler(ctx, jobCtx, cd.logger, cd.client, sess, name, spec)
	cd.logger.Info("job added/updated", "name", name)
}

func runJobScheduler(daemonCtx, jobCtx context.Context, logger *slog.Logger, client *clientv3.Client, sess *core.CoreSession, name string, spec JobSpec) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	sched, err := parser.Parse(spec.Schedule)
	if err != nil {
		logger.Error("failed to parse schedule for job", "name", name, "err", err)
		return
	}

	for {
		now := time.Now()
		next := sched.Next(now)
		dur := time.Until(next)

		logger.Info("scheduled next tick for job", "name", name, "time", next.Format(time.RFC3339), "wait", dur)

		select {
		case <-jobCtx.Done():
			return
		case <-time.After(dur):
		}

		// At tick time, add up to 500ms random jitter (politeness, not correctness)
		jitter := time.Duration(rand.Int63n(500)) * time.Millisecond
		select {
		case <-jobCtx.Done():
			return
		case <-time.After(jitter):
		}

		tickUnix := next.Unix()
		fireKey := core.CronFireKey(name, tickUnix)

		// Create holder JSON
		holderVal, err := core.NewHolderJSON(spec.Cmd)
		if err != nil {
			logger.Error("failed to create holder JSON", "err", err)
			continue
		}

		// 1. Grant a 25h lease for the fire key
		leaseResp, err := client.Grant(daemonCtx, 25*3600)
		if err != nil {
			logger.Error("failed to grant lease for fire tick", "err", err)
			continue
		}

		// 2. Race: Txn If(CreateRevision(fireKey) == 0) Then(Put fireKey, TTL 25h)
		txn := client.Txn(daemonCtx).If(
			clientv3.Compare(clientv3.CreateRevision(fireKey), "=", 0),
		).Then(
			clientv3.OpPut(fireKey, string(holderVal), clientv3.WithLease(leaseResp.ID)),
		)

		txnResp, err := txn.Commit()
		if err != nil {
			logger.Error("failed to commit fire txn", "err", err)
			_, _ = client.Revoke(context.Background(), leaseResp.ID)
			continue
		}

		if !txnResp.Succeeded {
			logger.Info("lost race for tick", "name", name, "tick", tickUnix)
			_, _ = client.Revoke(context.Background(), leaseResp.ID)
			continue
		}

		// We won the race! Run the job
		logger.Info("won race for tick, executing job", "name", name, "tick", tickUnix)

		cronHolder := &CronHolder{
			name: name,
			rev:  txnResp.Header.Revision,
		}

		runStart := time.Now()

		runTTL := 10 * time.Minute
		if spec.RunTTL != "" {
			if d, err := time.ParseDuration(spec.RunTTL); err == nil {
				runTTL = d
			}
		}

		runCtx, runCancel := context.WithTimeout(daemonCtx, runTTL)
		exitCode, _, runErr := core.Run(runCtx, logger, sess, cronHolder, spec.Cmd, 5*time.Second)
		runCancel()

		duration := time.Since(runStart)

		// Write result JSON with TTL 14 days (336 hours)
		resultKey := core.CronResultKey(name, tickUnix)
		host, _ := os.Hostname()

		resultVal := ResultJSON{
			Node:     host,
			Exit:     exitCode,
			Started:  runStart.UTC().Format(time.RFC3339),
			Duration: fmt.Sprintf("%.1fs", duration.Seconds()),
		}
		resultBytes, _ := json.Marshal(resultVal)

		resLease, err := client.Grant(daemonCtx, 14*24*3600)
		if err == nil {
			_, _ = client.Put(daemonCtx, resultKey, string(resultBytes), clientv3.WithLease(resLease.ID))
		} else {
			_, _ = client.Put(daemonCtx, resultKey, string(resultBytes))
		}

		logger.Info("completed cron job tick", "name", name, "tick", tickUnix, "exit", exitCode, "err", runErr)
	}
}
