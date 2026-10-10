package main

import (
	"context"
	"flag"
	"fmt"
	clientv3 "go.etcd.io/etcd/client/v3"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alien43/conch/internal/core"
	"github.com/alien43/conch/internal/cron"
	"github.com/alien43/conch/internal/elect"
	"github.com/alien43/conch/internal/sema"
)

// version is stamped at build time with -ldflags "-X main.version=<tag>"
// (.github/workflows/release.yml, flake.nix, the parent repo's nixos/flake.nix).
var version = "dev"

func getEnvOrDefault(key, defaultVal string) string {
	if val := os.Getenv(key); val != "" {
		return val
	}
	return defaultVal
}

func main() {
	if len(os.Args) < 2 {
		printUsageAndExit()
	}

	subcommand := os.Args[1]

	switch subcommand {
	case "elect":
		handleElect(os.Args[2:])
	case "sema":
		handleSema(os.Args[2:])
	case "cron":
		handleCron(os.Args[2:])
	case "conchd":
		handleConchd(os.Args[2:])
	case "version", "-v", "--version":
		fmt.Println(version)
		os.Exit(0)
	case "-h", "--help", "help":
		printUsageAndExit()
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand: %s\n", subcommand)
		os.Exit(64)
	}
}

func printUsageAndExit() {
	fmt.Fprintf(os.Stderr, `Usage: conch <subcommand> [options]

Subcommands:
  elect <office> [--restart] [--kill-after 5s] [--wait <dur>] [--nonblock] [--on-acquire CMD] [--on-lose CMD] [--hook-timeout 30s] [--fence CMD --fence-budget D] [--watchdog [--watchdog-heartbeat PATH --watchdog-stale D]] [--campaign-after D] -- <cmd...>
  elect <office> --who [--json]
  elect <office> --watch [--json]
  elect <office> --assert [--min-rev N] [--json]

  sema <name> --max N [--wait <dur>] [--nonblock] [--spread] -- <cmd...>
  sema <name> --max N --who [--json]

  cron add <name> --schedule '<cron>' [--run-ttl 10m] [--exclusive] -- <cmd...>
  cron rm <name>
  cron ls [--last] [--json]

  conchd

Global Env Options (can be passed as flags too):
  CONCH_ENDPOINTS (default: localhost:2379)
  CONCH_DIAL_TIMEOUT (default: 5s)
  CONCH_TTL (default: 10s)
  CONCH_CACERT, CONCH_CERT, CONCH_KEY   etcd TLS (--cacert, --cert, --key); use https:// endpoints
  CONCH_USER, CONCH_PASSWORD            etcd auth (--user; password via env or --password-file)
`)
	os.Exit(64)
}

func registerGlobalFlags(fs *flag.FlagSet) (endpointsStr *string, dialTimeoutStr *string, ttlStr *string, quietFlag *bool) {
	endpointsStr = fs.String("endpoints", getEnvOrDefault("CONCH_ENDPOINTS", "localhost:2379"), "comma-separated etcd endpoints")
	dialTimeoutStr = fs.String("dial-timeout", getEnvOrDefault("CONCH_DIAL_TIMEOUT", "5s"), "dial timeout duration")
	ttlStr = fs.String("ttl", getEnvOrDefault("CONCH_TTL", "10s"), "session TTL duration")
	quietFlag = fs.Bool("quiet", false, "suppress logs below WARN")
	secFlags.caCert = fs.String("cacert", os.Getenv("CONCH_CACERT"), "etcd TLS: CA certificate (PEM) to verify etcd's server certificate")
	secFlags.cert = fs.String("cert", os.Getenv("CONCH_CERT"), "etcd TLS: client certificate (PEM)")
	secFlags.key = fs.String("key", os.Getenv("CONCH_KEY"), "etcd TLS: client key (PEM)")
	secFlags.user = fs.String("user", os.Getenv("CONCH_USER"), "etcd auth user; the password comes from CONCH_PASSWORD or --password-file")
	secFlags.passwordFile = fs.String("password-file", os.Getenv("CONCH_PASSWORD_FILE"), "etcd auth: file holding the password")
	return
}

// secFlags are the etcd security flags every subcommand takes.
var secFlags struct {
	caCert, cert, key, user, passwordFile *string
}

// applySecurity turns the security flags into core.SetSecurity, exiting 64
// on a bad combination.
func applySecurity() {
	if secFlags.caCert == nil {
		return
	}
	pw, err := core.ReadPassword(*secFlags.passwordFile, os.Getenv("CONCH_PASSWORD"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(64)
	}
	core.SetSecurity(core.Security{CACert: *secFlags.caCert, Cert: *secFlags.cert, Key: *secFlags.key, User: *secFlags.user, Password: pw})
	if _, err := core.ClientConfig(nil, 0); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(64)
	}
}

func parseGlobalValues(endpointsStr, dialTimeoutStr, ttlStr *string, quietFlag *bool) (endpoints []string, dialTimeout, ttl time.Duration, quiet bool) {
	endpoints = strings.Split(*endpointsStr, ",")

	var err error
	dialTimeout, err = time.ParseDuration(*dialTimeoutStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid dial-timeout: %v\n", err)
		os.Exit(64)
	}

	ttl, err = time.ParseDuration(*ttlStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid ttl: %v\n", err)
		os.Exit(64)
	}

	quiet = *quietFlag
	applySecurity()
	return
}

func setupLogger(quiet bool) *slog.Logger {
	level := slog.LevelInfo
	if quiet {
		level = slog.LevelWarn
	}
	opts := &slog.HandlerOptions{
		Level: level,
	}
	return slog.New(slog.NewTextHandler(os.Stderr, opts))
}

func splitChildCmd(args []string) ([]string, []string) {
	for i, arg := range args {
		if arg == "--" {
			return args[:i], args[i+1:]
		}
	}
	return args, nil
}

func parseCommon(fs *flag.FlagSet, args []string, killAfterStr *string) (endpoints []string, dialTimeout, ttl time.Duration, killAfter time.Duration, logger *slog.Logger) {
	endpointsStr, dialTimeoutStr, ttlStr, quietFlag := registerGlobalFlags(fs)
	_ = fs.Parse(args)

	endpoints, dialTimeout, ttl, quiet := parseGlobalValues(endpointsStr, dialTimeoutStr, ttlStr, quietFlag)
	logger = setupLogger(quiet)

	if killAfterStr != nil && *killAfterStr != "" {
		var err error
		killAfter, err = time.ParseDuration(*killAfterStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid kill-after: %v\n", err)
			os.Exit(64)
		}
		explicit := false
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "kill-after" {
				explicit = true
			}
		})
		fitted, warning := core.FitKillAfter(ttl, killAfter, explicit)
		if warning != "" {
			logger.Warn("kill-after does not fit ttl", "detail", warning)
		} else if fitted != killAfter {
			logger.Info("kill-after lowered to fit ttl", "kill_after", fitted, "ttl", ttl)
		}
		killAfter = fitted
	}
	return
}

func setupSignalCancel(ctx context.Context, cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		select {
		case <-sigChan:
			cancel()
		case <-ctx.Done():
		}
	}()
}

func handleElect(args []string) {
	fs := flag.NewFlagSet("elect", flag.ExitOnError)

	restart := fs.Bool("restart", false, "re-campaign and re-run forever")
	killAfterStr := fs.String("kill-after", core.DefaultKillAfter.String(), "SIGTERM -> SIGKILL escalation delay (default lowered to fit --ttl)")
	waitStr := fs.String("wait", "", "max time to campaign before giving up")
	nonblock := fs.Bool("nonblock", false, "equivalent to --wait 0")
	who := fs.Bool("who", false, "print current leader, exit")
	watch := fs.Bool("watch", false, "stream leadership changes")
	useJSON := fs.Bool("json", false, "print output as JSON")
	assert := fs.Bool("assert", false, "assert if this host holds the office")
	minRev := fs.Int64("min-rev", 0, "minimum create revision for assert")
	onAcquire := fs.String("on-acquire", "", "command to run after winning, before child starts")
	onLose := fs.String("on-lose", "", "command to run after child is killed, before re-campaigning")
	hookTimeoutStr := fs.String("hook-timeout", "30s", "timeout for on-acquire and on-lose hooks")
	fenceCmd := fs.String("fence", "", "command that stops what the child started; must confirm (exit 0) within --fence-budget, before the lease can expire")
	fenceBudgetStr := fs.String("fence-budget", "", "hard deadline for --fence (required with --fence)")
	watchdog := fs.Bool("watchdog", false, "pet systemd's watchdog (WatchdogSec=) only while fit")
	heartbeat := fs.String("watchdog-heartbeat", "", "with --watchdog: file the child touches; stale while holding = unfit")
	staleStr := fs.String("watchdog-stale", "", "with --watchdog-heartbeat: how old the heartbeat may get")
	campaignAfterStr := fs.String("campaign-after", "", "campaign only once the office has been vacant this long (static preference: lower for preferred hosts)")

	wrapperArgs, childCmd := splitChildCmd(args)

	if len(wrapperArgs) < 1 || strings.HasPrefix(wrapperArgs[0], "-") {
		fmt.Fprintf(os.Stderr, "office name is required\n")
		os.Exit(64)
	}
	office := wrapperArgs[0]
	wrapperArgs = wrapperArgs[1:]

	endpoints, dialTimeout, ttl, killAfter, logger := parseCommon(fs, wrapperArgs, killAfterStr)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Read-only modes do not need a session
	if *who || *watch || *assert {
		setupSignalCancel(ctx, cancel)
		runElectReadOnly(ctx, logger, endpoints, dialTimeout, office, *who, *watch, *assert, *minRev, *useJSON)
		return
	}

	if len(childCmd) == 0 {
		fmt.Fprintf(os.Stderr, "command to run is required after --\n")
		os.Exit(64)
	}

	// Campaign wait limit
	var waitLimit time.Duration
	if *nonblock {
		waitLimit = 1 * time.Nanosecond // Virtually 0
	} else if *waitStr != "" {
		var err error
		waitLimit, err = time.ParseDuration(*waitStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid wait duration: %v\n", err)
			os.Exit(64)
		}
	}

	hookTimeout, err := time.ParseDuration(*hookTimeoutStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid hook-timeout: %v\n", err)
		os.Exit(64)
	}

	var fence elect.Fence
	if *fenceCmd != "" || *fenceBudgetStr != "" {
		if *fenceCmd == "" || *fenceBudgetStr == "" {
			fmt.Fprintf(os.Stderr, "--fence and --fence-budget go together\n")
			os.Exit(64)
		}
		budget, err := time.ParseDuration(*fenceBudgetStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid fence-budget: %v\n", err)
			os.Exit(64)
		}
		if why := core.FitFence(ttl, budget); why != "" {
			fmt.Fprintf(os.Stderr, "%s\n", why)
			os.Exit(64)
		}
		fence = elect.Fence{Cmd: *fenceCmd, Budget: budget}
	}

	if *watchdog {
		var stale time.Duration
		if *heartbeat != "" {
			if *staleStr == "" {
				fmt.Fprintf(os.Stderr, "--watchdog-heartbeat needs --watchdog-stale\n")
				os.Exit(64)
			}
			var err error
			if stale, err = time.ParseDuration(*staleStr); err != nil || stale <= 0 {
				fmt.Fprintf(os.Stderr, "invalid watchdog-stale: %q\n", *staleStr)
				os.Exit(64)
			}
		}
		wd, why := core.NewWatchdogFromEnv(*heartbeat, stale, logger)
		if wd == nil {
			logger.Warn("watchdog-disabled", "reason", why)
		}
		fence.Watchdog = wd
	} else if *heartbeat != "" || *staleStr != "" {
		fmt.Fprintf(os.Stderr, "--watchdog-heartbeat/--watchdog-stale need --watchdog\n")
		os.Exit(64)
	}

	if *campaignAfterStr != "" {
		d, err := time.ParseDuration(*campaignAfterStr)
		if err != nil || d < 0 {
			fmt.Fprintf(os.Stderr, "invalid campaign-after: %q\n", *campaignAfterStr)
			os.Exit(64)
		}
		if *nonblock && d > 0 {
			fmt.Fprintf(os.Stderr, "--nonblock and --campaign-after cannot go together\n")
			os.Exit(64)
		}
		fence.CampaignAfter = d
	}

	exitCode, _ := elect.RunElectFenced(ctx, logger, endpoints, dialTimeout, ttl, killAfter, *restart, office, waitLimit, 60*time.Second, *onAcquire, *onLose, hookTimeout, fence, childCmd)
	os.Exit(exitCode)
}

func runElectReadOnly(ctx context.Context, logger *slog.Logger, endpoints []string, dialTimeout time.Duration, office string, who, watch, assert bool, minRev int64, useJSON bool) {
	cli, err := core.NewClient(endpoints, dialTimeout)
	if err != nil {
		logger.Error("failed to connect to etcd", "err", err)
		os.Exit(69)
	}
	defer cli.Close()

	if assert {
		octx, ocancel := oneShot(ctx, dialTimeout)
		defer ocancel()
		code, err := elect.CmdAssert(octx, cli, office, minRev, useJSON)
		if err != nil {
			logger.Error("assert failed", "err", err)
			os.Exit(69)
		}
		os.Exit(code)
	}

	if who {
		octx, ocancel := oneShot(ctx, dialTimeout)
		defer ocancel()
		code, err := elect.CmdWho(octx, cli, office, useJSON)
		if err != nil {
			logger.Error("failed to get leader info", "err", err)
			os.Exit(69)
		}
		os.Exit(code)
	}

	if watch {
		// The stream itself has no end, but an etcd we cannot use at all
		// should exit 69, not wait forever: probe it once first.
		octx, ocancel := oneShot(ctx, dialTimeout)
		_, perr := cli.Get(octx, core.ElectPrefix(office), clientv3.WithCountOnly())
		ocancel()
		if perr != nil {
			logger.Error("failed to watch leader", "err", perr)
			os.Exit(69)
		}
		code, err := elect.CmdWatch(ctx, cli, office, useJSON)
		if err != nil {
			logger.Error("failed to watch leader", "err", err)
			os.Exit(69)
		}
		os.Exit(code)
	}
}

func handleSema(args []string) {
	fs := flag.NewFlagSet("sema", flag.ExitOnError)

	max := fs.Int("max", 0, "capacity N >= 1 (required)")
	waitStr := fs.String("wait", "", "max time to wait for a slot")
	nonblock := fs.Bool("nonblock", false, "equivalent to --wait 0")
	spread := fs.Bool("spread", false, "at most one slot per node")
	who := fs.Bool("who", false, "list current holders and waiters")
	useJSON := fs.Bool("json", false, "print output as JSON")
	killAfterStr := fs.String("kill-after", core.DefaultKillAfter.String(), "SIGTERM -> SIGKILL escalation delay (default lowered to fit --ttl)")

	wrapperArgs, childCmd := splitChildCmd(args)

	if len(wrapperArgs) < 1 || strings.HasPrefix(wrapperArgs[0], "-") {
		fmt.Fprintf(os.Stderr, "semaphore name is required\n")
		os.Exit(64)
	}
	name := wrapperArgs[0]
	wrapperArgs = wrapperArgs[1:]

	endpoints, dialTimeout, ttl, killAfter, logger := parseCommon(fs, wrapperArgs, killAfterStr)

	if *max <= 0 {
		fmt.Fprintf(os.Stderr, "--max capacity (>=1) is required\n")
		os.Exit(64)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --who mode
	if *who {
		setupSignalCancel(ctx, cancel)

		cli, err := core.NewClient(endpoints, dialTimeout)
		if err != nil {
			logger.Error("failed to connect to etcd", "err", err)
			os.Exit(69)
		}
		defer cli.Close()

		octx, ocancel := oneShot(ctx, dialTimeout)
		defer ocancel()
		code, err := sema.CmdWho(octx, cli, name, *max, *useJSON)
		if err != nil {
			logger.Error("failed to get semaphore info", "err", err)
			os.Exit(69)
		}
		os.Exit(code)
	}

	if len(childCmd) == 0 {
		fmt.Fprintf(os.Stderr, "command to run is required after --\n")
		os.Exit(64)
	}

	// Calculate wait limit
	var waitLimit time.Duration = -1 // -1 means infinite
	if *nonblock {
		waitLimit = 0
	} else if *waitStr != "" {
		var err error
		waitLimit, err = time.ParseDuration(*waitStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "invalid wait duration: %v\n", err)
			os.Exit(64)
		}
	}

	exitCode, _ := sema.RunSema(ctx, logger, endpoints, dialTimeout, ttl, killAfter, name, *max, *spread, waitLimit, childCmd)
	os.Exit(exitCode)
}

func handleCron(args []string) {
	if len(args) < 1 {
		printUsageAndExit()
	}

	action := args[0]
	args = args[1:]

	fs := flag.NewFlagSet("cron", flag.ExitOnError)

	schedule := fs.String("schedule", "", "cron schedule expression (for add)")
	runTTL := fs.String("run-ttl", "10m", "max expected runtime (for add)")
	exclusive := fs.Bool("exclusive", false, "skip a tick while a previous run is still live on any node (for add)")
	showLast := fs.Bool("last", false, "show last result info (for ls)")
	useJSON := fs.Bool("json", false, "print output as JSON")

	var name string
	if action == "add" || action == "rm" {
		if len(args) < 1 || strings.HasPrefix(args[0], "-") {
			fmt.Fprintf(os.Stderr, "job name is required\n")
			os.Exit(64)
		}
		name = args[0]
		args = args[1:]
	}

	wrapperArgs, childCmd := splitChildCmd(args)

	endpoints, dialTimeout, _, _, logger := parseCommon(fs, wrapperArgs, nil)

	// add, rm and ls are one-shot: bounded, so an unusable etcd exits 69.
	ctx, cancel := oneShot(context.Background(), dialTimeout)
	defer cancel()

	cli, err := core.NewClient(endpoints, dialTimeout)
	if err != nil {
		logger.Error("failed to connect to etcd", "err", err)
		os.Exit(69)
	}
	defer cli.Close()

	switch action {
	case "add":
		if *schedule == "" {
			fmt.Fprintf(os.Stderr, "--schedule is required for add\n")
			os.Exit(64)
		}
		if len(childCmd) == 0 {
			fmt.Fprintf(os.Stderr, "command to run is required after --\n")
			os.Exit(64)
		}

		code, err := cron.CmdAdd(ctx, cli, name, *schedule, *runTTL, *exclusive, childCmd)
		if err != nil {
			logger.Error("failed to add job", "err", err)
			os.Exit(code)
		}
		os.Exit(0)

	case "rm":
		code, err := cron.CmdRm(ctx, cli, name)
		if err != nil {
			logger.Error("failed to remove job", "err", err)
			os.Exit(code)
		}
		os.Exit(0)

	case "ls":
		code, err := cron.CmdLs(ctx, cli, *showLast, *useJSON)
		if err != nil {
			logger.Error("failed to list jobs", "err", err)
			os.Exit(code)
		}
		os.Exit(0)

	default:
		fmt.Fprintf(os.Stderr, "unknown cron action: %s\n", action)
		os.Exit(64)
	}
}

func handleConchd(args []string) {
	fs := flag.NewFlagSet("conchd", flag.ExitOnError)
	statusAddr := fs.String("status-addr", "", "HTTP address to listen on for status queries (e.g., :9191)")
	endpoints, dialTimeout, ttl, _, logger := parseCommon(fs, args, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	conchd, err := cron.NewConchd(endpoints, dialTimeout, ttl, logger)
	if err != nil {
		logger.Error("failed to initialize conchd", "err", err)
		os.Exit(69)
	}

	addr := *statusAddr
	if addr == "" {
		addr = os.Getenv("CONCH_STATUS_ADDR")
	}
	conchd.StatusAddr = addr

	logger.Info("starting conchd daemon")
	if err := conchd.Run(ctx); err != nil {
		logger.Error("conchd exited with error", "err", err)
		os.Exit(1)
	}
}

// oneShot bounds a one-shot CLI call. clientv3 dials lazily and retries an
// RPC until its context ends, so without a deadline an etcd that can't be
// used (down, wrong scheme, TLS refused) hangs the command instead of exiting
// 69.
func oneShot(ctx context.Context, dialTimeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, max(2*dialTimeout, 2*time.Second))
}
