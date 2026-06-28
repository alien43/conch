package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alien43/conch/internal/cron"
	"github.com/alien43/conch/internal/elect"
	"github.com/alien43/conch/internal/sema"

	clientv3 "go.etcd.io/etcd/client/v3"
)

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
		fmt.Println("0.1.1")
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
  elect <office> [--restart] [--kill-after 5s] [--wait <dur>] [--nonblock] [--on-acquire CMD] [--on-lose CMD] [--hook-timeout 30s] -- <cmd...>
  elect <office> --who [--json]
  elect <office> --watch [--json]
  elect <office> --assert [--min-rev N] [--json]

  sema <name> --max N [--wait <dur>] [--nonblock] [--spread] -- <cmd...>
  sema <name> --max N --who [--json]

  cron add <name> --schedule '<cron>' [--run-ttl 10m] -- <cmd...>
  cron rm <name>
  cron ls [--last] [--json]

  conchd

Global Env Options (can be passed as flags too):
  CONCH_ENDPOINTS (default: localhost:2379)
  CONCH_DIAL_TIMEOUT (default: 5s)
  CONCH_TTL (default: 10s)
`)
	os.Exit(64)
}

func registerGlobalFlags(fs *flag.FlagSet) (endpointsStr *string, dialTimeoutStr *string, ttlStr *string, quietFlag *bool) {
	endpointsStr = fs.String("endpoints", getEnvOrDefault("CONCH_ENDPOINTS", "localhost:2379"), "comma-separated etcd endpoints")
	dialTimeoutStr = fs.String("dial-timeout", getEnvOrDefault("CONCH_DIAL_TIMEOUT", "5s"), "dial timeout duration")
	ttlStr = fs.String("ttl", getEnvOrDefault("CONCH_TTL", "10s"), "session TTL duration")
	quietFlag = fs.Bool("quiet", false, "suppress logs below WARN")
	return
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
	killAfterStr := fs.String("kill-after", "5s", "SIGTERM -> SIGKILL escalation delay")
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

	exitCode, _ := elect.RunElect(ctx, logger, endpoints, dialTimeout, ttl, killAfter, *restart, office, waitLimit, 60*time.Second, *onAcquire, *onLose, hookTimeout, childCmd)
	os.Exit(exitCode)
}

func runElectReadOnly(ctx context.Context, logger *slog.Logger, endpoints []string, dialTimeout time.Duration, office string, who, watch, assert bool, minRev int64, useJSON bool) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
	if err != nil {
		logger.Error("failed to connect to etcd", "err", err)
		os.Exit(69)
	}
	defer cli.Close()

	if assert {
		code, err := elect.CmdAssert(ctx, cli, office, minRev, useJSON)
		if err != nil {
			logger.Error("assert failed", "err", err)
			os.Exit(69)
		}
		os.Exit(code)
	}

	if who {
		code, err := elect.CmdWho(ctx, cli, office, useJSON)
		if err != nil {
			logger.Error("failed to get leader info", "err", err)
			os.Exit(69)
		}
		os.Exit(code)
	}

	if watch {
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
	killAfterStr := fs.String("kill-after", "5s", "SIGTERM -> SIGKILL escalation delay")

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

		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: dialTimeout,
		})
		if err != nil {
			logger.Error("failed to connect to etcd", "err", err)
			os.Exit(69)
		}
		defer cli.Close()

		code, err := sema.CmdWho(ctx, cli, name, *max, *useJSON)
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: dialTimeout,
	})
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

		code, err := cron.CmdAdd(ctx, cli, name, *schedule, *runTTL, childCmd)
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
