package testutil

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type TestEtcd struct {
	Cmd       *exec.Cmd
	ClientURL string
	Port      int
}

func getFreePort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func findEtcdBinary() (string, error) {
	// 1. Check if etcd is in the system PATH
	path, err := exec.LookPath("etcd")
	if err == nil {
		return path, nil
	}

	// 2. Look in common Homebrew / local installation paths
	brewPaths := []string{
		"/home/linuxbrew/.linuxbrew/bin/etcd",
		"/opt/homebrew/bin/etcd",
		"/usr/local/bin/etcd",
	}
	for _, bp := range brewPaths {
		if _, err := os.Stat(bp); err == nil {
			return bp, nil
		}
	}

	// 3. Look in typical Nix store paths as a fallback (avoiding nix run / nix shell)
	matches, _ := filepath.Glob("/nix/store/*-etcd*/bin/etcd")
	if len(matches) > 0 {
		return matches[0], nil
	}
	matches2, _ := filepath.Glob("/nix/store/*-etcdserver*/bin/etcd")
	if len(matches2) > 0 {
		return matches2[0], nil
	}

	return "", fmt.Errorf("etcd binary not found in PATH, Homebrew, or /nix/store")
}

func StartEtcd(dataDir string) (*TestEtcd, error) {
	binaryPath, err := findEtcdBinary()
	if err != nil {
		return nil, err
	}

	clientPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("failed to get free client port: %w", err)
	}

	peerPort, err := getFreePort()
	if err != nil {
		return nil, fmt.Errorf("failed to get free peer port: %w", err)
	}

	clientURL := fmt.Sprintf("http://127.0.0.1:%d", clientPort)
	peerURL := fmt.Sprintf("http://127.0.0.1:%d", peerPort)

	// Build etcd command
	cmd := exec.Command(binaryPath,
		"--data-dir", dataDir,
		"--listen-client-urls", clientURL,
		"--advertise-client-urls", clientURL,
		"--listen-peer-urls", peerURL,
		"--initial-advertise-peer-urls", peerURL,
		"--initial-cluster", fmt.Sprintf("default=%s", peerURL),
		"--log-level", "warn", // reduce noise
	)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("failed to start etcd binary: %w", err)
	}

	// Wait for etcd to start accepting connections
	ready := false
	for i := 0; i < 50; i++ {
		cli, err := clientv3.New(clientv3.Config{
			Endpoints:   []string{fmt.Sprintf("127.0.0.1:%d", clientPort)},
			DialTimeout: 100 * time.Millisecond,
		})
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err = cli.Get(ctx, "/")
			cancel()
			cli.Close()
			if err == nil {
				ready = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !ready {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return nil, fmt.Errorf("etcd did not become ready in time")
	}

	return &TestEtcd{
		Cmd:       cmd,
		ClientURL: fmt.Sprintf("127.0.0.1:%d", clientPort),
		Port:      clientPort,
	}, nil
}

func (te *TestEtcd) Stop() {
	if te.Cmd != nil && te.Cmd.Process != nil {
		_ = te.Cmd.Process.Kill()
		_ = te.Cmd.Wait()
	}
}
