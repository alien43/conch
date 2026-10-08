package core

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Security is how conch authenticates to etcd: TLS (a CA to trust, and an
// optional client certificate) and/or etcd's user/password auth. The zero
// value is plaintext and unauthenticated, conch's behaviour before.
type Security struct {
	CACert string // PEM file of the CA that signed etcd's server certificate
	Cert   string // client certificate (PEM), for --client-cert-auth etcd
	Key    string // its key
	User   string
	// Password is never taken as a flag (it would show in ps): it comes from
	// CONCH_PASSWORD or a file.
	Password string
}

var (
	secMu sync.RWMutex
	sec   Security
)

// SetSecurity sets the process-wide etcd security. conch is one process per
// invocation, so cmd/conch sets it once from flags before any client exists.
func SetSecurity(s Security) {
	secMu.Lock()
	defer secMu.Unlock()
	sec = s
}

// ReadPassword returns the password from file (trailing newline trimmed),
// or env if file is empty.
func ReadPassword(file, env string) (string, error) {
	if file == "" {
		return env, nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return "", fmt.Errorf("password file: %w", err)
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

// ClientConfig is the clientv3 config every conch client uses: endpoints,
// dial timeout and the process-wide Security.
func ClientConfig(endpoints []string, dialTimeout time.Duration) (clientv3.Config, error) {
	secMu.RLock()
	s := sec
	secMu.RUnlock()
	cfg := clientv3.Config{Endpoints: endpoints, DialTimeout: dialTimeout, Username: s.User, Password: s.Password}
	if (s.Cert == "") != (s.Key == "") {
		return cfg, fmt.Errorf("etcd TLS: --cert and --key go together")
	}
	if s.User != "" && s.Password == "" {
		return cfg, fmt.Errorf("etcd auth: --user needs a password (CONCH_PASSWORD or --password-file)")
	}
	if s.CACert != "" || s.Cert != "" {
		info := transport.TLSInfo{TrustedCAFile: s.CACert, CertFile: s.Cert, KeyFile: s.Key}
		tlsCfg, err := info.ClientConfig()
		if err != nil {
			return cfg, fmt.Errorf("etcd TLS: %w", err)
		}
		cfg.TLS = tlsCfg
	}
	return cfg, nil
}

// NewClient is clientv3.New over ClientConfig.
func NewClient(endpoints []string, dialTimeout time.Duration) (*clientv3.Client, error) {
	cfg, err := ClientConfig(endpoints, dialTimeout)
	if err != nil {
		return nil, err
	}
	return clientv3.New(cfg)
}
