package core

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/alien43/conch/internal/testutil"
)

func withSecurity(t *testing.T, s Security) {
	t.Helper()
	SetSecurity(s)
	t.Cleanup(func() { SetSecurity(Security{}) })
}

// TestTLSClientCertEtcd: an etcd serving TLS only, with client-cert auth.
// Plaintext and "TLS without a client cert" are refused; the configured
// security gets a full session (lease, keepalive, a put on the lease).
func TestTLSClientCertEtcd(t *testing.T) {
	certs := testutil.NewCerts(t)
	etcd, err := testutil.StartEtcdTLS(t.TempDir(), certs.ServerCert, certs.ServerKey, certs.CA, certs.ClientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer etcd.Stop()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eps := []string{etcd.ClientURL}

	// No security: refused.
	if _, err := NewCoreSession(context.Background(), eps, time.Second, 6*time.Second, logger); err == nil {
		t.Fatal("plaintext session against a TLS-only etcd succeeded")
	}
	// The CA but no client cert: refused (--client-cert-auth).
	withSecurity(t, Security{CACert: certs.CA})
	if _, err := NewCoreSession(context.Background(), eps, time.Second, 6*time.Second, logger); err == nil {
		t.Fatal("session without a client certificate succeeded")
	}
	// Full TLS: a working session.
	withSecurity(t, Security{CACert: certs.CA, Cert: certs.ClientCert, Key: certs.ClientKey})
	sess, err := NewCoreSession(context.Background(), eps, time.Second, 6*time.Second, logger)
	if err != nil {
		t.Fatalf("TLS session: %v", err)
	}
	defer sess.Close()
	if _, err := sess.Client.Put(context.Background(), "/tls-probe", "x", clientv3.WithLease(sess.LeaseID)); err != nil {
		t.Fatal(err)
	}
	if !sess.ValidUntil().After(time.Now()) {
		t.Fatal("no validity bound over TLS")
	}
}

// TestUserPasswordAuthEtcd: with etcd auth enabled, no credentials are
// refused and the configured user/password gets a session.
func TestUserPasswordAuthEtcd(t *testing.T) {
	etcd, err := testutil.StartEtcd(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer etcd.Stop()
	admin, err := clientv3.New(clientv3.Config{Endpoints: []string{etcd.ClientURL}, DialTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	for _, step := range []func() error{
		func() error { _, err := admin.UserAdd(ctx, "root", "s3cret"); return err },
		func() error { _, err := admin.RoleAdd(ctx, "root"); return err },
		func() error { _, err := admin.UserGrantRole(ctx, "root", "root"); return err },
		func() error { _, err := admin.AuthEnable(ctx); return err },
	} {
		if err := step(); err != nil {
			t.Fatal(err)
		}
	}
	admin.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	eps := []string{etcd.ClientURL}

	if _, err := NewCoreSession(ctx, eps, time.Second, 6*time.Second, logger); err == nil {
		t.Fatal("unauthenticated session succeeded with auth enabled")
	}
	withSecurity(t, Security{User: "root", Password: "wrong"})
	if _, err := NewCoreSession(ctx, eps, time.Second, 6*time.Second, logger); err == nil {
		t.Fatal("wrong password accepted")
	}
	withSecurity(t, Security{User: "root", Password: "s3cret"})
	sess, err := NewCoreSession(ctx, eps, time.Second, 6*time.Second, logger)
	if err != nil {
		t.Fatalf("authenticated session: %v", err)
	}
	sess.Close()
}

func TestSecurityValidation(t *testing.T) {
	for name, s := range map[string]Security{
		"cert without key":      {Cert: "c.pem"},
		"key without cert":      {Key: "k.pem"},
		"user without password": {User: "root"},
		"missing CA file":       {CACert: "/nonexistent/ca.pem"},
	} {
		withSecurity(t, s)
		if _, err := ClientConfig(nil, 0); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	SetSecurity(Security{})
	if _, err := ClientConfig(nil, 0); err != nil {
		t.Fatalf("zero value must stay plaintext and valid: %v", err)
	}
	pf := filepath.Join(t.TempDir(), "pw")
	if err := os.WriteFile(pf, []byte("s3cret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if pw, err := ReadPassword(pf, "from-env"); err != nil || pw != "s3cret" {
		t.Fatalf("password file: %q %v", pw, err)
	}
	if pw, _ := ReadPassword("", "from-env"); pw != "from-env" {
		t.Fatalf("env fallback: %q", pw)
	}
}
