package elect

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/alien43/conch/internal/testutil"
)

// TestCLIOverTLS: the --cacert/--cert/--key flags and the CONCH_* env reach
// every client. Against a TLS-only etcd with client-cert auth, `elect --who`
// on a vacant office exits 1 ("no leader") with them and 69 ("etcd
// unreachable", or a hang) without; and a real `elect -- true` runs over TLS.
func TestCLIOverTLS(t *testing.T) {
	conch := buildConch(t)
	certs := testutil.NewCerts(t)
	etcd, err := testutil.StartEtcdTLS(t.TempDir(), certs.ServerCert, certs.ServerKey, certs.CA, certs.ClientTLS)
	if err != nil {
		t.Fatal(err)
	}
	defer etcd.Stop()

	run := func(env []string, args ...string) int {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, conch, args...)
		cmd.Env = append(os.Environ(), env...)
		err := cmd.Run()
		var ee *exec.ExitError
		if errors.As(err, &ee) {
			return ee.ExitCode()
		}
		if err != nil {
			t.Fatalf("%v: %v", args, err)
		}
		return 0
	}
	tlsFlags := []string{"--cacert", certs.CA, "--cert", certs.ClientCert, "--key", certs.ClientKey}
	base := []string{"elect", "tls-office", "--endpoints", etcd.ClientURL, "--dial-timeout", "2s"}

	// Without TLS the read-only modes hang rather than exit 69: pre-existing
	// (no timeout on their etcd calls), not about TLS. What matters here is
	// that they never get an answer (-1: killed at the test's timeout).
	if code := run(nil, append(base, "--who")...); code == 0 || code == 1 {
		t.Errorf("--who without TLS: exit %d, want a failure (69, or killed)", code)
	}
	if code := run(nil, append(append(base, tlsFlags...), "--who")...); code != 1 {
		t.Errorf("--who with TLS flags: exit %d, want 1 (vacant)", code)
	}
	env := []string{"CONCH_CACERT=" + certs.CA, "CONCH_CERT=" + certs.ClientCert, "CONCH_KEY=" + certs.ClientKey}
	if code := run(env, append(base, "--who")...); code != 1 {
		t.Errorf("--who with CONCH_* env: exit %d, want 1 (vacant)", code)
	}
	if code := run(nil, append(append(base, tlsFlags...), "--", "sh", "-c", "exit 7")...); code != 7 {
		t.Errorf("elect over TLS: exit %d, want the child's 7", code)
	}
	if code := run(nil, append(append(base, "--cert", certs.ClientCert), "--who")...); code != 64 {
		t.Errorf("--cert without --key: exit %d, want 64", code)
	}
}
