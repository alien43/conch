package testutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Certs is a throwaway PKI for a TLS etcd on 127.0.0.1: a CA, a server
// certificate and a client certificate it signed, as PEM files in Dir.
type Certs struct {
	Dir                       string
	CA, ServerCert, ServerKey string
	ClientCert, ClientKey     string
	ClientTLS                 *tls.Config // trusts the CA, presents the client cert
}

// NewCerts writes a fresh PKI into a temp dir.
func NewCerts(t testing.TB) *Certs {
	t.Helper()
	dir := t.TempDir()
	c := &Certs{Dir: dir}
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "conch-test-ca"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, _ := x509.ParseCertificate(caDER)
	c.CA = writePEM(t, dir, "ca.pem", "CERTIFICATE", caDER)

	leaf := func(name string, serial int64, usage x509.ExtKeyUsage) (string, string) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{usage},
			IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
		if err != nil {
			t.Fatal(err)
		}
		kb, _ := x509.MarshalECPrivateKey(key)
		return writePEM(t, dir, name+".pem", "CERTIFICATE", der), writePEM(t, dir, name+"-key.pem", "EC PRIVATE KEY", kb)
	}
	c.ServerCert, c.ServerKey = leaf("server", 2, x509.ExtKeyUsageServerAuth)
	c.ClientCert, c.ClientKey = leaf("client", 3, x509.ExtKeyUsageClientAuth)

	pair, err := tls.LoadX509KeyPair(c.ClientCert, c.ClientKey)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	c.ClientTLS = &tls.Config{RootCAs: pool, Certificates: []tls.Certificate{pair}}
	return c
}

func writePEM(t testing.TB, dir, name, typ string, der []byte) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.WriteFile(p, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
