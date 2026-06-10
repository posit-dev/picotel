// Copyright (C) 2026 by Posit Software, PBC.

package picotel

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
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// TLS test helpers (prefix: tls)
// ---------------------------------------------------------------------------

// tlsCA holds a generated CA certificate and its PEM bytes.
type tlsCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	pemData []byte
}

// tlsGenCA generates a self-signed CA certificate for testing.
func tlsGenCA(t *testing.T) tlsCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlsGenCA: generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "picotel-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("tlsGenCA: create cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlsGenCA: parse cert: %v", err)
	}
	pemData := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return tlsCA{cert: parsed, key: key, pemData: pemData}
}

// tlsGenServerCert generates a server certificate signed by the given CA,
// writing cert and key PEMs to t.TempDir(). Returns (certPath, keyPath).
func tlsGenServerCert(t *testing.T, ca tlsCA) (certPath, keyPath string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlsGenServerCert: generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("tlsGenServerCert: create cert: %v", err)
	}

	dir := t.TempDir()
	certPath = filepath.Join(dir, "server.crt")
	keyPath = filepath.Join(dir, "server.key")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	if err := os.WriteFile(certPath, certPEM, 0600); err != nil {
		t.Fatalf("tlsGenServerCert: write cert: %v", err)
	}

	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("tlsGenServerCert: marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		t.Fatalf("tlsGenServerCert: write key: %v", err)
	}

	return certPath, keyPath
}

// tlsGenClientCert generates a client certificate signed by the given CA,
// writing a combined cert+key PEM to t.TempDir(). Returns the PEM path.
func tlsGenClientCert(t *testing.T, ca tlsCA) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlsGenClientCert: generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "picotel-test-client"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("tlsGenClientCert: create cert: %v", err)
	}

	dir := t.TempDir()
	pemPath := filepath.Join(dir, "client.pem")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("tlsGenClientCert: marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	combined := append(certPEM, keyPEM...)
	if err := os.WriteFile(pemPath, combined, 0600); err != nil {
		t.Fatalf("tlsGenClientCert: write pem: %v", err)
	}
	return pemPath
}

// tlsWriteCA writes the CA PEM to a temp file and returns the path.
func tlsWriteCA(t *testing.T, ca tlsCA) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(path, ca.pemData, 0600); err != nil {
		t.Fatalf("tlsWriteCA: %v", err)
	}
	return path
}

// tlsNewServerWithCA builds an httptest.Server (TLS) using the given server
// cert/key signed by the given CA. If requireClientCert is true, the server
// requires and verifies a client certificate.
func tlsNewServerWithCA(t *testing.T, ca tlsCA, certPath, keyPath string, requireClientCert bool) *httptest.Server {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("tlsNewServerWithCA: load keypair: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	clientAuth := tls.NoClientCert
	if requireClientCert {
		clientAuth = tls.RequireAndVerifyClientCert
	}
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientCAs:    pool,
		ClientAuth:   clientAuth,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

// ---------------------------------------------------------------------------
// Unit tests: computeTLSConfig
// ---------------------------------------------------------------------------

func TestComputeTLSConfig_NilWhenNoEnv(t *testing.T) {
	resetCaches()
	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatalf("expected nil config, got %+v", cfg)
	}
}

func TestComputeTLSConfig_SkipVerifyTrue(t *testing.T) {
	resetCaches()
	t.Setenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", "true")
	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=true")
	}
}

func TestComputeTLSConfig_SkipVerify1(t *testing.T) {
	resetCaches()
	t.Setenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", "1")
	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil || !cfg.InsecureSkipVerify {
		t.Fatal("expected skip-verify config for '1'")
	}
}

func TestComputeTLSConfig_SkipVerifyTRUE(t *testing.T) {
	resetCaches()
	t.Setenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", "TRUE")
	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil || !cfg.InsecureSkipVerify {
		t.Fatal("expected skip-verify config for 'TRUE'")
	}
}

func TestComputeTLSConfig_SkipVerifyFalseValues(t *testing.T) {
	for _, val := range []string{"false", "0", "yes", "no"} {
		val := val
		t.Run(val, func(t *testing.T) {
			resetCaches()
			t.Setenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", val)
			cfg, err := computeTLSConfig("traces")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg != nil && cfg.InsecureSkipVerify {
				t.Fatalf("expected no skip-verify for value %q, got InsecureSkipVerify=true", val)
			}
		})
	}
}

// Test that PICOTEL_PREFIX does NOT remap PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY.
func TestComputeTLSConfig_SkipVerifyNotRemappedByPrefix(t *testing.T) {
	resetCaches()
	// Raw PICOTEL_ name still honoured under an arbitrary prefix.
	t.Setenv("PICOTEL_PREFIX", "FOO")
	t.Setenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", "true")

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil || !cfg.InsecureSkipVerify {
		t.Fatal("expected skip-verify; PICOTEL_ raw name not honoured under prefix")
	}
}

func TestComputeTLSConfig_SkipVerifyRemappedNameHasNoEffect(t *testing.T) {
	resetCaches()
	// Only a remapped name is set; the raw PICOTEL_ name is absent.
	t.Setenv("PICOTEL_PREFIX", "FOO")
	t.Setenv("FOO_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", "true")

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be nil because no CA and no skip-verify via raw name.
	if cfg != nil && cfg.InsecureSkipVerify {
		t.Fatal("remapped FOO_ name must not trigger skip-verify")
	}
}

func TestComputeTLSConfig_CAFile(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	caPath := tlsWriteCA(t, ca)
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", caPath)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.RootCAs == nil {
		t.Fatal("expected RootCAs to be set")
	}
	if cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=false")
	}
}

func TestComputeTLSConfig_SignalSpecificCATakesPrec_Traces(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	signalPath := tlsWriteCA(t, ca)

	// Put a bad path in the general var to prove it's ignored.
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/bogus/general.pem")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE", signalPath)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("expected non-nil config with RootCAs using signal-specific cert")
	}
}

func TestComputeTLSConfig_SignalSpecificCATakesPrec_Logs(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	signalPath := tlsWriteCA(t, ca)

	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/bogus/general.pem")
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE", signalPath)

	cfg, err := computeTLSConfig("logs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("expected non-nil config with RootCAs using logs signal-specific cert")
	}
}

func TestComputeTLSConfig_TracesCADoesNotApplyToLogs(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	signalPath := tlsWriteCA(t, ca)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE", signalPath)

	cfg, err := computeTLSConfig("logs")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("traces-specific CA must not apply to logs signal; expected nil")
	}
}

func TestComputeTLSConfig_LogsCADoesNotApplyToTraces(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	signalPath := tlsWriteCA(t, ca)
	t.Setenv("OTEL_EXPORTER_OTLP_LOGS_CERTIFICATE", signalPath)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("logs-specific CA must not apply to traces signal; expected nil")
	}
}

func TestComputeTLSConfig_BadCAPath(t *testing.T) {
	resetCaches()
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/does/not/exist/ca.pem")
	_, err := computeTLSConfig("traces")
	if err == nil {
		t.Fatal("expected error for bad CA path")
	}
}

func TestComputeTLSConfig_GarbagePEM(t *testing.T) {
	resetCaches()
	dir := t.TempDir()
	badPath := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(badPath, []byte("this is not a PEM"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", badPath)
	_, err := computeTLSConfig("traces")
	if err == nil {
		t.Fatal("expected error for garbage PEM content")
	}
}

func TestComputeTLSConfig_ClientCertOnly(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	clientPEM := tlsGenClientCert(t, ca)
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", clientPEM)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config when client cert is set")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 certificate, got %d", len(cfg.Certificates))
	}
	if cfg.RootCAs != nil {
		t.Fatal("expected nil RootCAs (system trust) when only client cert is set")
	}
}

func TestComputeTLSConfig_ClientCertWithCA(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	caPath := tlsWriteCA(t, ca)
	clientPEM := tlsGenClientCert(t, ca)
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", caPath)
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", clientPEM)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if cfg.RootCAs == nil {
		t.Fatal("expected RootCAs to be set")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}
}

func TestComputeTLSConfig_ClientKeyWithoutCertIgnored(t *testing.T) {
	resetCaches()
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_KEY", "/some/client.key")
	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("key without cert must yield nil config")
	}
}

func TestComputeTLSConfig_SkipVerifyStillLoadsClientCert(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	clientPEM := tlsGenClientCert(t, ca)
	t.Setenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", "true")
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", clientPEM)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil {
		t.Fatal("expected non-nil config")
	}
	if !cfg.InsecureSkipVerify {
		t.Fatal("expected InsecureSkipVerify=true")
	}
	if len(cfg.Certificates) != 1 {
		t.Fatalf("skip-verify must still load client cert, got %d certs", len(cfg.Certificates))
	}
}

func TestComputeTLSConfig_PrefixRemapCA(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	caPath := tlsWriteCA(t, ca)
	t.Setenv("PICOTEL_PREFIX", "PICOTEL")
	t.Setenv("PICOTEL_EXPORTER_OTLP_CERTIFICATE", caPath)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg == nil || cfg.RootCAs == nil {
		t.Fatal("expected RootCAs set via prefix-remapped CA var")
	}
}

func TestComputeTLSConfig_PrefixRemapIgnoresUnprefixedCA(t *testing.T) {
	resetCaches()
	ca := tlsGenCA(t)
	caPath := tlsWriteCA(t, ca)
	t.Setenv("PICOTEL_PREFIX", "PICOTEL")
	// Set the standard name — should be IGNORED under PICOTEL_PREFIX.
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", caPath)

	cfg, err := computeTLSConfig("traces")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg != nil {
		t.Fatal("standard OTEL_ name must be ignored when PICOTEL_PREFIX is set")
	}
}

// ---------------------------------------------------------------------------
// Live round-trip tests
// ---------------------------------------------------------------------------

func TestPostJSON_TLSSkipVerify(t *testing.T) {
	// Use a standard httptest.NewTLSServer (self-signed cert, no custom CA).
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	t.Setenv("PICOTEL_EXPORTER_OTLP_INSECURE_SKIP_VERIFY", "true")
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("expected success with skip-verify, got: %v", err)
	}
}

func TestPostJSON_CustomCA(t *testing.T) {
	ca := tlsGenCA(t)
	certPath, keyPath := tlsGenServerCert(t, ca)
	caPath := tlsWriteCA(t, ca)

	srv := tlsNewServerWithCA(t, ca, certPath, keyPath, false)

	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", caPath)
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("expected success with custom CA, got: %v", err)
	}
}

func TestPostJSON_CustomCA_NoCAEnvFails(t *testing.T) {
	ca := tlsGenCA(t)
	certPath, keyPath := tlsGenServerCert(t, ca)

	srv := tlsNewServerWithCA(t, ca, certPath, keyPath, false)

	// No CA env var — system trust won't know this CA.
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err == nil {
		t.Fatal("expected TLS error without CA env var, got nil")
	}
}

func TestPostJSON_mTLS(t *testing.T) {
	ca := tlsGenCA(t)
	certPath, keyPath := tlsGenServerCert(t, ca)
	clientPEM := tlsGenClientCert(t, ca)
	caPath := tlsWriteCA(t, ca)

	srv := tlsNewServerWithCA(t, ca, certPath, keyPath, true /*requireClientCert*/)

	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", caPath)
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", clientPEM)
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("expected mTLS success, got: %v", err)
	}
}

func TestPostJSON_mTLS_CombinedPEM(t *testing.T) {
	// Combined PEM variant: only CLIENT_CERTIFICATE is set (no CLIENT_KEY),
	// and the file embeds both cert and key.
	ca := tlsGenCA(t)
	certPath, keyPath := tlsGenServerCert(t, ca)
	clientPEM := tlsGenClientCert(t, ca)
	caPath := tlsWriteCA(t, ca)

	srv := tlsNewServerWithCA(t, ca, certPath, keyPath, true)

	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", caPath)
	t.Setenv("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", clientPEM)
	// CLIENT_KEY is intentionally NOT set — combined PEM path.
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("expected mTLS success with combined PEM, got: %v", err)
	}
}

func TestPostJSON_HTTPS_BadCertPathFails(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/does/not/exist/ca.pem")
	resetCaches()

	err := postJSON("https://localhost:4318/v1/traces", map[string]any{"x": "y"}, "traces", time.Second)
	if err == nil {
		t.Fatal("expected error for https with bad cert path")
	}
}

func TestPostJSON_HTTP_BadCertPathSucceeds(t *testing.T) {
	// Scheme gate: bad cert path must not break http:// sends.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	t.Setenv("OTEL_EXPORTER_OTLP_CERTIFICATE", "/does/not/exist/ca.pem")
	resetCaches()

	err := postJSON(srv.URL, map[string]any{"x": "y"}, "traces", 5*time.Second)
	if err != nil {
		t.Fatalf("expected http success despite bad cert path, got: %v", err)
	}
}
