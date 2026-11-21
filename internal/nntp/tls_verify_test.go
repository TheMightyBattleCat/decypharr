package nntp

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
)

// testCert makes a certificate for the given IPs and DNS names, signed by
// parent (self-signed when parent is nil). It returns the tls pair and the
// parsed certificate.
func testCert(t *testing.T, parent *x509.Certificate, parentKey *ecdsa.PrivateKey, isCA bool, ips []net.IP, names []string) (tls.Certificate, *x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "decypharr test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           ips,
		DNSNames:              names,
		BasicConstraintsValid: true,
		IsCA:                  isCA,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	signer, signerKey := tmpl, key
	if parent != nil {
		signer, signerKey = parent, parentKey
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, cert, key
}

// startTLSGreeter serves NNTP over TLS with cert: a 200 greeting, then it
// reads and ignores commands. It returns the port.
func startTLSGreeter(t *testing.T, cert tls.Certificate) int {
	t.Helper()
	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{Certificates: []tls.Certificate{cert}})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
				if _, err := conn.Write([]byte("200 test server ready\r\n")); err != nil {
					return
				}
				r := bufio.NewReader(conn)
				for {
					if _, err := r.ReadString('\n'); err != nil {
						return
					}
				}
			}()
		}
	}()
	return ln.Addr().(*net.TCPAddr).Port
}

func dialTLSProvider(t *testing.T, p config.UsenetProvider) (*Client, error) {
	t.Helper()
	c := newStatTestClient(t, []config.UsenetProvider{p}, 100)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := c.createConnection(ctx, p)
	if conn != nil {
		_ = conn.Close()
	}
	return c, err
}

func isCertError(err error) bool {
	var certErr *tls.CertificateVerificationError
	return errors.As(err, &certErr)
}

// An SSL provider's certificate is verified against its host name: a
// self-signed one fails the dial unless the provider has tls_skip_verify.
func TestSSLProviderVerifiesCertificate(t *testing.T) {
	loopback := []net.IP{net.ParseIP("127.0.0.1")}
	selfSigned, _, _ := testCert(t, nil, nil, false, loopback, nil)
	port := startTLSGreeter(t, selfSigned)
	p := config.UsenetProvider{Host: "127.0.0.1", Port: port, SSL: true, MaxConnections: 2}

	c, err := dialTLSProvider(t, p)
	if err == nil {
		t.Fatal("dial to a self-signed certificate succeeded with verification on")
	}
	if !isCertError(err) {
		t.Fatalf("dial failed, but not on the certificate: %v", err)
	}
	if !c.pools[p.Host].certWarned.Load() {
		t.Error("certificate failure not noted for the once-per-provider log")
	}

	p.TLSSkipVerify = true
	if _, err := dialTLSProvider(t, p); err != nil {
		t.Fatalf("dial with tls_skip_verify failed: %v", err)
	}
}

// A certificate signed by a trusted CA verifies when it names the host, and
// fails when it names another host.
func TestSSLProviderTrustedCertificate(t *testing.T) {
	_, caCert, caKey := testCert(t, nil, nil, true, nil, nil)
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	old := tlsRootCAs
	tlsRootCAs = pool
	t.Cleanup(func() { tlsRootCAs = old })

	good, _, _ := testCert(t, caCert, caKey, false, []net.IP{net.ParseIP("127.0.0.1")}, nil)
	p := config.UsenetProvider{Host: "127.0.0.1", Port: startTLSGreeter(t, good), SSL: true, MaxConnections: 2}
	if _, err := dialTLSProvider(t, p); err != nil {
		t.Fatalf("dial to a CA-signed certificate for the host failed: %v", err)
	}

	other, _, _ := testCert(t, caCert, caKey, false, nil, []string{"news.example"})
	p.Port = startTLSGreeter(t, other)
	_, err := dialTLSProvider(t, p)
	if err == nil {
		t.Fatal("dial to a certificate for another host name succeeded")
	}
	if !isCertError(err) {
		t.Fatalf("dial failed, but not on the certificate: %v", err)
	}
}
