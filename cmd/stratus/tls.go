package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"strings"
	"time"
)

// tlsOptions is how the HTTPS listener gets its certificate.
type tlsOptions struct {
	certFile string
	keyFile  string
	// hosts are the names and addresses a generated certificate is valid for.
	hosts string
	// certOut is where a generated certificate is written for clients to trust.
	certOut string
}

// certificate loads the configured certificate, or generates a self-signed
// one when none is configured.
func (o tlsOptions) certificate() (tls.Certificate, error) {
	if (o.certFile == "") != (o.keyFile == "") {
		return tls.Certificate{}, fmt.Errorf("-tls-cert and -tls-key must be given together")
	}
	if o.certFile != "" {
		return tls.LoadX509KeyPair(o.certFile, o.keyFile)
	}
	cert, certPEM, err := selfSigned(strings.Split(o.hosts, ","))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generating a certificate: %w", err)
	}
	if o.certOut != "" {
		if err := os.WriteFile(o.certOut, certPEM, 0o644); err != nil { //nolint:gosec // a public certificate
			return tls.Certificate{}, fmt.Errorf("writing the certificate: %w", err)
		}
	}
	return cert, nil
}

// selfSigned makes a certificate that is its own authority, valid for hosts.
func selfSigned(hosts []string) (tls.Certificate, []byte, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "stratus"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(10, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	for _, host := range hosts {
		host = strings.TrimSpace(host)
		if host == "" {
			continue
		}
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
			continue
		}
		template.DNSNames = append(template.DNSNames, host)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, certPEM, nil
}
