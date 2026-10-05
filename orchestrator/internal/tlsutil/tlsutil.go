// Package tlsutil generates and configures Trellis mutual TLS materials.
package tlsutil

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
	"net/url"
	"time"

	"github.com/google/uuid"
)

// ServerName is the DNS identity used by the Trellis API.
const ServerName = "trellis"

const nodeIdentityScheme = "trellis-node"

// APICertificateLifetime is the validity period of a certificate used to
// serve the Trellis API.
const APICertificateLifetime = 24 * time.Hour

// NodeServerName returns the unique DNS identity of a node's agent endpoint.
func NodeServerName(id uuid.UUID) string {
	return id.String() + ".node.trellis"
}

// Materials contains a CA and node certificate key pair.
type Materials struct {
	CACert  []byte
	CAKey   []byte
	Cert    []byte
	Key     []byte
	APICert []byte
	APIKey  []byte
}

// GenerateCA generates a self-signed cluster certificate authority.
func GenerateCA() (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate CA key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, fmt.Errorf("generate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{Organization: []string{"Trellis Cluster"}},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(10 * 365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		MaxPathLen:            0,
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create CA certificate: %w", err)
	}
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal CA key: %w", err)
	}
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

// GenerateNodeCert generates a node certificate signed by the cluster CA.
// extraSANs is retained for source compatibility but deliberately ignored:
// node identities may only claim their UUID-derived DNS name.
func GenerateNodeCert(caCertPEM, caKeyPEM []byte, nodeID uuid.UUID, extraSANs ...string) (certPEM, keyPEM []byte, err error) {
	if nodeID == uuid.Nil {
		return nil, nil, fmt.Errorf("node ID is required")
	}
	_ = extraSANs
	csrPEM, keyPEM, err := GenerateCSR()
	if err != nil {
		return nil, nil, err
	}
	certPEM, err = SignNodeCSR(caCertPEM, caKeyPEM, csrPEM, nodeID)
	return certPEM, keyPEM, err
}

// GenerateCSR creates a name-free signing request with a fresh P-256 key.
func GenerateCSR() (csrPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate certificate key: %w", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return nil, nil, fmt.Errorf("create certificate request: %w", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, fmt.Errorf("marshal certificate key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}), pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}

// SignNodeCSR signs only a request's public key and assigns the requested node
// identity. Names and extensions supplied by the request are ignored.
func SignNodeCSR(caCertPEM, caKeyPEM, csrPEM []byte, nodeID uuid.UUID) ([]byte, error) {
	if nodeID == uuid.Nil {
		return nil, fmt.Errorf("node ID is required")
	}
	csr, err := parseCSR(csrPEM)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"Trellis Node"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(5 * 365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{NodeServerName(nodeID)},
		URIs:         []*url.URL{{Scheme: nodeIdentityScheme, Opaque: nodeID.String()}},
	}
	return signCertificate(caCertPEM, caKeyPEM, template, csr.PublicKey)
}

// SignAPICSR signs only a request's public key for serving the Trellis API.
// Names and extensions supplied by the request are ignored.
func SignAPICSR(caCertPEM, caKeyPEM, csrPEM []byte) ([]byte, error) {
	csr, err := parseCSR(csrPEM)
	if err != nil {
		return nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"Trellis API"}},
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(-time.Minute).Add(APICertificateLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{ServerName},
	}
	return signCertificate(caCertPEM, caKeyPEM, template, csr.PublicKey)
}

// GenerateAPICert creates a fresh key and a short-lived API certificate.
func GenerateAPICert(caCertPEM, caKeyPEM []byte) (certPEM, keyPEM []byte, err error) {
	csrPEM, keyPEM, err := GenerateCSR()
	if err != nil {
		return nil, nil, err
	}
	certPEM, err = SignAPICSR(caCertPEM, caKeyPEM, csrPEM)
	return certPEM, keyPEM, err
}

func parseCSR(csrPEM []byte) (*x509.CertificateRequest, error) {
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		return nil, fmt.Errorf("decode certificate request PEM")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse certificate request: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("verify certificate request signature: %w", err)
	}
	return csr, nil
}

func signCertificate(caCertPEM, caKeyPEM []byte, template *x509.Certificate, publicKey any) ([]byte, error) {
	caBlock, _ := pem.Decode(caCertPEM)
	if caBlock == nil {
		return nil, fmt.Errorf("decode CA certificate PEM")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA certificate: %w", err)
	}
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		return nil, fmt.Errorf("decode CA key PEM")
	}
	caKey, err := x509.ParseECPrivateKey(caKeyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA key: %w", err)
	}
	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, publicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create certificate: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), nil
}

// NodeID returns the immutable node identity encoded in a node certificate.
func NodeID(cert *x509.Certificate) (uuid.UUID, error) {
	if cert == nil {
		return uuid.Nil, fmt.Errorf("node certificate is required")
	}
	for _, uri := range cert.URIs {
		if uri.Scheme != nodeIdentityScheme {
			continue
		}
		id, err := uuid.Parse(uri.Opaque)
		if err != nil || id == uuid.Nil {
			return uuid.Nil, fmt.Errorf("invalid node identity URI")
		}
		return id, nil
	}
	return uuid.Nil, fmt.Errorf("certificate does not contain a Trellis node identity")
}

// ValidateMaterials verifies that the node key pair chains to the configured
// CA and identifies the expected immutable node ID.
func ValidateMaterials(m *Materials, expectedNodeID uuid.UUID) error {
	cert, pool, err := buildCertAndPool(m)
	if err != nil {
		return err
	}
	if len(cert.Certificate) == 0 {
		return fmt.Errorf("node certificate chain is empty")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return fmt.Errorf("parse node certificate: %w", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}); err != nil {
		return fmt.Errorf("verify node certificate: %w", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool, DNSName: NodeServerName(expectedNodeID), KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}); err != nil {
		return fmt.Errorf("verify node server certificate: %w", err)
	}
	id, err := NodeID(leaf)
	if err != nil {
		return err
	}
	if id != expectedNodeID {
		return fmt.Errorf("node certificate identifies %s, expected %s", id, expectedNodeID)
	}
	return nil
}

func buildCertAndPool(m *Materials) (tls.Certificate, *x509.CertPool, error) {
	cert, err := tls.X509KeyPair(m.Cert, m.Key)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load key pair: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(m.CACert) {
		return tls.Certificate{}, nil, fmt.Errorf("append CA certificate to pool")
	}
	return cert, pool, nil
}

// ServerTLSConfig creates a mutually authenticated server configuration.
func ServerTLSConfig(m *Materials) (*tls.Config, error) {
	cert, pool, err := buildCertAndPool(m)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// LeaderTLSConfig creates a server configuration that permits enrollment
// clients authenticated by a bearer credential over pinned server TLS.
func LeaderTLSConfig(m *Materials) (*tls.Config, error) {
	cert, err := tls.X509KeyPair(m.APICert, m.APIKey)
	if err != nil {
		return nil, fmt.Errorf("load API key pair: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(m.CACert) {
		return nil, fmt.Errorf("append CA certificate to pool")
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.VerifyClientCertIfGiven,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ClientTLSConfig creates a mutually authenticated client configuration.
func ClientTLSConfig(m *Materials) (*tls.Config, error) {
	cert, pool, err := buildCertAndPool(m)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   ServerName,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// PeerTLSConfig creates a configuration suitable for both peer client and server use.
func PeerTLSConfig(m *Materials) (*tls.Config, error) {
	cert, pool, err := buildCertAndPool(m)
	if err != nil {
		return nil, err
	}
	config := &tls.Config{
		Certificates:       []tls.Certificate{cert},
		ClientAuth:         tls.RequireAndVerifyClientCert,
		ClientCAs:          pool,
		RootCAs:            pool,
		InsecureSkipVerify: true, //nolint:gosec // VerifyConnection verifies chain and URI; Raft Dial separately pins the target UUID and DNS name.
		MinVersion:         tls.VersionTLS13,
	}
	config.VerifyConnection = func(state tls.ConnectionState) error {
		if len(state.PeerCertificates) == 0 {
			return fmt.Errorf("peer certificate is missing")
		}
		intermediates := x509.NewCertPool()
		for _, certificate := range state.PeerCertificates[1:] {
			intermediates.AddCert(certificate)
		}
		if _, err := state.PeerCertificates[0].Verify(x509.VerifyOptions{Roots: pool, Intermediates: intermediates, KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageAny}}); err != nil {
			return fmt.Errorf("verify peer certificate: %w", err)
		}
		if _, err := NodeID(state.PeerCertificates[0]); err != nil {
			return fmt.Errorf("verify peer identity: %w", err)
		}
		return nil
	}
	return config, nil
}

// CAClientTLSConfig creates a client configuration using only the cluster CA.
func CAClientTLSConfig(caCertPEM []byte) (*tls.Config, error) {
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caCertPEM) {
		return nil, fmt.Errorf("append CA certificate to pool")
	}
	return &tls.Config{
		RootCAs:    pool,
		ServerName: ServerName,
		MinVersion: tls.VersionTLS13,
	}, nil
}
