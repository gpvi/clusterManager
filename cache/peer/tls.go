package peer

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// TLSOption specifies the TLS mode for gRPC connections.
type TLSOption int

const (
	// TLSInsecure disables TLS (development only).
	TLSInsecure TLSOption = iota
	// TLSServerOnly enables TLS without client certificate verification.
	TLSServerOnly
	// TLSMutual enables mTLS with client certificate verification.
	TLSMutual
)

// TLSConfig holds the paths for TLS certificates and keys.
type TLSConfig struct {
	Mode     TLSOption
	CertFile string // server certificate
	KeyFile  string // server private key
	CAFile   string // CA certificate for mTLS (empty = skip client verification)
}

// ServerCredentials builds gRPC server transport credentials from TLS config.
func ServerCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	if cfg.Mode == TLSInsecure {
		return insecure.NewCredentials(), nil
	}

	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key: %w", err)
	}

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	if cfg.Mode == TLSMutual && cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsCfg.ClientAuth = tls.RequireAndVerifyClientCert
		tlsCfg.ClientCAs = pool
	}

	return credentials.NewTLS(tlsCfg), nil
}

// ClientCredentials builds gRPC client transport credentials from TLS config.
func ClientCredentials(cfg TLSConfig) (credentials.TransportCredentials, error) {
	if cfg.Mode == TLSInsecure {
		return insecure.NewCredentials(), nil
	}

	tlsCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}

	// If server cert verification is needed (not using a public CA).
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsCfg.RootCAs = pool
	}

	// For mTLS, also present a client certificate.
	if cfg.Mode == TLSMutual && cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client cert/key: %w", err)
		}
		tlsCfg.Certificates = append(tlsCfg.Certificates, cert)
	}

	return credentials.NewTLS(tlsCfg), nil
}
