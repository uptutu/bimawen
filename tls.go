package bimawen

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig contains TLS/SSL configuration
type TLSConfig struct {
	// Enable TLS/SSL
	Enabled bool
	
	// InsecureSkipVerify controls whether a client verifies the server's certificate chain and host name
	InsecureSkipVerify bool
	
	// CACert is the path to the CA certificate file
	CACertFile string
	
	// ClientCert is the path to the client certificate file
	ClientCertFile string
	
	// ClientKey is the path to the client private key file
	ClientKeyFile string
	
	// ServerName is used to verify the hostname on the returned certificates
	ServerName string
	
	// MinVersion contains the minimum SSL/TLS version that is acceptable
	MinVersion uint16
	
	// MaxVersion contains the maximum SSL/TLS version that is acceptable
	MaxVersion uint16
}

// DefaultTLSConfig returns a default TLS configuration
func DefaultTLSConfig() *TLSConfig {
	return &TLSConfig{
		Enabled:            false,
		InsecureSkipVerify: false,
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS13,
	}
}

// BuildTLSConfig builds a Go TLS config from the TLSConfig
func (tc *TLSConfig) BuildTLSConfig() (*tls.Config, error) {
	if !tc.Enabled {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		InsecureSkipVerify: tc.InsecureSkipVerify,
		ServerName:         tc.ServerName,
		MinVersion:         tc.MinVersion,
		MaxVersion:         tc.MaxVersion,
	}

	// Load CA certificate if specified
	if tc.CACertFile != "" {
		caCert, err := os.ReadFile(tc.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA certificate: %w", err)
		}

		caCertPool := x509.NewCertPool()
		if !caCertPool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate")
		}
		tlsConfig.RootCAs = caCertPool
	}

	// Load client certificate if specified
	if tc.ClientCertFile != "" && tc.ClientKeyFile != "" {
		clientCert, err := tls.LoadX509KeyPair(tc.ClientCertFile, tc.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{clientCert}
	}

	return tlsConfig, nil
}

// ParseTLSOptionsFromURI parses TLS options from URI query parameters
func ParseTLSOptionsFromURI(options map[string]string) *TLSConfig {
	config := DefaultTLSConfig()

	// Check if TLS is enabled based on scheme or explicit option
	if enabled, ok := options["tls"]; ok && (enabled == "true" || enabled == "1") {
		config.Enabled = true
	}

	if skipVerify, ok := options["tls_insecure"]; ok && (skipVerify == "true" || skipVerify == "1") {
		config.InsecureSkipVerify = true
	}

	if caCert, ok := options["tls_ca_cert"]; ok {
		config.CACertFile = caCert
	}

	if clientCert, ok := options["tls_client_cert"]; ok {
		config.ClientCertFile = clientCert
	}

	if clientKey, ok := options["tls_client_key"]; ok {
		config.ClientKeyFile = clientKey
	}

	if serverName, ok := options["tls_server_name"]; ok {
		config.ServerName = serverName
	}

	return config
}