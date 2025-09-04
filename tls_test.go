package bimawen

import (
	"testing"
)

func TestTLSConfig(t *testing.T) {
	t.Run("Default TLS Config", func(t *testing.T) {
		config := DefaultTLSConfig()
		
		if config.Enabled {
			t.Error("Default TLS config should be disabled")
		}
		
		if config.InsecureSkipVerify {
			t.Error("Default TLS config should not skip verification")
		}
		
		tlsConfig, err := config.BuildTLSConfig()
		if err != nil {
			t.Errorf("Failed to build default TLS config: %v", err)
		}
		
		if tlsConfig != nil {
			t.Error("Disabled TLS config should return nil")
		}
	})
	
	t.Run("Enabled TLS Config", func(t *testing.T) {
		config := &TLSConfig{
			Enabled:            true,
			InsecureSkipVerify: true,
			ServerName:         "test.example.com",
		}
		
		tlsConfig, err := config.BuildTLSConfig()
		if err != nil {
			t.Errorf("Failed to build TLS config: %v", err)
		}
		
		if tlsConfig == nil {
			t.Error("Enabled TLS config should not return nil")
		}
		
		if !tlsConfig.InsecureSkipVerify {
			t.Error("TLS config should skip verification")
		}
		
		if tlsConfig.ServerName != "test.example.com" {
			t.Errorf("Expected server name 'test.example.com', got '%s'", tlsConfig.ServerName)
		}
	})
}

func TestParseTLSOptionsFromURI(t *testing.T) {
	tests := []struct {
		name     string
		options  map[string]string
		expected *TLSConfig
	}{
		{
			name:    "No TLS options",
			options: map[string]string{},
			expected: &TLSConfig{
				Enabled: false,
			},
		},
		{
			name: "TLS enabled",
			options: map[string]string{
				"tls": "true",
			},
			expected: &TLSConfig{
				Enabled: true,
			},
		},
		{
			name: "TLS with insecure skip verify",
			options: map[string]string{
				"tls":          "true",
				"tls_insecure": "true",
			},
			expected: &TLSConfig{
				Enabled:            true,
				InsecureSkipVerify: true,
			},
		},
		{
			name: "TLS with server name",
			options: map[string]string{
				"tls":             "true",
				"tls_server_name": "rabbitmq.example.com",
			},
			expected: &TLSConfig{
				Enabled:    true,
				ServerName: "rabbitmq.example.com",
			},
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := ParseTLSOptionsFromURI(tt.options)
			
			if config.Enabled != tt.expected.Enabled {
				t.Errorf("Enabled: expected %v, got %v", tt.expected.Enabled, config.Enabled)
			}
			
			if config.InsecureSkipVerify != tt.expected.InsecureSkipVerify {
				t.Errorf("InsecureSkipVerify: expected %v, got %v", tt.expected.InsecureSkipVerify, config.InsecureSkipVerify)
			}
			
			if config.ServerName != tt.expected.ServerName {
				t.Errorf("ServerName: expected %s, got %s", tt.expected.ServerName, config.ServerName)
			}
		})
	}
}

func TestParseURIWithTLS(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		expectTLS   bool
		expectError bool
	}{
		{
			name:      "AMQP without TLS",
			uri:       "amqp://guest:guest@localhost:5672/",
			expectTLS: false,
		},
		{
			name:      "AMQPS with TLS",
			uri:       "amqps://guest:guest@localhost:5671/",
			expectTLS: true,
		},
		{
			name:      "Redis without TLS",
			uri:       "redis://localhost:6379/0",
			expectTLS: false,
		},
		{
			name:      "Redis with TLS",
			uri:       "rediss://localhost:6380/0",
			expectTLS: true,
		},
		{
			name:      "AMQP with explicit TLS option",
			uri:       "amqp://guest:guest@localhost:5672/?tls=true",
			expectTLS: true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info, err := ParseURI(tt.uri)
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}
			
			tlsEnabled := info.Options["tls"] == "true"
			if tlsEnabled != tt.expectTLS {
				t.Errorf("TLS enabled: expected %v, got %v", tt.expectTLS, tlsEnabled)
			}
		})
	}
}

func TestReplaceScheme(t *testing.T) {
	tests := []struct {
		name      string
		url       string
		newScheme string
		expected  string
	}{
		{
			name:      "Replace AMQP with AMQPS",
			url:       "amqp://guest:guest@localhost:5672/",
			newScheme: "amqps",
			expected:  "amqps://guest:guest@localhost:5672/",
		},
		{
			name:      "Replace Redis with Rediss",
			url:       "redis://localhost:6379/0",
			newScheme: "rediss",
			expected:  "rediss://localhost:6379/0",
		},
		{
			name:      "URL without scheme",
			url:       "localhost:5672",
			newScheme: "amqps",
			expected:  "localhost:5672",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := replaceScheme(tt.url, tt.newScheme)
			if result != tt.expected {
				t.Errorf("Expected %s, got %s", tt.expected, result)
			}
		})
	}
}