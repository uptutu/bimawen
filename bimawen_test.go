package bimawen

import (
	"testing"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		expected *URIInfo
		hasError bool
	}{
		{
			name: "RabbitMQ URI",
			uri:  "amqp://guest:password@localhost:5672/vhost",
			expected: &URIInfo{
				DriverType: DriverRabbitMQ,
				Host:       "localhost",
				Port:       5672,
				Username:   "guest",
				Password:   "password",
				VHost:      "/vhost",
				Options:    make(map[string]string),
			},
		},
		{
			name: "Redis URI",
			uri:  "redis://user:pass@redis-host:6379/2",
			expected: &URIInfo{
				DriverType: DriverRedis,
				Host:       "redis-host",
				Port:       6379,
				Username:   "user",
				Password:   "pass",
				Database:   2,
				Options:    make(map[string]string),
			},
		},
		{
			name: "RabbitMQ with default port",
			uri:  "amqp://guest:password@localhost/",
			expected: &URIInfo{
				DriverType: DriverRabbitMQ,
				Host:       "localhost",
				Port:       5672,
				Username:   "guest",
				Password:   "password",
				VHost:      "/",
				Options:    make(map[string]string),
			},
		},
		{
			name: "Redis with default port and database",
			uri:  "redis://localhost",
			expected: &URIInfo{
				DriverType: DriverRedis,
				Host:       "localhost",
				Port:       6379,
				Database:   0,
				Options:    make(map[string]string),
			},
		},
		{
			name:     "Invalid scheme",
			uri:      "mysql://localhost:3306",
			hasError: true,
		},
		{
			name:     "Empty URI",
			uri:      "",
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := ParseURI(tt.uri)

			if tt.hasError {
				if err == nil {
					t.Errorf("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			if result.DriverType != tt.expected.DriverType {
				t.Errorf("DriverType = %v, want %v", result.DriverType, tt.expected.DriverType)
			}

			if result.Host != tt.expected.Host {
				t.Errorf("Host = %v, want %v", result.Host, tt.expected.Host)
			}

			if result.Port != tt.expected.Port {
				t.Errorf("Port = %v, want %v", result.Port, tt.expected.Port)
			}

			if result.Username != tt.expected.Username {
				t.Errorf("Username = %v, want %v", result.Username, tt.expected.Username)
			}

			if result.Password != tt.expected.Password {
				t.Errorf("Password = %v, want %v", result.Password, tt.expected.Password)
			}

			if result.DriverType == DriverRabbitMQ && result.VHost != tt.expected.VHost {
				t.Errorf("VHost = %v, want %v", result.VHost, tt.expected.VHost)
			}

			if result.DriverType == DriverRedis && result.Database != tt.expected.Database {
				t.Errorf("Database = %v, want %v", result.Database, tt.expected.Database)
			}
		})
	}
}

func TestJSONSerializer(t *testing.T) {
	serializer := NewJSONSerializer()

	// Test data
	data := map[string]interface{}{
		"id":      "test-123",
		"message": "hello world",
		"count":   42,
	}

	// Test serialization
	serialized, err := serializer.Serialize(data)
	if err != nil {
		t.Errorf("Serialization failed: %v", err)
	}

	// Test deserialization
	var result map[string]interface{}
	err = serializer.Deserialize(serialized, &result)
	if err != nil {
		t.Errorf("Deserialization failed: %v", err)
	}

	// Check content type
	if serializer.ContentType() != "application/json" {
		t.Errorf("ContentType = %v, want application/json", serializer.ContentType())
	}

	// Verify data integrity
	if result["id"] != data["id"] {
		t.Errorf("ID = %v, want %v", result["id"], data["id"])
	}

	if result["message"] != data["message"] {
		t.Errorf("Message = %v, want %v", result["message"], data["message"])
	}

	// JSON numbers are float64 by default
	if result["count"] != float64(42) {
		t.Errorf("Count = %v, want %v", result["count"], float64(42))
	}
}

func TestRetryConfig(t *testing.T) {
	config := DefaultRetryConfig()

	if config.MaxRetries != 3 {
		t.Errorf("MaxRetries = %v, want 3", config.MaxRetries)
	}

	if config.Multiplier != 2.0 {
		t.Errorf("Multiplier = %v, want 2.0", config.Multiplier)
	}
}

func TestSerializerRegistry(t *testing.T) {
	registry := NewSerializerRegistry()

	// Test default JSON serializer
	jsonSerializer, err := registry.Get("json")
	if err != nil {
		t.Errorf("Failed to get JSON serializer: %v", err)
	}

	if jsonSerializer.ContentType() != "application/json" {
		t.Errorf("ContentType = %v, want application/json", jsonSerializer.ContentType())
	}

	// Test default serializer
	defaultSerializer := registry.GetDefault()
	if defaultSerializer.ContentType() != "application/json" {
		t.Errorf("Default serializer ContentType = %v, want application/json", defaultSerializer.ContentType())
	}

	// Test non-existent serializer
	_, err = registry.Get("xml")
	if err == nil {
		t.Errorf("Expected error for non-existent serializer")
	}
}