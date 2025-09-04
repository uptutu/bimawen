package bimawen

import (
	"testing"
)

func TestParseURIRedisStreams(t *testing.T) {
	// Test basic Redis Streams URI
	info, err := ParseURI("redis-streams://localhost:6379/0")
	if err != nil {
		t.Fatalf("Failed to parse Redis Streams URI: %v", err)
	}
	
	if info.DriverType != DriverRedisStreams {
		t.Errorf("Expected driver type %s, got %s", DriverRedisStreams, info.DriverType)
	}
	if info.Host != "localhost" {
		t.Errorf("Expected host localhost, got %s", info.Host)
	}
	if info.Port != 6379 {
		t.Errorf("Expected port 6379, got %d", info.Port)
	}
	if info.Database != 0 {
		t.Errorf("Expected database 0, got %d", info.Database)
	}

	// Test Redis Streams with TLS
	info, err = ParseURI("rediss-streams://user:pass@redis.example.com:6380/5?timeout=30s")
	if err != nil {
		t.Fatalf("Failed to parse Redis Streams TLS URI: %v", err)
	}
	
	if info.DriverType != DriverRedisStreams {
		t.Errorf("Expected driver type %s, got %s", DriverRedisStreams, info.DriverType)
	}
	if info.Host != "redis.example.com" {
		t.Errorf("Expected host redis.example.com, got %s", info.Host)
	}
	if info.Port != 6380 {
		t.Errorf("Expected port 6380, got %d", info.Port)
	}
	if info.Username != "user" {
		t.Errorf("Expected username user, got %s", info.Username)
	}
	if info.Password != "pass" {
		t.Errorf("Expected password pass, got %s", info.Password)
	}
	if info.Database != 5 {
		t.Errorf("Expected database 5, got %d", info.Database)
	}
	if info.Options["tls"] != "true" {
		t.Errorf("Expected TLS option to be true, got %s", info.Options["tls"])
	}
	if info.Options["timeout"] != "30s" {
		t.Errorf("Expected timeout option to be 30s, got %s", info.Options["timeout"])
	}
}

func TestFactoryCreateRedisStreams(t *testing.T) {
	// Test creating Redis Streams driver through factory
	driver, err := DefaultDriverFactory.Create("redis-streams://localhost:6379/1")
	if err != nil {
		t.Skipf("Redis not available for testing: %v", err)
	}
	defer driver.Close()
	
	// Verify it's the right type of driver
	_, ok := driver.(*RedisStreamsDriver)
	if !ok {
		t.Errorf("Expected RedisStreamsDriver, got %T", driver)
	}
}