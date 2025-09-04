package bimawen

import (
	"testing"
)

func TestRabbitMQExchangeConfiguration(t *testing.T) {
	// Test that ProducerOptions can be configured with RabbitMQ exchange options
	opts := DefaultProducerOptions()
	opts.DriverOptions = make(map[string]interface{})
	opts.DriverOptions[RabbitMQExchange] = "test-exchange"
	opts.DriverOptions[RabbitMQMandatory] = true
	opts.DriverOptions[RabbitMQImmediate] = false

	// Verify the values can be retrieved
	if exchange := opts.DriverOptions[RabbitMQExchange]; exchange != "test-exchange" {
		t.Errorf("Expected exchange 'test-exchange', got %v", exchange)
	}

	if mandatory := opts.DriverOptions[RabbitMQMandatory]; mandatory != true {
		t.Errorf("Expected mandatory true, got %v", mandatory)
	}

	if immediate := opts.DriverOptions[RabbitMQImmediate]; immediate != false {
		t.Errorf("Expected immediate false, got %v", immediate)
	}

	// Test constants
	if RabbitMQExchange != "exchange" {
		t.Errorf("Expected RabbitMQExchange constant to be 'exchange', got %s", RabbitMQExchange)
	}

	if RabbitMQMandatory != "mandatory" {
		t.Errorf("Expected RabbitMQMandatory constant to be 'mandatory', got %s", RabbitMQMandatory)
	}

	if RabbitMQImmediate != "immediate" {
		t.Errorf("Expected RabbitMQImmediate constant to be 'immediate', got %s", RabbitMQImmediate)
	}
}

func TestRabbitMQProducerExchangeConfiguration(t *testing.T) {
	// Skip if RabbitMQ is not available
	uri := "amqp://guest:guest@localhost:5672/"
	info, err := ParseURI(uri)
	if err != nil {
		t.Skipf("Skipping RabbitMQ exchange configuration test: %v", err)
	}

	driver, err := NewRabbitMQDriver(info)
	if err != nil {
		t.Skipf("Skipping RabbitMQ exchange configuration test - RabbitMQ not available: %v", err)
	}
	defer driver.Close()

	// Test producer with exchange configuration
	opts := DefaultProducerOptions()
	opts.DriverOptions = map[string]interface{}{
		RabbitMQExchange:  "test-exchange",
		RabbitMQMandatory: false,
		RabbitMQImmediate: false,
	}

	producer, err := driver.NewProducer(opts)
	if err != nil {
		t.Fatalf("Failed to create RabbitMQ producer with exchange config: %v", err)
	}
	defer producer.Close()

	// Verify the producer was created successfully
	if producer == nil {
		t.Error("Producer should not be nil")
	}

	// The exchange configuration is tested through actual usage in integration tests
	// This test verifies that the configuration can be set without errors
	t.Log("RabbitMQ producer created successfully with exchange configuration")
}