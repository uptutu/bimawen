package bimawen

import (
	"context"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestPublisherConfirms(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping publisher confirms test in short mode")
	}

	// Test URI parsing with confirms option
	info, err := ParseURI("amqp://guest:guest@localhost:5672/?confirms=true")
	if err != nil {
		t.Skipf("Skipping test - failed to parse URI: %v", err)
	}

	driver, err := NewRabbitMQDriver(info)
	if err != nil {
		t.Skipf("Skipping test - RabbitMQ not available: %v", err)
	}
	defer driver.Close()

	// Create producer with confirms enabled
	producerOpts := DefaultProducerOptions()
	producerOpts.EnableConfirms = true
	producer, err := driver.NewProducer(producerOpts)
	if err != nil {
		t.Errorf("Failed to create producer with confirms: %v", err)
	}
	defer producer.Close()

	// Test sending a message
	message := &Message{
		ID:        "test-confirm-msg",
		Body:      []byte("test message with publisher confirms"),
		Headers:   map[string]interface{}{"test": "confirms"},
		Timestamp: time.Now(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	err = producer.Send(ctx, "test-confirms-queue", message)
	if err != nil {
		t.Errorf("Failed to send message with confirms: %v", err)
	}

	// Test async send
	errChan := producer.SendAsync(ctx, "test-confirms-queue", message)
	if err := <-errChan; err != nil {
		t.Errorf("Failed to send async message with confirms: %v", err)
	}
}

func TestConfirmChannelBasic(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping confirm channel test in short mode")
	}

	// This test requires a real RabbitMQ connection
	// Skip if not available
	info, err := ParseURI("amqp://guest:guest@localhost:5672/")
	if err != nil {
		t.Skipf("Skipping test - failed to parse URI: %v", err)
	}

	driver, err := NewRabbitMQDriver(info)
	if err != nil {
		t.Skipf("Skipping test - RabbitMQ not available: %v", err)
	}
	defer driver.Close()

	// Get a channel from the pool
	ch, err := driver.pool.GetChannel()
	if err != nil {
		t.Errorf("Failed to get channel: %v", err)
	}
	defer driver.pool.ReturnChannel(ch)

	// Create confirm channel
	cc, err := NewConfirmChannel(ch)
	if err != nil {
		t.Errorf("Failed to create confirm channel: %v", err)
	}
	defer cc.Close()

	// Test publish with confirm
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Declare a test queue
	_, err = ch.QueueDeclare("test-confirm-channel", true, false, false, false, nil)
	if err != nil {
		t.Errorf("Failed to declare queue: %v", err)
	}

	// Publish message with confirmation
	confirm, err := cc.PublishWithConfirm(ctx, "", "test-confirm-channel", false, false, amqp.Publishing{
		Body: []byte("test message"),
	})

	if err != nil {
		t.Errorf("Failed to publish with confirm: %v", err)
	}

	if confirm == nil {
		t.Error("Expected confirmation but got nil")
	} else if !confirm.Ack {
		t.Errorf("Message was nacked: %v", confirm.Error)
	}
}

func TestProducerOptionsWithConfirms(t *testing.T) {
	opts := DefaultProducerOptions()
	
	// Test default state
	if opts.EnableConfirms {
		t.Error("Default producer options should not have confirms enabled")
	}
	
	// Test enabling confirms
	opts.EnableConfirms = true
	if !opts.EnableConfirms {
		t.Error("Failed to enable publisher confirms")
	}
}