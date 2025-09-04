package bimawen

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestHeaderFilter(t *testing.T) {
	// Create test message
	message := &Message{
		ID:   "test-msg-1",
		Body: []byte("test body"),
		Headers: map[string]interface{}{
			"type":     "order",
			"priority": "high",
			"source":   "web",
			"count":    42,
		},
		Timestamp: time.Now(),
	}

	// Test exact match filter
	filter := NewHeaderFilter("type", "order")
	if !filter.Match(message) {
		t.Error("Expected header filter to match 'type' = 'order'")
	}

	// Test non-matching filter
	filter2 := NewHeaderFilter("type", "payment")
	if filter2.Match(message) {
		t.Error("Expected header filter to not match 'type' = 'payment'")
	}

	// Test numeric header filter
	filter3 := NewHeaderFilter("count", 42)
	if !filter3.Match(message) {
		t.Error("Expected header filter to match 'count' = 42")
	}

	// Test regex filter
	regexFilter, err := NewHeaderRegexFilter("source", "w.*")
	if err != nil {
		t.Fatalf("Failed to create regex filter: %v", err)
	}

	if !regexFilter.Match(message) {
		t.Error("Expected regex filter to match 'source' with pattern 'w.*'")
	}

	// Test non-existent header
	filter4 := NewHeaderFilter("nonexistent", "value")
	if filter4.Match(message) {
		t.Error("Expected filter to not match non-existent header")
	}
}

func TestContentFilter(t *testing.T) {
	message := &Message{
		ID:        "test-msg-2",
		Body:      []byte("This is a test message with order information"),
		Timestamp: time.Now(),
	}

	// Test exact string match
	filter := NewContentFilter("order information")
	if !filter.Match(message) {
		t.Error("Expected content filter to match 'order information'")
	}

	// Test non-matching string
	filter2 := NewContentFilter("payment details")
	if filter2.Match(message) {
		t.Error("Expected content filter to not match 'payment details'")
	}

	// Test regex filter
	regexFilter, err := NewContentRegexFilter("test.*order")
	if err != nil {
		t.Fatalf("Failed to create regex content filter: %v", err)
	}

	if !regexFilter.Match(message) {
		t.Error("Expected regex content filter to match pattern 'test.*order'")
	}
}

func TestPriorityFilter(t *testing.T) {
	message := &Message{
		ID:        "test-msg-3",
		Body:      []byte("priority test"),
		Priority:  5,
		Timestamp: time.Now(),
	}

	// Test priority range filter
	filter := NewPriorityFilter(3, 7)
	if !filter.Match(message) {
		t.Error("Expected priority filter to match priority 5 in range [3,7]")
	}

	// Test out-of-range priority
	filter2 := NewPriorityFilter(8, 10)
	if filter2.Match(message) {
		t.Error("Expected priority filter to not match priority 5 for range [8,10]")
	}

	// Test exact priority match
	filter3 := NewPriorityFilter(5, 5)
	if !filter3.Match(message) {
		t.Error("Expected priority filter to match exact priority 5")
	}
}

func TestCompositeFilter(t *testing.T) {
	message := &Message{
		ID:       "test-msg-4",
		Body:     []byte("urgent order processing"),
		Priority: 8,
		Headers: map[string]interface{}{
			"type":   "order",
			"status": "urgent",
		},
		Timestamp: time.Now(),
	}

	// Create individual filters
	headerFilter := NewHeaderFilter("type", "order")
	contentFilter := NewContentFilter("urgent")
	priorityFilter := NewPriorityFilter(7, 10)

	// Test AND composite filter
	andFilter := NewCompositeFilter(FilterAND, headerFilter, contentFilter, priorityFilter)
	if !andFilter.Match(message) {
		t.Error("Expected AND composite filter to match message")
	}

	// Test OR composite filter with one non-matching filter
	nonMatchingHeader := NewHeaderFilter("type", "payment")
	orFilter := NewCompositeFilter(FilterOR, headerFilter, nonMatchingHeader)
	if !orFilter.Match(message) {
		t.Error("Expected OR composite filter to match message")
	}

	// Test NOT filter
	notFilter := NewCompositeFilter(FilterNOT, nonMatchingHeader)
	if !notFilter.Match(message) {
		t.Error("Expected NOT composite filter to match message")
	}

	// Test AND filter with one non-matching filter
	andFilter2 := NewCompositeFilter(FilterAND, headerFilter, nonMatchingHeader)
	if andFilter2.Match(message) {
		t.Error("Expected AND composite filter to not match message")
	}
}

func TestFilterRegistry(t *testing.T) {
	registry := NewFilterRegistry()

	// Test registration
	filter1 := NewHeaderFilter("type", "order")
	registry.Register("order-filter", filter1)

	// Test retrieval
	retrieved, exists := registry.Get("order-filter")
	if !exists {
		t.Error("Expected to find registered filter")
	}

	if retrieved.String() != filter1.String() {
		t.Error("Retrieved filter doesn't match registered filter")
	}

	// Test list
	names := registry.List()
	if len(names) != 1 || names[0] != "order-filter" {
		t.Error("Expected to find one registered filter named 'order-filter'")
	}

	// Test non-existent filter
	_, exists = registry.Get("nonexistent")
	if exists {
		t.Error("Expected to not find non-existent filter")
	}

	// Test clear
	registry.Clear()
	names = registry.List()
	if len(names) != 0 {
		t.Error("Expected no filters after clear")
	}
}

func TestRoutingTable(t *testing.T) {
	table := NewRoutingTable()

	// Create test routes
	route1 := Route{
		Name:        "orders-to-processing",
		Source:      "orders.*",
		Destination: "order-processing",
		Filter:      NewHeaderFilter("type", "order"),
		Enabled:     true,
		Priority:    10,
	}

	route2 := Route{
		Name:        "urgent-to-priority",
		Source:      "*",
		Destination: "priority-queue",
		Filter:      NewPriorityFilter(8, 10),
		Enabled:     true,
		Priority:    20,
	}

	// Test adding routes
	err := table.AddRoute(route1)
	if err != nil {
		t.Fatalf("Failed to add route1: %v", err)
	}

	err = table.AddRoute(route2)
	if err != nil {
		t.Fatalf("Failed to add route2: %v", err)
	}

	// Test duplicate route name
	err = table.AddRoute(route1)
	if err == nil {
		t.Error("Expected error when adding duplicate route name")
	}

	// Test listing routes (should be sorted by priority)
	routes := table.ListRoutes()
	if len(routes) != 2 {
		t.Fatalf("Expected 2 routes, got %d", len(routes))
	}

	// Higher priority route should be first
	if routes[0].Name != "urgent-to-priority" {
		t.Error("Expected higher priority route to be first")
	}

	// Test getting route
	retrieved, err := table.GetRoute("orders-to-processing")
	if err != nil {
		t.Fatalf("Failed to get route: %v", err)
	}

	if retrieved.Name != route1.Name {
		t.Error("Retrieved route doesn't match expected")
	}

	// Test removing route
	err = table.RemoveRoute("orders-to-processing")
	if err != nil {
		t.Fatalf("Failed to remove route: %v", err)
	}

	routes = table.ListRoutes()
	if len(routes) != 1 {
		t.Error("Expected 1 route after removal")
	}
}

func TestRoutingTableMatching(t *testing.T) {
	table := NewRoutingTable()

	// Add test routes
	route1 := Route{
		Name:        "order-route",
		Source:      "orders.*",
		Destination: "order-processing",
		Filter:      NewHeaderFilter("type", "order"),
		Enabled:     true,
		Priority:    10,
	}

	route2 := Route{
		Name:        "priority-route",
		Source:      "*",
		Destination: "priority-queue",
		Filter:      NewPriorityFilter(8, 10),
		Enabled:     true,
		Priority:    20,
	}

	route3 := Route{
		Name:        "disabled-route",
		Source:      "*",
		Destination: "disabled-queue",
		Filter:      nil,
		Enabled:     false, // Disabled
		Priority:    15,
	}

	table.AddRoute(route1)
	table.AddRoute(route2)
	table.AddRoute(route3)

	// Test message that matches both enabled routes
	message := &Message{
		ID:       "test-msg",
		Body:     []byte("test order"),
		Priority: 9,
		Headers: map[string]interface{}{
			"type": "order",
		},
		Timestamp: time.Now(),
	}

	// Test topic matching orders.*
	matches := table.FindMatchingRoutes("orders.new", message)
	expectedMatches := 2 // Should match both route1 and route2
	if len(matches) != expectedMatches {
		t.Errorf("Expected %d matches for 'orders.new', got %d", expectedMatches, len(matches))
	}

	// Verify priority ordering
	if matches[0].Name != "priority-route" {
		t.Error("Expected higher priority route first")
	}

	// Test topic that doesn't match orders.*
	matches2 := table.FindMatchingRoutes("payments.new", message)
	expectedMatches2 := 1 // Should only match route2 (wildcard *)
	if len(matches2) != expectedMatches2 {
		t.Errorf("Expected %d matches for 'payments.new', got %d", expectedMatches2, len(matches2))
	}

	// Test message that doesn't match any filters
	message2 := &Message{
		ID:       "test-msg-2",
		Body:     []byte("test payment"),
		Priority: 3,
		Headers: map[string]interface{}{
			"type": "payment",
		},
		Timestamp: time.Now(),
	}

	matches3 := table.FindMatchingRoutes("payments.new", message2)
	expectedMatches3 := 0 // Should match no routes (doesn't match filters)
	if len(matches3) != expectedMatches3 {
		t.Errorf("Expected %d matches for message2, got %d", expectedMatches3, len(matches3))
	}
}

func TestMessageRouter(t *testing.T) {
	// Create a mock producer for testing
	mockProducer := &MockProducer{}
	router := NewMessageRouter(mockProducer)

	// Add a test route
	route := Route{
		Name:        "test-route",
		Source:      "input.*",
		Destination: "output-queue",
		Filter:      NewHeaderFilter("type", "test"),
		Enabled:     true,
		Priority:    10,
	}

	err := router.AddRoute(route)
	if err != nil {
		t.Fatalf("Failed to add route: %v", err)
	}

	// Test message routing
	message := &Message{
		ID:   "test-msg",
		Body: []byte("test message"),
		Headers: map[string]interface{}{
			"type": "test",
		},
		Timestamp: time.Now(),
	}

	ctx := context.Background()
	err = router.RouteMessage(ctx, "input.data", message)
	if err != nil {
		t.Fatalf("Failed to route message: %v", err)
	}

	// Verify message was sent to mock producer
	if len(mockProducer.sentMessages) != 1 {
		t.Errorf("Expected 1 message sent to producer, got %d", len(mockProducer.sentMessages))
	}

	sentMsg := mockProducer.sentMessages[0]
	if sentMsg.topic != "output-queue" {
		t.Errorf("Expected message sent to 'output-queue', got '%s'", sentMsg.topic)
	}

	// Test routing with no matching routes
	message2 := &Message{
		ID:   "test-msg-2",
		Body: []byte("non-matching message"),
		Headers: map[string]interface{}{
			"type": "other",
		},
		Timestamp: time.Now(),
	}

	err = router.RouteMessage(ctx, "input.data", message2)
	if err == nil {
		t.Error("Expected error when no routes match")
	}
}

func TestMessageTransform(t *testing.T) {
	// Create a transform function that adds a header
	transform := func(message *Message) (*Message, error) {
		transformed := &Message{
			ID:        message.ID,
			Body:      message.Body,
			Headers:   make(map[string]interface{}),
			Priority:  message.Priority,
			TTL:       message.TTL,
			Delay:     message.Delay,
			Timestamp: message.Timestamp,
		}

		// Copy existing headers
		for k, v := range message.Headers {
			transformed.Headers[k] = v
		}

		// Add transformation marker
		transformed.Headers["transformed"] = true
		transformed.Headers["transform_time"] = time.Now().Unix()

		return transformed, nil
	}

	// Test the transform function
	original := &Message{
		ID:   "test-msg",
		Body: []byte("original message"),
		Headers: map[string]interface{}{
			"type": "order",
		},
		Priority:  5,
		Timestamp: time.Now(),
	}

	transformed, err := transform(original)
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}

	// Verify transformation
	if transformed.Headers["transformed"] != true {
		t.Error("Expected 'transformed' header to be true")
	}

	if _, exists := transformed.Headers["transform_time"]; !exists {
		t.Error("Expected 'transform_time' header to exist")
	}

	if transformed.Headers["type"] != "order" {
		t.Error("Expected original 'type' header to be preserved")
	}
}

// MockProducer for testing
type MockProducer struct {
	sentMessages []struct {
		topic   string
		message *Message
	}
	closed bool
}

func (mp *MockProducer) Send(ctx context.Context, topic string, message *Message) error {
	mp.sentMessages = append(mp.sentMessages, struct {
		topic   string
		message *Message
	}{topic, message})
	return nil
}

func (mp *MockProducer) SendAsync(ctx context.Context, topic string, message *Message) <-chan error {
	errChan := make(chan error, 1)
	errChan <- mp.Send(ctx, topic, message)
	close(errChan)
	return errChan
}

func (mp *MockProducer) SendBatch(ctx context.Context, topic string, messages []*Message) error {
	for _, message := range messages {
		if err := mp.Send(ctx, topic, message); err != nil {
			return err
		}
	}
	return nil
}

func (mp *MockProducer) Close() error {
	mp.closed = true
	return nil
}

func (mp *MockProducer) HealthCheck(ctx context.Context) error {
	if mp.closed {
		return fmt.Errorf("producer is closed")
	}
	return nil
}