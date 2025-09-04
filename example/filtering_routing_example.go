package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"local.git/libs/bimawen.git"
)

func main() {
	fmt.Println("=== Message Filtering and Routing Example ===")

	// Example 1: Basic Message Filtering
	fmt.Println("\n1. Basic Message Filtering:")
	testBasicFiltering()

	// Example 2: Composite Filters
	fmt.Println("\n2. Composite Filters:")
	testCompositeFiltering()

	// Example 3: Message Routing
	fmt.Println("\n3. Message Routing:")
	testMessageRouting()

	// Example 4: Routing with Transformations
	fmt.Println("\n4. Routing with Transformations:")
	testRoutingWithTransforms()

	fmt.Println("\n=== Filtering and Routing Example Complete ===")
}

func testBasicFiltering() {
	// Create test messages
	orderMessage := &bimawen.Message{
		ID:   "order-001",
		Body: []byte("New customer order for premium subscription"),
		Headers: map[string]interface{}{
			"type":     "order",
			"category": "premium",
			"customer": "john.doe@example.com",
		},
		Priority:  8,
		Timestamp: time.Now(),
	}

	paymentMessage := &bimawen.Message{
		ID:   "payment-001",
		Body: []byte("Payment processed successfully"),
		Headers: map[string]interface{}{
			"type":   "payment",
			"status": "completed",
			"amount": 99.99,
		},
		Priority:  5,
		Timestamp: time.Now(),
	}

	// Create different types of filters
	orderFilter := bimawen.NewHeaderFilter("type", "order")
	priorityFilter := bimawen.NewPriorityFilter(7, 10)
	contentFilter := bimawen.NewContentFilter("premium")

	// Test filters
	fmt.Printf("Order message matches order filter: %v\n", orderFilter.Match(orderMessage))
	fmt.Printf("Order message matches priority filter: %v\n", priorityFilter.Match(orderMessage))
	fmt.Printf("Order message matches content filter: %v\n", contentFilter.Match(orderMessage))

	fmt.Printf("Payment message matches order filter: %v\n", orderFilter.Match(paymentMessage))
	fmt.Printf("Payment message matches priority filter: %v\n", priorityFilter.Match(paymentMessage))

	// Regex filter example
	regexFilter, err := bimawen.NewHeaderRegexFilter("customer", ".*@example.com")
	if err != nil {
		log.Printf("Error creating regex filter: %v", err)
		return
	}

	fmt.Printf("Order message matches email regex filter: %v\n", regexFilter.Match(orderMessage))
	fmt.Printf("Payment message matches email regex filter: %v\n", regexFilter.Match(paymentMessage))
}

func testCompositeFiltering() {
	// Create a test message
	message := &bimawen.Message{
		ID:   "composite-test",
		Body: []byte("Urgent order processing required"),
		Headers: map[string]interface{}{
			"type":     "order",
			"priority": "urgent",
			"category": "enterprise",
		},
		Priority:  9,
		Timestamp: time.Now(),
	}

	// Create individual filters
	orderFilter := bimawen.NewHeaderFilter("type", "order")
	urgentFilter := bimawen.NewHeaderFilter("priority", "urgent")
	highPriorityFilter := bimawen.NewPriorityFilter(8, 10)
	enterpriseFilter := bimawen.NewHeaderFilter("category", "enterprise")

	// Test AND composite filter (all must match)
	andFilter := bimawen.NewCompositeFilter(
		bimawen.FilterAND,
		orderFilter,
		urgentFilter,
		highPriorityFilter,
	)

	fmt.Printf("Message matches AND filter (order + urgent + high priority): %v\n", andFilter.Match(message))
	fmt.Printf("AND Filter description: %s\n", andFilter.String())

	// Test OR composite filter (at least one must match)
	orFilter := bimawen.NewCompositeFilter(
		bimawen.FilterOR,
		bimawen.NewHeaderFilter("type", "payment"),
		urgentFilter,
		bimawen.NewHeaderFilter("category", "basic"),
	)

	fmt.Printf("Message matches OR filter (payment OR urgent OR basic): %v\n", orFilter.Match(message))
	fmt.Printf("OR Filter description: %s\n", orFilter.String())

	// Test NOT composite filter
	notFilter := bimawen.NewCompositeFilter(
		bimawen.FilterNOT,
		bimawen.NewHeaderFilter("type", "payment"),
	)

	fmt.Printf("Message matches NOT payment filter: %v\n", notFilter.Match(message))
	fmt.Printf("NOT Filter description: %s\n", notFilter.String())

	// Complex nested filter
	complexFilter := bimawen.NewCompositeFilter(
		bimawen.FilterAND,
		andFilter, // Previous AND filter
		bimawen.NewCompositeFilter(
			bimawen.FilterOR,
			enterpriseFilter,
			bimawen.NewHeaderFilter("category", "premium"),
		),
	)

	fmt.Printf("Message matches complex nested filter: %v\n", complexFilter.Match(message))
	fmt.Printf("Complex Filter description: %s\n", complexFilter.String())
}

func testMessageRouting() {
	// Create a mock producer (in real usage, this would be a real producer)
	producer := &MockProducer{}

	// Create message router
	router := bimawen.NewMessageRouter(producer)

	// Define routing rules
	routes := []bimawen.Route{
		{
			Name:        "high-priority-orders",
			Source:      "orders.*",
			Destination: "priority-processing",
			Filter: bimawen.NewCompositeFilter(
				bimawen.FilterAND,
				bimawen.NewHeaderFilter("type", "order"),
				bimawen.NewPriorityFilter(8, 10),
			),
			Enabled:  true,
			Priority: 100,
		},
		{
			Name:        "regular-orders",
			Source:      "orders.*",
			Destination: "standard-processing",
			Filter:      bimawen.NewHeaderFilter("type", "order"),
			Enabled:     true,
			Priority:    50,
		},
		{
			Name:        "payment-processing",
			Source:      "payments.*",
			Destination: "payment-processor",
			Filter:      bimawen.NewHeaderFilter("type", "payment"),
			Enabled:     true,
			Priority:    75,
		},
	}

	// Add routes to router
	for _, route := range routes {
		err := router.AddRoute(route)
		if err != nil {
			log.Printf("Error adding route %s: %v", route.Name, err)
			continue
		}
		fmt.Printf("Added route: %s (%s -> %s)\n", route.Name, route.Source, route.Destination)
	}

	// Test routing different types of messages
	testMessages := []struct {
		topic   string
		message *bimawen.Message
		desc    string
	}{
		{
			topic: "orders.new",
			message: &bimawen.Message{
				ID:   "order-high-priority",
				Body: []byte("Urgent enterprise order"),
				Headers: map[string]interface{}{
					"type":     "order",
					"category": "enterprise",
				},
				Priority:  9,
				Timestamp: time.Now(),
			},
			desc: "High priority order",
		},
		{
			topic: "orders.update",
			message: &bimawen.Message{
				ID:   "order-regular",
				Body: []byte("Regular order update"),
				Headers: map[string]interface{}{
					"type":     "order",
					"category": "standard",
				},
				Priority:  5,
				Timestamp: time.Now(),
			},
			desc: "Regular order",
		},
		{
			topic: "payments.processed",
			message: &bimawen.Message{
				ID:   "payment-001",
				Body: []byte("Payment completed"),
				Headers: map[string]interface{}{
					"type":   "payment",
					"amount": 99.99,
				},
				Priority:  7,
				Timestamp: time.Now(),
			},
			desc: "Payment message",
		},
	}

	ctx := context.Background()
	for _, test := range testMessages {
		fmt.Printf("\nRouting %s from topic '%s':\n", test.desc, test.topic)
		
		err := router.RouteMessage(ctx, test.topic, test.message)
		if err != nil {
			fmt.Printf("  Error: %v\n", err)
		} else {
			// Check where the message was routed
			if len(producer.sentMessages) > 0 {
				lastSent := producer.sentMessages[len(producer.sentMessages)-1]
				fmt.Printf("  Routed to: %s\n", lastSent.topic)
			}
		}
	}

	fmt.Printf("\nTotal messages routed: %d\n", len(producer.sentMessages))
}

func testRoutingWithTransforms() {
	producer := &MockProducer{}
	router := bimawen.NewMessageRouter(producer)

	// Create a transformation that enriches messages
	enrichTransform := func(message *bimawen.Message) (*bimawen.Message, error) {
		// Create a new message with enriched headers
		enriched := &bimawen.Message{
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
			enriched.Headers[k] = v
		}

		// Add enrichment data
		enriched.Headers["processed_at"] = time.Now().Unix()
		enriched.Headers["enriched"] = true
		enriched.Headers["processing_node"] = "node-001"

		// Modify body to add processing info
		enriched.Body = []byte(fmt.Sprintf("[PROCESSED] %s", string(message.Body)))

		return enriched, nil
	}

	// Add route with transformation
	route := bimawen.Route{
		Name:        "enrich-orders",
		Source:      "raw.orders",
		Destination: "processed.orders",
		Filter:      bimawen.NewHeaderFilter("type", "order"),
		Transform:   enrichTransform,
		Enabled:     true,
		Priority:    100,
	}

	err := router.AddRoute(route)
	if err != nil {
		log.Printf("Error adding transform route: %v", err)
		return
	}

	// Test message with transformation
	originalMessage := &bimawen.Message{
		ID:   "transform-test",
		Body: []byte("Raw order data"),
		Headers: map[string]interface{}{
			"type":        "order",
			"customer_id": "12345",
		},
		Priority:  6,
		Timestamp: time.Now(),
	}

	fmt.Printf("Original message body: %s\n", string(originalMessage.Body))
	fmt.Printf("Original headers: %v\n", originalMessage.Headers)

	ctx := context.Background()
	err = router.RouteMessage(ctx, "raw.orders", originalMessage)
	if err != nil {
		fmt.Printf("Error routing message: %v\n", err)
		return
	}

	// Check the transformed message
	if len(producer.sentMessages) > 0 {
		transformedMsg := producer.sentMessages[len(producer.sentMessages)-1]
		fmt.Printf("\nTransformed message body: %s\n", string(transformedMsg.message.Body))
		fmt.Printf("Transformed headers: %v\n", transformedMsg.message.Headers)
		fmt.Printf("Routed to topic: %s\n", transformedMsg.topic)
	}
}

// MockProducer for demonstration purposes
type MockProducer struct {
	sentMessages []struct {
		topic   string
		message *bimawen.Message
	}
}

func (mp *MockProducer) Send(ctx context.Context, topic string, message *bimawen.Message) error {
	mp.sentMessages = append(mp.sentMessages, struct {
		topic   string
		message *bimawen.Message
	}{topic, message})
	return nil
}

func (mp *MockProducer) SendAsync(ctx context.Context, topic string, message *bimawen.Message) <-chan error {
	errChan := make(chan error, 1)
	errChan <- mp.Send(ctx, topic, message)
	close(errChan)
	return errChan
}

func (mp *MockProducer) SendBatch(ctx context.Context, topic string, messages []*bimawen.Message) error {
	for _, message := range messages {
		if err := mp.Send(ctx, topic, message); err != nil {
			return err
		}
	}
	return nil
}

func (mp *MockProducer) Close() error {
	return nil
}

func (mp *MockProducer) HealthCheck(ctx context.Context) error {
	return nil
}