package bimawen

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// RoutingKey represents a routing key for message routing
type RoutingKey string

// Route represents a message route configuration
type Route struct {
	// Name is the unique identifier for the route
	Name string
	
	// Source is the source topic/queue pattern
	Source string
	
	// Destination is the target topic/queue
	Destination string
	
	// Filter is the message filter for this route
	Filter MessageFilter
	
	// Transform is an optional message transformation function
	Transform MessageTransform
	
	// Enabled indicates if the route is active
	Enabled bool
	
	// Priority determines route evaluation order (higher = first)
	Priority int
}

// MessageTransform represents a function that can transform messages during routing
type MessageTransform func(*Message) (*Message, error)

// RoutingTable manages message routing rules
type RoutingTable struct {
	routes []Route
	mu     sync.RWMutex
}

// NewRoutingTable creates a new routing table
func NewRoutingTable() *RoutingTable {
	return &RoutingTable{
		routes: make([]Route, 0),
	}
}

// AddRoute adds a new route to the routing table
func (rt *RoutingTable) AddRoute(route Route) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	// Check for duplicate route names
	for _, existingRoute := range rt.routes {
		if existingRoute.Name == route.Name {
			return fmt.Errorf("route with name '%s' already exists", route.Name)
		}
	}
	
	rt.routes = append(rt.routes, route)
	
	// Sort routes by priority (highest first)
	rt.sortRoutes()
	
	return nil
}

// RemoveRoute removes a route by name
func (rt *RoutingTable) RemoveRoute(name string) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	for i, route := range rt.routes {
		if route.Name == name {
			rt.routes = append(rt.routes[:i], rt.routes[i+1:]...)
			return nil
		}
	}
	
	return fmt.Errorf("route with name '%s' not found", name)
}

// UpdateRoute updates an existing route
func (rt *RoutingTable) UpdateRoute(name string, updatedRoute Route) error {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	
	for i, route := range rt.routes {
		if route.Name == name {
			updatedRoute.Name = name // Preserve the original name
			rt.routes[i] = updatedRoute
			rt.sortRoutes()
			return nil
		}
	}
	
	return fmt.Errorf("route with name '%s' not found", name)
}

// GetRoute retrieves a route by name
func (rt *RoutingTable) GetRoute(name string) (Route, error) {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	
	for _, route := range rt.routes {
		if route.Name == name {
			return route, nil
		}
	}
	
	return Route{}, fmt.Errorf("route with name '%s' not found", name)
}

// ListRoutes returns all routes
func (rt *RoutingTable) ListRoutes() []Route {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	routes := make([]Route, len(rt.routes))
	copy(routes, rt.routes)
	return routes
}

// FindMatchingRoutes finds all routes that match the given topic and message
func (rt *RoutingTable) FindMatchingRoutes(topic string, message *Message) []Route {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	
	var matchingRoutes []Route
	
	for _, route := range rt.routes {
		if !route.Enabled {
			continue
		}
		
		// Check if source pattern matches the topic
		if !rt.matchesPattern(route.Source, topic) {
			continue
		}
		
		// Check if message matches the filter
		if route.Filter != nil && !route.Filter.Match(message) {
			continue
		}
		
		matchingRoutes = append(matchingRoutes, route)
	}
	
	return matchingRoutes
}

// sortRoutes sorts routes by priority (highest first)
func (rt *RoutingTable) sortRoutes() {
	// Simple bubble sort by priority
	n := len(rt.routes)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if rt.routes[j].Priority < rt.routes[j+1].Priority {
				rt.routes[j], rt.routes[j+1] = rt.routes[j+1], rt.routes[j]
			}
		}
	}
}

// matchesPattern checks if a pattern matches a topic
func (rt *RoutingTable) matchesPattern(pattern, topic string) bool {
	// Simple wildcard matching
	// "*" matches any single segment
	// "**" matches multiple segments
	// For now, implement simple exact matching and basic wildcard
	
	if pattern == "*" || pattern == "**" {
		return true
	}
	
	if pattern == topic {
		return true
	}
	
	// TODO: Implement more sophisticated pattern matching
	// For now, check if pattern contains wildcards
	if stringContains(pattern, "*") {
		return rt.matchWildcard(pattern, topic)
	}
	
	return false
}

// matchWildcard performs basic wildcard matching
func (rt *RoutingTable) matchWildcard(pattern, topic string) bool {
	// Simple implementation - convert * to regex and match
	// This is a basic implementation, could be enhanced
	
	// Replace * with [^.]*  and ** with .*
	regexPattern := pattern
	regexPattern = strings.ReplaceAll(regexPattern, "**", "DOUBLE_WILDCARD")
	regexPattern = strings.ReplaceAll(regexPattern, "*", "[^.]*")
	regexPattern = strings.ReplaceAll(regexPattern, "DOUBLE_WILDCARD", ".*")
	
	// Add anchors
	regexPattern = "^" + regexPattern + "$"
	
	matched, err := regexp.MatchString(regexPattern, topic)
	if err != nil {
		return false
	}
	
	return matched
}

// MessageRouter handles routing of messages based on routing table
type MessageRouter struct {
	table     *RoutingTable
	producer  Producer
	consumers map[string]Consumer
	mu        sync.RWMutex
	running   bool
	stopCh    chan struct{}
}

// NewMessageRouter creates a new message router
func NewMessageRouter(producer Producer) *MessageRouter {
	return &MessageRouter{
		table:     NewRoutingTable(),
		producer:  producer,
		consumers: make(map[string]Consumer),
		stopCh:    make(chan struct{}),
	}
}

// AddRoute adds a routing rule
func (mr *MessageRouter) AddRoute(route Route) error {
	return mr.table.AddRoute(route)
}

// RemoveRoute removes a routing rule
func (mr *MessageRouter) RemoveRoute(name string) error {
	return mr.table.RemoveRoute(name)
}

// GetRoutingTable returns the routing table
func (mr *MessageRouter) GetRoutingTable() *RoutingTable {
	return mr.table
}

// RouteMessage routes a single message according to routing rules
func (mr *MessageRouter) RouteMessage(ctx context.Context, sourceTopic string, message *Message) error {
	routes := mr.table.FindMatchingRoutes(sourceTopic, message)
	
	if len(routes) == 0 {
		return fmt.Errorf("no matching routes found for topic: %s", sourceTopic)
	}
	
	var lastErr error
	routed := false
	
	for _, route := range routes {
		// Apply transformation if specified
		routedMessage := message
		if route.Transform != nil {
			transformed, err := route.Transform(message)
			if err != nil {
				lastErr = fmt.Errorf("failed to transform message for route %s: %w", route.Name, err)
				continue
			}
			routedMessage = transformed
		}
		
		// Send message to destination
		err := mr.producer.Send(ctx, route.Destination, routedMessage)
		if err != nil {
			lastErr = fmt.Errorf("failed to route message to %s: %w", route.Destination, err)
			continue
		}
		
		routed = true
		
		// For now, route to first matching route
		// Could be enhanced to support multi-cast routing
		break
	}
	
	if !routed {
		return fmt.Errorf("failed to route message: %w", lastErr)
	}
	
	return nil
}

// StartAutoRouting starts automatic routing by consuming from source topics
func (mr *MessageRouter) StartAutoRouting(ctx context.Context) error {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	
	if mr.running {
		return fmt.Errorf("router is already running")
	}
	
	mr.running = true
	
	// Start consumers for all source topics in routing table
	sourceTopics := mr.getSourceTopics()
	
	for _, topic := range sourceTopics {
		consumer, err := NewConsumer("redis://localhost:6379") // TODO: Make configurable
		if err != nil {
			return fmt.Errorf("failed to create consumer for topic %s: %w", topic, err)
		}
		
		mr.consumers[topic] = consumer
		
		// Start consuming in a goroutine
		go mr.consumeAndRoute(ctx, topic, consumer)
	}
	
	return nil
}

// StopAutoRouting stops automatic routing
func (mr *MessageRouter) StopAutoRouting() error {
	mr.mu.Lock()
	defer mr.mu.Unlock()
	
	if !mr.running {
		return fmt.Errorf("router is not running")
	}
	
	mr.running = false
	close(mr.stopCh)
	
	// Stop all consumers
	var lastErr error
	for topic, consumer := range mr.consumers {
		if err := consumer.Close(); err != nil {
			lastErr = fmt.Errorf("failed to close consumer for topic %s: %w", topic, err)
		}
		delete(mr.consumers, topic)
	}
	
	// Reset stop channel for next start
	mr.stopCh = make(chan struct{})
	
	return lastErr
}

// consumeAndRoute consumes messages and routes them
func (mr *MessageRouter) consumeAndRoute(ctx context.Context, topic string, consumer Consumer) {
	handler := func(ctx context.Context, message *Message) error {
		return mr.RouteMessage(ctx, topic, message)
	}
	
	// Use context cancellation to stop consumption
	routingCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	
	// Listen for stop signal
	go func() {
		select {
		case <-mr.stopCh:
			cancel()
		case <-routingCtx.Done():
		}
	}()
	
	// Start consuming
	consumer.Consume(routingCtx, topic, handler)
}

// getSourceTopics extracts unique source topics from routing table
func (mr *MessageRouter) getSourceTopics() []string {
	routes := mr.table.ListRoutes()
	topicSet := make(map[string]bool)
	
	for _, route := range routes {
		if route.Enabled {
			topicSet[route.Source] = true
		}
	}
	
	var topics []string
	for topic := range topicSet {
		topics = append(topics, topic)
	}
	
	return topics
}

// RoutingStats provides statistics about routing operations
type RoutingStats struct {
	TotalRoutedMessages uint64            `json:"total_routed_messages"`
	FailedRoutings      uint64            `json:"failed_routings"`
	RouteStats          map[string]uint64 `json:"route_stats"`
}

// GetStats returns routing statistics
func (mr *MessageRouter) GetStats() RoutingStats {
	// TODO: Implement statistics tracking
	return RoutingStats{
		RouteStats: make(map[string]uint64),
	}
}

// Helper function to check if string contains substring
func stringContains(s, substr string) bool {
	return strings.Contains(s, substr)
}