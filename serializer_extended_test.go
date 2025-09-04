package bimawen

import (
	"bytes"
	"testing"
	"time"
	"reflect"
)

// TestMessage is a simple message struct for testing serializers
type TestMessage struct {
	ID        string                 `json:"id" msgpack:"id"`
	Content   string                 `json:"content" msgpack:"content"`
	Timestamp int64                  `json:"timestamp" msgpack:"timestamp"`
	Tags      []string               `json:"tags" msgpack:"tags"`
	Metadata  map[string]interface{} `json:"metadata" msgpack:"metadata"`
}

func TestJSONSerializerExtended(t *testing.T) {
	serializer := NewJSONSerializer()
	
	// Test with complex message
	msg := &TestMessage{
		ID:        "test-123",
		Content:   "Hello, World!",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"test", "message"},
		Metadata: map[string]interface{}{
			"priority": "high",
			"source":   "test-suite",
		},
	}
	
	// Serialize
	data, err := serializer.Serialize(msg)
	if err != nil {
		t.Fatalf("Serialization failed: %v", err)
	}
	
	// Deserialize
	var result TestMessage
	err = serializer.Deserialize(data, &result)
	if err != nil {
		t.Fatalf("Deserialization failed: %v", err)
	}
	
	// Verify
	if result.ID != msg.ID {
		t.Errorf("ID mismatch: expected %s, got %s", msg.ID, result.ID)
	}
	if result.Content != msg.Content {
		t.Errorf("Content mismatch: expected %s, got %s", msg.Content, result.Content)
	}
	if len(result.Tags) != len(msg.Tags) {
		t.Errorf("Tags length mismatch: expected %d, got %d", len(msg.Tags), len(result.Tags))
	}
	
	if serializer.ContentType() != "application/json" {
		t.Errorf("Wrong content type: expected application/json, got %s", serializer.ContentType())
	}
}

func TestMessagePackSerializer(t *testing.T) {
	serializer := NewMessagePackSerializer()
	
	// Test with various data types
	testData := map[string]interface{}{
		"string":  "hello world",
		"int":     42,
		"float":   3.14159,
		"bool":    true,
		"array":   []interface{}{1, 2, 3, "four"},
		"object": map[string]interface{}{
			"nested": "value",
			"number": 123,
		},
	}
	
	// Serialize
	data, err := serializer.Serialize(testData)
	if err != nil {
		t.Fatalf("MessagePack serialization failed: %v", err)
	}
	
	// Deserialize
	var result map[string]interface{}
	err = serializer.Deserialize(data, &result)
	if err != nil {
		t.Fatalf("MessagePack deserialization failed: %v", err)
	}
	
	// Verify basic fields
	if result["string"] != testData["string"] {
		t.Errorf("String mismatch: expected %v, got %v", testData["string"], result["string"])
	}
	if result["bool"] != testData["bool"] {
		t.Errorf("Bool mismatch: expected %v, got %v", testData["bool"], result["bool"])
	}
	
	if serializer.ContentType() != "application/msgpack" {
		t.Errorf("Wrong content type: expected application/msgpack, got %s", serializer.ContentType())
	}
}

func TestMessagePackWithStruct(t *testing.T) {
	serializer := NewMessagePackSerializer()
	
	msg := &TestMessage{
		ID:        "msgpack-test",
		Content:   "MessagePack serialization test",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"msgpack", "binary", "fast"},
		Metadata: map[string]interface{}{
			"format": "binary",
			"speed":  "fast",
		},
	}
	
	// Serialize
	data, err := serializer.Serialize(msg)
	if err != nil {
		t.Fatalf("MessagePack struct serialization failed: %v", err)
	}
	
	// Deserialize
	var result TestMessage
	err = serializer.Deserialize(data, &result)
	if err != nil {
		t.Fatalf("MessagePack struct deserialization failed: %v", err)
	}
	
	// Verify
	if result.ID != msg.ID {
		t.Errorf("ID mismatch: expected %s, got %s", msg.ID, result.ID)
	}
	if result.Content != msg.Content {
		t.Errorf("Content mismatch: expected %s, got %s", msg.Content, result.Content)
	}
	if !reflect.DeepEqual(result.Tags, msg.Tags) {
		t.Errorf("Tags mismatch: expected %v, got %v", msg.Tags, result.Tags)
	}
	if result.Timestamp != msg.Timestamp {
		t.Errorf("Timestamp mismatch: expected %d, got %d", msg.Timestamp, result.Timestamp)
	}
}

func TestProtobufSerializerErrorHandling(t *testing.T) {
	serializer := NewProtobufSerializer()
	
	// Test with non-protobuf data
	invalidData := map[string]interface{}{
		"not": "protobuf",
	}
	
	_, err := serializer.Serialize(invalidData)
	if err == nil {
		t.Error("Expected error when serializing non-protobuf data")
	}
	
	// Test deserialize with non-protobuf target
	var invalidTarget map[string]interface{}
	err = serializer.Deserialize([]byte("invalid"), &invalidTarget)
	if err == nil {
		t.Error("Expected error when deserializing to non-protobuf target")
	}
	
	if serializer.ContentType() != "application/x-protobuf" {
		t.Errorf("Wrong content type: expected application/x-protobuf, got %s", serializer.ContentType())
	}
}

func TestSerializerRegistryExtended(t *testing.T) {
	registry := NewSerializerRegistry()
	
	// Test all registered serializers
	expectedTypes := []string{"json", "application/json", "protobuf", "application/x-protobuf", "msgpack", "application/msgpack"}
	
	available := registry.GetAvailable()
	if len(available) != len(expectedTypes) {
		t.Errorf("Expected %d serializers, got %d", len(expectedTypes), len(available))
	}
	
	// Test GetByName
	tests := []struct {
		name     string
		expected string
	}{
		{"json", "application/json"},
		{"protobuf", "application/x-protobuf"},
		{"proto", "application/x-protobuf"},
		{"pb", "application/x-protobuf"},
		{"msgpack", "application/msgpack"},
		{"messagepack", "application/msgpack"},
	}
	
	for _, test := range tests {
		serializer, err := registry.GetByName(test.name)
		if err != nil {
			t.Errorf("Failed to get serializer by name %s: %v", test.name, err)
			continue
		}
		
		if serializer.ContentType() != test.expected {
			t.Errorf("Wrong serializer for name %s: expected %s, got %s", 
				test.name, test.expected, serializer.ContentType())
		}
	}
	
	// Test invalid name
	_, err := registry.GetByName("invalid")
	if err == nil {
		t.Error("Expected error for invalid serializer name")
	}
}

func TestSerializerPerformanceComparison(t *testing.T) {
	// Create test data
	msg := &TestMessage{
		ID:        "perf-test-123456789",
		Content:   "This is a performance test message with some content to serialize",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"performance", "test", "serialization", "benchmark", "comparison"},
		Metadata: map[string]interface{}{
			"iteration": "multiple",
			"size":      "medium",
			"type":      "performance-test",
		},
	}
	
	// Test JSON
	jsonSerializer := NewJSONSerializer()
	jsonData, err := jsonSerializer.Serialize(msg)
	if err != nil {
		t.Fatalf("JSON serialization failed: %v", err)
	}
	t.Logf("JSON serialized size: %d bytes", len(jsonData))
	
	// Test MessagePack
	msgpackSerializer := NewMessagePackSerializer()
	msgpackData, err := msgpackSerializer.Serialize(msg)
	if err != nil {
		t.Fatalf("MessagePack serialization failed: %v", err)
	}
	t.Logf("MessagePack serialized size: %d bytes", len(msgpackData))
	
	// MessagePack should generally be smaller than JSON
	if len(msgpackData) < len(jsonData) {
		compressionRatio := float64(len(jsonData)-len(msgpackData)) / float64(len(jsonData)) * 100
		t.Logf("MessagePack is %.1f%% smaller than JSON", compressionRatio)
	} else {
		t.Logf("Note: MessagePack (%d bytes) is not smaller than JSON (%d bytes) for this data", 
			len(msgpackData), len(jsonData))
	}
}

func TestSerializerWithMessage(t *testing.T) {
	// Test integration with the Message struct
	serializers := map[string]Serializer{
		"JSON":        NewJSONSerializer(),
		"MessagePack": NewMessagePackSerializer(),
	}
	
	originalMsg := &Message{
		ID:    "serializer-test",
		Body:  []byte("Test message body"),
		Topic: "test-topic",
		Headers: map[string]interface{}{
			"content-type": "text/plain",
			"source":       "test",
		},
		Timestamp:     time.Now().Round(time.Second), // Round to avoid precision issues
		DeliveryCount: 1,
		Priority:      5,
		TTL:          1 * time.Hour,
	}
	
	for name, serializer := range serializers {
		t.Run(name, func(t *testing.T) {
			// Serialize
			data, err := serializer.Serialize(originalMsg)
			if err != nil {
				t.Fatalf("%s serialization failed: %v", name, err)
			}
			
			// Deserialize
			var deserializedMsg Message
			err = serializer.Deserialize(data, &deserializedMsg)
			if err != nil {
				t.Fatalf("%s deserialization failed: %v", name, err)
			}
			
			// Verify key fields
			if deserializedMsg.ID != originalMsg.ID {
				t.Errorf("ID mismatch: expected %s, got %s", originalMsg.ID, deserializedMsg.ID)
			}
			if !bytes.Equal(deserializedMsg.Body, originalMsg.Body) {
				t.Errorf("Body mismatch: expected %s, got %s", originalMsg.Body, deserializedMsg.Body)
			}
			if deserializedMsg.Topic != originalMsg.Topic {
				t.Errorf("Topic mismatch: expected %s, got %s", originalMsg.Topic, deserializedMsg.Topic)
			}
			if deserializedMsg.DeliveryCount != originalMsg.DeliveryCount {
				t.Errorf("DeliveryCount mismatch: expected %d, got %d", 
					originalMsg.DeliveryCount, deserializedMsg.DeliveryCount)
			}
		})
	}
}

func TestSerializerBinaryDataHandling(t *testing.T) {
	// Test how different serializers handle binary data
	binaryData := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE, 0xFD}
	
	msg := &Message{
		ID:   "binary-test",
		Body: binaryData,
		Headers: map[string]interface{}{
			"content-type": "application/octet-stream",
		},
	}
	
	serializers := []struct {
		name       string
		serializer Serializer
	}{
		{"JSON", NewJSONSerializer()},
		{"MessagePack", NewMessagePackSerializer()},
	}
	
	for _, test := range serializers {
		t.Run(test.name, func(t *testing.T) {
			// Serialize
			data, err := test.serializer.Serialize(msg)
			if err != nil {
				t.Fatalf("Serialization failed: %v", err)
			}
			
			// Deserialize
			var result Message
			err = test.serializer.Deserialize(data, &result)
			if err != nil {
				t.Fatalf("Deserialization failed: %v", err)
			}
			
			// Verify binary data is preserved
			if !bytes.Equal(result.Body, binaryData) {
				t.Errorf("Binary data corruption in %s: expected %v, got %v", 
					test.name, binaryData, result.Body)
			}
		})
	}
}

// Benchmark tests for serializers
func BenchmarkJSONSerializationExtended(b *testing.B) {
	serializer := NewJSONSerializer()
	msg := &TestMessage{
		ID:        "bench-test",
		Content:   "Benchmark test message",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"benchmark", "json", "performance"},
		Metadata: map[string]interface{}{
			"size": "small",
			"type": "benchmark",
		},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializer.Serialize(msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessagePackSerialization(b *testing.B) {
	serializer := NewMessagePackSerializer()
	msg := &TestMessage{
		ID:        "bench-test",
		Content:   "Benchmark test message",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"benchmark", "msgpack", "performance"},
		Metadata: map[string]interface{}{
			"size": "small",
			"type": "benchmark",
		},
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := serializer.Serialize(msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkJSONDeserializationExtended(b *testing.B) {
	serializer := NewJSONSerializer()
	msg := &TestMessage{
		ID:        "bench-test",
		Content:   "Benchmark test message",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"benchmark", "json", "performance"},
		Metadata: map[string]interface{}{
			"size": "small",
			"type": "benchmark",
		},
	}
	
	data, err := serializer.Serialize(msg)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result TestMessage
		err := serializer.Deserialize(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMessagePackDeserialization(b *testing.B) {
	serializer := NewMessagePackSerializer()
	msg := &TestMessage{
		ID:        "bench-test",
		Content:   "Benchmark test message",
		Timestamp: time.Now().Unix(),
		Tags:      []string{"benchmark", "msgpack", "performance"},
		Metadata: map[string]interface{}{
			"size": "small",
			"type": "benchmark",
		},
	}
	
	data, err := serializer.Serialize(msg)
	if err != nil {
		b.Fatal(err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result TestMessage
		err := serializer.Deserialize(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}