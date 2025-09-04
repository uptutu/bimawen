package bimawen

import (
	"encoding/json"
	"fmt"

	"google.golang.org/protobuf/proto"
	"github.com/vmihailenco/msgpack/v5"
)

// JSONSerializer implements JSON serialization
type JSONSerializer struct{}

// NewJSONSerializer creates a new JSON serializer
func NewJSONSerializer() *JSONSerializer {
	return &JSONSerializer{}
}

// Serialize converts data to JSON bytes
func (s *JSONSerializer) Serialize(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

// Deserialize converts JSON bytes back to data
func (s *JSONSerializer) Deserialize(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// ContentType returns the JSON content type
func (s *JSONSerializer) ContentType() string {
	return "application/json"
}

// ProtobufSerializer implements Protocol Buffers serialization
type ProtobufSerializer struct{}

// NewProtobufSerializer creates a new Protobuf serializer
func NewProtobufSerializer() *ProtobufSerializer {
	return &ProtobufSerializer{}
}

// Serialize converts protobuf message to bytes
func (s *ProtobufSerializer) Serialize(data interface{}) ([]byte, error) {
	msg, ok := data.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("data must implement proto.Message interface, got %T", data)
	}
	return proto.Marshal(msg)
}

// Deserialize converts bytes back to protobuf message
func (s *ProtobufSerializer) Deserialize(data []byte, v interface{}) error {
	msg, ok := v.(proto.Message)
	if !ok {
		return fmt.Errorf("target must implement proto.Message interface, got %T", v)
	}
	return proto.Unmarshal(data, msg)
}

// ContentType returns the protobuf content type
func (s *ProtobufSerializer) ContentType() string {
	return "application/x-protobuf"
}

// MessagePackSerializer implements MessagePack serialization
type MessagePackSerializer struct{}

// NewMessagePackSerializer creates a new MessagePack serializer
func NewMessagePackSerializer() *MessagePackSerializer {
	return &MessagePackSerializer{}
}

// Serialize converts data to MessagePack bytes
func (s *MessagePackSerializer) Serialize(data interface{}) ([]byte, error) {
	return msgpack.Marshal(data)
}

// Deserialize converts MessagePack bytes back to data
func (s *MessagePackSerializer) Deserialize(data []byte, v interface{}) error {
	return msgpack.Unmarshal(data, v)
}

// ContentType returns the MessagePack content type
func (s *MessagePackSerializer) ContentType() string {
	return "application/msgpack"
}

// SerializerRegistry manages available serializers
type SerializerRegistry struct {
	serializers map[string]Serializer
}

// NewSerializerRegistry creates a new serializer registry
func NewSerializerRegistry() *SerializerRegistry {
	registry := &SerializerRegistry{
		serializers: make(map[string]Serializer),
	}
	
	// Register default serializers
	registry.Register("json", NewJSONSerializer())
	registry.Register("application/json", NewJSONSerializer())
	registry.Register("protobuf", NewProtobufSerializer())
	registry.Register("application/x-protobuf", NewProtobufSerializer())
	registry.Register("msgpack", NewMessagePackSerializer())
	registry.Register("application/msgpack", NewMessagePackSerializer())
	
	return registry
}

// Register registers a serializer with a content type
func (r *SerializerRegistry) Register(contentType string, serializer Serializer) {
	r.serializers[contentType] = serializer
}

// Get retrieves a serializer by content type
func (r *SerializerRegistry) Get(contentType string) (Serializer, error) {
	serializer, exists := r.serializers[contentType]
	if !exists {
		return nil, fmt.Errorf("serializer not found for content type: %s", contentType)
	}
	return serializer, nil
}

// GetDefault returns the default JSON serializer
func (r *SerializerRegistry) GetDefault() Serializer {
	return r.serializers["json"]
}

// GetAvailable returns a list of available serializer content types
func (r *SerializerRegistry) GetAvailable() []string {
	var types []string
	for contentType := range r.serializers {
		types = append(types, contentType)
	}
	return types
}

// GetByName returns a serializer by short name (json, protobuf, msgpack)
func (r *SerializerRegistry) GetByName(name string) (Serializer, error) {
	switch name {
	case "json":
		return r.Get("json")
	case "protobuf", "proto", "pb":
		return r.Get("protobuf")
	case "msgpack", "messagepack":
		return r.Get("msgpack")
	default:
		return r.Get(name) // Try as content type
	}
}

// DefaultSerializerRegistry is the global serializer registry
var DefaultSerializerRegistry = NewSerializerRegistry()