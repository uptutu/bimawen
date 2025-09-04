package bimawen

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// DriverType represents the type of message queue driver
type DriverType string

const (
	DriverRabbitMQ     DriverType = "rabbitmq"
	DriverRedis        DriverType = "redis"
	DriverRedisStreams DriverType = "redis-streams"
)

// URIInfo contains parsed URI information
type URIInfo struct {
	DriverType DriverType
	Host       string
	Port       int
	Username   string
	Password   string
	VHost      string  // For RabbitMQ
	Database   int     // For Redis
	Options    map[string]string
}

// ParseURI parses a connection URI and returns driver type and connection info
func ParseURI(uri string) (*URIInfo, error) {
	if uri == "" {
		return nil, fmt.Errorf("URI cannot be empty")
	}

	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid URI format: %w", err)
	}

	info := &URIInfo{
		Options: make(map[string]string),
	}

	// Parse scheme to determine driver type and TLS
	switch strings.ToLower(u.Scheme) {
	case "amqp", "amqps":
		info.DriverType = DriverRabbitMQ
		if u.Scheme == "amqps" {
			info.Options["tls"] = "true"
		}
	case "redis", "rediss":
		info.DriverType = DriverRedis
		if u.Scheme == "rediss" {
			info.Options["tls"] = "true"
		}
	case "redis-streams", "rediss-streams":
		info.DriverType = DriverRedisStreams
		if u.Scheme == "rediss-streams" {
			info.Options["tls"] = "true"
		}
	default:
		return nil, fmt.Errorf("unsupported scheme: %s", u.Scheme)
	}

	// Parse host and port
	info.Host = u.Hostname()
	if info.Host == "" {
		info.Host = "localhost"
	}

	port := u.Port()
	if port == "" {
		// Set default ports based on driver type
		switch info.DriverType {
		case DriverRabbitMQ:
			if u.Scheme == "amqps" {
				info.Port = 5671
			} else {
				info.Port = 5672
			}
		case DriverRedis, DriverRedisStreams:
			if u.Scheme == "rediss" || u.Scheme == "rediss-streams" {
				info.Port = 6380
			} else {
				info.Port = 6379
			}
		}
	} else {
		info.Port, err = strconv.Atoi(port)
		if err != nil {
			return nil, fmt.Errorf("invalid port: %s", port)
		}
	}

	// Parse authentication
	if u.User != nil {
		info.Username = u.User.Username()
		if password, ok := u.User.Password(); ok {
			info.Password = password
		}
	}

	// Parse path and database specific info
	switch info.DriverType {
	case DriverRabbitMQ:
		// For RabbitMQ, path represents vhost
		info.VHost = strings.TrimPrefix(u.Path, "/")
		if info.VHost == "" {
			info.VHost = "/"
		} else {
			info.VHost = "/" + info.VHost
		}
	case DriverRedis, DriverRedisStreams:
		// For Redis, path represents database number
		if u.Path != "" && u.Path != "/" {
			dbStr := strings.TrimPrefix(u.Path, "/")
			if dbStr != "" {
				info.Database, err = strconv.Atoi(dbStr)
				if err != nil {
					return nil, fmt.Errorf("invalid database number: %s", dbStr)
				}
				if info.Database < 0 {
					return nil, fmt.Errorf("database number cannot be negative: %d", info.Database)
				}
			}
		}
	}

	// Parse query parameters as options
	for key, values := range u.Query() {
		if len(values) > 0 {
			info.Options[key] = values[0]
		}
	}

	return info, nil
}

// DriverFactory creates drivers based on URI
type DriverFactory struct {
	drivers map[DriverType]DriverCreator
}

// DriverCreator is a function that creates a driver instance
type DriverCreator func(info *URIInfo) (Driver, error)

// NewDriverFactory creates a new driver factory
func NewDriverFactory() *DriverFactory {
	return &DriverFactory{
		drivers: make(map[DriverType]DriverCreator),
	}
}

// Register registers a driver creator for a specific driver type
func (f *DriverFactory) Register(driverType DriverType, creator DriverCreator) {
	f.drivers[driverType] = creator
}

// Create creates a driver instance based on the URI
func (f *DriverFactory) Create(uri string) (Driver, error) {
	info, err := ParseURI(uri)
	if err != nil {
		return nil, err
	}

	creator, exists := f.drivers[info.DriverType]
	if !exists {
		return nil, fmt.Errorf("driver not registered for type: %s", info.DriverType)
	}

	return creator(info)
}

// DefaultDriverFactory is the global driver factory instance
var DefaultDriverFactory = NewDriverFactory()

// NewProducer creates a new producer using the default factory
func NewProducer(uri string, opts ...*ProducerOptions) (Producer, error) {
	driver, err := DefaultDriverFactory.Create(uri)
	if err != nil {
		return nil, err
	}

	// Merge options
	options := DefaultProducerOptions()
	if len(opts) > 0 && opts[0] != nil {
		if opts[0].Timeout > 0 {
			options.Timeout = opts[0].Timeout
		}
		if opts[0].RetryConfig != nil {
			options.RetryConfig = opts[0].RetryConfig
		}
		if opts[0].BatchSize > 0 {
			options.BatchSize = opts[0].BatchSize
		}
		if opts[0].Serializer != nil {
			options.Serializer = opts[0].Serializer
		}
		for k, v := range opts[0].DriverOptions {
			options.DriverOptions[k] = v
		}
	}

	options.URI = uri
	if options.Serializer == nil {
		options.Serializer = DefaultSerializerRegistry.GetDefault()
	}

	return driver.NewProducer(options)
}

// NewConsumer creates a new consumer using the default factory
func NewConsumer(uri string, opts ...*ConsumerOptions) (Consumer, error) {
	driver, err := DefaultDriverFactory.Create(uri)
	if err != nil {
		return nil, err
	}

	// Merge options
	options := DefaultConsumerOptions()
	if len(opts) > 0 && opts[0] != nil {
		if opts[0].Concurrency > 0 {
			options.Concurrency = opts[0].Concurrency
		}
		if opts[0].RetryConfig != nil {
			options.RetryConfig = opts[0].RetryConfig
		}
		options.AutoAck = opts[0].AutoAck
		if opts[0].Serializer != nil {
			options.Serializer = opts[0].Serializer
		}
		for k, v := range opts[0].DriverOptions {
			options.DriverOptions[k] = v
		}
	}

	options.URI = uri
	if options.Serializer == nil {
		options.Serializer = DefaultSerializerRegistry.GetDefault()
	}

	return driver.NewConsumer(options)
}