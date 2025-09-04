# Bimawen - 通用 MQ 工具库

Bimawen 是一个用 Go 编写的通用消息队列工具库，支持多种 MQ 系统，提供统一的 API 接口。

## 特性

- 🔌 **统一接口**: 通过 URI 字符串自动识别并连接不同的 MQ 系统
- 🔄 **重试机制**: 内置指数退避重试策略
- ⚡ **异步支持**: 支持同步和异步消息发送
- 📦 **批量操作**: 支持批量发送消息
- 🎯 **并发控制**: 可配置的消费者并发数
- 🔧 **可扩展**: 支持自定义序列化器
- 📈 **生产就绪**: 包含连接管理、健康检查等生产环境必需功能

## 支持的 MQ 系统

- **RabbitMQ** - 使用官方 `github.com/rabbitmq/amqp091-go`
- **Redis** - 使用热门 `github.com/redis/go-redis/v9`

## 安装

```bash
go get local.git/libs/bimawen.git
```

## 快速开始

### RabbitMQ 示例

```go
package main

import (
    "context"
    "log"
    "time"

    "local.git/libs/bimawen.git"
)

func main() {
    ctx := context.Background()

    // 创建生产者
    producer, err := bimawen.NewProducer("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // 发送消息
    message := &bimawen.Message{
        ID:        "msg-001",
        Body:      []byte(`{"order_id": "12345", "amount": 100.50}`),
        Headers:   map[string]interface{}{"source": "order-service"},
        Timestamp: time.Now(),
        Priority:  1,
    }

    err = producer.Send(ctx, "orders", message)
    if err != nil {
        log.Fatal(err)
    }

    // 创建消费者
    consumer, err := bimawen.NewConsumer("amqp://guest:guest@localhost:5672/")
    if err != nil {
        log.Fatal(err)
    }
    defer consumer.Close()

    // 消费消息
    err = consumer.Consume(ctx, "orders", func(ctx context.Context, msg *bimawen.Message) error {
        log.Printf("Received: %s", string(msg.Body))
        return nil
    })
}
```

### Redis 示例

```go
package main

import (
    "context"
    "log"
    "time"

    "local.git/libs/bimawen.git"
)

func main() {
    ctx := context.Background()

    // 创建生产者 (Redis)
    producer, err := bimawen.NewProducer("redis://localhost:6379/0")
    if err != nil {
        log.Fatal(err)
    }
    defer producer.Close()

    // 发送延迟消息
    message := &bimawen.Message{
        ID:        "delayed-msg-001",
        Body:      []byte(`{"reminder": "Meeting in 5 minutes"}`),
        Timestamp: time.Now(),
        Delay:     5 * time.Second, // 延迟 5 秒
    }

    err = producer.Send(ctx, "notifications", message)
    if err != nil {
        log.Fatal(err)
    }

    // 批量发送
    messages := []*bimawen.Message{
        {ID: "batch-1", Body: []byte(`{"data": 1}`), Timestamp: time.Now()},
        {ID: "batch-2", Body: []byte(`{"data": 2}`), Timestamp: time.Now()},
        {ID: "batch-3", Body: []byte(`{"data": 3}`), Timestamp: time.Now()},
    }

    err = producer.SendBatch(ctx, "batch-queue", messages)
    if err != nil {
        log.Fatal(err)
    }
}
```

## API 文档

### 核心接口

#### Producer

```go
type Producer interface {
    // Send 同步发送消息
    Send(ctx context.Context, topic string, message *Message) error
    
    // SendAsync 异步发送消息，返回错误通道
    SendAsync(ctx context.Context, topic string, message *Message) <-chan error
    
    // SendBatch 批量发送消息
    SendBatch(ctx context.Context, topic string, messages []*Message) error
    
    // Close 关闭生产者
    Close() error
}
```

#### Consumer

```go
type Consumer interface {
    // Consume 开始消费消息
    Consume(ctx context.Context, topic string, handler MessageHandler) error
    
    // ConsumeWithOptions 使用选项消费消息
    ConsumeWithOptions(ctx context.Context, topic string, handler MessageHandler, opts *ConsumeOptions) error
    
    // Close 关闭消费者
    Close() error
}
```

#### Message

```go
type Message struct {
    ID            string                 // 消息唯一标识
    Topic         string                 // 主题/队列名
    Body          []byte                 // 消息体
    Headers       map[string]interface{} // 消息头
    Timestamp     time.Time              // 时间戳
    DeliveryCount int                    // 投递次数
    Priority      uint8                  // 优先级 (0-255)
    TTL           time.Duration          // 生存时间
    Delay         time.Duration          // 延迟时间
}
```

### 配置选项

#### 生产者配置

```go
opts := &bimawen.ProducerOptions{
    Timeout:     30 * time.Second,
    RetryConfig: bimawen.DefaultRetryConfig(),
    BatchSize:   100,
    Serializer:  bimawen.NewJSONSerializer(),
}

producer, err := bimawen.NewProducer("amqp://...", opts)
```

#### RabbitMQ 交换机配置

RabbitMQ 支持通过 `DriverOptions` 配置交换机和发布选项：

```go
// 配置 RabbitMQ 生产者使用指定交换机
opts := bimawen.DefaultProducerOptions()
opts.DriverOptions = map[string]interface{}{
    bimawen.RabbitMQExchange:  "my-direct-exchange",  // 交换机名称
    bimawen.RabbitMQMandatory: false,                 // 强制投递标志
    bimawen.RabbitMQImmediate: false,                 // 立即投递标志
}

producer, err := bimawen.NewProducer("amqp://guest:guest@localhost:5672/", opts)
if err != nil {
    log.Fatal(err)
}

// 发送消息到指定交换机和路由键
message := &bimawen.Message{
    ID:        "msg-001",
    Body:      []byte(`{"order_id": "12345"}`),
    Timestamp: time.Now(),
}

// 消息将发送到 "my-direct-exchange" 交换机，路由键为 "order.created"
err = producer.Send(ctx, "order.created", message)
```

**支持的 RabbitMQ 配置选项：**

- **`bimawen.RabbitMQExchange`** (`"exchange"`): 交换机名称 (默认: `""` 使用默认交换机)
- **`bimawen.RabbitMQMandatory`** (`"mandatory"`): 强制投递标志 (默认: `false`)
- **`bimawen.RabbitMQImmediate`** (`"immediate"`): 立即投递标志 (默认: `false`)

**交换机类型使用场景：**

```go
// Direct 交换机 - 精确匹配路由键
opts.DriverOptions[bimawen.RabbitMQExchange] = "orders-direct"
producer.Send(ctx, "order.created", message)  // 路由到绑定 "order.created" 的队列

// Topic 交换机 - 模式匹配路由键
opts.DriverOptions[bimawen.RabbitMQExchange] = "orders-topic" 
producer.Send(ctx, "order.payment.success", message)  // 匹配 "order.*" 或 "*.success" 模式

// Fanout 交换机 - 广播到所有绑定队列
opts.DriverOptions[bimawen.RabbitMQExchange] = "orders-fanout"
producer.Send(ctx, "", message)  // 路由键被忽略，发送到所有绑定队列

// 默认交换机 - 路由键必须与队列名完全匹配
opts.DriverOptions[bimawen.RabbitMQExchange] = ""
producer.Send(ctx, "order-queue", message)  // 直接发送到名为 "order-queue" 的队列
```

#### 消费者配置

```go
opts := &bimawen.ConsumerOptions{
    Concurrency: 10,
    AutoAck:     false,
    RetryConfig: bimawen.DefaultRetryConfig(),
}

consumer, err := bimawen.NewConsumer("redis://...", opts)
```

### 重试配置

```go
retryConfig := &bimawen.RetryConfig{
    MaxRetries:          5,
    InitialInterval:     1 * time.Second,
    MaxInterval:        30 * time.Second,
    Multiplier:          2.0,
    RandomizationFactor: 0.1,
}
```

## URI 格式

### RabbitMQ

```
amqp://[username:password@]host[:port][/vhost][?options]
amqps://[username:password@]host[:port][/vhost][?options]
```

示例:
- `amqp://guest:guest@localhost:5672/`
- `amqps://user:pass@rabbitmq.example.com:5671/myapp`

### Redis

```
redis://[username:password@]host[:port][/database][?options]
rediss://[username:password@]host[:port][/database][?options]
```

示例:
- `redis://localhost:6379/0`
- `rediss://user:pass@redis.example.com:6380/1?timeout=5s`

## 高级功能

### 自定义序列化器

```go
type CustomSerializer struct{}

func (s *CustomSerializer) Serialize(data interface{}) ([]byte, error) {
    // 自定义序列化逻辑
    return []byte{}, nil
}

func (s *CustomSerializer) Deserialize(data []byte, v interface{}) error {
    // 自定义反序列化逻辑
    return nil
}

func (s *CustomSerializer) ContentType() string {
    return "application/custom"
}

// 注册自定义序列化器
bimawen.DefaultSerializerRegistry.Register("custom", &CustomSerializer{})
```

### 错误处理

```go
// 创建可重试错误
retryableErr := bimawen.NewRetryableError(errors.New("temporary failure"))

// 创建不可重试错误  
nonRetryableErr := bimawen.NewNonRetryableError(errors.New("permanent failure"))

// 检查是否可重试
if bimawen.IsRetryableError(err) {
    // 可以重试
}
```

### 健康检查

```go
// 通过工厂创建驱动进行健康检查
driver, err := bimawen.DefaultDriverFactory.Create("amqp://localhost:5672")
if err != nil {
    log.Fatal(err)
}
defer driver.Close()

ctx := context.Background()
if err := driver.HealthCheck(ctx); err != nil {
    log.Printf("Health check failed: %v", err)
}
```

## 连接对象访问接口

Bimawen 提供类型安全的连接对象访问接口，让您可以直接使用底层驱动的原生功能。

### RabbitMQ 连接访问

```go
// 获取生产者的底层 AMQP 连接对象
producer, err := bimawen.NewProducer("amqp://guest:guest@localhost:5672/")
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

// 类型断言获取 RabbitMQ 连接访问器
if accessor, ok := producer.(bimawen.RabbitMQConnectionAccessor); ok {
    // 获取 AMQP 通道进行直接操作
    channel := accessor.GetChannel()
    if channel != nil {
        // 使用原生 AMQP 功能
        err := channel.ExchangeDeclare("my-exchange", "direct", false, false, false, false, nil)
        if err != nil {
            log.Printf("Exchange declaration failed: %v", err)
        }
        // 通道会自动返回到连接池
    }
    
    // 获取 AMQP 连接进行高级管理
    connection := accessor.GetConnection()
    if connection != nil && !connection.IsClosed() {
        log.Println("Connection is healthy")
    }
}
```

### Redis 连接访问

```go
// 获取生产者的底层 Redis 客户端
producer, err := bimawen.NewProducer("redis://localhost:6379/0")
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

// 类型断言获取 Redis 连接访问器
if accessor, ok := producer.(bimawen.RedisConnectionAccessor); ok {
    client := accessor.GetClient()
    if client != nil {
        // 使用原生 Redis 功能
        ctx := context.Background()
        result := client.Set(ctx, "my-key", "my-value", time.Hour)
        if result.Err() != nil {
            log.Printf("Redis SET failed: %v", result.Err())
        }
        
        // 获取值
        val := client.Get(ctx, "my-key")
        log.Printf("Retrieved value: %s", val.Val())
    }
}
```

### Redis Streams 连接访问

```go
// 获取 Redis Streams 生产者的底层客户端
producer, err := bimawen.NewProducer("redis-streams://localhost:6379/0")
if err != nil {
    log.Fatal(err)
}
defer producer.Close()

// 类型断言获取 Redis Streams 连接访问器
if accessor, ok := producer.(bimawen.RedisStreamsConnectionAccessor); ok {
    client := accessor.GetClient()
    if client != nil {
        // 使用原生 Redis Streams 功能
        ctx := context.Background()
        result := client.XAdd(ctx, &redis.XAddArgs{
            Stream: "my-stream",
            ID:     "*",
            Values: map[string]interface{}{
                "field1": "value1",
                "field2": "value2",
            },
        })
        
        if result.Err() == nil {
            log.Printf("Added to stream with ID: %s", result.Val())
        }
    }
}
```

### 连接访问接口特性

- **类型安全**: 每种驱动都有专门的接口，避免使用 `any` 或类型判断
- **连接池集成**: 自动与现有连接池集成，无需手动管理连接生命周期
- **线程安全**: 所有连接访问操作都是并发安全的
- **资源管理**: 连接和通道会自动返回到池中重复使用
- **统一接口**: 生产者和消费者都支持相同的连接访问接口

## 最佳实践

1. **连接管理**: 重用生产者和消费者实例，避免频繁创建和销毁
2. **错误处理**: 区分可重试和不可重试的错误
3. **资源清理**: 始终调用 `Close()` 方法清理资源
4. **并发控制**: 根据系统负载调整消费者并发数
5. **超时设置**: 为操作设置合理的超时时间
6. **消息确认**: 谨慎使用 `AutoAck`，确保消息处理完成后再确认

## 测试

```bash
# 运行测试
go test -v

# 运行示例
go run example/main.go
```

## 许可证

MIT License