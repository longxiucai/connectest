package connector

import (
	"context"

	"github.com/longxiucai/connectest/internal/config"
)

// Connector 服务连接器接口
type Connector interface {
	// Name 返回服务名称
	Name() string
	// TestConnection 测试基础连接，ctx 用于取消连接尝试
	TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error)
	// SupportedActions 返回该服务支持的所有操作
	SupportedActions() []config.Action
	// ExecuteAction 执行指定操作，ctx 用于取消长时间运行的操作
	ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error)
}

// Registry 连接器注册表
type Registry struct {
	connectors map[config.ServiceType]Connector
}

// NewRegistry 创建新的注册表
func NewRegistry() *Registry {
	r := &Registry{
		connectors: make(map[config.ServiceType]Connector),
	}
	// 注册所有连接器
	r.Register(config.ServiceMySQL, &MySQLConnector{})
	r.Register(config.ServicePostgreSQL, &PostgreSQLConnector{})
	r.Register(config.ServiceMongoDB, &MongoDBConnector{})
	r.Register(config.ServiceRedis, &RedisConnector{})
	r.Register(config.ServiceRabbitMQ, &RabbitMQConnector{})
	r.Register(config.ServiceKafka, &KafkaConnector{})
	r.Register(config.ServiceMinIO, &MinIOConnector{})
	r.Register(config.ServiceEtcdAPI, &EtcdAPIConnector{})
	r.Register(config.ServiceEtcdK8S, &EtcdK8SConnector{})
	return r
}

// Register 注册连接器
func (r *Registry) Register(serviceType config.ServiceType, c Connector) {
	r.connectors[serviceType] = c
}

// Get 获取连接器
func (r *Registry) Get(serviceType config.ServiceType) (Connector, bool) {
	c, ok := r.connectors[serviceType]
	return c, ok
}

// All 返回所有连接器
func (r *Registry) All() map[config.ServiceType]Connector {
	return r.connectors
}
