package config

// Config 通用连接配置
type Config struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	// TLS/SSL
	UseTLS bool
	// PostgreSQL SSL 模式
	SslMode string // disable, allow, prefer, require, verify-ca, verify-full
	// mTLS 证书路径（用于 etcd 等需要双向证书认证的服务）
	CACert string // CA 证书路径
	Cert   string // 客户端证书路径
	Key    string // 客户端私钥路径
	// 额外参数（不同服务可能需要不同的参数）
	Extra map[string]string
}

// Result 操作结果
type Result struct {
	Success    bool
	Message    string
	Details    string
	ServerInfo *ServerInfo // 连接测试时返回的服务器信息
}

// ServerInfo 服务器基本信息（连接测试后展示在固定面板）
type ServerInfo struct {
	Version   string       // 版本号
	Status    string       // 运行状态
	InfoItems []InfoItem   // 基本信息键值对列表
	Cluster   *ClusterInfo // 集群信息（可选）
}

// InfoItem 信息条目
type InfoItem struct {
	Label string
	Value string
}

// ClusterInfo 集群信息
type ClusterInfo struct {
	Mode    string     // 集群模式：Standalone / Cluster / ReplicaSet / Sentinel 等
	Nodes   []NodeInfo // 节点列表
	Summary string     // 集群概要描述
}

// NodeInfo 节点信息
type NodeInfo struct {
	Name    string // 节点名称
	Address string
	Role    string // master / slave / broker / controller / follower 等
	Status  string // running / stopped / online 等
	Info    string // 额外信息
}

// Action 定义服务支持的操作
type Action struct {
	Name        string
	Label       string
	Description string
	// 操作需要的额外参数
	Params []ActionParam
}

// ActionParam 操作参数定义
type ActionParam struct {
	Name        string
	Label       string
	Placeholder string
	Required    bool
	Default     string
}

// ServiceType 服务类型
type ServiceType string

const (
	ServiceMySQL      ServiceType = "mysql"
	ServicePostgreSQL ServiceType = "postgresql"
	ServiceMongoDB    ServiceType = "mongodb"
	ServiceRedis      ServiceType = "redis"
	ServiceRabbitMQ   ServiceType = "rabbitmq"
	ServiceKafka      ServiceType = "kafka"
	ServiceMinIO      ServiceType = "minio"
	ServiceEtcdAPI    ServiceType = "etcd-api"
	ServiceEtcdK8S    ServiceType = "etcd-k8s"
)

// ServiceMeta 服务元数据
type ServiceMeta struct {
	Type          ServiceType
	Name          string
	DefaultPort   int
	HasUser       bool
	HasPassword   bool
	HasDatabase   bool
	HasMgmtPort   bool   // 是否有额外的管理端口（如 RabbitMQ Management）
	MgmtPortLabel string // 管理端口的显示标签
	HasCerts      bool   // 是否需要 TLS 证书字段（CA 证书、客户端证书、客户端密钥）
}

// AllServices 返回所有支持的服务元数据
func AllServices() []ServiceMeta {
	return []ServiceMeta{
		{ServiceMySQL, "MySQL", 3306, true, true, true, false, "", true},
		{ServicePostgreSQL, "PostgreSQL", 5432, true, true, true, false, "", true},
		{ServiceMongoDB, "MongoDB", 27017, true, true, true, false, "", true},
		{ServiceRedis, "Redis", 6379, false, true, false, false, "", true},
		{ServiceRabbitMQ, "RabbitMQ", 5672, true, true, true, true, "管理端口", true},
		{ServiceKafka, "Kafka", 9092, false, false, false, false, "", true},
		{ServiceMinIO, "MinIO", 9000, true, true, false, false, "", true},
		{ServiceEtcdAPI, "etcd-api", 2379, true, true, false, false, "", true},
		{ServiceEtcdK8S, "etcd-k8s", 2379, false, false, false, false, "", true},
	}
}
