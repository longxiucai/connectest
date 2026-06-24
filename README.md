# Connectest

多服务连接测试工具，支持 CLI 和 GUI 两种模式。快速验证数据库、消息队列、对象存储、分布式协调等服务的连通性和功能。

## 功能特性

- 支持 9 种常见后端服务的连接测试
- CLI 和 GUI 双模式，满足不同使用场景
- 每种服务支持独立的功能操作（如执行 SQL、发送消息、浏览数据等）
- CLI 支持循环执行和并发测试，适合压测和稳定性验证
- 支持 TLS/SSL 及 mTLS 双向证书认证（etcd 等）
- 信号处理：Ctrl+C 优雅中断，自动清理连接
- 跨平台：CLI 可交叉编译为 Linux/macOS/Windows（amd64/arm64）

## 支持的服务

| 服务 | 基础连接 | 扩展功能 |
|------|---------|---------|
| MySQL | ✅ | 执行 SQL 查询、列出数据库 |
| PostgreSQL | ✅ | 执行 SQL 查询、列出数据库 |
| MongoDB | ✅ | 列出数据库/集合、执行文档查询 |
| Redis | ✅ | SET/GET 测试、列出 Key、INFO 信息 |
| RabbitMQ | ✅ | 创建队列、发送消息 (Producer)、消费消息 (Consumer) |
| Kafka | ✅ | 创建 Topic、发送消息 (Producer)、消费消息 (Consumer) |
| MinIO | ✅ | 列出桶、创建桶、上传/下载文件、列出对象 |
| etcd | ✅ | PUT/GET/DELETE 键值对、前缀搜索、TTL 租约 |
| K8s-etcd | ✅ | 列出 Namespaces/Nodes/Pods/Services、资源统计、protobuf 解码、Leader 选举查看 |

## 安装

### 系统依赖（Linux，Fyne GUI 需要）

GUI 模式基于 Fyne 框架，编译时需要 C 工具链和 X11 开发库：

```bash
sudo apt-get install -y gcc \
  libxi-dev libxcursor-dev libxinerama-dev \
  libxxf86vm-dev libgl1-mesa-dev libxrandr-dev \
  libx11-dev libxrender-dev
```

### 编译

```bash
# 完整编译（GUI + CLI）
go mod tidy
go build -o bin/connectest ./cmd/connectest

# 或使用 Makefile
make build
```

### CLI 独立编译（无需 CGO）

纯 CLI 版本不依赖 GUI 库，可交叉编译到任意平台：

```bash
# 编译当前平台 CLI
CGO_ENABLED=0 go build -o bin/connectest-cli ./cmd/connectest-cli

# 或使用 Makefile 编译全平台
make build-cli-all
```

支持的编译目标：

| 目标 | Makefile 命令 |
|------|--------------|
| Linux amd64 | `make build-cli-linux-amd64` |
| Linux arm64 | `make build-cli-linux-arm64` |
| macOS amd64 | `make build-cli-darwin-amd64` |
| macOS arm64 | `make build-cli-darwin-arm64` |
| Windows amd64 | `make build-cli-windows-amd64` |
| Windows arm64 | `make build-cli-windows-arm64` |

## 使用

### GUI 模式（默认）

```bash
# 直接运行，启动图形界面
./bin/connectest

# 或显式指定
./bin/connectest gui
```

GUI 布局：
- **左侧**：服务列表（支持页面缓存，切换不丢失已填表单）
- **右侧上方**：连接参数表单 + 服务器信息面板
- **右侧下方**：操作按钮 + 结果输出区

### CLI 模式

#### 连接测试

```bash
# 测试 MySQL 连接
./bin/connectest-cli mysql --host 127.0.0.1 --port 3306 --user root --password mypass

# 测试 Redis 连接
./bin/connectest-cli redis -H 127.0.0.1 -p mypass

# 测试 etcd 连接（HTTP API 模式）
./bin/connectest-cli etcd-api -H 127.0.0.1 -P 2379

# 测试 K8s etcd 连接（gRPC 模式，模拟 K8s API Server）
./bin/connectest-cli etcd-k8s -H 127.0.0.1 -P 2379
```

#### 执行功能操作

```bash
# MySQL 执行 SQL
./bin/connectest-cli mysql -H 127.0.0.1 -u root -p mypass \
  --action query -k sql="SELECT 1"

# MySQL 列出数据库
./bin/connectest-cli mysql -H 127.0.0.1 -u root -p mypass --action list_databases

# Redis SET/GET 测试
./bin/connectest-cli redis -H 127.0.0.1 --action setget -k key=test -k value=hello

# Redis 列出 Key
./bin/connectest-cli redis -H 127.0.0.1 --action keys -k pattern="*"

# RabbitMQ 发送消息
./bin/connectest-cli rabbitmq -H 127.0.0.1 -u guest -p guest \
  --action produce -k queue=test-queue -k message="hello world"

# RabbitMQ 消费消息
./bin/connectest-cli rabbitmq -H 127.0.0.1 -u guest -p guest \
  --action consume -k queue=test-queue

# Kafka 发送消息
./bin/connectest-cli kafka -H 127.0.0.1 --action produce \
  -k topic=test-topic -k message="hello kafka"

# Kafka 消费消息
./bin/connectest-cli kafka -H 127.0.0.1 --action consume \
  -k topic=test-topic -k group=test-group

# MinIO 上传文件
./bin/connectest-cli minio -H 127.0.0.1 -u minioadmin -p minioadmin \
  --action upload -k bucket=test -k object=test.txt -k content="hello"

# etcd 写入键值对
./bin/connectest-cli etcd-api -H 127.0.0.1 -P 2379 \
  --action put -k key=mykey -k value=myvalue -k ttl=60

# etcd 前缀搜索
./bin/connectest-cli etcd-api -H 127.0.0.1 -P 2379 \
  --action prefix -k prefix="" -k keys_only=true

# K8s-etcd 列出 Pods
./bin/connectest-cli etcd-k8s -H 127.0.0.1 -P 2379 \
  --action pods -k namespace=kube-system

# K8s-etcd 资源统计
./bin/connectest-cli etcd-k8s -H 127.0.0.1 -P 2379 --action resource_count

# K8s-etcd Leader 选举
./bin/connectest-cli etcd-k8s -H 127.0.0.1 -P 2379 --action leases
```

#### 列出服务支持的操作

```bash
# 列出 MySQL 所有操作及参数
./bin/connectest-cli mysql --list-actions

# 列出 Redis 所有操作
./bin/connectest-cli redis --list-actions

# 列出 K8s-etcd 所有操作
./bin/connectest-cli etcd-k8s --list-actions
```

#### 循环与并发测试

```bash
# 单线程循环 10 次
./bin/connectest-cli redis -H 127.0.0.1 \
  --action setget -k key=test -k value=hello \
  -n 10

# 5 个并发 worker，每个执行 100 次
./bin/connectest-cli redis -H 127.0.0.1 \
  --action setget -k key=test -k value=hello \
  -n 100 -c 5

# 3 个并发 worker，每次间隔 500ms
./bin/connectest-cli redis -H 127.0.0.1 \
  --action setget -k key=test -k value=hello \
  -n 50 -c 3 -i 500

# 执行过程中按 Ctrl+C 可优雅停止
```

#### TLS / mTLS 连接

```bash
# 启用 TLS（跳过证书验证）
./bin/connectest-cli etcd-api -H etcd.example.com -P 2379 --tls

# mTLS 双向认证
./bin/connectest-cli etcd-api -H etcd.example.com -P 2379 \
  --tls \
  --ca-cert /path/to/ca.pem \
  --cert /path/to/client.pem \
  --key /path/to/client-key.pem

# 证书也支持直接粘贴 PEM 内容
./bin/connectest-cli etcd-api -H etcd.example.com -P 2379 \
  --tls \
  --ca-cert "-----BEGIN CERTIFICATE-----MIIC..."
```

#### 日志级别

```bash
# 使用 debug 模式查看详细日志
./bin/connectest-cli redis -H 127.0.0.1 --log-level debug

# 仅显示错误
./bin/connectest-cli redis -H 127.0.0.1 --log-level error
```

#### 查看帮助

```bash
./bin/connectest-cli --help
./bin/connectest-cli mysql --help
```

### CLI 参数说明

| 参数 | 缩写 | 说明 | 默认值 |
|------|------|------|--------|
| `--host` | `-H` | 服务器地址 | `127.0.0.1` |
| `--port` | `-P` | 端口号 | 各服务默认端口 |
| `--user` | `-u` | 用户名 | - |
| `--password` | `-p` | 密码 | - |
| `--database` | `-d` | 数据库名 | - |
| `--action` | `-a` | 执行操作（留空则测试连接） | - |
| `--param` | `-k` | 操作参数（`key=value` 格式，可多次指定） | - |
| `--tls` | - | 启用 TLS/SSL | `false` |
| `--ca-cert` | - | CA 证书（路径或 PEM 内容） | - |
| `--cert` | - | 客户端证书（路径或 PEM 内容） | - |
| `--key` | - | 客户端私钥（路径或 PEM 内容） | - |
| `--loop` | `-n` | 循环次数 | `1` |
| `--concurrency` | `-c` | 并发 worker 数 | `1` |
| `--interval` | `-i` | 循环间隔（毫秒） | `0` |
| `--list-actions` | - | 列出该服务支持的所有操作 | `false` |
| `--log-level` | - | 日志级别（DEBUG/INFO/WARN/ERROR） | `INFO` |

> 不同服务的默认端口：MySQL=3306, PostgreSQL=5432, MongoDB=27017, Redis=6379, RabbitMQ=5672, Kafka=9092, MinIO=9000, etcd=2379

## 项目结构

```
connectest/
├── cmd/
│   ├── connectest/                # 主程序入口（GUI + CLI）
│   │   └── main.go
│   └── connectest-cli/            # 纯 CLI 入口（无需 CGO）
│       └── main.go
├── internal/
│   ├── cli/                       # CLI 命令实现 (Cobra)
│   │   └── cli.go
│   ├── gui/                       # GUI 界面 (Fyne)
│   │   ├── app.go                 # 主窗口和布局
│   │   ├── forms.go               # 表单和操作按钮
│   │   └── tabs.go                # Tab 页扩展（预留）
│   ├── connector/                 # 服务连接器实现
│   │   ├── connector.go           # Connector 接口 + 注册表
│   │   ├── mysql.go               # MySQL
│   │   ├── postgres.go            # PostgreSQL
│   │   ├── mongo.go               # MongoDB
│   │   ├── redis.go               # Redis
│   │   ├── rabbitmq.go            # RabbitMQ
│   │   ├── kafka.go               # Kafka
│   │   ├── minio.go               # MinIO
│   │   ├── etcd.go                # etcd (HTTP REST API)
│   │   ├── k8s_etcd.go            # K8s etcd (gRPC clientv3)
│   │   └── k8s_etcd_decoder.go    # K8s protobuf 解码
│   ├── config/                    # 配置和数据结构
│   │   └── config.go
│   └── logger/                    # 日志模块
│       └── logger.go
├── .github/workflows/
│   └── release.yml                # GitHub Actions 自动发布
├── Makefile
├── go.mod
├── go.sum
└── README.md
```

## 技术栈

- **Go 1.26+**
- [Fyne v2](https://fyne.io/) - 跨平台 GUI 框架
- [Cobra](https://github.com/spf13/cobra) - CLI 命令框架
- [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql) - MySQL 驱动
- [lib/pq](https://github.com/lib/pq) - PostgreSQL 驱动
- [mongo-driver](https://go.mongodb.org/mongo-driver) - MongoDB 驱动
- [go-redis](https://github.com/redis/go-redis) - Redis 客户端
- [amqp091-go](https://github.com/rabbitmq/amqp091-go) - RabbitMQ 客户端
- [kafka-go](https://github.com/segmentio/kafka-go) - Kafka 客户端
- [minio-go](https://github.com/minio/minio-go) - MinIO 客户端
- [etcd clientv3](https://go.etcd.io/etcd/client/v3) - etcd gRPC 客户端
- [k8s.io/api](https://kubernetes.io/docs/reference/) - Kubernetes API 类型定义

## License

MIT
