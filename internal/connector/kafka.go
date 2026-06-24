package connector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/segmentio/kafka-go"
)

type KafkaConnector struct{}

func (c *KafkaConnector) Name() string { return "Kafka" }

func (c *KafkaConnector) brokerAddr(cfg config.Config) string {
	return fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)
}

func (c *KafkaConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", c.brokerAddr(cfg))
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("连接失败: %v", err)}, nil
	}
	defer conn.Close()

	si := &config.ServerInfo{Status: "Running"}

	brokers, err := conn.Brokers()
	if err != nil {
		return &config.Result{Success: true, Message: "连接成功（无法获取 Broker 列表）", ServerInfo: si}, nil
	}

	controller, _ := conn.Controller()

	si.InfoItems = append(si.InfoItems,
		config.InfoItem{Label: "Broker 数量", Value: fmt.Sprintf("%d", len(brokers))},
		config.InfoItem{Label: "连接地址", Value: c.brokerAddr(cfg)},
	)

	// API 版本（可推断大致版本范围）
	if apiVersions, err := conn.ApiVersions(); err == nil && len(apiVersions) > 0 {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "支持的 API 数", Value: fmt.Sprintf("%d", len(apiVersions))})
	}

	// Topic 统计
	if partitions, err := conn.ReadPartitions(); err == nil {
		topicMap := make(map[string]int)
		for _, p := range partitions {
			topicMap[p.Topic]++
		}
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "Topic 总数", Value: fmt.Sprintf("%d", len(topicMap))})
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "分区总数", Value: fmt.Sprintf("%d", len(partitions))})
	}

	// 集群信息
	if len(brokers) > 1 {
		si.Cluster = &config.ClusterInfo{
			Mode:    "Cluster",
			Summary: fmt.Sprintf("%d 个 Broker", len(brokers)),
		}
	} else {
		si.Cluster = &config.ClusterInfo{Mode: "Standalone"}
	}

	for _, b := range brokers {
		role := "Broker"
		status := "running"
		extra := fmt.Sprintf("ID: %d", b.ID)
		if controller.Host != "" && b.ID == controller.ID {
			role = "Controller"
			extra += " (Controller)"
		}
		si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
			Address: fmt.Sprintf("%s:%d", b.Host, b.Port),
			Role:    role,
			Status:  status,
			Info:    extra,
		})
	}

	return &config.Result{
		Success:    true,
		Message:    fmt.Sprintf("连接成功 - Kafka (%d 个 Broker)", len(brokers)),
		ServerInfo: si,
	}, nil
}

func (c *KafkaConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "create_topic",
			Label:       "创建 Topic",
			Description: "创建一个新的 Kafka Topic",
			Params: []config.ActionParam{
				{Name: "topic", Label: "Topic 名称", Placeholder: "test-topic", Required: true, Default: "connectest-test-topic"},
				{Name: "partitions", Label: "分区数", Placeholder: "3", Required: false, Default: "3"},
			},
		},
		{
			Name:        "list_topics",
			Label:       "列出 Topic",
			Description: "列出所有 Topic",
		},
		{
			Name:        "message_test",
			Label:       "发送/消费消息",
			Description: "向 Topic 发送消息或从 Topic 消费消息，支持同时发送和消费",
			Params: []config.ActionParam{
				{Name: "do_produce", Label: "启用发送", Default: "true"},
				{Name: "do_consume", Label: "启用消费", Default: "true"},
				{Name: "produce_topic", Label: "发送 Topic", Placeholder: "test-topic", Default: "connectest-test-topic"},
				{Name: "message", Label: "消息内容", Placeholder: "hello world", Default: "hello from connectest"},
				{Name: "key", Label: "消息 Key", Placeholder: "key1", Default: ""},
				{Name: "consume_topic", Label: "消费 Topic", Placeholder: "test-topic", Default: "connectest-test-topic"},
				{Name: "group", Label: "消费组", Placeholder: "test-group", Default: "connectest-test-group"},
			},
		},
	}
}

func (c *KafkaConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	broker := c.brokerAddr(cfg)

	switch action {
	case "create_topic":
		topicName := params["topic"]
		if topicName == "" {
			return nil, fmt.Errorf("Topic 名称不能为空")
		}
		partitions := 3
		if p, ok := params["partitions"]; ok {
			fmt.Sscanf(p, "%d", &partitions)
		}

		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			return nil, fmt.Errorf("连接失败: %w", err)
		}
		defer conn.Close()

		controller, err := conn.Controller()
		if err != nil {
			return nil, fmt.Errorf("获取 Controller 失败: %w", err)
		}

		controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
		if err != nil {
			return nil, fmt.Errorf("连接 Controller 失败: %w", err)
		}
		defer controllerConn.Close()

		err = controllerConn.CreateTopics(kafka.TopicConfig{
			Topic:             topicName,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		})
		if err != nil {
			if strings.Contains(err.Error(), "already exists") {
				return &config.Result{
					Success: true,
					Message: fmt.Sprintf("Topic '%s' 已存在", topicName),
				}, nil
			}
			return &config.Result{Success: false, Message: fmt.Sprintf("创建 Topic 失败: %v", err)}, nil
		}

		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("Topic '%s' 创建成功（%d 个分区）", topicName, partitions),
		}, nil

	case "list_topics":
		conn, err := kafka.DialContext(ctx, "tcp", broker)
		if err != nil {
			return nil, fmt.Errorf("连接失败: %w", err)
		}
		defer conn.Close()

		partitions, err := conn.ReadPartitions()
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("获取 Topic 列表失败: %v", err)}, nil
		}

		topicMap := make(map[string]int)
		for _, p := range partitions {
			topicMap[p.Topic]++
		}

		var output strings.Builder
		for topic, count := range topicMap {
			output.WriteString(fmt.Sprintf("%-30s (%d 分区)\n", topic, count))
		}

		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("共 %d 个 Topic", len(topicMap)),
			Details: output.String(),
		}, nil

	case "message_test":
		doProduce := params["do_produce"] == "true"
		doConsume := params["do_consume"] == "true"
		produceTopic := params["produce_topic"]
		consumeTopic := params["consume_topic"]
		message := params["message"]
		key := params["key"]
		group := params["group"]

		if !doProduce && !doConsume {
			return nil, fmt.Errorf("请至少启用发送或消费")
		}
		if group == "" {
			group = "connectest-test-group"
		}

		var output strings.Builder

		// ─── 发送消息 ───
		if doProduce {
			if produceTopic == "" {
				output.WriteString("❌ 发送 Topic 不能为空\n")
			} else {
				if message == "" {
					message = "hello from connectest"
				}
				w := &kafka.Writer{
					Addr:     kafka.TCP(broker),
					Topic:    produceTopic,
					Balancer: &kafka.LeastBytes{},
				}
				msg := kafka.Message{Value: []byte(message)}
				if key != "" {
					msg.Key = []byte(key)
				}
				if err := w.WriteMessages(ctx, msg); err != nil {
					output.WriteString(fmt.Sprintf("❌ 发送失败: %v\n", err))
				} else {
					output.WriteString(fmt.Sprintf("✅ 发送成功 → Topic: %s, 消息: %s\n", produceTopic, message))
				}
				w.Close()
			}
		}

		// ─── 消费消息 ───
		if doConsume {
			if consumeTopic == "" {
				output.WriteString("❌ 消费 Topic 不能为空\n")
			} else {
				r := kafka.NewReader(kafka.ReaderConfig{
					Brokers:        []string{broker},
					Topic:          consumeTopic,
					GroupID:        group,
					MinBytes:       1,
					MaxBytes:       10e6,
					CommitInterval: time.Second,
					StartOffset:    kafka.LastOffset,
				})
				readCtx, readCancel := context.WithTimeout(ctx, 10*time.Second)
				msg, err := r.ReadMessage(readCtx)
				readCancel()
				r.Close()
				if err != nil {
					if readCtx.Err() != nil {
						output.WriteString("⚠ 消费超时 (10秒)，暂无新消息\n")
					} else {
						output.WriteString(fmt.Sprintf("❌ 消费失败: %v\n", err))
					}
				} else {
					output.WriteString(fmt.Sprintf("✅ 消费成功 ← Topic: %s, 分区: %d, Offset: %d, 消息: %s\n",
						msg.Topic, msg.Partition, msg.Offset, string(msg.Value)))
				}
			}
		}

		result := output.String()
		success := !strings.Contains(result, "❌")
		msg := "消息测试完成"
		if !success {
			msg = "消息测试完成（存在错误）"
		}
		return &config.Result{
			Success: success,
			Message: msg,
			Details: result,
		}, nil

	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}
