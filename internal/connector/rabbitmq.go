package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

var rabbitLog = logger.NewModule("RabbitMQ")

// mgmtGet 请求 RabbitMQ Management API
func mgmtGet(url, user, password string) (interface{}, error) {
	client := &http.Client{Timeout: 3 * time.Second}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(user, password)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	var result interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

type RabbitMQConnector struct{}

func (c *RabbitMQConnector) Name() string { return "RabbitMQ" }

func (c *RabbitMQConnector) buildURL(cfg config.Config) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)
}

func (c *RabbitMQConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	rabbitLog.Debug("连接 %s:%d, user=%s", cfg.Host, cfg.Port, cfg.User)

	// 上下文感知的连接：支持取消
	type dialResult struct {
		conn *amqp.Connection
		err  error
	}
	ch := make(chan dialResult, 1)
	go func() {
		conn, err := amqp.DialConfig(c.buildURL(cfg), amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
		})
		ch <- dialResult{conn, err}
	}()

	select {
	case <-ctx.Done():
		return &config.Result{Success: false, Message: "连接已取消"}, nil
	case dr := <-ch:
		if dr.err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("连接失败: %v", dr.err)}, nil
		}
		defer dr.conn.Close()
		return c.doTestConnection(ctx, cfg, dr.conn)
	}
}

func (c *RabbitMQConnector) doTestConnection(ctx context.Context, cfg config.Config, conn *amqp.Connection) (*config.Result, error) {

	ch, err := conn.Channel()
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("创建 Channel 失败: %v", err)}, nil
	}
	defer ch.Close()

	// 从连接属性中读取服务器信息
	props := conn.Properties
	product, _ := props["product"].(string)
	version, _ := props["version"].(string)
	platform, _ := props["platform"].(string)
	clusterName, _ := props["cluster_name"].(string)

	si := &config.ServerInfo{
		Status:  "Running",
		Version: version,
	}
	si.InfoItems = append(si.InfoItems,
		config.InfoItem{Label: "服务器地址", Value: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)},
	)
	if product != "" {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "产品", Value: product})
	}
	if platform != "" {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "平台", Value: platform})
	}

	// 通过 Management HTTP API 获取更多运行时信息（如果可用）
	// 管理端口默认 = AMQP端口 + 10000（5672→15672），可通过 Extra["mgmt_port"] 自定义
	mgmtPort := cfg.Port + 10000
	if mp, ok := cfg.Extra["mgmt_port"]; ok {
		fmt.Sscanf(mp, "%d", &mgmtPort)
	}
	scheme := "http"
	if cfg.UseTLS {
		scheme = "https"
	}
	mgmtURL := fmt.Sprintf("%s://%s:%d/api/overview", scheme, cfg.Host, mgmtPort)
	overviewRaw, mgmtErr := mgmtGet(mgmtURL, cfg.User, cfg.Password)
	if mgmtErr != nil {
		// Management API 不可用，作为警告显示，不影响连接测试
		si.InfoItems = append(si.InfoItems, config.InfoItem{
			Label: "⚠ Management API",
			Value: fmt.Sprintf("不可用 (%s:%d) - %v，请确认已启用 rabbitmq_management 插件", cfg.Host, mgmtPort, mgmtErr),
		})
	} else {
		overview, _ := overviewRaw.(map[string]interface{})
		if node, ok := overview["node"].(string); ok {
			si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "节点", Value: node})
		}
		if qTotals, ok := overview["object_totals"].(map[string]interface{}); ok {
			if queues, ok := qTotals["queues"].(float64); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "队列总数", Value: fmt.Sprintf("%.0f", queues)})
			}
			if conns, ok := qTotals["connections"].(float64); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "连接数", Value: fmt.Sprintf("%.0f", conns)})
			}
			if channels, ok := qTotals["channels"].(float64); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "Channel 数", Value: fmt.Sprintf("%.0f", channels)})
			}
		}

		// 集群节点信息
		nodesURL := fmt.Sprintf("%s://%s:%d/api/nodes", scheme, cfg.Host, mgmtPort)
		if nodesRaw, err := mgmtGet(nodesURL, cfg.User, cfg.Password); err == nil {
			if nodesArr, ok := nodesRaw.([]interface{}); ok && len(nodesArr) > 1 {
				si.Cluster = &config.ClusterInfo{
					Mode:    "Cluster",
					Summary: fmt.Sprintf("%d 个节点，集群: %s", len(nodesArr), clusterName),
				}
				for _, n := range nodesArr {
					if nodeMap, ok := n.(map[string]interface{}); ok {
						name, _ := nodeMap["name"].(string)
						running, _ := nodeMap["running"].(bool)
						nodeType, _ := nodeMap["type"].(string)
						status := "running"
						if !running {
							status = "stopped"
						}
						si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
							Address: name,
							Role:    nodeType,
							Status:  status,
						})
					}
				}
			} else if len(nodesArr) == 1 {
				si.Cluster = &config.ClusterInfo{Mode: "Standalone"}
			}
		}
	}

	// 如果 management API 不可用，至少根据 cluster_name 判断
	if si.Cluster == nil && clusterName != "" {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "集群名", Value: clusterName})
	}

	msg := "连接成功 - RabbitMQ"
	if version != "" {
		msg = fmt.Sprintf("连接成功 - RabbitMQ %s", version)
	}

	return &config.Result{
		Success:    true,
		Message:    msg,
		ServerInfo: si,
	}, nil
}

func (c *RabbitMQConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "declare_queue",
			Label:       "创建队列",
			Description: "声明一个新队列",
			Params: []config.ActionParam{
				{Name: "queue", Label: "队列名", Placeholder: "test-queue", Required: true, Default: "connectest-test-queue"},
			},
		},
		{
			Name:        "message_test",
			Label:       "发送/消费消息",
			Description: "向队列发送消息或从队列消费消息，支持同时发送和消费",
			Params: []config.ActionParam{
				{Name: "do_produce", Label: "启用发送", Default: "true"},
				{Name: "do_consume", Label: "启用消费", Default: "true"},
				{Name: "produce_queue", Label: "发送队列", Placeholder: "test-queue", Default: "connectest-test-queue"},
				{Name: "message", Label: "消息内容", Placeholder: "hello world", Default: "hello from connectest"},
				{Name: "exchange", Label: "交换机", Placeholder: "(默认)", Default: ""},
				{Name: "consume_queue", Label: "消费队列", Placeholder: "test-queue", Default: "connectest-test-queue"},
			},
		},
		{
			Name:        "delete_queue",
			Label:       "删除队列",
			Description: "删除指定队列",
			Params: []config.ActionParam{
				{Name: "queue", Label: "队列名", Placeholder: "test-queue", Required: true, Default: "connectest-test-queue"},
			},
		},
		{
			Name:        "list_queues",
			Label:       "列出队列",
			Description: "列出所有队列及其消息数量",
		},
	}
}

func (c *RabbitMQConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// 上下文感知的连接
	type dialResult struct {
		conn *amqp.Connection
		err  error
	}
	dialCh := make(chan dialResult, 1)
	go func() {
		conn, err := amqp.DialConfig(c.buildURL(cfg), amqp.Config{
			Heartbeat: 10 * time.Second,
			Locale:    "en_US",
		})
		dialCh <- dialResult{conn, err}
	}()

	select {
	case <-ctx.Done():
		return nil, fmt.Errorf("连接已取消")
	case dr := <-dialCh:
		if dr.err != nil {
			return nil, fmt.Errorf("连接失败: %w", dr.err)
		}
		defer dr.conn.Close()

		ch, err := dr.conn.Channel()
		if err != nil {
			return nil, fmt.Errorf("创建 Channel 失败: %w", err)
		}
		defer ch.Close()
		return c.doExecuteAction(ctx, cfg, action, params, ch)
	}
}

func (c *RabbitMQConnector) doExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string, ch *amqp.Channel) (*config.Result, error) {
	switch action {
	case "declare_queue":
		queueName := params["queue"]
		if queueName == "" {
			return nil, fmt.Errorf("队列名不能为空")
		}

		q, err := ch.QueueDeclare(queueName, true, false, false, false, nil)
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("创建队列失败: %v", err)}, nil
		}
		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("队列 '%s' 创建成功", q.Name),
			Details: fmt.Sprintf("名称: %s\n消息数: %d\n消费者数: %d", q.Name, q.Messages, q.Consumers),
		}, nil

	case "message_test":
		doProduce := params["do_produce"] == "true"
		doConsume := params["do_consume"] == "true"
		produceQueue := params["produce_queue"]
		consumeQueue := params["consume_queue"]
		message := params["message"]
		exchange := params["exchange"]

		rabbitLog.Info("消息测试: produce=%v(queue=%s), consume=%v(queue=%s)", doProduce, produceQueue, doConsume, consumeQueue)

		if !doProduce && !doConsume {
			return nil, fmt.Errorf("请至少启用发送或消费")
		}

		var output strings.Builder

		// ─── 发送消息 ───
		if doProduce {
			if produceQueue == "" {
				output.WriteString("❌ 发送队列不能为空\n")
			} else {
				if message == "" {
					message = "hello from connectest"
				}
				_, err := ch.QueueDeclare(produceQueue, true, false, false, false, nil)
				if err != nil {
					output.WriteString(fmt.Sprintf("❌ 声明队列失败: %v\n", err))
				} else {
					err = ch.PublishWithContext(ctx, exchange, produceQueue, false, false, amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte(message),
						Timestamp:   time.Now(),
					})
					if err != nil {
						output.WriteString(fmt.Sprintf("❌ 发送失败: %v\n", err))
					} else {
						output.WriteString(fmt.Sprintf("✅ 发送成功 → 队列: %s, 消息: %s\n", produceQueue, message))
					}
				}
			}
		}

		// ─── 消费消息 ───
		if doConsume {
			if consumeQueue == "" {
				output.WriteString("❌ 消费队列不能为空\n")
			} else {
				if err := ch.Qos(1, 0, false); err != nil {
					output.WriteString(fmt.Sprintf("❌ 设置 QoS 失败: %v\n", err))
				} else {
					msgs, err := ch.ConsumeWithContext(ctx, consumeQueue, "", false, false, false, false, nil)
					if err != nil {
						output.WriteString(fmt.Sprintf("❌ 消费失败: %v\n", err))
					} else {
						select {
						case msg := <-msgs:
							msg.Ack(false)
							output.WriteString(fmt.Sprintf("✅ 消费成功 ← 队列: %s, 内容: %s\n", consumeQueue, string(msg.Body)))
						case <-ctx.Done():
							output.WriteString("⏹ 消费已取消\n")
						case <-time.After(5 * time.Second):
							output.WriteString("⚠ 消费超时 (5秒)，队列中可能没有消息\n")
						}
					}
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

	case "delete_queue":
		queueName := params["queue"]
		if queueName == "" {
			return nil, fmt.Errorf("队列名不能为空")
		}

		msgCount, err := ch.QueueDelete(queueName, false, false, false)
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("删除队列失败: %v", err)}, nil
		}
		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("队列 '%s' 删除成功（丢弃了 %d 条未消费消息）", queueName, msgCount),
		}, nil

	case "list_queues":
		queues := []string{"connectest-test-queue"}
		var output strings.Builder

		for _, qName := range queues {
			q, err := ch.QueueInspect(qName)
			if err != nil {
				continue
			}
			output.WriteString(fmt.Sprintf("%-30s 消息: %-6d 消费者: %d\n", q.Name, q.Messages, q.Consumers))
		}

		if output.Len() == 0 {
			return &config.Result{
				Success: true,
				Message: "未检测到已知队列（可通过创建队列来测试）",
			}, nil
		}

		return &config.Result{
			Success: true,
			Message: "队列信息获取成功",
			Details: output.String(),
		}, nil

	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}
