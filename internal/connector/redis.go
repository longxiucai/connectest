package connector

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/logger"
	"github.com/redis/go-redis/v9"
)

var redisLog = logger.NewModule("Redis")

type RedisConnector struct{}

func (c *RedisConnector) Name() string { return "Redis" }

func (c *RedisConnector) newClient(cfg config.Config) *redis.Client {
	opts := &redis.Options{
		Addr: fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		DB:   0,
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	return redis.NewClient(opts)
}

func (c *RedisConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	redisLog.Info("开始测试连接 %s:%d", cfg.Host, cfg.Port)
	client := c.newClient(cfg)
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		redisLog.Error("Ping 失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("连接失败: %v", err)}, nil
	}
	redisLog.Info("Ping 成功，开始获取服务器信息")

	si := &config.ServerInfo{Status: "Running"}

	// 解析 server info
	parseInfo := func(section string) map[string]string {
		result := make(map[string]string)
		data, err := client.Info(ctx, section).Result()
		if err != nil {
			return result
		}
		for line := range strings.SplitSeq(data, "\n") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				result[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
		return result
	}

	serverInfo := parseInfo("server")
	si.Version = serverInfo["redis_version"]
	redisLog.Info("Redis 版本: %s, 模式: %s", si.Version, serverInfo["redis_mode"])
	if mode, ok := serverInfo["redis_mode"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "运行模式", Value: mode})
	}
	if os, ok := serverInfo["os"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "操作系统", Value: os})
	}
	if uptime, ok := serverInfo["uptime_in_days"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "运行天数", Value: uptime})
	}
	if tcpPort, ok := serverInfo["tcp_port"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "TCP 端口", Value: tcpPort})
	}

	// 内存和客户端信息
	memInfo := parseInfo("memory")
	if usedMem, ok := memInfo["used_memory_human"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "已用内存", Value: usedMem})
	}
	if peakMem, ok := memInfo["used_memory_peak_human"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "内存峰值", Value: peakMem})
	}

	clientInfo := parseInfo("clients")
	if connCount, ok := clientInfo["connected_clients"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "连接客户端数", Value: connCount})
	}

	statsInfo := parseInfo("stats")
	if cmdCount, ok := statsInfo["total_commands_processed"]; ok {
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "总命令处理数", Value: cmdCount})
	}

	// keyspace
	ksInfo := parseInfo("keyspace")
	if len(ksInfo) > 0 {
		redisLog.Info("Keyspace 信息: %d 个数据库", len(ksInfo))
		var dbs []string
		for db, info := range ksInfo {
			dbs = append(dbs, fmt.Sprintf("%s(%s)", db, info))
		}
		si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "数据库", Value: strings.Join(dbs, ", ")})
	}

	replInfo := parseInfo("replication")
	role := replInfo["role"]
	redisLog.Info("角色: %s", role)
	if serverInfo["redis_mode"] != "cluster" &&
		serverInfo["redis_mode"] != "sentinel" {
		if role == "master" {
			slaveCount := 0
			fmt.Sscanf(replInfo["connected_slaves"], "%d", &slaveCount)

			if slaveCount > 0 {
				si.Cluster = &config.ClusterInfo{
					Mode:    "Master-Slave Replication",
					Summary: fmt.Sprintf("Master，%d 个从节点", slaveCount),
				}
				replRaw, _ := client.Info(ctx, "replication").Result()
				for _, line := range strings.Split(replRaw, "\n") {
					if !strings.HasPrefix(line, "slave") {
						continue
					}
					parts := strings.SplitN(line, ":", 2)
					if len(parts) != 2 {
						continue
					}
					node := config.NodeInfo{
						Role:   "Slave",
						Status: "online",
					}
					for _, f := range strings.Split(parts[1], ",") {
						kv := strings.SplitN(f, "=", 2)
						if len(kv) != 2 {
							continue
						}
						switch kv[0] {
						case "ip":
							node.Address = kv[1]
						case "port":
							node.Address += ":" + kv[1]
						case "state":
							node.Status = kv[1]
						}
					}
					si.Cluster.Nodes = append(si.Cluster.Nodes, node)
				}
			}

		} else if role == "slave" {
			si.Cluster = &config.ClusterInfo{
				Mode: "Master-Slave Replication (Slave)",
			}
			host := replInfo["master_host"]
			port := replInfo["master_port"]
			si.Cluster.Summary = fmt.Sprintf("Master: %s:%s", host, port)
			si.Cluster.Nodes = append(
				si.Cluster.Nodes,
				config.NodeInfo{
					Address: host + ":" + port,
					Role:    "Master",
					Status:  "online",
				},
			)
		}
	}

	// Sentinel 检测
	if sentinelInfo, ok := serverInfo["redis_mode"]; ok && sentinelInfo == "sentinel" {
		redisLog.Info("检测到 Sentinel 模式")
		si.Cluster = &config.ClusterInfo{Mode: "Sentinel"}
		// 获取监控的主节点列表
		mastersResult, err := client.Do(ctx, "SENTINEL", "MASTERS").Result()
		if err == nil && mastersResult != nil {
			if mastersList, ok := mastersResult.([]any); ok {
				si.Cluster.Summary = fmt.Sprintf("Sentinel 模式，监控 %d 个主节点", len(mastersList))

				for i, masterRaw := range mastersList {
					if masterSlice, ok := masterRaw.([]interface{}); ok {
						masterName := fmt.Sprintf("master-%d", i)
						masterAddr := ""
						slaveCount := 0
						sentinelCount := 0
						masterStatus := "unknown"

						var masterIP, masterPort string
						var lastOkPingReply, linkPendingCommands string
						for j := 0; j < len(masterSlice)-1; j += 2 {
							if key, ok := masterSlice[j].(string); ok {
								if val, ok := masterSlice[j+1].(string); ok {
									switch key {
									case "name":
										masterName = val
									case "ip":
										masterIP = val
									case "port":
										masterPort = val
									case "num-slaves":
										fmt.Sscanf(val, "%d", &slaveCount)
									case "num-other-sentinels":
										fmt.Sscanf(val, "%d", &sentinelCount)
									case "flags":
										if strings.Contains(val, "s_down") {
											masterStatus = "s_down"
										} else if strings.Contains(val, "o_down") {
											masterStatus = "o_down"
										} else if strings.Contains(val, "master") {
											masterStatus = "online"
										}
									case "last-ok-ping-reply":
										lastOkPingReply = val
									case "link-pending-commands":
										linkPendingCommands = val
									}
								}
							}
						}
						masterAddr = masterIP + ":" + masterPort

						nodeInfo := fmt.Sprintf("从节点: %d, 哨兵: %d", slaveCount, sentinelCount)
						if lastOkPingReply != "" {
							nodeInfo += fmt.Sprintf(" 最近回包: %sms", lastOkPingReply)
						}
						if linkPendingCommands != "" {
							nodeInfo += fmt.Sprintf(" link_pending_commands: %s", linkPendingCommands)
						}

						if masterStatus != "online" {
							nodeInfo += fmt.Sprintf(" status: %s", masterStatus)
						}

						si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
							Address: masterAddr,
							Role:    "Master",
							Status:  masterStatus,
							Info:    nodeInfo,
							Name:    masterName,
						})

						// 获取该主节点的从节点
						slavesResult, err := client.Do(ctx, "SENTINEL", "SLAVES", masterName).Result()
						if err == nil && slavesResult != nil {
							if slavesList, ok := slavesResult.([]any); ok {
								for _, slaveRaw := range slavesList {
									if slaveSlice, ok := slaveRaw.([]any); ok {
										var slaveIP, slavePort string
										slaveStatus := "unknown"
										slaveLink := "unknown"
										var slaveLastOkPingReply, slaveLinkPendingCommands string

										for k := 0; k < len(slaveSlice)-1; k += 2 {
											if key, ok := slaveSlice[k].(string); ok {
												if val, ok := slaveSlice[k+1].(string); ok {
													switch key {
													case "ip":
														slaveIP = val
													case "port":
														slavePort = val
													case "flags":
														if strings.Contains(val, "s_down") {
															slaveStatus = "s_down"
														} else if strings.Contains(val, "o_down") {
															slaveStatus = "o_down"
														} else if strings.Contains(val, "slave") {
															slaveStatus = "online"
														}
													case "master-link-status":
														slaveLink = val
													case "last-ok-ping-reply":
														slaveLastOkPingReply = val
													case "link-pending-commands":
														slaveLinkPendingCommands = val
													}
												}
											}
										}

										slaveAddr := slaveIP + ":" + slavePort
										slaveInfo := fmt.Sprintf("连接: %s", slaveLink)
										if slaveLastOkPingReply != "" {
											slaveInfo += fmt.Sprintf(" 最近回包: %sms", slaveLastOkPingReply)
										}
										if slaveLinkPendingCommands != "" {
											slaveInfo += fmt.Sprintf(" link_pending_commands: %s", slaveLinkPendingCommands)
										}
										si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
											Address: slaveAddr,
											Role:    "Slave",
											Status:  slaveStatus,
											Info:    slaveInfo,
											Name:    masterName,
										})
									}
								}
							}
						}
					}
				}
			}
		}

		// 获取当前 Sentinel 的 ID
		sentinelID, err := client.Do(ctx, "SENTINEL", "MYID").Result()
		if err == nil && sentinelID != nil {
			if idBytes, ok := sentinelID.([]byte); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{
					Label: "Sentinel ID",
					Value: string(idBytes),
				})
			}
		}

		// 获取 Sentinel 监控的主节点数量
		sentinelMasterCount, err := client.Do(ctx, "SENTINEL", "MASTER", "mymaster").Result()
		if err == nil && sentinelMasterCount != nil {
			if masterSlice, ok := sentinelMasterCount.([]interface{}); ok && len(masterSlice) > 0 {
				si.InfoItems = append(si.InfoItems, config.InfoItem{
					Label: "监控节点",
					Value: fmt.Sprintf("%d 个", len(si.Cluster.Nodes)),
				})
			}
		}
	}

	// Redis Cluster
	if clusterInfo, ok := serverInfo["redis_mode"]; ok && clusterInfo == "cluster" {
		redisLog.Info("检测到 Redis Cluster 模式")
		clusterInfoRaw, _ := client.ClusterInfo(ctx).Result()
		clusterInfoMap := make(map[string]string)
		for _, line := range strings.Split(clusterInfoRaw, "\n") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				clusterInfoMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}

		clusterState := clusterInfoMap["cluster_state"]
		knownNodes := clusterInfoMap["cluster_known_nodes"]
		slotsAssigned := clusterInfoMap["cluster_slots_assigned"]
		slotsOk := clusterInfoMap["cluster_slots_ok"]
		slotsFail := clusterInfoMap["cluster_slots_fail"]

		summary := fmt.Sprintf("状态: %s, %s 个节点, 槽位: %s/%s",
			clusterState, knownNodes, slotsOk, slotsAssigned)
		if slotsFail != "0" {
			summary += fmt.Sprintf(" (故障槽位: %s)", slotsFail)
		}

		si.Cluster = &config.ClusterInfo{
			Mode:    "Redis Cluster",
			Summary: summary,
		}

		nodesRaw, _ := client.ClusterNodes(ctx).Result()
		if nodesRaw != "" {
			redisLog.Info("Cluster 节点信息获取成功")
			for _, line := range strings.Split(nodesRaw, "\n") {
				line = strings.TrimSpace(line)
				if line == "" {
					continue
				}
				fields := strings.Fields(line)
				if len(fields) < 8 {
					continue
				}

				nodeID := fields[0]
				addr := fields[1]
				flags := fields[2]
				masterID := fields[3]
				linkState := fields[7]

				role := "master"
				if strings.Contains(flags, "slave") || strings.Contains(flags, "failover") {
					role = "slave"
				}

				status := "connected"
				if linkState != "connected" {
					status = linkState
				}
				if strings.Contains(flags, "fail") {
					status = "fail"
				}

				var slotInfo string
				if role == "master" && len(fields) > 8 {
					slotRanges := fields[8:]
					slotCount := 0
					var ranges []string
					for i := 0; i < len(slotRanges); i++ {
						s := slotRanges[i]
						if strings.Contains(s, "-") {
							parts := strings.SplitN(s, "-", 2)
							if len(parts) == 2 {
								var start, end int
								fmt.Sscanf(parts[0], "%d", &start)
								fmt.Sscanf(parts[1], "%d", &end)
								slotCount += end - start + 1
								ranges = append(ranges, s)
							}
						} else {
							slotCount++
							ranges = append(ranges, s)
						}
					}
					slotInfo = fmt.Sprintf("槽位: %d 个", slotCount)
					if len(ranges) <= 3 {
						slotInfo += fmt.Sprintf(" (%s)", strings.Join(ranges, ", "))
					}
				} else if role == "slave" && masterID != "-" {
					slotInfo = fmt.Sprintf("主节点: %s...", masterID[:8])
				}

				nodeFlags := []string{}
				if strings.Contains(flags, "myself") {
					nodeFlags = append(nodeFlags, "本节点")
				}
				if strings.Contains(flags, "fail") {
					nodeFlags = append(nodeFlags, "故障")
				}

				var info string
				parts := []string{fmt.Sprintf("ID: %s...", nodeID[:8])}
				if slotInfo != "" {
					parts = append(parts, slotInfo)
				}
				if len(nodeFlags) > 0 {
					parts = append(parts, strings.Join(nodeFlags, ", "))
				}
				info = strings.Join(parts, " | ")

				si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
					Address: addr,
					Role:    role,
					Status:  status,
					Info:    info,
				})
			}
		}
	}

	msg := "连接成功"
	if si.Version != "" {
		msg = fmt.Sprintf("连接成功 - Redis %s", si.Version)
	}
	redisLog.Info("连接测试完成: %s", msg)

	return &config.Result{
		Success:    true,
		Message:    msg,
		ServerInfo: si,
	}, nil
}

func (c *RedisConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "message_test",
			Label:       "设置/读取测试",
			Description: "向 Redis 写入键值对或读取键值对，支持同时设置和读取",
			Params: []config.ActionParam{
				{Name: "do_set", Label: "启用设置", Default: "true"},
				{Name: "do_get", Label: "启用读取", Default: "true"},
				{Name: "set_key", Label: "设置键名", Placeholder: "test_key", Default: "connectest_test_key"},
				{Name: "set_value", Label: "设置值", Placeholder: "hello world", Default: "hello_connectest"},
				{Name: "set_ttl", Label: "过期时间(秒)", Placeholder: "60", Default: "60"},
				{Name: "get_key", Label: "读取键名", Placeholder: "test_key", Default: "connectest_test_key"},
			},
		},
		{
			Name:        "keys",
			Label:       "列出 Key",
			Description: "列出匹配模式的所有键",
			Params: []config.ActionParam{
				{Name: "pattern", Label: "匹配模式", Placeholder: "*", Required: true, Default: "*"},
			},
		},
		{
			Name:        "info",
			Label:       "服务器信息",
			Description: "获取 Redis 服务器详细信息",
			Params: []config.ActionParam{
				{Name: "section", Label: "信息段", Placeholder: "all", Required: false, Default: "server"},
			},
		},
		{
			Name:        "command",
			Label:       "执行命令",
			Description: "执行任意 Redis 命令，如 cluster nodes、dbsize、client list 等",
			Params: []config.ActionParam{
				{Name: "command", Label: "命令", Placeholder: "cluster nodes", Required: true, Default: "ping"},
			},
		},
	}
}

func formatRedisValue(val any) string {
	if val == nil {
		return "(nil)"
	}
	switch v := val.(type) {
	case string:
		if v == "" {
			return "(empty string)"
		}
		return v
	case int64:
		return fmt.Sprintf("(integer) %d", v)
	case []byte:
		if len(v) == 0 {
			return "(empty bulk string)"
		}
		return string(v)
	case []any:
		if len(v) == 0 {
			return "(empty array)"
		}
		var lines []string
		for i, item := range v {
			lines = append(lines, fmt.Sprintf("%d) %s", i+1, formatRedisValue(item)))
		}
		return strings.Join(lines, "\n")
	default:
		rv := reflect.ValueOf(val)
		if rv.Kind() == reflect.Slice {
			return fmt.Sprintf("%v", val)
		}
		return fmt.Sprintf("%v", val)
	}
}

func (c *RedisConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	redisLog.Info("执行操作: %s, 参数: %v", action, params)

	client := c.newClient(cfg)
	defer client.Close()

	switch action {
	case "message_test":
		redisLog.Debug("执行 SET/GET 测试")
		doSet := params["do_set"] == "true"
		doGet := params["do_get"] == "true"
		setKey := params["set_key"]
		setValue := params["set_value"]
		ttlStr := params["set_ttl"]
		getKey := params["get_key"]

		if !doSet && !doGet {
			return nil, fmt.Errorf("请至少启用设置或读取")
		}

		var output strings.Builder

		// ─── SET 操作 ───
		if doSet {
			if setKey == "" {
				output.WriteString("❌ 设置键名不能为空\n")
			} else {
				if setValue == "" {
					setValue = "hello_connectest"
				}
				ttl := 60 * time.Second
				if ttlStr != "" {
					var ttlSec int
					if _, err := fmt.Sscanf(ttlStr, "%d", &ttlSec); err == nil && ttlSec > 0 {
						ttl = time.Duration(ttlSec) * time.Second
					}
				}
				redisLog.Debug("SET %s = %s (TTL: %s)", setKey, setValue, ttl)
				if err := client.Set(ctx, setKey, setValue, ttl).Err(); err != nil {
					redisLog.Error("SET 失败: %v", err)
					output.WriteString(fmt.Sprintf("❌ 设置失败: %v\n", err))
				} else {
					output.WriteString(fmt.Sprintf("✅ 设置成功 → 键: %s, 值: %s, TTL: %s\n", setKey, setValue, ttl))
				}
			}
		}

		// ─── GET 操作 ───
		if doGet {
			if getKey == "" {
				output.WriteString("❌ 读取键名不能为空\n")
			} else {
				redisLog.Debug("GET %s", getKey)
				got, err := client.Get(ctx, getKey).Result()
				if err != nil {
					redisLog.Error("GET 失败: %v", err)
					output.WriteString(fmt.Sprintf("❌ 读取失败: %v\n", err))
				} else {
					output.WriteString(fmt.Sprintf("✅ 读取成功 ← 键: %s, 值: %s\n", getKey, got))
				}
			}
		}

		result := output.String()
		success := !strings.Contains(result, "❌")
		msg := "设置/读取测试完成"
		if !success {
			msg = "设置/读取测试完成（存在错误）"
		}
		return &config.Result{
			Success: success,
			Message: msg,
			Details: result,
		}, nil

	case "keys":
		pattern := params["pattern"]
		if pattern == "" {
			pattern = "*"
		}

		redisLog.Info("执行 KEYS %s", pattern)
		keys, err := client.Keys(ctx, pattern).Result()
		if err != nil {
			redisLog.Error("KEYS 失败: %v", err)
			return &config.Result{Success: false, Message: fmt.Sprintf("KEYS 失败: %v", err)}, nil
		}
		redisLog.Info("KEYS 返回 %d 个键", len(keys))

		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("匹配到 %d 个键", len(keys)),
			Details: strings.Join(keys, "\n"),
		}, nil

	case "info":
		section := params["section"]
		if section == "" {
			section = "server"
		}

		redisLog.Info("执行 INFO %s", section)
		info, err := client.Info(ctx, section).Result()
		if err != nil {
			redisLog.Error("INFO 失败: %v", err)
			return &config.Result{Success: false, Message: fmt.Sprintf("INFO 失败: %v", err)}, nil
		}

		return &config.Result{
			Success: true,
			Message: "获取服务器信息成功",
			Details: info,
		}, nil

	case "command":
		cmdStr := strings.TrimSpace(params["command"])
		if cmdStr == "" {
			return &config.Result{Success: false, Message: "命令不能为空"}, nil
		}

		parts := strings.Fields(cmdStr)
		parts[0] = strings.ToUpper(parts[0])
		allArgs := make([]any, len(parts))
		for i, a := range parts {
			allArgs[i] = a
		}

		redisLog.Info("执行自定义命令: %s", cmdStr)
		cmd := client.Do(ctx, allArgs...)
		if cmd.Err() != nil {
			redisLog.Error("命令 %s 执行失败: %v", cmdStr, cmd.Err())
			return &config.Result{
				Success: false,
				Message: fmt.Sprintf("命令执行失败: %v", cmd.Err()),
				Details: fmt.Sprintf("CMD: %s\nERROR: %v", cmdStr, cmd.Err()),
			}, nil
		}

		val := cmd.Val()
		detail := formatRedisValue(val)
		redisLog.Info("命令 %s 执行成功", cmdStr)

		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("命令 %s 执行成功", cmdStr),
			Details: detail,
		}, nil

	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}
