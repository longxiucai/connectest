package connector

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/logger"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var k8sLog = logger.NewModule("K8s-etcd")

// ─── K8s 如何使用 etcd ───
//
// K8s API Server 通过 gRPC 连接 etcd (mTLS)，使用 clientv3 客户端。
// 并非使用 HTTP REST API (/v3/kv/put 等)，而是直接走 gRPC 协议：
//
//   clientv3.New() -> gRPC dial -> etcd server (默认 2379)
//
// 数据存储在 /registry/ 前缀下，格式为 protobuf：
//
//   集群级资源:     /registry/{resource}/{name}
//   命名空间级资源: /registry/{resource}/{namespace}/{name}
//
// 常见路径：
//   /registry/namespaces/{name}           - 命名空间
//   /registry/minions/{name}              - 节点
//   /registry/pods/{ns}/{name}            - Pod
//   /registry/services/specs/{ns}/{name}  - Service
//   /registry/deployments/{ns}/{name}     - Deployment
//   /registry/configmaps/{ns}/{name}      - ConfigMap
//   /registry/secrets/{ns}/{name}         - Secret
//   /registry/serviceaccounts/{ns}/{name} - ServiceAccount
//   /registry/leases/{ns}/{name}          - Lease (leader election)
//   /registry/events/{ns}/{name}          - Event
//   /registry/clusterroles/{name}         - ClusterRole
//   /registry/clusterrolebindings/{name}  - ClusterRoleBinding

// K8sEtcdConnector 模拟 K8s API Server 连接和使用 etcd 的方式
type EtcdK8SConnector struct{}

func (c *EtcdK8SConnector) Name() string { return "K8s-etcd" }

func (c *EtcdK8SConnector) newClient(cfg config.Config) (*clientv3.Client, error) {
	endpoint := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 10 * time.Second,
		TLS:         buildTLSConfig(cfg),
		Username:    cfg.User,
		Password:    cfg.Password,
	})
	if err != nil {
		return nil, fmt.Errorf("创建 etcd client 失败: %w", err)
	}
	return client, nil
}



func (c *EtcdK8SConnector) newClientToEndpoint(endpoint string, tlsCfg *tls.Config) (*clientv3.Client, error) {
	client, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
		TLS:         tlsCfg,
	})
	if err != nil {
		return nil, err
	}
	return client, nil
}

func (c *EtcdK8SConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	k8sLog.Debug("连接 %s:%d (gRPC)", cfg.Host, cfg.Port)

	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: err.Error()}, nil
	}
	defer client.Close()

	endpoint := fmt.Sprintf("%s:%d", cfg.Host, cfg.Port)

	// 获取当前连接节点的状态
	start := time.Now()
	selfStatus, err := client.Status(ctx, endpoint)
	healthMs := time.Since(start).Milliseconds()
	if err != nil {
		k8sLog.Warn("获取自身状态失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("连接失败: %v", err)}, nil
	}
	k8sLog.Debug("状态获取成功, 耗时=%dms, 版本=%s, leader=%d", healthMs, selfStatus.Version, selfStatus.Leader)

	// 获取集群成员列表
	memberResp, err := client.MemberList(ctx)
	if err != nil {
		k8sLog.Warn("获取成员列表失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("获取成员列表失败: %v", err)}, nil
	}
	k8sLog.Debug("获取到 %d 个成员", len(memberResp.Members))

	// 构建 TLS 配置（供临时连接使用）
	tlsCfg := buildTLSConfig(cfg)

	si := &config.ServerInfo{
		Status:  "Running",
		Version: selfStatus.Version,
	}

	// 收集每个成员的状态
	type memberNode struct {
		addr    string
		name    string
		role    string
		dbSize  int64
		dbUse   int64
		raftIdx uint64
		online  bool
		took    int64
	}
	var nodes []memberNode

	for _, m := range memberResp.Members {
		mn := memberNode{name: m.Name}
		if len(m.ClientURLs) > 0 {
			mn.addr = strings.Join(m.ClientURLs, ", ")
		}

		if len(m.ClientURLs) == 0 {
			mn.online = true
			nodes = append(nodes, mn)
			continue
		}

		// 判断是否为当前连接的节点
		memberEndpoint := m.ClientURLs[0]
		isSelf := memberEndpoint == endpoint || memberEndpoint == fmt.Sprintf("https://%s", endpoint) || memberEndpoint == fmt.Sprintf("http://%s", endpoint)

		var st *clientv3.StatusResponse
		if isSelf {
			st = selfStatus
			mn.took = healthMs
		} else {
			// 创建临时 client 连接到该成员
			tmpClient, err := c.newClientToEndpoint(memberEndpoint, tlsCfg)
			if err != nil {
				k8sLog.Debug("创建到 %s 的临时连接失败: %v", m.Name, err)
				mn.online = true
				nodes = append(nodes, mn)
				continue
			}
			sStart := time.Now()
			st, err = tmpClient.Status(ctx, memberEndpoint)
			mn.took = time.Since(sStart).Milliseconds()
			tmpClient.Close()
			if err != nil {
				k8sLog.Debug("获取 %s 状态失败: %v", m.Name, err)
				mn.online = true
				nodes = append(nodes, mn)
				continue
			}
		}

		mn.online = true
		mn.dbSize = int64(st.DbSize)
		mn.dbUse = int64(st.DbSizeInUse)
		mn.raftIdx = st.RaftIndex

		// 判断角色
		if m.IsLearner {
			mn.role = "learner"
		} else if uint64(m.ID) == st.Leader {
			mn.role = "leader"
		} else {
			mn.role = "follower"
		}

		nodes = append(nodes, mn)
	}

	// 基本信息
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "存储前缀", Value: "/registry/",
	})
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "成员数量", Value: fmt.Sprintf("%d", len(memberResp.Members)),
	})
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "Health 延迟", Value: fmt.Sprintf("%dms", healthMs),
	})
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "Raft Term", Value: fmt.Sprintf("%d", selfStatus.RaftTerm),
	})

	// 统计 /registry 下的资源类型
	typeCounts := make(map[string]int)
	kvsResp, err := client.Get(ctx, "/registry/", clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(10000))
	if err == nil {
		for _, kv := range kvsResp.Kvs {
			key := strings.TrimPrefix(string(kv.Key), "/registry/")
			parts := strings.SplitN(key, "/", 2)
			if len(parts) >= 1 && parts[0] != "" {
				typeCounts[parts[0]]++
			}
		}
	}
	total := 0
	for _, v := range typeCounts {
		total += v
	}
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "资源类型数", Value: fmt.Sprintf("%d", len(typeCounts)),
	})
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "资源总数", Value: fmt.Sprintf("%d", total),
	})

	// 找 leader 名称
	leaderName := ""
	for _, mn := range nodes {
		if mn.role == "leader" {
			leaderName = mn.name
			break
		}
	}

	// 集群信息（含每节点状态）
	si.Cluster = &config.ClusterInfo{
		Mode:    "etcd Cluster",
		Summary: fmt.Sprintf("%d 个成员, Leader: %s", len(memberResp.Members), leaderName),
	}
	for _, mn := range nodes {
		status := "online"
		if !mn.online {
			status = "unreachable"
		}
		role := mn.role
		if role == "" {
			role = "unknown"
		}
		var info string
		if mn.online {
			parts := []string{}
			if mn.took > 0 {
				parts = append(parts, fmt.Sprintf("took=%dms", mn.took))
			}
			if mn.raftIdx > 0 {
				parts = append(parts, fmt.Sprintf("raft=%d", mn.raftIdx))
			}
			if mn.dbSize > 0 {
				parts = append(parts, fmt.Sprintf("db=%s", formatBytes(mn.dbSize)))
			}
			info = mn.name
			if len(parts) > 0 {
				info += " | " + strings.Join(parts, ", ")
			}
		} else {
			info = mn.name
		}
		si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
			Address: mn.addr,
			Role:    role,
			Status:  status,
			Info:    info,
		})
	}

	// 列出部分资源类型
	var typeList []string
	for t := range typeCounts {
		typeList = append(typeList, t)
	}
	sort.Strings(typeList)
	if len(typeList) > 15 {
		typeList = typeList[:15]
		typeList = append(typeList, "...")
	}
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "资源类型", Value: strings.Join(typeList, ", "),
	})

	msg := fmt.Sprintf("连接成功 - etcd %s (共 %d 个成员, health=%dms)", selfStatus.Version, len(memberResp.Members), healthMs)
	return &config.Result{
		Success:    true,
		Message:    msg,
		ServerInfo: si,
	}, nil
}

func (c *EtcdK8SConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "namespaces",
			Label:       "列出 Namespaces",
			Description: "列出集群中所有命名空间 (读取 /registry/namespaces/)",
		},
		{
			Name:        "resource_count",
			Label:       "资源统计",
			Description: "按资源类型统计 /registry/ 下的对象数量",
		},
		{
			Name:        "nodes",
			Label:       "列出 Nodes",
			Description: "列出集群中所有节点 (读取 /registry/minions/)",
		},
		{
			Name:        "pods",
			Label:       "列出 Pods",
			Description: "列出 Pod (可按 namespace 过滤)",
			Params: []config.ActionParam{
				{Name: "namespace", Label: "命名空间 (留空=全部)", Placeholder: "留空列出所有", Required: false, Default: ""},
			},
		},
		{
			Name:        "services",
			Label:       "列出 Services",
			Description: "列出 Service (可按 namespace 过滤)",
			Params: []config.ActionParam{
				{Name: "namespace", Label: "命名空间 (留空=全部)", Placeholder: "留空列出所有", Required: false, Default: ""},
			},
		},
		{
			Name:        "explore",
			Label:       "探索 /registry",
			Description: "探索 etcd 中 K8s 存储的键结构 (类似 etcdctl get /registry --prefix --keys-only)",
			Params: []config.ActionParam{
				{Name: "prefix", Label: "键前缀", Placeholder: "/registry", Required: false, Default: "/registry"},
				{Name: "limit", Label: "最大返回数 (0=不限)", Placeholder: "0", Required: false, Default: "200"},
				{Name: "depth", Label: "展示深度 (1-4)", Placeholder: "2", Required: false, Default: "2"},
			},
		},
		{
			Name:        "decode",
			Label:       "解码资源",
			Description: "读取并解码 K8s 存储的 protobuf 对象，提取 Kind/Namespace/Name 等元数据",
			Params: []config.ActionParam{
				{Name: "key", Label: "完整 key 路径", Placeholder: "/registry/pods/kube-system/coredns-xxx", Required: true, Default: ""},
			},
		},
		{
			Name:        "leases",
			Label:       "Leader 选举",
			Description: "查看 K8s 组件的 leader election leases (/registry/leases/kube-system/)",
		},
	}
}

func (c *EtcdK8SConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: err.Error()}, nil
	}
	defer client.Close()

	k8sLog.Debug("执行操作: %s, 参数: %v", action, params)

	switch action {
	case "namespaces":
		return c.actionNamespaces(ctx, client)
	case "resource_count":
		return c.actionResourceCount(ctx, client)
	case "nodes":
		return c.actionNodes(ctx, client)
	case "pods":
		return c.actionPods(ctx, client, params)
	case "services":
		return c.actionServices(ctx, client, params)
	case "explore":
		return c.actionExplore(ctx, client, params)
	case "decode":
		return c.actionDecode(ctx, client, params)
	case "leases":
		return c.actionLeases(ctx, client)
	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}

// ─── K8s 操作实现 ───
// 使用 etcd clientv3 gRPC 客户端，与 K8s apiserver 相同的方式

func (c *EtcdK8SConnector) actionNamespaces(ctx context.Context, client *clientv3.Client) (*config.Result, error) {
	k8sLog.Debug("查询 /registry/namespaces/")
	resp, err := client.Get(ctx, "/registry/namespaces/", clientv3.WithPrefix())
	if err != nil {
		k8sLog.Warn("查询 Namespaces 失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	count := len(resp.Kvs)
	k8sLog.Debug("找到 %d 个 Namespace", count)
	if count == 0 {
		return &config.Result{Success: true, Message: "未找到任何 Namespace"}, nil
	}

	var details strings.Builder
	for _, kv := range resp.Kvs {
		ns := lastSegment(string(kv.Key), "/registry/namespaces/")
		dobj, err := decodeFullK8sObject(kv.Value)
		details.WriteString(fmt.Sprintf("  %s", ns))
		if err == nil && dobj.UID != "" {
			details.WriteString(fmt.Sprintf("  (uid: %s)", dobj.UID))
		}
		details.WriteString("\n")
	}
	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("共 %d 个 Namespace", count),
		Details: details.String(),
	}, nil
}

func (c *EtcdK8SConnector) actionResourceCount(ctx context.Context, client *clientv3.Client) (*config.Result, error) {
	k8sLog.Debug("统计 /registry/ 下资源数量")
	resp, err := client.Get(ctx, "/registry/", clientv3.WithPrefix(), clientv3.WithKeysOnly(), clientv3.WithLimit(50000))
	if err != nil {
		k8sLog.Warn("统计资源失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	k8sLog.Debug("获取到 %d 个键", len(resp.Kvs))

	typeCounts := make(map[string]int)
	for _, kv := range resp.Kvs {
		key := strings.TrimPrefix(string(kv.Key), "/registry/")
		parts := strings.SplitN(key, "/", 2)
		if len(parts) >= 1 && parts[0] != "" {
			typeCounts[parts[0]]++
		}
	}

	type kv2 struct {
		key   string
		count int
	}
	var sorted []kv2
	for k, v := range typeCounts {
		sorted = append(sorted, kv2{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].count > sorted[j].count
	})

	var details strings.Builder
	details.WriteString(fmt.Sprintf("%-30s  %s\n", "资源类型", "数量"))
	details.WriteString(strings.Repeat("─", 45) + "\n")
	total := 0
	for _, item := range sorted {
		details.WriteString(fmt.Sprintf("%-30s  %d\n", item.key, item.count))
		total += item.count
	}
	details.WriteString(strings.Repeat("─", 45) + "\n")
	details.WriteString(fmt.Sprintf("%-30s  %d\n", "总计", total))

	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("共 %d 种资源类型, %d 个对象", len(typeCounts), total),
		Details: details.String(),
	}, nil
}

func (c *EtcdK8SConnector) actionNodes(ctx context.Context, client *clientv3.Client) (*config.Result, error) {
	k8sLog.Debug("查询 /registry/minions/")
	resp, err := client.Get(ctx, "/registry/minions/", clientv3.WithPrefix())
	if err != nil {
		k8sLog.Warn("查询 Nodes 失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	count := len(resp.Kvs)
	k8sLog.Debug("找到 %d 个 Node", count)
	if count == 0 {
		return &config.Result{Success: true, Message: "未找到任何 Node"}, nil
	}

	var details strings.Builder
	for _, kv := range resp.Kvs {
		name := lastSegment(string(kv.Key), "/registry/minions/")
		dobj, err := decodeFullK8sObject(kv.Value)
		line := fmt.Sprintf("  %s", name)
		if err == nil && dobj.UID != "" {
			line += fmt.Sprintf("  uid=%s", dobj.UID)
		}
		details.WriteString(line + "\n")
	}
	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("共 %d 个 Node", count),
		Details: details.String(),
	}, nil
}

func (c *EtcdK8SConnector) actionPods(ctx context.Context, client *clientv3.Client, params map[string]string) (*config.Result, error) {
	prefix := "/registry/pods/"
	if ns := params["namespace"]; ns != "" {
		prefix = "/registry/pods/" + ns + "/"
	}
	k8sLog.Debug("查询 Pods, prefix=%s", prefix)
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		k8sLog.Warn("查询 Pods 失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	count := len(resp.Kvs)
	k8sLog.Debug("找到 %d 个 Pod", count)
	if count == 0 {
		return &config.Result{Success: true, Message: "未找到任何 Pod"}, nil
	}

	var details strings.Builder
	details.WriteString(fmt.Sprintf("%-30s  %-40s  %s\n", "NAMESPACE", "NAME", "UID"))
	details.WriteString(strings.Repeat("─", 50) + "\n")
	for _, kv := range resp.Kvs {
		ns, name := nsAndName(string(kv.Key), "/registry/pods/")
		dobj, err := decodeFullK8sObject(kv.Value)
		uid := ""
		if err == nil {
			uid = dobj.UID
		}
		details.WriteString(fmt.Sprintf("%-30s  %-40s  %s\n", ns, name, uid))
	}
	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("共 %d 个 Pod", count),
		Details: details.String(),
	}, nil
}

func (c *EtcdK8SConnector) actionServices(ctx context.Context, client *clientv3.Client, params map[string]string) (*config.Result, error) {
	prefix := "/registry/services/specs/"
	if ns := params["namespace"]; ns != "" {
		prefix = "/registry/services/specs/" + ns + "/"
	}
	k8sLog.Debug("查询 Services, prefix=%s", prefix)
	resp, err := client.Get(ctx, prefix, clientv3.WithPrefix())
	if err != nil {
		k8sLog.Warn("查询 Services 失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	count := len(resp.Kvs)
	k8sLog.Debug("找到 %d 个 Service", count)
	if count == 0 {
		return &config.Result{Success: true, Message: "未找到任何 Service"}, nil
	}

	var details strings.Builder
	details.WriteString(fmt.Sprintf("%-30s  %-40s  %s\n", "NAMESPACE", "NAME", "UID"))
	details.WriteString(strings.Repeat("─", 50) + "\n")
	for _, kv := range resp.Kvs {
		ns, name := nsAndName(string(kv.Key), "/registry/services/specs/")
		dobj, err := decodeFullK8sObject(kv.Value)
		uid := ""
		if err == nil {
			uid = dobj.UID
		}
		details.WriteString(fmt.Sprintf("%-30s  %-40s  %s\n", ns, name, uid))
	}
	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("共 %d 个 Service", count),
		Details: details.String(),
	}, nil
}

func (c *EtcdK8SConnector) actionExplore(ctx context.Context, client *clientv3.Client, params map[string]string) (*config.Result, error) {
	prefix := params["prefix"]
	if prefix == "" {
		prefix = "/registry"
	}
	var limit int64 = 200
	if l := params["limit"]; l != "" && l != "0" {
		fmt.Sscanf(l, "%d", &limit)
	}
	depth := 2
	if d := params["depth"]; d != "" {
		fmt.Sscanf(d, "%d", &depth)
		if depth < 1 {
			depth = 1
		}
		if depth > 4 {
			depth = 4
		}
	}
	k8sLog.Debug("探索 %s, limit=%d, depth=%d", prefix, limit, depth)

	opts := []clientv3.OpOption{
		clientv3.WithPrefix(),
		clientv3.WithKeysOnly(),
	}
	if limit > 0 {
		opts = append(opts, clientv3.WithLimit(limit))
	}

	resp, err := client.Get(ctx, prefix, opts...)
	if err != nil {
		k8sLog.Warn("探索 %s 失败: %v", prefix, err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	count := len(resp.Kvs)
	k8sLog.Debug("在 %s 下找到 %d 个键", prefix, count)
	if count == 0 {
		return &config.Result{Success: true, Message: fmt.Sprintf("'%s' 下没有找到任何键", prefix)}, nil
	}

	tree := make(map[string]int)
	for _, kv := range resp.Kvs {
		trimmed := strings.TrimPrefix(string(kv.Key), prefix)
		trimmed = strings.TrimPrefix(trimmed, "/")
		parts := strings.Split(trimmed, "/")
		if len(parts) > depth {
			parts = parts[:depth]
		}
		key := strings.Join(parts, "/")
		tree[key]++
	}

	var sorted []string
	for k := range tree {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	var details strings.Builder
	for _, k := range sorted {
		cnt := tree[k]
		indent := strings.Count(k, "/")
		label := lastPart(k)
		details.WriteString(fmt.Sprintf("%s%s/", strings.Repeat("  ", indent), label))
		if cnt > 1 || !strings.HasSuffix(k, label) {
			details.WriteString(fmt.Sprintf(" (%d)", cnt))
		}
		details.WriteString("\n")
	}

	msg := fmt.Sprintf("'%s' 下 %d 个键", prefix, count)
	if int64(count) >= limit {
		msg += fmt.Sprintf(" (已达到 %d 上限)", limit)
	}
	return &config.Result{
		Success: true,
		Message: msg,
		Details: details.String(),
	}, nil
}

func (c *EtcdK8SConnector) actionDecode(ctx context.Context, client *clientv3.Client, params map[string]string) (*config.Result, error) {
	key := params["key"]
	if key == "" {
		return nil, fmt.Errorf("key 路径不能为空")
	}

	k8sLog.Debug("解码资源: %s", key)
	resp, err := client.Get(ctx, key)
	if err != nil {
		k8sLog.Warn("读取 key=%s 失败: %v", key, err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	if len(resp.Kvs) == 0 {
		k8sLog.Debug("key=%s 不存在", key)
		return &config.Result{Success: true, Message: fmt.Sprintf("键 '%s' 不存在", key)}, nil
	}

	kv := resp.Kvs[0]
	k8sLog.Debug("key=%s, size=%d bytes, rev=create:%d,mod:%d,ver:%d", key, len(kv.Value), kv.CreateRevision, kv.ModRevision, kv.Version)
	dobj, err := decodeFullK8sObject(kv.Value)
	if err != nil {
		k8sLog.Warn("解码 key=%s 失败: %v", key, err)
		return &config.Result{Success: false, Message: fmt.Sprintf("解码失败: %v", err)}, nil
	}
	k8sLog.Debug("解码成功: %s/%s (ns=%s)", dobj.Kind, dobj.Name, dobj.Namespace)

	var details strings.Builder
	details.WriteString(fmt.Sprintf("Key:    %s\n", key))
	details.WriteString(fmt.Sprintf("Size:   %d bytes\n", len(kv.Value)))
	details.WriteString(fmt.Sprintf("Rev:    create=%d, mod=%d, ver=%d\n",
		kv.CreateRevision, kv.ModRevision, kv.Version))
	details.WriteString(strings.Repeat("─", 50) + "\n")
	details.WriteString("K8s 元数据 (protobuf 解码):\n")
	msg, fmtDetails := formatDecodedK8sObject(dobj)
	details.WriteString(fmtDetails)

	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("解码成功: %s", msg),
		Details: details.String(),
	}, nil
}

func (c *EtcdK8SConnector) actionLeases(ctx context.Context, client *clientv3.Client) (*config.Result, error) {
	k8sLog.Debug("查询 /registry/leases/kube-system/")
	resp, err := client.Get(ctx, "/registry/leases/kube-system/", clientv3.WithPrefix())
	if err != nil {
		k8sLog.Warn("查询 Leases 失败: %v", err)
		return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
	}
	count := len(resp.Kvs)
	k8sLog.Debug("找到 %d 个 Lease", count)
	if count == 0 {
		return &config.Result{Success: true, Message: "未找到任何 Lease (leader election)"}, nil
	}

	var details strings.Builder
	details.WriteString(fmt.Sprintf("%-35s  %s\n", "LEASE NAME", "HOLDER (IDENTITY)"))
	details.WriteString(strings.Repeat("─", 50) + "\n")
	for _, kv := range resp.Kvs {
		name := lastSegment(string(kv.Key), "/registry/leases/kube-system/")
		dobj, err := decodeFullK8sObject(kv.Value)
		holder := ""
		if err == nil && dobj.RawJSON != "" {
			var raw map[string]json.RawMessage
			if json.Unmarshal([]byte(dobj.RawJSON), &raw) == nil {
				if specRaw, ok := raw["spec"]; ok {
					var spec struct {
						HolderIdentity *string `json:"holderIdentity"`
					}
					if json.Unmarshal(specRaw, &spec) == nil && spec.HolderIdentity != nil {
						holder = *spec.HolderIdentity
					}
				}
			}
		}
		if holder == "" && err == nil {
			holder = dobj.Name
		}
		details.WriteString(fmt.Sprintf("%-35s  %s\n", name, holder))
	}
	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("共 %d 个 Lease (leader election)", count),
		Details: details.String(),
	}, nil
}

// ─── 辅助函数 ───

func lastSegment(key, prefix string) string {
	s := strings.TrimPrefix(key, prefix)
	if idx := strings.LastIndex(s, "/"); idx >= 0 {
		return s[idx+1:]
	}
	return s
}

func nsAndName(key, prefix string) (string, string) {
	s := strings.TrimPrefix(key, prefix)
	parts := strings.SplitN(s, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "", s
}

func lastPart(path string) string {
	if idx := strings.LastIndex(path, "/"); idx >= 0 {
		return path[idx+1:]
	}
	return path
}
