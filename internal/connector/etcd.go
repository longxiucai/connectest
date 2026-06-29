package connector

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/logger"
)

var etcdLog = logger.NewModule("etcd")

// ─── etcd v3 gRPC-Gateway REST API 客户端 ───
// 使用纯标准库 net/http 实现，不依赖 go.etcd.io/etcd 等外部库。
// etcd v3.3+ 在同一端口（默认 2379）上提供 gRPC-Gateway REST API。

type etcdClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func newEtcdClient(cfg config.Config) (*etcdClient, error) {
	scheme := "http"
	if cfg.UseTLS {
		scheme = "https"
	}

	transport := &http.Transport{}
	if cfg.UseTLS {
		tlsCfg := &tls.Config{}

		// 加载 CA 证书
		if cfg.CACert != "" {
			caCert, err := resolvePEM(cfg.CACert)
			if err != nil {
				return nil, fmt.Errorf("加载 CA 证书失败: %w", err)
			}
			etcdLog.Debug("CA 证书已加载: %d 字节, 首行: %.60s", len(caCert), firstLine(caCert))
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				block, _ := pem.Decode(caCert)
				if block == nil {
					preview := string(caCert)
					if len(preview) > 120 {
						preview = preview[:120] + "..."
					}
					return nil, fmt.Errorf("CA 证书中未找到有效 PEM 块，内容预览:\n%s", preview)
				}
				return nil, fmt.Errorf("CA 证书 PEM 块类型为 %q，期望 CERTIFICATE", block.Type)
			}
			tlsCfg.RootCAs = caCertPool
		} else {
			// 未提供 CA 证书时跳过验证
			tlsCfg.InsecureSkipVerify = true
		}

		// 加载客户端证书和密钥（mTLS）
		if cfg.Cert != "" && cfg.Key != "" {
			certPEM, err := resolvePEM(cfg.Cert)
			if err != nil {
				return nil, fmt.Errorf("加载客户端证书失败: %w", err)
			}
			keyPEM, err := resolvePEM(cfg.Key)
			if err != nil {
				return nil, fmt.Errorf("加载客户端私钥失败: %w", err)
			}
			cert, err := tls.X509KeyPair(certPEM, keyPEM)
			if err != nil {
				return nil, fmt.Errorf("解析客户端证书/私钥失败: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{cert}
		}

		transport.TLSClientConfig = tlsCfg
	}

	return &etcdClient{
		baseURL: fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, cfg.Port),
		httpClient: &http.Client{
			Timeout:   30 * time.Second,
			Transport: transport,
		},
	}, nil
}

// doRequest 发送 HTTP 请求到 etcd gRPC-Gateway REST API
func (c *etcdClient) doRequest(method, path string, body interface{}) (map[string]interface{}, error) {
	var reqBody io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("序列化请求失败: %w", err)
		}
		reqBody = bytes.NewReader(data)
	}

	req, err := http.NewRequest(method, c.baseURL+path, reqBody)
	if err != nil {
		return nil, fmt.Errorf("创建请求失败: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", c.token)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求失败: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取响应失败: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		// 尝试解析 etcd 错误格式 {"error": "...", "code": N}
		var errResp struct {
			Error   string `json:"error"`
			Message string `json:"message"`
		}
		if json.Unmarshal(respBody, &errResp) == nil {
			msg := errResp.Error
			if msg == "" {
				msg = errResp.Message
			}
			if msg != "" {
				return nil, fmt.Errorf("etcd 返回错误 [%d]: %s", resp.StatusCode, msg)
			}
		}
		return nil, fmt.Errorf("etcd 返回错误 [%d]: %s", resp.StatusCode, string(respBody))
	}

	if len(respBody) == 0 {
		return map[string]interface{}{}, nil
	}

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		// 非 JSON 响应（如 /health 可能返回纯文本）
		return map[string]interface{}{"_raw": string(respBody)}, nil
	}
	return result, nil
}

// auth 通过用户名密码获取认证 token
func (c *etcdClient) auth(user, password string) error {
	result, err := c.doRequest("POST", "/v3/auth/authenticate", map[string]string{
		"name":     user,
		"password": password,
	})
	if err != nil {
		return fmt.Errorf("认证失败: %w", err)
	}
	if token, ok := result["token"].(string); ok && token != "" {
		c.token = token
	}
	return nil
}

// ─── 类型化 API 封装 ───

type etcdMember struct {
	ID         string   `json:"ID"`
	Name       string   `json:"name"`
	PeerURLs   []string `json:"peerURLs"`
	ClientURLs []string `json:"clientURLs"`
	IsLearner  bool     `json:"isLearner"`
}

type etcdMemberListResp struct {
	Members []etcdMember `json:"members"`
}

type etcdMemberStatusResp struct {
	Header struct {
		ClusterID string `json:"cluster_id"`
		MemberID  string `json:"member_id"`
	} `json:"header"`
	Version     string `json:"version"`
	DBSize      string `json:"dbSize"`
	DBSizeInUse string `json:"dbSizeInUse"`
	Leader      string `json:"leader"`
	RaftIndex   string `json:"raftIndex"`
	RaftTerm    string `json:"raftTerm"`
	RaftApplied string `json:"raftAppliedIndex"`
	IsLearner   bool   `json:"isLearner"`
	Errors      []struct {
		EtcdALARM int `json:"etcdalarm"`
	} `json:"errors"`
}

func (c *etcdClient) memberList() (*etcdMemberListResp, error) {
	result, err := c.doRequest("POST", "/v3/cluster/member/list", nil)
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	var resp etcdMemberListResp
	if err := json.Unmarshal(data, &resp); err != nil {
		return nil, fmt.Errorf("解析 member list 响应失败: %w", err)
	}
	return &resp, nil
}

func (c *etcdClient) memberStatus(memberClientURL string) (*etcdMemberStatusResp, error) {
	req, err := http.NewRequest("POST", memberClientURL+"/v3/maintenance/status", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", c.token)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status request failed [%d]", resp.StatusCode)
	}

	var status etcdMemberStatusResp
	if err := json.Unmarshal(body, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// put 写入键值对，lease 为租约 ID（0 表示无租约）
func (c *etcdClient) put(key, value string, lease int64) error {
	body := map[string]interface{}{
		"key":   base64.StdEncoding.EncodeToString([]byte(key)),
		"value": base64.StdEncoding.EncodeToString([]byte(value)),
	}
	if lease > 0 {
		body["lease"] = fmt.Sprintf("%d", lease)
	}
	_, err := c.doRequest("POST", "/v3/kv/put", body)
	return err
}

// kvPair 键值对（GET 响应）
type kvPair struct {
	Key            string
	Value          string
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Lease          int64
}

// get 读取指定 key 的值
func (c *etcdClient) get(key string) ([]kvPair, int64, error) {
	body := map[string]interface{}{
		"key": base64.StdEncoding.EncodeToString([]byte(key)),
	}
	return c.doRange(body)
}

// getPrefix 前缀搜索，空前缀表示列出所有 key
func (c *etcdClient) getPrefix(prefix string, keysOnly bool, limit int64) ([]kvPair, int64, error) {
	var keyBytes, rangeEnd []byte
	if prefix == "" {
		// 列出所有 key：key=\x00, range_end=\x00
		keyBytes = []byte{0}
		rangeEnd = []byte{0}
	} else {
		keyBytes = []byte(prefix)
		rangeEnd = make([]byte, len(keyBytes)+1)
		copy(rangeEnd, keyBytes)
	}
	body := map[string]interface{}{
		"key":       base64.StdEncoding.EncodeToString(keyBytes),
		"range_end": base64.StdEncoding.EncodeToString(rangeEnd),
	}
	if keysOnly {
		body["keys_only"] = true
	}
	if limit > 0 {
		body["limit"] = fmt.Sprintf("%d", limit)
	}
	return c.doRange(body)
}

func (c *etcdClient) doRange(body map[string]interface{}) ([]kvPair, int64, error) {
	result, err := c.doRequest("POST", "/v3/kv/range", body)
	if err != nil {
		return nil, 0, err
	}

	var count int64
	if countStr, ok := result["count"].(string); ok {
		count, _ = strconv.ParseInt(countStr, 10, 64)
	}

	var kvs []kvPair
	if kvsRaw, ok := result["kvs"].([]interface{}); ok {
		for _, kvRaw := range kvsRaw {
			if kv, ok := kvRaw.(map[string]interface{}); ok {
				pair := kvPair{}
				if k, ok := kv["key"].(string); ok {
					decoded, err := base64.StdEncoding.DecodeString(k)
					if err == nil {
						pair.Key = string(decoded)
					}
				}
				if v, ok := kv["value"].(string); ok {
					decoded, err := base64.StdEncoding.DecodeString(v)
					if err == nil {
						pair.Value = string(decoded)
					}
				}
				if cr, ok := kv["create_revision"].(string); ok {
					pair.CreateRevision, _ = strconv.ParseInt(cr, 10, 64)
				}
				if mr, ok := kv["mod_revision"].(string); ok {
					pair.ModRevision, _ = strconv.ParseInt(mr, 10, 64)
				}
				if ver, ok := kv["version"].(string); ok {
					pair.Version, _ = strconv.ParseInt(ver, 10, 64)
				}
				if lease, ok := kv["lease"].(string); ok {
					pair.Lease, _ = strconv.ParseInt(lease, 10, 64)
				}
				kvs = append(kvs, pair)
			}
		}
	}
	return kvs, count, nil
}

// del 删除指定 key
func (c *etcdClient) del(key string) (int64, error) {
	body := map[string]interface{}{
		"key":     base64.StdEncoding.EncodeToString([]byte(key)),
		"prev_kv": true,
	}
	result, err := c.doRequest("POST", "/v3/kv/deleterange", body)
	if err != nil {
		return 0, err
	}
	var deleted int64
	if d, ok := result["deleted"].(string); ok {
		deleted, _ = strconv.ParseInt(d, 10, 64)
	}
	return deleted, nil
}

// ─── EtcdConnector 实现 Connector 接口 ───

type EtcdAPIConnector struct{}

func (c *EtcdAPIConnector) Name() string { return "etcd" }

func (c *EtcdAPIConnector) newClient(cfg config.Config) (*etcdClient, error) {
	client, err := newEtcdClient(cfg)
	if err != nil {
		return nil, err
	}
	// 如果提供了用户名和密码，尝试获取认证 token
	if cfg.User != "" && cfg.Password != "" {
		if err := client.auth(cfg.User, cfg.Password); err != nil {
			etcdLog.Debug("认证跳过 (可能未启用 auth): %v", err)
			client.token = ""
		} else {
			etcdLog.Debug("认证成功")
		}
	}
	return client, nil
}

func (c *EtcdAPIConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	etcdLog.Debug("连接 %s:%d", cfg.Host, cfg.Port)
	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: err.Error()}, nil
	}

	// 1. 尝试获取集群成员列表
	memberResp, err := client.memberList()
	if err != nil {
		// 2. 回退到 health 检查
		start := time.Now()
		_, healthErr := client.doRequest("GET", "/health", nil)
		healthMs := time.Since(start).Milliseconds()
		if healthErr != nil {
			return &config.Result{
				Success: false,
				Message: fmt.Sprintf("连接失败: %v", err),
			}, nil
		}
		// health 通过但 member list 失败
		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("连接成功 (health took=%dms，无法获取集群详细信息)", healthMs),
			ServerInfo: &config.ServerInfo{
				Status: "Running",
			},
		}, nil
	}

	// 收集各节点的 status 信息
	si := &config.ServerInfo{Status: "Running"}
	var clusterVersion string
	var totalDBSize, totalDBSizeInUse int64
	var leaderID, raftTerm string
	var memberCount int

	// 用于收集每个节点的详细信息
	type nodeStatus struct {
		name     string
		addr     string
		role     string // leader / follower / learner
		dbSize   int64
		dbInUse  int64
		raftIdx  int64
		healthMs int64 // proposal commit 延迟
		online   bool
	}
	var nodes []nodeStatus

	for _, member := range memberResp.Members {
		memberCount++
		addr := strings.Join(member.ClientURLs, ",")
		ns := nodeStatus{name: member.Name, addr: addr}

		if len(member.ClientURLs) == 0 {
			nodes = append(nodes, ns)
			continue
		}

		// 获取 status
		status, err := client.memberStatus(member.ClientURLs[0])
		if err != nil {
			etcdLog.Debug("获取 %s 状态失败: %v", member.Name, err)
			nodes = append(nodes, ns)
			continue
		}
		ns.online = true
		if status.Version != "" {
			clusterVersion = status.Version
		}
		if status.Leader != "" {
			leaderID = status.Leader
		}
		if status.RaftTerm != "" {
			raftTerm = status.RaftTerm
		}

		// 判断角色：Leader 的 MemberID == Leader 字段
		if member.IsLearner {
			ns.role = "learner"
		} else if status.Header.MemberID != "" && status.Header.MemberID == status.Leader {
			ns.role = "leader"
		} else {
			ns.role = "follower"
		}

		if v, err := strconv.ParseInt(status.DBSize, 10, 64); err == nil {
			ns.dbSize = v
			totalDBSize += v
		}
		if v, err := strconv.ParseInt(status.DBSizeInUse, 10, 64); err == nil {
			ns.dbInUse = v
			totalDBSizeInUse += v
		}
		if v, err := strconv.ParseInt(status.RaftIndex, 10, 64); err == nil {
			ns.raftIdx = v
		}

		// 测量 health 延迟（模拟 etcdctl endpoint health）
		start := time.Now()
		_, hErr := client.doRequest("GET", "/health", nil)
		ns.healthMs = time.Since(start).Milliseconds()
		if hErr != nil {
			ns.online = false
		}

		nodes = append(nodes, ns)
	}

	// ─── 基本信息面板 ───
	si.Version = clusterVersion
	si.InfoItems = append(si.InfoItems, config.InfoItem{
		Label: "成员数量", Value: fmt.Sprintf("%d", memberCount),
	})
	if totalDBSize > 0 {
		dbInfo := formatBytes(totalDBSize)
		if totalDBSizeInUse > 0 {
			dbInfo += fmt.Sprintf(" (实际使用 %s)", formatBytes(totalDBSizeInUse))
		}
		si.InfoItems = append(si.InfoItems, config.InfoItem{
			Label: "数据库大小", Value: dbInfo,
		})
	}
	if raftTerm != "" {
		si.InfoItems = append(si.InfoItems, config.InfoItem{
			Label: "Raft Term", Value: raftTerm,
		})
	}
	if leaderID != "" {
		// 找到 leader 的名字
		leaderName := leaderID
		for _, ns := range nodes {
			if ns.role == "leader" {
				leaderName = ns.name
				break
			}
		}
		si.InfoItems = append(si.InfoItems, config.InfoItem{
			Label: "Leader", Value: leaderName,
		})
	}

	// ─── 集群信息（含每节点详细状态）───
	si.Cluster = &config.ClusterInfo{
		Mode:    "etcd Cluster",
		Summary: fmt.Sprintf("%d 个成员", memberCount),
	}
	for _, ns := range nodes {
		status := "online"
		if !ns.online {
			status = "unreachable"
		}
		role := ns.role
		if role == "" {
			role = "unknown"
		}
		var info string
		if ns.online {
			parts := []string{fmt.Sprintf("took=%dms", ns.healthMs)}
			if ns.raftIdx > 0 {
				parts = append(parts, fmt.Sprintf("raft=%d", ns.raftIdx))
			}
			if ns.dbSize > 0 {
				parts = append(parts, fmt.Sprintf("db=%s", formatBytes(ns.dbSize)))
			}
			info = strings.Join(parts, ", ")
		}
		si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
			Address: ns.addr,
			Role:    role,
			Status:  status,
			Info:    ns.name + " | " + info,
		})
	}

	msg := "连接成功"
	if si.Version != "" {
		msg = fmt.Sprintf("连接成功 - etcd %s", si.Version)
	}
	return &config.Result{
		Success:    true,
		Message:    msg,
		ServerInfo: si,
	}, nil
}

func (c *EtcdAPIConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "put",
			Label:       "SET 键值对",
			Description: "设置一个键值对",
			Params: []config.ActionParam{
				{Name: "key", Label: "键名", Placeholder: "test_key", Required: true, Default: "connectest_test_key"},
				{Name: "value", Label: "值", Placeholder: "hello world", Required: true, Default: "hello_connectest"},
				{Name: "ttl", Label: "TTL (秒, 0=永久)", Placeholder: "0", Required: false, Default: "60"},
			},
		},
		{
			Name:        "get",
			Label:       "GET 键值",
			Description: "获取指定键的值",
			Params: []config.ActionParam{
				{Name: "key", Label: "键名", Placeholder: "test_key", Required: true, Default: "connectest_test_key"},
			},
		},
		{
			Name:        "delete",
			Label:       "DELETE 键",
			Description: "删除指定键",
			Params: []config.ActionParam{
				{Name: "key", Label: "键名", Placeholder: "test_key", Required: true, Default: "connectest_test_key"},
			},
		},
		{
			Name:        "prefix",
			Label:       "前缀搜索",
			Description: "按前缀搜索键值对 (前缀留空 = 列出所有 key，同 etcdctl get \"\" --prefix)",
			Params: []config.ActionParam{
				{Name: "prefix", Label: "键前缀 (留空=全部)", Placeholder: "留空列出所有 key", Required: false, Default: ""},
				{Name: "keys_only", Label: "仅显示键名", Placeholder: "", Required: false, Default: "true"},
				{Name: "limit", Label: "最大返回数 (0=不限)", Placeholder: "0", Required: false, Default: "0"},
			},
		},
	}
}

func (c *EtcdAPIConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	switch action {
	case "put":
		return c.actionPut(cfg, params)
	case "get":
		return c.actionGet(cfg, params)
	case "delete":
		return c.actionDelete(cfg, params)
	case "prefix":
		return c.actionPrefix(cfg, params)
	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}

func (c *EtcdAPIConnector) actionPut(cfg config.Config, params map[string]string) (*config.Result, error) {
	key := params["key"]
	value := params["value"]
	if key == "" {
		return nil, fmt.Errorf("键名不能为空")
	}

	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: err.Error()}, nil
	}

	var lease int64
	if ttlStr, ok := params["ttl"]; ok && ttlStr != "" && ttlStr != "0" {
		ttl, err := strconv.ParseInt(ttlStr, 10, 64)
		if err == nil && ttl > 0 {
			// 创建租约
			leaseResult, err := client.doRequest("POST", "/v3/lease/grant", map[string]interface{}{
				"TTL": fmt.Sprintf("%d", ttl),
			})
			if err != nil {
				etcdLog.Debug("创建租约失败: %v, 将不使用 TTL", err)
			} else if idStr, ok := leaseResult["ID"].(string); ok {
				lease, _ = strconv.ParseInt(idStr, 10, 64)
			}
		}
	}

	if err := client.put(key, value, lease); err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("PUT 失败: %v", err)}, nil
	}

	msg := fmt.Sprintf("PUT 成功: %s = %s", key, value)
	if lease > 0 {
		msg += fmt.Sprintf(" (TTL: %ss)", params["ttl"])
	}
	return &config.Result{Success: true, Message: msg}, nil
}

func (c *EtcdAPIConnector) actionGet(cfg config.Config, params map[string]string) (*config.Result, error) {
	key := params["key"]
	if key == "" {
		return nil, fmt.Errorf("键名不能为空")
	}

	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: err.Error()}, nil
	}
	kvs, count, err := client.get(key)
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("GET 失败: %v", err)}, nil
	}

	if count == 0 || len(kvs) == 0 {
		return &config.Result{Success: true, Message: fmt.Sprintf("键 '%s' 不存在", key)}, nil
	}

	kv := kvs[0]
	details := fmt.Sprintf("Key: %s\nValue: %s\nCreate Revision: %d\nMod Revision: %d\nVersion: %d",
		kv.Key, kv.Value, kv.CreateRevision, kv.ModRevision, kv.Version)
	if kv.Lease > 0 {
		details += fmt.Sprintf("\nLease: %d", kv.Lease)
	}

	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("GET 成功: %s", key),
		Details: details,
	}, nil
}

func (c *EtcdAPIConnector) actionDelete(cfg config.Config, params map[string]string) (*config.Result, error) {
	key := params["key"]
	if key == "" {
		return nil, fmt.Errorf("键名不能为空")
	}

	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: err.Error()}, nil
	}
	deleted, err := client.del(key)
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("DELETE 失败: %v", err)}, nil
	}

	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("DELETE 成功: 删除了 %d 个键", deleted),
	}, nil
}

func (c *EtcdAPIConnector) actionPrefix(cfg config.Config, params map[string]string) (*config.Result, error) {
	prefix := params["prefix"]
	keysOnly := params["keys_only"] == "true"
	var limit int64
	if l, ok := params["limit"]; ok && l != "" && l != "0" {
		limit, _ = strconv.ParseInt(l, 10, 64)
	}

	client, err := c.newClient(cfg)
	if err != nil {
		return &config.Result{Success: false, Message: err.Error()}, nil
	}
	kvs, count, err := client.getPrefix(prefix, keysOnly, limit)
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("前缀搜索失败: %v", err)}, nil
	}

	if count == 0 || len(kvs) == 0 {
		if prefix == "" {
			return &config.Result{Success: true, Message: "etcd 中没有任何键"}, nil
		}
		return &config.Result{Success: true, Message: fmt.Sprintf("未找到前缀为 '%s' 的键", prefix)}, nil
	}

	var details strings.Builder
	for _, kv := range kvs {
		if keysOnly {
			details.WriteString(kv.Key + "\n")
		} else {
			details.WriteString(fmt.Sprintf("%s = %s\n", kv.Key, kv.Value))
		}
	}

	label := "前缀搜索"
	if prefix == "" {
		label = "列出所有键"
	}
	return &config.Result{
		Success: true,
		Message: fmt.Sprintf("%s: 找到 %d 个键", label, count),
		Details: details.String(),
	}, nil
}

// firstLine 返回字节内容的第一行（用于调试日志）
func firstLine(data []byte) string {
	s := string(data)
	if idx := strings.IndexByte(s, '\n'); idx >= 0 {
		return s[:idx]
	}
	return s
}

// formatBytes 将字节数转换为人类可读格式
func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}
