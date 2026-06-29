package connector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

type MongoDBConnector struct{}

func (c *MongoDBConnector) Name() string { return "MongoDB" }

func (c *MongoDBConnector) buildURI(cfg config.Config) string {
	scheme := "mongodb"
	if cfg.UseTLS {
		scheme = "mongodb+srv"
	}
	if cfg.User != "" && cfg.Password != "" {
		return fmt.Sprintf("%s://%s:%s@%s:%d",
			scheme, cfg.User, cfg.Password, cfg.Host, cfg.Port)
	}
	return fmt.Sprintf("%s://%s:%d", scheme, cfg.Host, cfg.Port)
}

func (c *MongoDBConnector) newClientOptions(cfg config.Config) *options.ClientOptions {
	opts := options.Client().ApplyURI(c.buildURI(cfg))
	if cfg.UseTLS {
		tlsCfg := buildTLSConfig(cfg)
		opts.SetTLSConfig(tlsCfg)
	}
	return opts
}

func (c *MongoDBConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(c.newClientOptions(cfg))
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("连接失败: %v", err)}, nil
	}
	defer client.Disconnect(ctx)

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("Ping 失败: %v", err)}, nil
	}

	si := &config.ServerInfo{Status: "Running"}
	db := client.Database("admin")

	// buildInfo
	var buildInfo bson.M
	if err := db.RunCommand(ctx, bson.D{{Key: "buildInfo", Value: 1}}).Decode(&buildInfo); err == nil {
		si.Version, _ = buildInfo["version"].(string)
	}

	// serverStatus
	var serverStatus bson.M
	if err := db.RunCommand(ctx, bson.D{{Key: "serverStatus", Value: 1}}).Decode(&serverStatus); err == nil {
		if uptime, ok := serverStatus["uptime"].(float64); ok {
			days := int(uptime) / 86400
			hours := (int(uptime) % 86400) / 3600
			si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "运行时间", Value: fmt.Sprintf("%d天 %d小时", days, hours)})
		}
		if conns, ok := serverStatus["connections"].(bson.M); ok {
			if current, ok := conns["current"].(int32); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "当前连接数", Value: fmt.Sprintf("%d", current)})
			}
			if available, ok := conns["available"].(int32); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "可用连接数", Value: fmt.Sprintf("%d", available)})
			}
		}
		if host, ok := serverStatus["host"].(string); ok {
			si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "主机名", Value: host})
		}
		if process, ok := serverStatus["process"].(string); ok {
			si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "进程", Value: process})
		}
	}

	// dbStats
	if cfg.Database != "" {
		var dbStats bson.M
		if err := client.Database(cfg.Database).RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&dbStats); err == nil {
			if dataSize, ok := dbStats["dataSize"].(float64); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "数据库大小", Value: fmt.Sprintf("%.2f MB", dataSize/1024/1024)})
			}
			if collections, ok := dbStats["collections"].(int32); ok {
				si.InfoItems = append(si.InfoItems, config.InfoItem{Label: "集合数", Value: fmt.Sprintf("%d", collections)})
			}
		}
	}

	// 副本集检测
	var replStatus bson.M
	if err := db.RunCommand(ctx, bson.D{{Key: "replSetGetStatus", Value: 1}}).Decode(&replStatus); err == nil {
		setName, _ := replStatus["set"].(string)
		si.Cluster = &config.ClusterInfo{
			Mode:    "ReplicaSet",
			Summary: "副本集: " + setName,
		}
		if members, ok := replStatus["members"].(bson.A); ok {
			for _, m := range members {
				if member, ok := m.(bson.M); ok {
					name, _ := member["name"].(string)
					stateStr, _ := member["stateStr"].(string)
					health, _ := member["health"].(float64)
					status := "healthy"
					if health == 0 {
						status = "unhealthy"
					}
					si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
						Address: name,
						Role:    stateStr,
						Status:  status,
					})
				}
			}
		}
	}

	// Sharded Cluster 检测
	var isMaster bson.M
	if err := db.RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}}).Decode(&isMaster); err == nil {
		if msg, ok := isMaster["msg"].(string); ok && msg == "isdbgrid" {
			if si.Cluster == nil {
				si.Cluster = &config.ClusterInfo{}
			}
			si.Cluster.Mode = "Sharded Cluster (mongos)"
			// 获取 shard 信息
			var shardList bson.M
			if err := db.RunCommand(ctx, bson.D{{Key: "listShards", Value: 1}}).Decode(&shardList); err == nil {
				if shards, ok := shardList["shards"].(bson.A); ok {
					si.Cluster.Summary = fmt.Sprintf("分片集群，%d 个 Shard", len(shards))
					for _, s := range shards {
						if shard, ok := s.(bson.M); ok {
							id, _ := shard["_id"].(string)
							host, _ := shard["host"].(string)
							si.Cluster.Nodes = append(si.Cluster.Nodes, config.NodeInfo{
								Address: host,
								Role:    "Shard",
								Info:    id,
							})
						}
					}
				}
			}
		}
	}

	msg := "连接成功"
	if si.Version != "" {
		msg = fmt.Sprintf("连接成功 - MongoDB %s", si.Version)
	}

	return &config.Result{
		Success:    true,
		Message:    msg,
		ServerInfo: si,
	}, nil
}

func (c *MongoDBConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "list_databases",
			Label:       "列出所有数据库",
			Description: "列出服务器上所有数据库",
		},
		{
			Name:        "list_collections",
			Label:       "列出集合",
			Description: "列出指定数据库中的所有集合",
			Params: []config.ActionParam{
				{Name: "database", Label: "数据库名", Placeholder: "test", Required: true, Default: "test"},
			},
		},
		{
			Name:        "find",
			Label:       "查询文档",
			Description: "在指定集合中查询文档",
			Params: []config.ActionParam{
				{Name: "database", Label: "数据库名", Placeholder: "test", Required: true, Default: "test"},
				{Name: "collection", Label: "集合名", Placeholder: "users", Required: true},
				{Name: "limit", Label: "返回数量", Placeholder: "10", Required: false, Default: "10"},
			},
		},
	}
}

func (c *MongoDBConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := mongo.Connect(c.newClientOptions(cfg))
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer client.Disconnect(ctx)

	switch action {
	case "list_databases":
		result, err := client.ListDatabases(ctx, bson.D{})
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
		}
		var output strings.Builder
		for _, db := range result.Databases {
			output.WriteString(fmt.Sprintf("%-30s (size: %.2f MB)\n", db.Name, float64(db.SizeOnDisk)/1024/1024))
		}
		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("共 %d 个数据库", len(result.Databases)),
			Details: output.String(),
		}, nil

	case "list_collections":
		dbName := params["database"]
		if dbName == "" {
			return nil, fmt.Errorf("数据库名不能为空")
		}
		db := client.Database(dbName)
		collections, err := db.ListCollectionNames(ctx, bson.D{})
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
		}
		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("数据库 '%s' 共 %d 个集合", dbName, len(collections)),
			Details: strings.Join(collections, "\n"),
		}, nil

	case "find":
		dbName := params["database"]
		collName := params["collection"]
		if dbName == "" || collName == "" {
			return nil, fmt.Errorf("数据库名和集合名不能为空")
		}
		limit := int64(10)
		if l, ok := params["limit"]; ok {
			fmt.Sscanf(l, "%d", &limit)
		}

		collection := client.Database(dbName).Collection(collName)
		cursor, err := collection.Find(ctx, bson.D{}, options.Find().SetLimit(limit))
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
		}
		defer cursor.Close(ctx)

		var output strings.Builder
		count := 0
		for cursor.Next(ctx) {
			var doc bson.M
			if err := cursor.Decode(&doc); err != nil {
				continue
			}
			jsonBytes, _ := bson.MarshalExtJSON(doc, true, true)
			output.WriteString(string(jsonBytes) + "\n")
			count++
		}
		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("查询成功，返回 %d 条文档", count),
			Details: output.String(),
		}, nil

	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}
