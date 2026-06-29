package connector

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"strings"
	"time"

	mysql "github.com/go-sql-driver/mysql"
	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/logger"
)

type MySQLConnector struct{}

var mysqlLog = logger.NewModule("MySQL")

func (c *MySQLConnector) Name() string { return "MySQL" }

func (c *MySQLConnector) buildDSN(cfg config.Config) string {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/",
		cfg.User, cfg.Password, cfg.Host, cfg.Port)
	if cfg.Database != "" {
		dsn = fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database)
	}
	if cfg.UseTLS {
		tlsCfg := buildTLSConfig(cfg)
		tlsName := "connectest-mysql-custom"
		mysqlRegisterTLSConfig(tlsName, tlsCfg)
		dsn += "?tls=" + tlsName
	}
	return dsn
}

func mysqlRegisterTLSConfig(name string, cfg *tls.Config) {
	if cfg == nil {
		return
	}
	_ = mysql.RegisterTLSConfig(name, cfg)
}

func (c *MySQLConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	mysqlLog.Debug("连接 %s:%d, user=%s, db=%s", cfg.Host, cfg.Port, cfg.User, cfg.Database)
	dsn := c.buildDSN(cfg)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("连接失败: %v", err)}, nil
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		return &config.Result{Success: false, Message: fmt.Sprintf("Ping 失败: %v", err)}, nil
	}

	info := &config.ServerInfo{Status: "Running"}

	// 获取版本
	var version string
	db.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version)
	info.Version = version

	// 获取基本状态信息
	variables := map[string]string{
		"uptime":               "运行时间",
		"max_connections":      "最大连接数",
		"datadir":              "数据目录",
		"character_set_server": "字符集",
		"collation_server":     "排序规则",
		"innodb_version":       "InnoDB 版本",
	}
	for varName, label := range variables {
		var val string
		if err := db.QueryRowContext(ctx, "SHOW VARIABLES LIKE '"+varName+"'").Scan(&varName, &val); err == nil {
			if varName == "uptime" {
				// 转换秒为可读格式
				var seconds int64
				fmt.Sscanf(val, "%d", &seconds)
				days := seconds / 86400
				hours := (seconds % 86400) / 3600
				mins := (seconds % 3600) / 60
				val = fmt.Sprintf("%d天 %d小时 %d分钟", days, hours, mins)
			}
			info.InfoItems = append(info.InfoItems, config.InfoItem{Label: label, Value: val})
		}
	}

	// 获取全局状态
	statusVars := map[string]string{
		"Threads_connected": "当前连接数",
		"Threads_running":   "活跃线程",
		"Questions":         "总查询数",
		"Slow_queries":      "慢查询数",
	}
	for varName, label := range statusVars {
		var val string
		if err := db.QueryRowContext(ctx, "SHOW GLOBAL STATUS LIKE '"+varName+"'").Scan(&varName, &val); err == nil {
			info.InfoItems = append(info.InfoItems, config.InfoItem{Label: label, Value: val})
		}
	}

	// 检测集群/复制状态
	var masterHost, slaveRunning, slaveIO, slaveSQL string
	err = db.QueryRowContext(ctx, "SHOW SLAVE STATUS").Scan(&masterHost)
	if err == nil {
		// 有复制配置
		info.Cluster = &config.ClusterInfo{Mode: "Replication (Slave)"}
	}
	// 尝试获取 group replication 信息
	var grMembers string
	err = db.QueryRowContext(ctx, "SELECT GROUP_CONCAT(MEMBER_HOST, ':', MEMBER_PORT, '(', MEMBER_STATE, ')') FROM performance_schema.replication_group_members").Scan(&grMembers)
	if err == nil && grMembers != "" {
		info.Cluster = &config.ClusterInfo{
			Mode:    "Group Replication",
			Summary: grMembers,
		}
	}
	// InnoDB Cluster
	var clusterName string
	err = db.QueryRowContext(ctx, "SELECT cluster_name FROM mysql_innodb_cluster_metadata.clusters LIMIT 1").Scan(&clusterName)
	if err == nil && clusterName != "" {
		if info.Cluster == nil {
			info.Cluster = &config.ClusterInfo{}
		}
		info.Cluster.Mode = "InnoDB Cluster"
		info.Cluster.Summary = "Cluster: " + clusterName
	}
	_ = slaveRunning
	_ = slaveIO
	_ = slaveSQL

	msg := "连接成功"
	if version != "" {
		msg = fmt.Sprintf("连接成功 - MySQL %s", version)
	}

	return &config.Result{
		Success:    true,
		Message:    msg,
		ServerInfo: info,
	}, nil
}

func (c *MySQLConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "query",
			Label:       "执行 SQL 查询",
			Description: "执行任意 SQL 查询语句",
			Params: []config.ActionParam{
				{Name: "sql", Label: "SQL 语句", Placeholder: "SHOW DATABASES", Required: true, Default: "SHOW DATABASES"},
			},
		},
		{
			Name:        "list_databases",
			Label:       "列出所有数据库",
			Description: "列出服务器上所有数据库",
		},
	}
}

func (c *MySQLConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	dsn := c.buildDSN(cfg)

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("连接失败: %w", err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	switch action {
	case "query":
		queryStr := params["sql"]
		if queryStr == "" {
			return nil, fmt.Errorf("SQL 语句不能为空")
		}

		// 判断是否为查询语句
		upper := strings.ToUpper(strings.TrimSpace(queryStr))
		if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "SHOW") || strings.HasPrefix(upper, "DESCRIBE") || strings.HasPrefix(upper, "EXPLAIN") {
			rows, err := db.QueryContext(ctx, queryStr)
			if err != nil {
				return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
			}
			defer rows.Close()

			cols, _ := rows.Columns()
			var result strings.Builder
			result.WriteString(strings.Join(cols, "\t") + "\n")
			result.WriteString(strings.Repeat("-", 60) + "\n")

			values := make([]sql.NullString, len(cols))
			scanArgs := make([]interface{}, len(cols))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			rowCount := 0
			for rows.Next() {
				if err := rows.Scan(scanArgs...); err != nil {
					continue
				}
				var row []string
				for _, v := range values {
					if v.Valid {
						row = append(row, v.String)
					} else {
						row = append(row, "NULL")
					}
				}
				result.WriteString(strings.Join(row, "\t") + "\n")
				rowCount++
			}
			result.WriteString(fmt.Sprintf("\n共 %d 行", rowCount))
			return &config.Result{Success: true, Message: "查询成功", Details: result.String()}, nil
		}

		res, err := db.ExecContext(ctx, queryStr)
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("执行失败: %v", err)}, nil
		}
		affected, _ := res.RowsAffected()
		return &config.Result{Success: true, Message: fmt.Sprintf("执行成功，影响 %d 行", affected)}, nil

	case "list_databases":
		rows, err := db.QueryContext(ctx, "SHOW DATABASES")
		if err != nil {
			return &config.Result{Success: false, Message: fmt.Sprintf("查询失败: %v", err)}, nil
		}
		defer rows.Close()

		var databases []string
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				continue
			}
			databases = append(databases, name)
		}
		return &config.Result{
			Success: true,
			Message: fmt.Sprintf("共 %d 个数据库", len(databases)),
			Details: strings.Join(databases, "\n"),
		}, nil

	default:
		return nil, fmt.Errorf("不支持的操作: %s", action)
	}
}
