package connector

import (
	"context"
	"crypto/md5"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/longxiucai/connectest/internal/config"
)

type PostgreSQLConnector struct{}

var (
	certCache sync.Map // md5(content) -> path
)

func (c *PostgreSQLConnector) Name() string { return "PostgreSQL" }

func (c *PostgreSQLConnector) buildDSN(cfg config.Config) string {
	var sslmode string
	if cfg.SslMode != "" {
		sslmode = cfg.SslMode
	} else if cfg.UseTLS {
		if cfg.CACert != "" {
			sslmode = "verify-full"
		} else {
			sslmode = "require"
		}
	} else {
		sslmode = "disable"
	}
	dsn := fmt.Sprintf("host=%s port=%d user=%s password=%s sslmode=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, sslmode)
	if cfg.Database != "" {
		dsn += fmt.Sprintf(" dbname=%s", cfg.Database)
	}
	if cfg.CACert != "" {
		if path, err := resolveCertPath(cfg.CACert); err == nil {
			dsn += fmt.Sprintf(" sslrootcert=%s", path)
		}
	}
	if cfg.Cert != "" {
		if path, err := resolveCertPath(cfg.Cert); err == nil {
			dsn += fmt.Sprintf(" sslcert=%s", path)
		}
	}
	if cfg.Key != "" {
		if path, err := resolveCertPath(cfg.Key); err == nil {
			dsn += fmt.Sprintf(" sslkey=%s", path)
		}
	}
	return dsn
}

func resolveCertPath(input string) (string, error) {
	trimmed := strings.TrimSpace(input)
	if strings.HasPrefix(trimmed, "-----BEGIN") {
		hash := fmt.Sprintf("%x", md5.Sum([]byte(trimmed)))
		if cached, ok := certCache.Load(hash); ok {
			return cached.(string), nil
		}
		path, err := createTempPEMFile(trimmed)
		if err != nil {
			return "", err
		}
		certCache.Store(hash, path)
		return path, nil
	}
	return trimmed, nil
}

func createTempPEMFile(content string) (string, error) {
	normalized := normalizePEM(content)
	tmpFile, err := os.CreateTemp("", "connectest-*.pem")
	if err != nil {
		return "", err
	}
	tmpPath := tmpFile.Name()
	if _, err := tmpFile.WriteString(normalized); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return "", err
	}
	tmpFile.Close()
	os.Chmod(tmpPath, 0600)
	return tmpPath, nil
}

func (c *PostgreSQLConnector) TestConnection(ctx context.Context, cfg config.Config) (*config.Result, error) {
	dsn := c.buildDSN(cfg)
	db, err := sql.Open("postgres", dsn)
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
	db.QueryRowContext(ctx, "SELECT version()").Scan(&version)
	info.Version = version

	// 基本设置
	settings := []struct {
		name, label string
	}{
		{"server_version", "服务器版本"},
		{"data_directory", "数据目录"},
		{"max_connections", "最大连接数"},
		{"shared_buffers", "共享缓冲区"},
		{"server_encoding", "编码"},
		{"listen_addresses", "监听地址"},
	}
	for _, s := range settings {
		var val string
		if err := db.QueryRowContext(ctx, "SHOW "+s.name).Scan(&val); err == nil {
			info.InfoItems = append(info.InfoItems, config.InfoItem{Label: s.label, Value: val})
		}
	}

	// 当前连接数
	var connCount int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_activity").Scan(&connCount); err == nil {
		info.InfoItems = append(info.InfoItems, config.InfoItem{Label: "当前连接数", Value: fmt.Sprintf("%d", connCount)})
	}

	// 数据库大小
	var dbSize string
	if cfg.Database != "" {
		if err := db.QueryRowContext(ctx, "SELECT pg_size_pretty(pg_database_size($1))", cfg.Database).Scan(&dbSize); err == nil {
			info.InfoItems = append(info.InfoItems, config.InfoItem{Label: "数据库大小", Value: dbSize})
		}
	}

	// 检测集群/复制
	var isReplica bool
	db.QueryRowContext(ctx, "SELECT pg_is_in_recovery()").Scan(&isReplica)
	if isReplica {
		info.Cluster = &config.ClusterInfo{Mode: "Replication (Standby)"}
		var primaryConninfo string
		db.QueryRowContext(ctx, "SELECT setting FROM pg_settings WHERE name='primary_conninfo'").Scan(&primaryConninfo)
		if primaryConninfo != "" {
			info.Cluster.Summary = "Primary: " + primaryConninfo
		}
	} else {
		// 检查是否有流复制从库
		var replicaCount int
		db.QueryRowContext(ctx, "SELECT count(*) FROM pg_stat_replication").Scan(&replicaCount)
		if replicaCount > 0 {
			info.Cluster = &config.ClusterInfo{
				Mode:    "Replication (Primary)",
				Summary: fmt.Sprintf("%d 个从库", replicaCount),
			}
			rows, _ := db.QueryContext(ctx, "SELECT client_addr, state, sent_lsn, write_lsn FROM pg_stat_replication")
			if rows != nil {
				defer rows.Close()
				for rows.Next() {
					var addr, state, sentLSN, writeLSN string
					rows.Scan(&addr, &state, &sentLSN, &writeLSN)
					info.Cluster.Nodes = append(info.Cluster.Nodes, config.NodeInfo{
						Address: addr,
						Role:    "Standby",
						Status:  state,
						Info:    fmt.Sprintf("Sent: %s, Write: %s", sentLSN, writeLSN),
					})
				}
			}
		}
	}

	msg := "连接成功"
	if version != "" {
		msg = fmt.Sprintf("连接成功 - PostgreSQL")
	}

	return &config.Result{
		Success:    true,
		Message:    msg,
		ServerInfo: info,
	}, nil
}

func (c *PostgreSQLConnector) SupportedActions() []config.Action {
	return []config.Action{
		{
			Name:        "query",
			Label:       "执行 SQL 查询",
			Description: "执行任意 SQL 查询语句",
			Params: []config.ActionParam{
				{Name: "sql", Label: "SQL 语句", Placeholder: "SELECT version()", Required: true, Default: "SELECT version()"},
			},
		},
		{
			Name:        "list_databases",
			Label:       "列出所有数据库",
			Description: "列出服务器上所有数据库",
		},
	}
}

func (c *PostgreSQLConnector) ExecuteAction(ctx context.Context, cfg config.Config, action string, params map[string]string) (*config.Result, error) {
	dsn := c.buildDSN(cfg)

	db, err := sql.Open("postgres", dsn)
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

		upper := strings.ToUpper(strings.TrimSpace(queryStr))
		if strings.HasPrefix(upper, "SELECT") || strings.HasPrefix(upper, "SHOW") || strings.HasPrefix(upper, "TABLE") {
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
		rows, err := db.QueryContext(ctx, "SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname")
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
