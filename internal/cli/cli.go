package cli

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/connector"
	"github.com/longxiucai/connectest/internal/logger"
	"github.com/spf13/cobra"
)

var registry = connector.NewRegistry()

var Version = "dev"

// NewRootCmd 创建根命令
func NewRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "connectest",
		Short: "多服务连接测试工具",
		Long: `Connectest - 支持 MySQL/PostgreSQL/MongoDB/Redis/RabbitMQ/Kafka/MinIO/etcd/K8s-etcd 的连接测试工具。

支持 CLI 和 GUI 两种模式。不传子命令时默认启动 GUI。`,
	}

	rootCmd.AddCommand(newCLICmd())
	rootCmd.AddCommand(newGUICmd())
	rootCmd.AddCommand(newVersionCmd())

	return rootCmd
}

func newVersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "显示版本信息",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("Connectest %s\n", Version)
		},
	}
}

func newCLICmd() *cobra.Command {
	cliCmd := &cobra.Command{
		Use:   "cli",
		Short: "命令行模式",
		Long:  "以命令行模式执行连接测试和操作",
	}

	for _, svc := range config.AllServices() {
		cliCmd.AddCommand(newServiceCmd(svc))
	}

	return cliCmd
}

func newServiceCmd(meta config.ServiceMeta) *cobra.Command {
	cmd := &cobra.Command{
		Use:   string(meta.Type),
		Short: fmt.Sprintf("测试 %s 连接", meta.Name),
		RunE: func(cmd *cobra.Command, args []string) error {
			listActions, _ := cmd.Flags().GetBool("list-actions")
			if listActions {
				return listServiceActions(meta)
			}
			return runServiceCmd(cmd, meta)
		},
	}

	// 连接参数 — 每个子命令独立注册，不共享变量
	cmd.Flags().StringP("host", "H", "127.0.0.1", "服务器地址")
	cmd.Flags().IntP("port", "P", meta.DefaultPort, "端口号")
	if meta.HasUser {
		cmd.Flags().StringP("user", "u", "", "用户名")
	}
	if meta.HasPassword {
		cmd.Flags().StringP("password", "p", "", "密码")
	}
	if meta.HasDatabase {
		cmd.Flags().StringP("database", "d", "", "数据库名")
	}
	if meta.HasMgmtPort {
		defaultMgmt := meta.DefaultPort + 10000
		cmd.Flags().Int("mgmt-port", defaultMgmt, meta.MgmtPortLabel+"(Management API)")
	}
	if meta.HasCerts {
		cmd.Flags().String("ca-cert", "", "CA 证书 (文件路径或 PEM 内容)")
		cmd.Flags().String("cert", "", "客户端证书 (文件路径或 PEM 内容)")
		cmd.Flags().String("key", "", "客户端私钥 (文件路径或 PEM 内容)")
	}
	cmd.Flags().Bool("tls", false, "启用 TLS/SSL")

	// 操作参数
	cmd.Flags().StringP("action", "a", "", "执行操作 (留空则测试连接, 使用 --list-actions 查看可用操作)")
	cmd.Flags().StringSliceP("param", "k", nil, "操作参数 (key=value 格式，可多次指定)")
	cmd.Flags().Bool("list-actions", false, "列出该服务支持的所有操作及参数")

	// 循环和并发控制
	cmd.Flags().IntP("loop", "n", 1, "循环次数")
	cmd.Flags().IntP("concurrency", "c", 1, "并发数")
	cmd.Flags().IntP("interval", "i", 0, "循环间隔 (毫秒)")

	return cmd
}

func runServiceCmd(cmd *cobra.Command, meta config.ServiceMeta) error {
	log := logger.NewModule("CLI/" + meta.Name)

	c, ok := registry.Get(meta.Type)
	if !ok {
		return fmt.Errorf("未找到 %s 连接器", meta.Name)
	}

	// 从 cmd.Flags() 读取（每个子命令独立的默认值，不会互相覆盖）
	host, _ := cmd.Flags().GetString("host")
	port, _ := cmd.Flags().GetInt("port")
	user, _ := cmd.Flags().GetString("user")
	password, _ := cmd.Flags().GetString("password")
	database, _ := cmd.Flags().GetString("database")
	useTLS, _ := cmd.Flags().GetBool("tls")
	mgmtPort, _ := cmd.Flags().GetInt("mgmt-port")
	var caCert, certPath, keyPath string
	if meta.HasCerts {
		caCert, _ = cmd.Flags().GetString("ca-cert")
		certPath, _ = cmd.Flags().GetString("cert")
		keyPath, _ = cmd.Flags().GetString("key")
		if caCert != "" || certPath != "" || keyPath != "" {
			useTLS = true
		}
	}
	action, _ := cmd.Flags().GetString("action")
	extraKV, _ := cmd.Flags().GetStringSlice("param")
	loopCount, _ := cmd.Flags().GetInt("loop")
	concurrency, _ := cmd.Flags().GetInt("concurrency")
	intervalMs, _ := cmd.Flags().GetInt("interval")

	cfg := config.Config{
		Host:     host,
		Port:     port,
		User:     user,
		Password: password,
		Database: database,
		UseTLS:   useTLS,
		CACert:   caCert,
		Cert:     certPath,
		Key:      keyPath,
		Extra:    parseExtra(extraKV),
	}
	if meta.HasMgmtPort && mgmtPort > 0 {
		cfg.Extra["mgmt_port"] = fmt.Sprintf("%d", mgmtPort)
	}

	if action != "" {
		for _, act := range c.SupportedActions() {
			if act.Name == action {
				for _, p := range act.Params {
					if _, ok := cfg.Extra[p.Name]; !ok && p.Default != "" {
						cfg.Extra[p.Name] = p.Default
					}
				}
				break
			}
		}
	}

	// 全局上下文：Ctrl+C 可中断连接测试和执行操作
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ─── 连接测试 ───
	if action == "" {
		log.Info("测试连接 → %s:%d", cfg.Host, cfg.Port)
		fmt.Printf("🔌 正在测试 %s 连接 (%s:%d)...\n", meta.Name, cfg.Host, cfg.Port)

		result, err := c.TestConnection(ctx, cfg)
		if err != nil {
			log.Error("连接失败: %v", err)
			fmt.Fprintf(os.Stderr, "❌ 错误: %v\n", err)
			return err
		}

		if result.Success {
			fmt.Printf("✅ %s\n", result.Message)
		} else {
			fmt.Printf("❌ %s\n", result.Message)
		}

		if result.ServerInfo != nil {
			printServerInfo(result.ServerInfo)
			log.Info("连接成功 - %s", result.ServerInfo.Version)
		} else {
			log.Info("连接结果: %s", result.Message)
		}

		if result.Details != "" {
			fmt.Printf("\n%s\n", result.Details)
		}
		return nil
	}

	// ─── 执行操作 ───
	log.Info("执行操作: %s, 参数: %v", action, cfg.Extra)

	// 单次执行
	if loopCount <= 1 && concurrency <= 1 {
		fmt.Printf("⚡ 执行 %s 操作: %s\n", meta.Name, action)
		result, err := c.ExecuteAction(context.Background(), cfg, action, cfg.Extra)
		if err != nil {
			log.Error("操作失败: %v", err)
			fmt.Fprintf(os.Stderr, "❌ 错误: %v\n", err)
			return err
		}
		if result.Success {
			log.Info("操作成功: %s", result.Message)
		} else {
			log.Warn("操作失败: %s", result.Message)
		}
		printResult(result)
		return nil
	}

	// ─── 多轮/并发执行 — worker 模式，支持 Ctrl+C 停止 ───
	interval := time.Duration(intervalMs) * time.Millisecond
	totalTasks := concurrency * loopCount
	fmt.Printf("⚡ 执行 %s 操作: %s (%d 个 worker × %d 轮 = %d 次, 间隔 %v)\n",
		meta.Name, action, concurrency, loopCount, totalTasks, interval)
	fmt.Println("   按 Ctrl+C 可停止执行")
	log.Info("执行 %s: %d worker × %d 轮, 间隔 %v", action, concurrency, loopCount, interval)
	log.Debug("参数: %v", cfg.Extra)

	startTime := time.Now()
	var successCount, failCount, totalExec int64
	var wg sync.WaitGroup
	var mu sync.Mutex

	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for iter := 0; iter < loopCount; iter++ {
				select {
				case <-ctx.Done():
					return
				default:
				}

				if iter > 0 && interval > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(interval):
					}
				}

				execNum := atomic.AddInt64(&totalExec, 1)
				log.Debug("[W%d] 第 %d 轮 (总第 %d 次)", workerID+1, iter+1, execNum)
				result, err := c.ExecuteAction(ctx, cfg, action, cfg.Extra)

				select {
				case <-ctx.Done():
					return
				default:
				}

				mu.Lock()
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					log.Error("[W%d] #%d 执行错误: %v", workerID+1, iter+1, err)
					fmt.Printf("  [W%d] #%d (总#%d) ❌ 错误: %v\n", workerID+1, iter+1, execNum, err)
				} else {
					if result.Success {
						atomic.AddInt64(&successCount, 1)
						fmt.Printf("  [W%d] #%d (总#%d) ✅ %s\n", workerID+1, iter+1, execNum, result.Message)
					} else {
						atomic.AddInt64(&failCount, 1)
						fmt.Printf("  [W%d] #%d (总#%d) ❌ %s\n", workerID+1, iter+1, execNum, result.Message)
					}
					if result.Details != "" {
						for _, line := range strings.Split(result.Details, "\n") {
							fmt.Printf("        %s\n", line)
						}
					}
				}
				mu.Unlock()
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(startTime)
	sc := atomic.LoadInt64(&successCount)
	fc := atomic.LoadInt64(&failCount)
	total := atomic.LoadInt64(&totalExec)

	cancelled := ctx.Err() != nil

	if cancelled {
		log.Info("执行已停止 (Ctrl+C): 完成 %d 次, %d 成功, %d 失败", total, sc, fc)
		fmt.Println()
		fmt.Printf("⏹ 已停止: 完成 %d/%d 次\n", total, totalTasks)
	} else {
		log.Info("执行完成: 共 %d 次, %d 成功, %d 失败, 耗时 %v", total, sc, fc, elapsed.Round(time.Millisecond))
		fmt.Println()
		fmt.Printf("📊 执行摘要: %d 个 worker × %d 轮 = %d 次\n", concurrency, loopCount, total)
	}
	fmt.Printf("✅ 成功: %d  ❌ 失败: %d\n", sc, fc)
	fmt.Printf("⏱  耗时: %s\n", elapsed.Round(time.Millisecond))

	return nil
}

// printServerInfo 在 CLI 中显示服务器信息面板
func printServerInfo(si *config.ServerInfo) {
	if si == nil {
		return
	}

	fmt.Println()
	fmt.Printf("📊 %s  |  状态: %s\n", si.Version, si.Status)
	fmt.Println(strings.Repeat("─", 50))

	if len(si.InfoItems) > 0 {
		maxLabel := 0
		for _, item := range si.InfoItems {
			if len(item.Label) > maxLabel {
				maxLabel = len(item.Label)
			}
		}
		for _, item := range si.InfoItems {
			fmt.Printf("  %-*s  %s\n", maxLabel, item.Label+":", item.Value)
		}
	}

	if si.Cluster != nil {
		fmt.Println(strings.Repeat("─", 50))
		fmt.Printf("🌐 集群信息\n")
		fmt.Printf("  模式: %s\n", si.Cluster.Mode)
		if si.Cluster.Summary != "" {
			fmt.Printf("  概要: %s\n", si.Cluster.Summary)
		}
		if len(si.Cluster.Nodes) > 0 {
			fmt.Printf("  节点:\n")
			for _, node := range si.Cluster.Nodes {
				line := fmt.Sprintf("    %-30s [%-12s] %s", node.Address, node.Role, node.Status)
				if node.Info != "" {
					line += "  " + node.Info
				}
				fmt.Println(line)
			}
		}
	}
	fmt.Println()
}

func printResult(result *config.Result) {
	if result.Success {
		fmt.Printf("✅ %s\n", result.Message)
	} else {
		fmt.Printf("❌ %s\n", result.Message)
	}
	if result.Details != "" {
		fmt.Printf("\n%s\n", result.Details)
	}
}

func parseExtra(kvPairs []string) map[string]string {
	extra := make(map[string]string)
	for _, kv := range kvPairs {
		parts := strings.SplitN(kv, "=", 2)
		if len(parts) == 2 {
			extra[parts[0]] = parts[1]
		}
	}
	return extra
}

// listServiceActions 列出服务支持的所有操作及参数
func listServiceActions(meta config.ServiceMeta) error {
	c, ok := registry.Get(meta.Type)
	if !ok {
		return fmt.Errorf("未找到 %s 连接器", meta.Name)
	}

	actions := c.SupportedActions()
	fmt.Printf("📋 %s 支持的操作:\n", meta.Name)
	fmt.Println(strings.Repeat("═", 60))

	for _, act := range actions {
		fmt.Printf("\n  %s\n", act.Label)
		fmt.Printf("     -a %s\n", act.Name)
		if act.Description != "" {
			fmt.Printf("     %s\n", act.Description)
		}
		if len(act.Params) > 0 {
			fmt.Printf("     参数 (-k key=value):\n")
			for _, p := range act.Params {
				required := ""
				if p.Required {
					required = " (必填)"
				}
				def := ""
				if p.Default != "" {
					def = fmt.Sprintf(" [默认: %s]", p.Default)
				}
				fmt.Printf("       %-20s %s%s%s\n", p.Name, p.Label, required, def)
			}
		}
	}

	fmt.Println()
	fmt.Println("使用示例:")
	if len(actions) > 0 {
		example := actions[0]
		fmt.Printf("  connectest cli %s -H <host> -a %s", string(meta.Type), example.Name)
		for _, p := range example.Params {
			if p.Required && p.Default != "" {
				fmt.Printf(" -k %s=%s", p.Name, p.Default)
			}
		}
		fmt.Println()
	}
	fmt.Println()

	return nil
}

func newGUICmd() *cobra.Command {
	return &cobra.Command{
		Use:   "gui",
		Short: "启动图形界面",
		RunE: func(cmd *cobra.Command, args []string) error {
			fmt.Println("GUI 模式 - 请使用 connectest gui 启动")
			return nil
		},
	}
}
