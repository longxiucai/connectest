package gui

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"

	"github.com/longxiucai/connectest/internal/config"
	"github.com/longxiucai/connectest/internal/connector"
	"github.com/longxiucai/connectest/internal/logger"
)

// formWidgets 存储表单控件引用
type formWidgets struct {
	host          *widget.Entry
	port          *widget.Entry
	user          *widget.Entry
	password      *widget.Entry
	database      *widget.Entry
	mgmtPort      *widget.Entry // 管理端口（如 RabbitMQ Management API）
	caCert        *widget.Entry // CA 证书路径
	cert          *widget.Entry // 客户端证书路径
	key           *widget.Entry // 客户端私钥路径
	useTLS        *widget.Check
	window        fyne.Window
	resultLabel   *widget.Label
	resultScroll  *container.Scroll
	serverInfoBox *fyne.Container // 固定的服务器信息面板
	progressLabel *widget.Label   // 并发执行时的实时进度显示
	cancelFunc       context.CancelFunc // 取消正在执行的操作
	testCancelFunc   context.CancelFunc // 取消正在执行的测试连接
	running          atomic.Bool        // 是否正在执行
	testRunning      atomic.Bool        // 测试连接是否正在执行
}

func newFormWidgets(meta config.ServiceMeta, w fyne.Window, c connector.Connector, resultLabel *widget.Label, resultScroll *container.Scroll) *formWidgets {
	fw := &formWidgets{
		host:         widget.NewEntry(),
		port:         widget.NewEntry(),
		window:       w,
		resultLabel:  resultLabel,
		resultScroll: resultScroll,
	}
	fw.host.SetPlaceHolder("127.0.0.1")
	fw.host.SetText("127.0.0.1")
	fw.port.SetPlaceHolder(strconv.Itoa(meta.DefaultPort))
	fw.port.SetText(strconv.Itoa(meta.DefaultPort))

	if meta.HasUser {
		fw.user = widget.NewEntry()
		fw.user.SetPlaceHolder("用户名")
	}
	if meta.HasPassword {
		fw.password = widget.NewPasswordEntry()
		fw.password.SetPlaceHolder("密码")
	}
	if meta.HasDatabase {
		fw.database = widget.NewEntry()
		fw.database.SetPlaceHolder("数据库名 (可选)")
	}
	if meta.HasMgmtPort {
		fw.mgmtPort = widget.NewEntry()
		defaultMgmtPort := meta.DefaultPort + 10000
		fw.mgmtPort.SetPlaceHolder(fmt.Sprintf("%d (默认)", defaultMgmtPort))
		fw.mgmtPort.SetText(fmt.Sprintf("%d", defaultMgmtPort))
	}
	if meta.HasCerts {
		fw.caCert = widget.NewEntry()
		fw.caCert.SetPlaceHolder("CA 证书 (路径或粘贴 PEM)")
		fw.cert = widget.NewEntry()
		fw.cert.SetPlaceHolder("客户端证书 (路径或粘贴 PEM)")
		fw.key = widget.NewEntry()
		fw.key.SetPlaceHolder("客户端私钥 (路径或粘贴 PEM)")
	}
	fw.useTLS = widget.NewCheck("启用 TLS/SSL", nil)

	// 初始化服务器信息面板（空容器，连接成功后填充）
	fw.serverInfoBox = container.NewVBox()

	// 初始化进度标签（并发执行时显示实时进度）
	fw.progressLabel = widget.NewLabel("")
	fw.progressLabel.Wrapping = fyne.TextWrapWord
	fw.progressLabel.Hide()

	return fw
}

// updateServerInfo 更新固定的服务器信息面板
func (fw *formWidgets) updateServerInfo(si *config.ServerInfo) {
	fw.serverInfoBox.Objects = nil

	if si == nil {
		fw.serverInfoBox.Add(widget.NewLabel("未获取到服务器信息"))
		fw.serverInfoBox.Refresh()
		return
	}

	// 版本和状态头
	header := fmt.Sprintf("📊 %s  |  状态: %s", si.Version, si.Status)
	headerLabel := widget.NewLabel(header)
	headerLabel.TextStyle = fyne.TextStyle{Bold: true}
	fw.serverInfoBox.Add(headerLabel)
	fw.serverInfoBox.Add(widget.NewSeparator())

	// 基本信息表格
	if len(si.InfoItems) > 0 {
		for _, item := range si.InfoItems {
			valLabel := widget.NewLabel(item.Value)
			valLabel.Wrapping = fyne.TextWrapWord
			row := container.NewGridWithColumns(2,
				widget.NewLabel(item.Label+":"),
				valLabel,
			)
			fw.serverInfoBox.Add(row)
		}
	}

	// 集群信息
	if si.Cluster != nil {
		fw.serverInfoBox.Add(widget.NewSeparator())
		clusterTitle := widget.NewLabel("🌐 集群信息")
		clusterTitle.TextStyle = fyne.TextStyle{Bold: true}
		fw.serverInfoBox.Add(clusterTitle)
		fw.serverInfoBox.Add(widget.NewLabel("模式: " + si.Cluster.Mode))
		if si.Cluster.Summary != "" {
			fw.serverInfoBox.Add(widget.NewLabel("概要: " + si.Cluster.Summary))
		}
		if len(si.Cluster.Nodes) > 0 {
			fw.serverInfoBox.Add(widget.NewLabel("节点列表:"))
			for _, node := range si.Cluster.Nodes {
				nodeText := fmt.Sprintf("  %s  [%s]  %s", node.Address, node.Role, node.Status)
				if node.Info != "" {
					nodeText += "  " + node.Info
				}
				nodeLabel := widget.NewLabel(nodeText)
				nodeLabel.TextStyle = fyne.TextStyle{Monospace: true}
				fw.serverInfoBox.Add(nodeLabel)
			}
		}
	}

	fw.serverInfoBox.Refresh()
}

// appendResult 追加执行结果到结果区（带时间戳）
func (fw *formWidgets) appendResult(title string, text string) {
	ts := time.Now().Format("15:04:05")
	entry := fmt.Sprintf("[%s] %s\n%s", ts, title, text)

	existing := fw.resultLabel.Text
	if existing == "" || existing == "等待执行..." {
		fw.resultLabel.SetText(entry)
	} else {
		fw.resultLabel.SetText(existing + "\n" + strings.Repeat("─", 50) + "\n" + entry)
	}
	// 滚动到底部
	fw.resultScroll.ScrollToBottom()
}

func (fw *formWidgets) toConfig(meta config.ServiceMeta) config.Config {
	port, _ := strconv.Atoi(strings.TrimSpace(fw.port.Text))
	if port == 0 {
		port = meta.DefaultPort
	}
	cfg := config.Config{
		Host:   strings.TrimSpace(fw.host.Text),
		Port:   port,
		UseTLS: fw.useTLS.Checked,
		Extra:  make(map[string]string),
	}
	if fw.user != nil {
		cfg.User = strings.TrimSpace(fw.user.Text)
	}
	if fw.password != nil {
		cfg.Password = fw.password.Text
	}
	if fw.database != nil {
		cfg.Database = strings.TrimSpace(fw.database.Text)
	}
	if fw.mgmtPort != nil {
		mp := strings.TrimSpace(fw.mgmtPort.Text)
		if mp != "" {
			cfg.Extra["mgmt_port"] = mp
		}
	}
	if fw.caCert != nil {
		cfg.CACert = strings.TrimSpace(fw.caCert.Text)
	}
	if fw.cert != nil {
		cfg.Cert = strings.TrimSpace(fw.cert.Text)
	}
	if fw.key != nil {
		cfg.Key = strings.TrimSpace(fw.key.Text)
	}
	// 如果填写了证书路径，自动启用 TLS
	if cfg.CACert != "" || cfg.Cert != "" || cfg.Key != "" {
		cfg.UseTLS = true
	}
	return cfg
}

func (fw *formWidgets) buildForm() fyne.CanvasObject {
	grid := container.NewGridWithColumns(2,
		widget.NewLabel("主机地址:"), fw.host,
		widget.NewLabel("端口:"), fw.port,
	)

	if fw.user != nil {
		grid.Add(widget.NewLabel("用户名:"))
		grid.Add(fw.user)
	}
	if fw.password != nil {
		grid.Add(widget.NewLabel("密码:"))
		grid.Add(fw.password)
	}
	if fw.database != nil {
		grid.Add(widget.NewLabel("数据库:"))
		grid.Add(fw.database)
	}
	if fw.mgmtPort != nil {
		grid.Add(widget.NewLabel("管理端口:"))
		grid.Add(fw.mgmtPort)
	}
	if fw.caCert != nil {
		grid.Add(widget.NewLabel("CA 证书:"))
		grid.Add(fw.caCert)
	}
	if fw.cert != nil {
		grid.Add(widget.NewLabel("客户端证书:"))
		grid.Add(fw.cert)
	}
	if fw.key != nil {
		grid.Add(widget.NewLabel("客户端私钥:"))
		grid.Add(fw.key)
	}
	grid.Add(widget.NewLabel(""))
	grid.Add(fw.useTLS)

	// 表单 + 服务器信息面板
	return container.NewVBox(grid, fw.serverInfoBox)
}

func (fw *formWidgets) buildActions(meta config.ServiceMeta, c connector.Connector) fyne.CanvasObject {
	actions := c.SupportedActions()

	log := logger.NewModule(meta.Name)

	// ─── 连接测试按钮（支持中途取消）───
	var testBtn *widget.Button
	testBtn = widget.NewButton("🔗 测试连接", func() {
		// 如果正在连接，点击则取消
		if fw.testRunning.Load() {
			log.Info("用户取消测试连接")
			testBtn.Disable()
			testBtn.SetText("⏳ 取消中...")
			if fw.testCancelFunc != nil {
				fw.testCancelFunc()
			}
			return
		}

		cfg := fw.toConfig(meta)
		testBtn.SetText("⏹ 取消连接")
		testBtn.Importance = widget.DangerImportance
		fw.testRunning.Store(true)
		log.Info("测试连接 → %s:%d", cfg.Host, cfg.Port)

		go func() {
			var cancelled bool
			connCtx, connCancel := context.WithTimeout(context.Background(), 30*time.Second)
			fw.testCancelFunc = func() {
				cancelled = true
				connCancel()
			}
			defer connCancel()

			result, err := c.TestConnection(connCtx, cfg)

			fw.testCancelFunc = nil
			fw.testRunning.Store(false)
			fyne.Do(func() {
				testBtn.SetText("🔗 测试连接")
				testBtn.Importance = widget.MediumImportance
				testBtn.Enable()

				if cancelled {
					fw.appendResult("测试连接", "⏹ 已取消连接")
					return
				}

				if err != nil {
					log.Error("连接失败: %v", err)
					fw.appendResult("测试连接", fmt.Sprintf("❌ 错误: %v", err))
					return
				}

				if result.ServerInfo != nil {
					log.Info("连接成功 - %s %s", meta.Name, result.ServerInfo.Version)
					fw.updateServerInfo(result.ServerInfo)
				} else {
					log.Info("连接成功 - %s", result.Message)
				}

				var output strings.Builder
				if result.Success {
					output.WriteString("✅ " + result.Message)
				} else {
					output.WriteString("❌ " + result.Message)
				}
				if result.Details != "" {
					output.WriteString("\n" + result.Details)
				}
				fw.appendResult("测试连接", output.String())
			})
		}()
	})

	if len(actions) == 0 {
		return container.NewVBox(testBtn)
	}

	// ─── 功能测试区 ───
	funcTitle := widget.NewLabel("⚡ 功能测试")
	funcTitle.TextStyle = fyne.TextStyle{Bold: true}

	// 循环和并发控制
	loopEntry := widget.NewEntry()
	loopEntry.SetText("1")
	loopEntry.SetPlaceHolder("1")
	concurrencyEntry := widget.NewEntry()
	concurrencyEntry.SetText("1")
	concurrencyEntry.SetPlaceHolder("1")
	intervalEntry := widget.NewEntry()
	intervalEntry.SetText("0")
	intervalEntry.SetPlaceHolder("0 (毫秒)")

	controlGrid := container.NewGridWithColumns(6,
		widget.NewLabel("循环次数:"), loopEntry,
		widget.NewLabel("并发数:"), concurrencyEntry,
		widget.NewLabel("间隔(ms):"), intervalEntry,
	)

	// 为所有操作预创建参数输入框
	allParamEntries := make(map[string]map[string]*widget.Entry)
	allParamChecks := make(map[string]map[string]*widget.Check) // 用于 message_test 的启用复选框
	for _, act := range actions {
		entries := make(map[string]*widget.Entry)
		checks := make(map[string]*widget.Check)
		for _, p := range act.Params {
			p := p
			entry := widget.NewEntry()
			entry.SetPlaceHolder(p.Placeholder)
			if p.Default != "" {
				entry.SetText(p.Default)
			}
			entries[p.Name] = entry

			// 为布尔类型参数创建 checkbox
			if p.Name == "do_produce" || p.Name == "do_consume" || p.Name == "do_set" || p.Name == "do_get" || p.Name == "keys_only" {
				chk := widget.NewCheck(p.Label, nil)
				chk.Checked = p.Default == "true"
				checks[p.Name] = chk
			}
		}
		allParamEntries[act.Name] = entries
		allParamChecks[act.Name] = checks
	}

	// 动态参数容器和描述标签
	paramContainer := container.NewVBox()
	descLabel := widget.NewLabel("")
	descLabel.TextStyle = fyne.TextStyle{Italic: true}

	var execBtn *widget.Button

	actionLabels := make([]string, len(actions))
	actionMap := make(map[string]config.Action)
	for i, act := range actions {
		actionLabels[i] = act.Label
		actionMap[act.Label] = act
	}

	switchAction := func(label string) {
		act := actionMap[label]
		paramContainer.Objects = nil

		if act.Description != "" {
			descLabel.SetText(act.Description)
			paramContainer.Add(descLabel)
		}

		entries := allParamEntries[act.Name]
		checks := allParamChecks[act.Name]

		if act.Name == "message_test" {
			// ─── 发送/消费分栏布局 ───
			// 将参数分为 produce_* / set_* 和 consume_* / get_* 两组
			produceGrid := container.NewGridWithColumns(2)
			consumeGrid := container.NewGridWithColumns(2)
			sharedGrid := container.NewGridWithColumns(2)

			for _, p := range act.Params {
				if p.Name == "do_produce" || p.Name == "do_consume" || p.Name == "do_set" || p.Name == "do_get" {
					continue // checkbox 放在标题行
				}
				if strings.HasPrefix(p.Name, "produce_") || strings.HasPrefix(p.Name, "set_") {
					produceGrid.Add(widget.NewLabel(p.Label + ":"))
					produceGrid.Add(entries[p.Name])
				} else if strings.HasPrefix(p.Name, "consume_") || strings.HasPrefix(p.Name, "get_") {
					consumeGrid.Add(widget.NewLabel(p.Label + ":"))
					consumeGrid.Add(entries[p.Name])
				} else {
					// 共享参数（message, key, exchange, group 等）
					// 根据上下文决定放哪边：message/exchange → 发送侧，group → 消费侧
					switch p.Name {
					case "message", "exchange":
						produceGrid.Add(widget.NewLabel(p.Label + ":"))
						produceGrid.Add(entries[p.Name])
					case "group":
						consumeGrid.Add(widget.NewLabel(p.Label + ":"))
						consumeGrid.Add(entries[p.Name])
					default:
						sharedGrid.Add(widget.NewLabel(p.Label + ":"))
						sharedGrid.Add(entries[p.Name])
					}
				}
			}

			// 左侧：发送/设置
			produceCheck := checks["do_produce"]
			if produceCheck == nil {
				produceCheck = checks["do_set"]
			}
			produceTitle := container.NewHBox(produceCheck)
			produceBox := container.NewVBox(
				widget.NewSeparator(),
				produceTitle,
				produceGrid,
			)

			// 右侧：消费/读取
			consumeCheck := checks["do_consume"]
			if consumeCheck == nil {
				consumeCheck = checks["do_get"]
			}
			consumeTitle := container.NewHBox(consumeCheck)
			consumeBox := container.NewVBox(
				widget.NewSeparator(),
				consumeTitle,
				consumeGrid,
			)

			split := container.NewHSplit(produceBox, consumeBox)
			split.Offset = 0.5
			paramContainer.Add(split)

			if len(sharedGrid.Objects) > 0 {
				paramContainer.Add(sharedGrid)
			}
		} else if len(act.Params) > 0 {
			// ─── 普通操作：标准网格 ───
			grid := container.NewGridWithColumns(2)
			for _, p := range act.Params {
				if chk, ok := checks[p.Name]; ok {
					// 布尔参数用 checkbox
					grid.Add(widget.NewLabel(""))
					grid.Add(chk)
				} else {
					grid.Add(widget.NewLabel(p.Label + ":"))
					grid.Add(entries[p.Name])
				}
			}
			paramContainer.Add(grid)
		}

		execBtn.SetText("▶ 执行 " + act.Label)
		paramContainer.Refresh()
	}

	actionSelect := widget.NewSelect(actionLabels, func(selected string) {
		switchAction(selected)
	})
	actionSelect.PlaceHolder = "选择操作..."

	// 执行按钮 - 支持循环和并发，可暂停
	var activeWorkers int64 // 当前活跃的 worker 数量

	execBtn = widget.NewButton("▶ 执行", func() {
		// 如果正在运行，点击则取消
		if fw.running.Load() {
			remaining := atomic.LoadInt64(&activeWorkers)
			log.Info("用户请求停止执行，剩余 %d 个 worker", remaining)
			execBtn.Disable()
			execBtn.SetText(fmt.Sprintf("⏳ 停止中 (%d 线程)...", remaining))
			if fw.cancelFunc != nil {
				fw.cancelFunc()
			}
			return
		}

		selected := actionSelect.Selected
		if selected == "" {
			fw.appendResult("功能测试", "⚠️ 请先选择一个操作")
			return
		}

		act := actionMap[selected]
		cfg := fw.toConfig(meta)
		params := make(map[string]string)
		for name, entry := range allParamEntries[act.Name] {
			params[name] = entry.Text
		}
		for name, chk := range allParamChecks[act.Name] {
			if chk.Checked {
				params[name] = "true"
			} else {
				params[name] = "false"
			}
		}

		loopCount := 1
		if v, err := strconv.Atoi(loopEntry.Text); err == nil && v > 0 {
			loopCount = v
		}
		concurrency := 1
		if v, err := strconv.Atoi(concurrencyEntry.Text); err == nil && v > 0 {
			concurrency = v
		}
		intervalMs := 0
		if v, err := strconv.Atoi(intervalEntry.Text); err == nil && v >= 0 {
			intervalMs = v
		}
		interval := time.Duration(intervalMs) * time.Millisecond

		// 创建可取消的 context
		ctx, cancel := context.WithCancel(context.Background())
		fw.cancelFunc = cancel
		fw.running.Store(true)
		atomic.StoreInt64(&activeWorkers, 0)

		execBtn.SetText("⏹ 停止")
		execBtn.Importance = widget.DangerImportance
		log.Info("执行 %s: %d轮 × %d并发, 间隔 %v", act.Label, loopCount, concurrency, interval)
		log.Debug("参数: %v", params)

		go func() {
			startTime := time.Now()
			var successCount, failCount int64

			if concurrency == 1 {
				// ─── 单线程顺序执行：逐条显示详细结果 ───
				log.Debug("顺序执行 %s: %d 轮", act.Name, loopCount)

			seqLoop:
				for i := 0; i < loopCount; i++ {
					select {
					case <-ctx.Done():
						break seqLoop
					default:
					}

					if i > 0 && interval > 0 {
						select {
						case <-ctx.Done():
							break seqLoop
						case <-time.After(interval):
						}
					}

					log.Debug("#%d 执行 %s", i+1, act.Name)
					result, err := c.ExecuteAction(ctx, cfg, act.Name, params)

					// 终端日志（独立于 GUI）
					if err != nil {
						log.Error("#%d 错误: %v", i+1, err)
					} else if result.Success {
						log.Info("#%d ✅ %s", i+1, result.Message)
						if result.Details != "" {
							log.Debug("#%d 详情:\n%s", i+1, result.Details)
						}
					} else {
						log.Warn("#%d ❌ %s", i+1, result.Message)
						if result.Details != "" {
							log.Debug("#%d 详情:\n%s", i+1, result.Details)
						}
					}

					// GUI 更新
					fyne.Do(func() {
						if err != nil {
							failCount++
							fw.appendResult(fmt.Sprintf("#%d", i+1), fmt.Sprintf("❌ 错误: %v", err))
							return
						}
						if result.Success {
							successCount++
						} else {
							failCount++
						}
						var output strings.Builder
						status := "✅"
						if !result.Success {
							status = "❌"
						}
						output.WriteString(status + " " + result.Message)
						if result.Details != "" {
							output.WriteString("\n" + result.Details)
						}
						fw.appendResult(fmt.Sprintf("#%d", i+1), output.String())
					})
				}

				elapsed := time.Since(startTime)
				cancelled := ctx.Err() != nil
				sc := successCount
				fc := failCount

				if cancelled {
					log.Info("执行已停止: 完成 %d 次, %d 成功, %d 失败", sc+fc, sc, fc)
				} else {
					log.Info("执行完成: 共 %d 次, %d 成功, %d 失败, 耗时 %v", sc+fc, sc, fc, elapsed.Round(time.Millisecond))
				}

				fyne.Do(func() {
					fw.running.Store(false)
					execBtn.Enable()
					execBtn.SetText("▶ 执行 " + act.Label)
					execBtn.Importance = widget.MediumImportance

					if cancelled {
						fw.appendResult(act.Label,
							fmt.Sprintf("⏹ 已停止: 完成 %d/%d 次\n✅ 成功: %d  ❌ 失败: %d\n⏱ 已运行: %s",
								sc+fc, loopCount, sc, fc, elapsed.Round(time.Millisecond)))
					} else if loopCount > 1 {
						fw.appendResult(act.Label,
							fmt.Sprintf("📊 执行完成: %d 轮\n✅ 成功: %d  ❌ 失败: %d\n⏱ 总耗时: %s",
								loopCount, sc, fc, elapsed.Round(time.Millisecond)))
					}
				})
				cancel()
				return
			}

			// ─── 多线程并发执行 — worker 模式 ───
			// 核心思路：worker 只做原子计数，所有 UI 更新由一个定时器统一刷新
			var wg sync.WaitGroup
			totalExec := int64(0)
			cancelled := false
			totalTarget := int64(concurrency * loopCount)

			// 显示进度面板，隐藏结果区滚动
			fyne.Do(func() {
				fw.progressLabel.SetText(fmt.Sprintf("🚀 %s: %d 个 worker × %d 轮 (共 %d 次)",
					act.Label, concurrency, loopCount, totalTarget))
				fw.progressLabel.Show()
			})

			log.Info("执行 %s: %d worker × %d 轮, 间隔 %v", act.Label, concurrency, loopCount, interval)

			// 统一的 UI 刷新定时器：每 200ms 更新进度和按钮
			refreshDone := make(chan struct{})
			go func() {
				ticker := time.NewTicker(200 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-refreshDone:
						return
					case <-ticker.C:
						done := atomic.LoadInt64(&totalExec)
						sc := atomic.LoadInt64(&successCount)
						fc := atomic.LoadInt64(&failCount)
						workers := atomic.LoadInt64(&activeWorkers)
						isCancelled := ctx.Err() != nil

						fyne.Do(func() {
							if isCancelled && workers > 0 {
								// 停止中
								execBtn.SetText(fmt.Sprintf("⏳ 停止中 (%d 线程)...", workers))
								fw.progressLabel.SetText(fmt.Sprintf("⏹ %s: 停止中... %d/%d | ✅ %d ❌ %d | 剩余 %d 线程",
									act.Label, done, totalTarget, sc, fc, workers))
							} else {
								// 运行中
								fw.progressLabel.SetText(fmt.Sprintf("🔄 %s: %d/%d | ✅ %d ❌ %d | %d worker 运行中",
									act.Label, done, totalTarget, sc, fc, workers))
							}
						})
					}
				}
			}()

			for w := 0; w < concurrency; w++ {
				wg.Add(1)
				atomic.AddInt64(&activeWorkers, 1)
				go func(workerID int) {
					defer wg.Done()
					defer atomic.AddInt64(&activeWorkers, -1)
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

						log.Debug("[W%d] 第 %d 轮", workerID+1, iter+1)
						result, err := c.ExecuteAction(ctx, cfg, act.Name, params)

						select {
						case <-ctx.Done():
							return
						default:
						}

						// 原子计数 + 终端日志，零 UI 开销
						atomic.AddInt64(&totalExec, 1)
						if err != nil {
							atomic.AddInt64(&failCount, 1)
							log.Error("[W%d] #%d 错误: %v", workerID+1, iter+1, err)
						} else if result.Success {
							atomic.AddInt64(&successCount, 1)
							log.Info("[W%d] #%d ✅ %s", workerID+1, iter+1, result.Message)
							if result.Details != "" {
								log.Debug("[W%d] #%d 详情:\n%s", workerID+1, iter+1, result.Details)
							}
						} else {
							atomic.AddInt64(&failCount, 1)
							log.Warn("[W%d] #%d ❌ %s", workerID+1, iter+1, result.Message)
							if result.Details != "" {
								log.Debug("[W%d] #%d 详情:\n%s", workerID+1, iter+1, result.Details)
							}
						}
					}
				}(w)
			}

			wg.Wait()
			close(refreshDone)

			elapsed := time.Since(startTime)
			sc := atomic.LoadInt64(&successCount)
			fc := atomic.LoadInt64(&failCount)
			total := atomic.LoadInt64(&totalExec)

			select {
			case <-ctx.Done():
				cancelled = true
			default:
			}

			if cancelled {
				log.Info("执行已停止: 已完成 %d 次, %d 成功, %d 失败", total, sc, fc)
			} else {
				log.Info("执行完成: 共 %d 次, %d 成功, %d 失败, 耗时 %v", total, sc, fc, elapsed.Round(time.Millisecond))
			}

			fyne.Do(func() {
				fw.running.Store(false)
				execBtn.Enable()
				execBtn.SetText("▶ 执行 " + act.Label)
				execBtn.Importance = widget.MediumImportance
				fw.progressLabel.Hide()

				if cancelled {
					fw.appendResult(act.Label,
						fmt.Sprintf("⏹ 已停止: 完成 %d/%d 次\n✅ 成功: %d  ❌ 失败: %d\n⏱ 已运行: %s",
							total, totalTarget, sc, fc, elapsed.Round(time.Millisecond)))
				} else {
					fw.appendResult(act.Label,
						fmt.Sprintf("📊 执行完成: %d 个 worker × %d 轮 = %d 次\n✅ 成功: %d  ❌ 失败: %d\n⏱ 总耗时: %s",
							concurrency, loopCount, total, sc, fc, elapsed.Round(time.Millisecond)))
				}
			})
			cancel()
		}()
	})

	actionSelect.SetSelected(actionLabels[0])

	funcSection := container.NewVBox(
		widget.NewSeparator(),
		funcTitle,
		container.NewGridWithColumns(2,
			widget.NewLabel("操作:"), actionSelect,
		),
		controlGrid,
		paramContainer,
		execBtn,
		fw.progressLabel,
	)

	return container.NewVBox(testBtn, funcSection)
}

