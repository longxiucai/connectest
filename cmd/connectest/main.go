package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/longxiucai/connectest/internal/cli"
	"github.com/longxiucai/connectest/internal/gui"
	"github.com/longxiucai/connectest/internal/logger"
)

func main() {
	// 从参数中提取日志级别
	logLevel := logger.INFO
	for i, arg := range os.Args {
		if arg == "--log-level" && i+1 < len(os.Args) {
			logLevel = logger.ParseLevel(os.Args[i+1])
			os.Args = append(os.Args[:i], os.Args[i+2:]...)
			break
		}
		if strings.HasPrefix(arg, "--log-level=") {
			logLevel = logger.ParseLevel(strings.TrimPrefix(arg, "--log-level="))
			os.Args = append(os.Args[:i], os.Args[i+1:]...)
			break
		}
	}

	// 初始化全局日志器，输出到 stderr
	logger.Init(logLevel, os.Stderr)
	logger.Info("Connectest 启动，日志级别: %s", logLevel.String())

	// 全局信号处理：确保程序退出前有机会执行 defer 清理连接
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		logger.Info("收到信号 %v，正在清理连接并退出...", sig)
		// 不直接 os.Exit，让 main 函数正常返回，触发所有 defer
	}()

	// 如果没有任何参数或第一个参数是 "gui"，启动 GUI
	if len(os.Args) <= 1 || (len(os.Args) > 1 && os.Args[1] == "gui") {
		logger.Debug("启动 GUI 模式")
		a := gui.NewApp()
		a.Run()
		logger.Info("GUI 退出，所有连接已关闭")
		return
	}

	// 否则使用 CLI 模式
	logger.Debug("启动 CLI 模式")
	rootCmd := cli.NewRootCmd()
	if err := rootCmd.Execute(); err != nil {
		logger.Error("执行失败: %v", err)
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
	logger.Info("CLI 退出，所有连接已关闭")
}
