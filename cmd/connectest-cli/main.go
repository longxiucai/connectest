package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/longxiucai/connectest/internal/cli"
	"github.com/longxiucai/connectest/internal/logger"
)

func main() {
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

	logger.Init(logLevel, os.Stderr)
	logger.Info("Connectest CLI 启动，日志级别: %s", logLevel.String())

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigCh
		logger.Info("收到信号 %v，正在清理连接并退出...", sig)
	}()

	rootCmd := cli.NewRootCmd()
	if err := rootCmd.Execute(); err != nil {
		logger.Error("执行失败: %v", err)
		fmt.Fprintf(os.Stderr, "错误: %v\n", err)
		os.Exit(1)
	}
	logger.Info("CLI 退出，所有连接已关闭")
}
