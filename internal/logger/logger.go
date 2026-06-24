package logger

import (
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

// Level 日志级别
type Level int

const (
	DEBUG Level = iota
	INFO
	WARN
	ERROR
)

func (l Level) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	default:
		return "UNKNOWN"
	}
}

// ParseLevel 解析日志级别字符串
func ParseLevel(s string) Level {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "DEBUG":
		return DEBUG
	case "INFO":
		return INFO
	case "WARN", "WARNING":
		return WARN
	case "ERROR":
		return ERROR
	default:
		return INFO
	}
}

// Logger 日志记录器
type Logger struct {
	mu       sync.Mutex
	level    Level
	output   io.Writer
	prefix   string
	listeners []func(level Level, msg string) // GUI 回调
}

var (
	defaultLogger *Logger
	once          sync.Once
)

// Init 初始化全局日志器
func Init(level Level, output io.Writer) {
	once = sync.Once{} // reset
	once.Do(func() {
		if output == nil {
			output = os.Stderr
		}
		defaultLogger = &Logger{
			level:  level,
			output: output,
		}
	})
}

// Default 获取全局日志器
func Default() *Logger {
	if defaultLogger == nil {
		Init(INFO, os.Stderr)
	}
	return defaultLogger
}

// SetLevel 设置日志级别
func SetLevel(level Level) {
	Default().mu.Lock()
	defer Default().mu.Unlock()
	Default().level = level
}

// GetLevel 获取当前日志级别
func GetLevel() Level {
	Default().mu.Lock()
	defer Default().mu.Unlock()
	return Default().level
}

// AddListener 添加日志监听器（GUI 用来把日志同步到界面）
func AddListener(fn func(level Level, msg string)) {
	Default().mu.Lock()
	defer Default().mu.Unlock()
	Default().listeners = append(Default().listeners, fn)
}

// ClearListeners 清除所有监听器
func ClearListeners() {
	Default().mu.Lock()
	defer Default().mu.Unlock()
	Default().listeners = nil
}

func (l *Logger) log(level Level, format string, args ...interface{}) {
	l.mu.Lock()
	if level < l.level {
		l.mu.Unlock()
		return
	}
	output := l.output
	listeners := make([]func(Level, string), len(l.listeners))
	copy(listeners, l.listeners)
	l.mu.Unlock()

	ts := time.Now().Format("2006-01-02 15:04:05.000")
	msg := fmt.Sprintf(format, args...)
	line := fmt.Sprintf("[%s] [%-5s] %s", ts, level.String(), msg)

	// 写终端
	if output != nil {
		fmt.Fprintln(output, line)
	}

	// 通知监听器
	for _, fn := range listeners {
		fn(level, line)
	}
}

// Debug 输出 DEBUG 日志
func Debug(format string, args ...interface{}) {
	Default().log(DEBUG, format, args...)
}

// Info 输出 INFO 日志
func Info(format string, args ...interface{}) {
	Default().log(INFO, format, args...)
}

// Warn 输出 WARN 日志
func Warn(format string, args ...interface{}) {
	Default().log(WARN, format, args...)
}

// Error 输出 ERROR 日志
func Error(format string, args ...interface{}) {
	Default().log(ERROR, format, args...)
}

// ─── 带模块前缀的子日志器 ───

// ModuleLogger 带模块名的日志器
type ModuleLogger struct {
	module string
}

// NewModule 创建带模块前缀的子日志器
func NewModule(module string) *ModuleLogger {
	return &ModuleLogger{module: module}
}

func (m *ModuleLogger) log(level Level, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	Default().log(level, "[%s] %s", m.module, msg)
}

func (m *ModuleLogger) Debug(format string, args ...interface{}) {
	m.log(DEBUG, format, args...)
}
func (m *ModuleLogger) Info(format string, args ...interface{}) {
	m.log(INFO, format, args...)
}
func (m *ModuleLogger) Warn(format string, args ...interface{}) {
	m.log(WARN, format, args...)
}
func (m *ModuleLogger) Error(format string, args ...interface{}) {
	m.log(ERROR, format, args...)
}
