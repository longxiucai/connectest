# 伪目标声明
.PHONY: all build build-all build-cli-all run deps install-deps clean
.PHONY: build-linux-amd64 build-linux-arm64
.PHONY: build-cli-linux-amd64 build-cli-linux-arm64
.PHONY: build-cli-darwin-amd64 build-cli-darwin-arm64
.PHONY: build-cli-windows-amd64 build-cli-windows-arm64

# 基础配置变量
APP_NAME     := connectest
BUILD_DIR    := ./bin
# 从git获取版本，无tag则为dev
VERSION      := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
# 输出主程序文件名
MAIN_BIN     := $(BUILD_DIR)/$(APP_NAME)-$(VERSION)

# 构建前自动创建输出目录
$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

# 整体打包入口：主程序全平台 + CLI全平台
all: build-all build-cli-all

# ====================== 依赖安装 ======================
# 系统依赖（仅安装缺失的包）
DEPS := gcc libxi-dev libxcursor-dev libxinerama-dev libxxf86vm-dev libxrandr-dev libx11-dev libxrender-dev libgl-dev libgl1-mesa-dev
MISSING_DEPS := $(shell for pkg in $(DEPS); do dpkg -l $$pkg 2>/dev/null | grep -q '^ii' || echo $$pkg; done)

install-deps:
	@if [ -n "$(MISSING_DEPS)" ]; then \
		echo "安装缺失的依赖: $(MISSING_DEPS)"; \
		sudo apt install -y $(MISSING_DEPS); \
	else \
		echo "所有依赖已安装，跳过"; \
	fi

# Go模块依赖整理
deps: install-deps
	go mod tidy

# ====================== GUI主程序构建（CGO启用，图形界面） ======================
# 当前本机编译
build: deps $(BUILD_DIR)
	go build -ldflags "-X github.com/longxiucai/connectest/internal/gui.Version=$(VERSION)" -o $(MAIN_BIN) .

# Linux amd64
build-linux-amd64: deps $(BUILD_DIR)
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/gui.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-$(VERSION)-linux-amd64 .

# Linux arm64
build-linux-arm64: deps $(BUILD_DIR)
	CGO_ENABLED=1 GOOS=linux GOARCH=arm64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/gui.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-$(VERSION)-linux-arm64 .

# GUI全平台打包
build-all: build-linux-amd64 build-linux-arm64

# ====================== CLI工具交叉编译（纯静态 CGO=0，无系统依赖） ======================
build-cli-linux-amd64: $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/cli.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-linux-amd64 ./cmd/connectest-cli

build-cli-linux-arm64: $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/cli.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-linux-arm64 ./cmd/connectest-cli

build-cli-darwin-amd64: $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/cli.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-darwin-amd64 ./cmd/connectest-cli

build-cli-darwin-arm64: $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/cli.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-darwin-arm64 ./cmd/connectest-cli

build-cli-windows-amd64: $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/cli.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-windows-amd64.exe ./cmd/connectest-cli

build-cli-windows-arm64: $(BUILD_DIR)
	CGO_ENABLED=0 GOOS=windows GOARCH=arm64 \
	go build -ldflags "-X github.com/longxiucai/connectest/internal/cli.Version=$(VERSION)" -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-windows-arm64.exe ./cmd/connectest-cli

# CLI 全部平台一键编译
build-cli-all: \
	build-cli-linux-amd64 build-cli-linux-arm64 \
	build-cli-darwin-amd64 build-cli-darwin-arm64 \
	build-cli-windows-amd64 build-cli-windows-arm64

# ====================== 运行 & 清理 ======================
# 编译后运行本机GUI程序
run: build
	$(MAIN_BIN)

# 清理编译产物
clean:
	rm -rf $(BUILD_DIR)