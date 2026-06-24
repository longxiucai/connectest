.PHONY: build build-cli-all build-cli-linux-amd64 build-cli-linux-arm64 build-cli-darwin-amd64 build-cli-darwin-arm64 build-cli-windows-amd64 build-cli-windows-arm64 run clean deps install-deps

APP_NAME=connectest
BUILD_DIR=./bin
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

build: deps
	go build -o $(BUILD_DIR)/$(APP_NAME)-$(VERSION) ./cmd/connectest

run: build
	$(BUILD_DIR)/$(APP_NAME)-$(VERSION)

# CLI 交叉编译 (无需 CGO)
build-cli-all: build-cli-linux-amd64 build-cli-linux-arm64 build-cli-darwin-amd64 build-cli-darwin-arm64 build-cli-windows-amd64 build-cli-windows-arm64

build-cli-linux-amd64:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-linux-amd64 ./cmd/connectest-cli

build-cli-linux-arm64:
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-linux-arm64 ./cmd/connectest-cli

build-cli-darwin-amd64:
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-darwin-amd64 ./cmd/connectest-cli

build-cli-darwin-arm64:
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-darwin-arm64 ./cmd/connectest-cli

build-cli-windows-amd64:
	CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-windows-amd64.exe ./cmd/connectest-cli

build-cli-windows-arm64:
	CGO_ENABLED=0 GOOS=windows GOARCH=arm64 go build -o $(BUILD_DIR)/$(APP_NAME)-cli-$(VERSION)-windows-arm64.exe ./cmd/connectest-cli

deps: install-deps
	go mod tidy

install-deps:
	sudo apt install -y gcc libxi-dev libxcursor-dev libxinerama-dev libxxf86vm-dev libgl-dev libx11-dev libxrender-dev

clean:
	rm -rf $(BUILD_DIR)
