# shardkv-demo Makefile
# 快速编译并启动 ShardKV 集群演示 Web 控制台

BINARY   := shardkv-demo
PORT     ?= 8080
CONFIG   ?= config.yaml
PIDFILE  := .$(BINARY).pid

.DEFAULT_GOAL := run

.PHONY: all build run start stop restart clean

all: build

# 编译二进制
build:
	@echo "==> 编译 shardkv-demo ..."
	go build -o $(BINARY) .
	@echo "==> 编译 daemon 二进制（kvsrv, kvraft, shardgrp）..."
	cd ../src && go build -o main/kvsrv    main/kvsrv.go
	cd ../src && go build -o main/kvraft   main/kvraft.go
	cd ../src && go build -o main/shardgrp main/shardgrp.go
	@echo "==> 编译完成"

# 编译并启动（默认目标）
run: build
	@echo "==> 启动 shardkv-demo (端口: $(PORT), 配置: $(CONFIG)) ..."
	./$(BINARY) -port $(PORT) -config $(CONFIG)

# 仅启动（不重新编译）
start:
	@echo "==> 启动 shardkv-demo (端口: $(PORT), 配置: $(CONFIG)) ..."
	./$(BINARY) -port $(PORT) -config $(CONFIG)

# 后台启动
start-bg: build
	@echo "==> 后台启动 shardkv-demo (端口: $(PORT), 配置: $(CONFIG)) ..."
	./$(BINARY) -port $(PORT) -config $(CONFIG) &
	@echo $$! > $(PIDFILE)
	@echo "==> PID: $$(cat $(PIDFILE))  日志: tail -f nohup.out"

# 停止后台进程
stop:
	@if [ -f $(PIDFILE) ]; then \
		echo "==> 停止 shardkv-demo (PID: $$(cat $(PIDFILE))) ..."; \
		kill $$(cat $(PIDFILE)) 2>/dev/null && rm -f $(PIDFILE) || true; \
	else \
		echo "==> 未找到 PID 文件，尝试 pkill ..."; \
		pkill -f "$(BINARY)" 2>/dev/null || echo "    未找到运行中的进程"; \
	fi

# 重启
restart: stop build start-bg

# 清理
clean:
	@echo "==> 清理 ..."
	rm -f $(BINARY) $(PIDFILE)
	@echo "==> 清理完成"
