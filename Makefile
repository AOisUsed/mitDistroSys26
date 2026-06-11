# ============================================================
# Root Makefile — ShardKV 分布式 KV 存储
# ============================================================

.PHONY: all docker run clean

all: docker

# 多阶段构建：Docker 内编译 + 打包，无需本地 Go
docker:
	docker build -t shardkv-demo:latest -f Dockerfile .

# 启动容器（映射本机 8080 端口）
# 通过 -v 挂载本地的 config.yaml，修改配置后无需重新 build 镜像
run:
	docker run --rm -p 8080:8080 \
		-v "$(PWD)/shardkv-demo/config.yaml:/app/shardkv-demo/config.yaml:ro" \
		shardkv-demo:latest

# 清理运行中的容器
clean:
	-docker rm -f shardkv-test 2>/dev/null || true
	-docker rmi shardkv-demo:latest 2>/dev/null || true
