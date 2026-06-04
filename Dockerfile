# ============================================================
# Dockerfile for shardkv-demo
# 多阶段构建：编译阶段 + 运行阶段
# ============================================================

# ---- 阶段 1: 编译 ----
FROM golang:1.22-alpine3.18 AS builder

ENV GOPROXY=https://goproxy.cn,direct

WORKDIR /app

# 1) 复制 src/ 模块（核心 kvstore 库）
COPY src/ ./src/

# 2) 复制 shardkv-demo 模块（Web 控制台）
COPY shardkv-demo/ ./shardkv-demo/

# 3) 编译 daemon 二进制：kvsrv、kvraft 和 shardgrp
#    这些会通过 path() 函数在运行时按 "src/main/<prog>" 路径查找
RUN cd src && \
    go build -ldflags="-s -w" -o main/kvsrv    main/kvsrv.go    && \
    go build -ldflags="-s -w" -o main/kvraft   main/kvraft.go   && \
    go build -ldflags="-s -w" -o main/shardgrp main/shardgrp.go

# 4) 编译 shardkv-demo 二进制
RUN cd shardkv-demo && \
    go build -ldflags="-s -w" -o shardkv-demo .

# ---- 阶段 2: 运行 ----
FROM alpine:3.18

# ca-certificates：Go 程序可能的 SSL 证书需求
RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

# 复制 daemon 二进制（按 path() 函数查找路径：src/main/<prog>）
COPY --from=builder /app/src/main/kvsrv    ./src/main/kvsrv
COPY --from=builder /app/src/main/kvraft   ./src/main/kvraft
COPY --from=builder /app/src/main/shardgrp ./src/main/shardgrp

# 复制 shardkv-demo 及其静态/配置文件
COPY --from=builder /app/shardkv-demo/shardkv-demo ./shardkv-demo/
COPY --from=builder /app/shardkv-demo/config.yaml  ./shardkv-demo/
COPY --from=builder /app/shardkv-demo/web/static/  ./shardkv-demo/web/static/

EXPOSE 8080

WORKDIR /app/shardkv-demo
ENTRYPOINT ["./shardkv-demo"]
