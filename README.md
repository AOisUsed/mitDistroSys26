# Distributed KV Store

基于 Raft 的高可用强一致性分布式键值存储系统。

 **本地模拟真实分布式环境** — 每个 Raft 节点运行在独立 OS 进程中，通过 Unix 域套接字通信。配合 Web 控制面板，可在单机上模拟 Join/Leave、故障注入、分片迁移等分布式场景。

## 系统要求

> **⚠️ 仅支持 Unix-like 操作系统**
>
> 本项目**无法在 Windows 上原生运行**。核心依赖包括：
>
> - **Unix 域套接字（Unix Domain Socket）** — 用于 tester 与各 Raft 服务器守护进程之间的跨进程 RPC 通信
> - **Unix 信号处理** — 捕获 `SIGINT` 等信号用于可视化输出
> - **类 Unix 文件系统路径** — 套接字文件存放于 `/tmp/` 目录

### Windows 用户

推荐使用以下方式在 Windows 上运行本项目：

| 方式 | 说明 | 推荐度 |
|------|------|--------|
| **WSL2** | Windows Subsystem for Linux 2，安装 Ubuntu/Debian 发行版后，在 WSL 终端中操作 | ⭐ 推荐 |
| **Docker** | 自行编写 Dockerfile，在 Linux 容器中运行 | ✅ 可行 |

## 快速开始

### 前置条件

- Go 1.22+
- Make 
- Unix-like 操作系统（macOS / Linux）

### 运行测试

> 以下命令均在 `src/` 目录下执行。

| 测试模块 | 说明 | 构建 + 运行测试 | 仅构建 |
|---------|------|----------------|--------|
| **Raft** | Leader 选举、日志复制、持久化、快照 | `make raft` | `make raft-build` |
| **KVRaft** | 基于 Raft 的线性一致 KV 服务 | `make kvraft` | `make kvraft-build` |
| **ShardKV** | 分片 KV + 动态重配置 | `make shardkv` | `make shardkv-build` |

```bash
# 以ShardKV为例，运行 ShardKV 全部测试（分片 KV + 动态重配置，含 race detection）
cd src
make shardkv

# 运行单个测试用例
make RUN="-run TestName" shardkv
```

### 启动 Web Demo 控制面板以互动测试/操作

```bash
cd shardkv-demo
make
```

然后打开浏览器访问 `http://localhost:8080`，使用 `Ctrl+C` 停止。

## 项目结构

```
├── Makefile                  # 顶层 Makefile
├── src/                      # 核心源码（Go module: kvstore）
│   ├── Makefile              # 测试构建入口
│   ├── go.mod
│   ├── main/                 # 可执行程序入口
│   │   ├── kvsrv.go          #   KV 服务器 daemon 入口
│   │   └── shardgrp.go       #   Shard Group daemon 入口
│   ├── raft/                 # Raft 共识算法实现
│   ├── raftapi/              # Raft 接口定义
│   ├── kvraft/               # 基于 Raft 的 KV 服务（线性一致性）
│   │   └── rsm/              #   Replicated State Machine
│   ├── kvsrv/                # 单节点 KV 服务
│   ├── shardkv/              # Sharded KV 存储（分片 + 迁移）
│   │   ├── shardcfg/         #   分片配置定义
│   │   ├── shardctrler/      #   分片控制器（配置管理 + 迁移调度）
│   │   └── shardgrp/         #   分片组服务器
│   ├── tester/               # 分布式测试框架
│   │   ├── sockrpc/          #   基于 Unix 域套接字的 RPC 通信
│   │   ├── demux/            #   多路复用传输层
│   │   └── ...
│   ├── kvtest/               # KV 线性一致性检验器
│   ├── rpc/                  # 通用 RPC 框架
│   ├── testgob/              # Gob 序列化工具
│   ├── models/               # 一致性模型定义
│   └── debug/                # 调试日志工具
└── shardkv-demo/             # Web 演示控制台
    ├── cluster/              #   集群管理器
    │   ├── manager.go        #   分组管理、Join/Leave、故障注入
    │   └── types.go          #   状态类型定义
    └── web/                  #   HTTP 服务 + 前端静态文件
```

## 架构

```
┌─────────────────────────────────────────────┐
│                  Tester                      │
│  (测试框架 / 编排器 / 可视化)                │
└──────────┬──────────┬──────────┬────────────┘
           │ Unix     │ Unix     │ Unix
           │ Socket   │ Socket   │ Socket
     ┌─────▼────┐┌────▼───┐┌────▼────┐
     │ Raft     ││ Raft   ││ Raft    │ ...
     │ Server 0 ││ Server1││ Server 2│
     └──────────┘└────────┘└─────────┘
       ┌──────────────────────────────┐
       │     Shard Controller         │
       │  (配置管理 / 分片迁移调度)       │
       └──────────────────────────────┘
```

每个 Raft 服务器运行在独立的 OS 进程中（daemon 模式），通过 **Unix 域套接字**与 tester 通信，模拟真实分布式环境。

## 主要特性

- **Raft 共识算法** — Leader 选举、日志复制、持久化
- **线性一致性 KV 存储** — 基于 Raft 的容错键值服务
- **分片（Sharding）** — 将键空间划分为多个分片
- **动态重配置** — 支持 Join/Leave 组，自动迁移分片
- **网络分区与故障注入** — 模拟网络隔离、节点崩溃
- **线性一致性检验** — 通过 [Porcupine](https://github.com/anishathalye/porcupine) 验证历史操作的线性一致性
- **可视化** — 测试结果可视化 HTML 输出 + Web Demo 控制台

## 致谢

本项目源自 [MIT 6.5840 (formerly 6.824) Distributed Systems](https://pdos.csail.mit.edu/6.824/) 课程实验，基于课程框架实现所有功能，并进行了扩展与重构，包括但不限于分片迁移调度优化、Web 可视化控制台、本地集群模拟等。

## 相关资源
- [Raft 论文](https://raft.github.io/raft.pdf)
- [Porcupine 线性一致性检验器](https://github.com/anishathalye/porcupine)
- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/)
