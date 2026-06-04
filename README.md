# Distributed KV Store

基于 Raft 共识算法 的高可用、强一致、可水平扩展的分布式分片键值存储系统。

> **核心特性：** 每个 Raft 节点运行在独立 OS 进程中，通过 Unix 域套接字通信，在单机上模拟真实分布式环境。配合 Web 控制面板，可视化演示键值读写，分片组加入/离开、故障注入、分片迁移等场景。

---

## 快速开始



### 方式一：使用 Docker 启动

```bash
# 构建镜像
make docker

# 启动容器（访问 http://localhost:8080）
make run
```

### 方式二：本地启动 Web Demo
#### 前置条件

- Go 1.22+
- Unix-like 操作系统（macOS / Linux）

```bash
cd shardkv-demo
make run
```

浏览器打开 `http://localhost:8080`，`Ctrl+C` 停止。

#### 运行测试

```bash
cd src
make shardkv          # 全部 ShardKV 测试
make raft             # Raft 测试
make RUN="-run TestJoinBasic5A" shardkv  # 单个用例
```

> 详细测试覆盖见 [TESTING.md](docs/TESTING.md)
---

## 主要特性

| 特性                  | 说明                                             |
|---------------------|------------------------------------------------|
| **Raft 共识算法**       | 完整的 Leader 选举、日志复制、冲突回退（快速回退优化）、日志压缩与快照安装      |
| **线性一致性键值读写**       | 基于版本号的条件更新（CAS），客户端请求去重，保证线性一致性                |
| **数据分片** | FNV-1a 哈希分片，支持动态分片组 加入/离开                      |
| **三阶段分片迁移**         | Freeze → Install → Delete，迁移过程保证数据一致性和幂等性      |
| **双配置机制**           | currentConfig / nextConfig + CAS 写入，支持多控制器安全并发 |
| **Web 控制面板**        | 实时拓扑展示、节点故障注入、网络分区模拟、CAS 并发写入演示                |
| **故障恢复**            | 全部节点崩溃重启后数据不丢失，未完成迁移可自动恢复                      |
| **线性一致性检验**         | 集成 Porcupine 验证器，测试中自动校验操作历史                   |

---

## 项目结构

```
├── src/                      # 核心源码
│   ├── raft/                 # Raft 共识算法实现
│   │   ├── raft.go           #   核心状态机
│   │   ├── proxy.go          #   RPC 代理
│   │   └── ...
│   ├── kvraft/               # 基于 Raft 的 KV 服务（配置仓库）
│   │   └── rsm/              #   复制状态机中间层
│   ├── kvsrv/                # 单节点 KV 服务
│   ├── shardkv/              # 分片 KV 存储
│   │   ├── shardcfg/         #   分片配置定义
│   │   ├── shardctrler/      #   分片控制器
│   │   └── shardgrp/         #   分片组服务器
│   ├── tester/               # 分布式测试框架
│   ├── kvtest/               # KV 一致性检验器
│   ├── rpc/                  # 通用 RPC 框架
│   └── main/                 # 可执行程序入口
├── shardkv-demo/             # Web 演示控制台
│   ├── cluster/              #   集群管理器
│   └── web/                  #   HTTP 服务 + 前端
├── Dockerfile                # 多阶段 Docker 构建
├── Makefile                  # 根目录构建脚本
└── docs/                     # 设计文档
    ├── ARCHITECTURE.md       #   整体架构
    ├── DESIGN.md             #   详细设计
    ├── API.md                #   接口文档
    ├── TESTING.md            #   测试覆盖说明
    └── BENCHMARKS.md         #   性能基准测试
```

---

## 技术文档

| 文档                                      | 内容 |
|-----------------------------------------|------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | 系统架构、分层设计、模块职责 |
| [DESIGN.md](docs/DESIGN.md)             | 关键设计决策详解（Raft、去重、CAS、迁移协议） |
| [API.md](docs/API.md)                   | RPC 接口定义、错误码语义 |
| [TESTING.md](docs/TESTING.md)           | 73 个测试的详细说明与覆盖分析 |
| [BENCHMARKS.md](docs/BENCHMARKS.md)     | 吞吐量、延迟、扩展性基准测试 |

---

## 致谢

本项目基于 [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/) 课程实验框架开发。

## 相关资源

- [Raft 论文](https://raft.github.io/raft.pdf)
- [Porcupine 线性一致性检验器](https://github.com/anishathalye/porcupine)
- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/)
- [本人最初使用仓库](https://github.com/AOisUsed/mitDistroSys25) - 包含 Raft, KVRaft, MapReduce等模块的开发历史。
