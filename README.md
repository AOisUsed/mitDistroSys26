# Distributed KV Store

基于 Raft 共识算法的高可用、强一致、可水平扩展的分布式分片键值存储系统。
> **简单描述：** 这是一个分布式的键值数据库，把全部数据拆成多个分片（shard），分散存储到不同的服务器组上。每组内部用 Raft 算法保证数据一致，即使部分机器宕机，服务也不中断。需要扩容时，加一组或多组新机器，数据会重新分布（即分片迁移），以提高系统整体吞吐量。

---

## ✨ 主要特性

| 特性             | 说明                                                                            |
|----------------|-------------------------------------------------------------------------------|
| **Raft 共识算法**  | 每个分片组独立运行 Raft，自动选举领导者，容忍少数节点故障                                               |
| **线性一致读写**     | 所有操作对外表现如同单机顺序执行，支持原子 [CAS 条件更新](https://zh.wikipedia.org/wiki/比较并交换)，客户端自动去重 |
| **数据分片**       | 数据按哈希自动分片，均匀分布到多个分片组，支持运行时扩缩容                                                 |
| **分片自动迁移**     | 组加入或离开时数据自动重新分布，迁移过程服务不中断                                                     |
| **多 Raft 组架构** | 每个分片组独立运行 Raft 实例，组间解耦，水平扩展无瓶颈                                                |
| **Web 控制面板**   | 实时拓扑可视化、节点故障注入、网络模拟、CAS 并发演示                                                  |
| **故障恢复**       | 全部节点崩溃重启后数据不丢失，未完成的分片迁移自动恢复                                                   |
| **线性一致性校验**    | 集成 Porcupine，测试中自动校验操作历史是否符合线性一致性规范                                           |

---

## 🚀 快速开始

### 方式一：使用 Docker 启动
```bash
# 构建镜像
make docker

# 启动容器 
make run # 编辑 shardkv-demo/config.yaml 可调整集群初始拓扑（每组节点数、组个数、网络可靠性等）
```
### 方式二：本地启动

#### 前置条件

- Go 1.22+
- Unix-like 操作系统（macOS / Linux）

```bash
cd shardkv-demo
make run # 编辑 shardkv-demo/config.yaml 可调整集群初始拓扑（每组节点数、组个数、网络可靠性等）
```

浏览器打开 `http://localhost:8080`，`Ctrl+C` 停止。

#### 运行测试

```bash
cd src
make raft             # Raft 测试
make shardkv          # 全部 ShardKV 测试
make RUN="-run TestJoinBasic5A" shardkv  # 单个用例
```

---

## 🕹️ 快速上手

服务启动后，通过以下 11 步体验系统核心能力：

|  步骤  | 操作               | 核心概念          |
|:----:|------------------|---------------|
|  1   | 观察初始拓扑           | Raft 组 + 分片分布 |
|  2   | Put/Get 一个键值对    | 线性一致读写        |
|  3   | 生成不同分片的 Key      | 数据分片          |
|  4   | Kill 一个节点（1/3）   | Raft 多数容错     |
|  5   | 再 Kill 一个节点（2/3） | Raft 容错边界     |
|  6   | Start 恢复节点       | Raft 自动重建     |
|  7   | 添加新组，观察分片迁移      | 水平扩容          |
|  8   | 让分片组 Leave       | 缩容与分片再平衡      |
|  9   | 开启不可靠网络后读写       | 网络故障容忍        |
|  10  | CAS 并发竞赛         | 乐观锁并发控制       |
|  11  | 启动混沌猴子           | 持续故障注入        |

> - 详细分步讲解（含操作描述和预期现象）见 [WALKTHROUGH.md](docs/WALKTHROUGH.md)
> - 控制面板各按钮功能说明见 [PANEL.md](docs/PANEL.md)

---

## 📁 项目结构

```
├── Dockerfile                # 多阶段 Docker 构建
├── Makefile                  # 根目录构建脚本
├── docs/                     # 设计文档
│   ├── API.md                #   接口文档
│   ├── ARCHITECTURE.md       #   整体架构
│   ├── DESIGN.md             #   详细设计
│   ├── PANEL.md              #   控制面板功能速查
│   ├── TESTING.md            #   测试覆盖说明
│   ├── WALKTHROUGH.md        #   控制面板分步操作指南
│   └── images/               #   架构图/流程图
├── shardkv-demo/             # Web 演示控制台
│   ├── cluster/              #   集群管理器
│   ├── config/               #   配置文件加载
│   └── web/                  #   HTTP 服务 + 前端
└── src/                      # 核心源码
    ├── debug/                #   Debug 日志
    ├── kvraft/               #   基于 Raft 的 KV 服务（配置仓库使用）
    │   └── rsm/              #     复制状态机中间层
    ├── kvsrv/                #   单节点 KV 服务
    │   └── rpcapi/           #     部分 RPC 错误码定义 + RPC 接口定义
    ├── kvtest/               #   KV 一致性检验器
    ├── main/                 #   可执行程序入口
    ├── models/               #   数据模型定义，一致性测试使用
    ├── raft/                 #   Raft 共识算法实现
    │   ├── raft.go           #     核心算法实现
    │   ├── proxy.go          #     RPC 代理
    │   └── ...
    ├── raftapi/              #   Raft RPC 接口定义
    ├── rpc/                  #   通用 RPC 框架
    ├── shardkv/              #   分片 KV 存储
    │   ├── shardcfg/         #     分片配置定义
    │   ├── shardctrler/      #     分片控制器
    │   └── shardgrp/         #     分片组服务器
    │       └── shardrpc/     #       部分 RPC 接口定义
    ├── tester/               #   分布式测试框架
    └── testgob/              #   Gob 序列化测试
    
```

---

## 📚 技术文档

| 文档                                      | 内容                    |
|-----------------------------------------|-----------------------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | 系统架构、分层设计、模块职责        |
| [DESIGN.md](docs/DESIGN.md)             | 各模块详细设计决策             |
| [API.md](docs/API.md)                   | 模块接口定义、RPC 接口定义、错误码语义 |
| [TESTING.md](docs/TESTING.md)           | 73 个测试的详细说明与覆盖分析      |
| [WALKTHROUGH.md](docs/WALKTHROUGH.md)   | 控制面板分步操作指南            |
| [PANEL.md](docs/PANEL.md)               | 控制面板功能速查表             |

---

## 🙏 致谢

本项目基于 [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/) 课程实验框架开发。

## 🔗 相关资源

- [Raft 论文](https://raft.github.io/raft.pdf)
- [Porcupine 线性一致性检验器](https://github.com/anishathalye/porcupine)
- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/)
- [本人最初使用仓库](https://github.com/AOisUsed/mitDistroSys25) - 包含 Raft, KVRaft, MapReduce等模块的开发历史。
