# Distributed KV Store

基于 Raft 共识算法的高可用、强一致、可水平扩展的分布式分片键值存储系统。
> **简单描述：** 这是一个分布式的键值数据库，把全部数据拆成多个分片（shard），分散存储到不同的服务器组上。每组内部用 Raft 算法保证数据一致，同时保证部分机器宕机，服务也不中断。需要扩容时，加一组或多组新机器，数据可以通过分片迁移重新分布，以提高系统整体吞吐量。

## ✨ 主要特性

| 特性                  | 说明                                                                         |
|---------------------|----------------------------------------------------------------------------|
| **自实现 Raft 共识算法**   | 从零实现的 Raft 算法，容忍少数节点故障，提供强一致基础                                             |
| **线性一致读写**          | 所有操作对外表现如同单机顺序执行，支持原子 [CAS 条件更新](https://zh.wikipedia.org/wiki/比较并交换)与操作去重 |
| **Multi-Raft 分片架构** | 数据按哈希自动分片，一个 Raft 实例包含多个分片组，组间解耦，水平扩展无瓶颈                                   |
| **在线分片迁移**          | 扩缩容时数据自动重新分布，迁移过程中未迁移分片服务不中断                                               |
| **故障恢复**            | 全部节点崩溃重启后数据不丢失，未完成的分片迁移自动恢复                                                |
| **Web 可视化控制台**      | 单页应用实时展示节点状态与分片分布，SSE 推送节点状态变更、观测日志等，提供多种网络故障模拟                            |

## 🚀 快速开始

### 方式一：使用 Docker 启动
```bash
# 构建镜像
make docker

# 启动容器 
make run 
# 前端默认端口 8080, 自定义则运行 make run PORT=XXXX (XXXX是自定义端口号)
# 编辑 shardkv-demo/config.yaml 可调整集群初始拓扑（每组节点数、组个数、网络可靠性、Raft快照阈值等）
```
浏览器打开 `http://localhost:8080`，`Ctrl+C` 停止。

### 方式二：本地启动

#### 前置条件

- Go 1.22+
- Unix-like 操作系统（macOS / Linux）

```bash
cd shardkv-demo
make run 
# 前端默认端口 8080, 自定义则运行 make run PORT=XXXX (XXXX是自定义端口号)
# 编辑 shardkv-demo/config.yaml 可调整集群初始拓扑（每组节点数、组个数、网络可靠性、Raft快照阈值等）
```

浏览器打开 `http://localhost:8080`，`Ctrl+C` 停止。

#### 运行测试

```bash
cd src
make raft             # Raft 全部测试
make kvraft           # 单集群KV系统 全部测试
make shardkv          # 分片KV系统 全部测试
```
详见 [TESTING.md](docs/TESTING.md)

## 🕹️ 快速上手

服务启动后，通过以下 13 步体验系统核心能力：

| 步骤 | 操作                                                    | 核心概念          |
|:--:|-------------------------------------------------------|---------------|
| 1  | [了解控制台布局](docs/WALKTHROUGH.md#第一步了解控制台布局)             | Raft 组 + 分片分布 |
| 2  | [Put/Get 一个键值对](docs/WALKTHROUGH.md#第二步写入与读取)         | 线性一致读写        |
| 3  | [生成不同分片的 Key](docs/WALKTHROUGH.md#第三步了解数据分片)          | 数据分片          |
| 4  | [CAS 并发竞赛](docs/WALKTHROUGH.md#第四步cas-并发竞赛)           | 乐观锁并发控制       |
| 5  | [批量随机写入](docs/WALKTHROUGH.md#第五步批量随机写入)               | 批量并发写入        |
| 6  | [终止一个节点（1/3）](docs/WALKTHROUGH.md#第六步节点宕机多数在线服务不中断)   | Raft 多数容错     |
| 7  | [再终止一个节点（2/3）](docs/WALKTHROUGH.md#第七步继续宕机多数离线服务中断)   | Raft 容错边界     |
| 8  | [重启节点](docs/WALKTHROUGH.md#第八步恢复节点服务自动恢复)             | Raft 自动重建     |
| 9  | [隔离 vs 终止节点](docs/WALKTHROUGH.md#第九步节点隔离-vs-终止两种故障模式) | 网络分区 vs 进程级故障 |
| 10 | [添加/移除分片组](docs/WALKTHROUGH.md#第十步动态扩缩容与分片迁移)         | 水平扩缩容与分片迁移    |
| 11 | [开启观测日志看系统内部](docs/WALKTHROUGH.md#第十一步开启观测日志看系统内部)    | SSE 实时推送事件流   |
| 12 | [开启不可靠网络后读写](docs/WALKTHROUGH.md#第十二步不可靠网络下的系统行为)     | 网络故障容忍        |
| 13 | [启动混沌猴子](docs/WALKTHROUGH.md#第十三步混沌猴子自动故障注入)          | 持续故障注入        |

> - 详细分步讲解见 [WALKTHROUGH.md](docs/WALKTHROUGH.md)
> - 控制台各控件功能速查见 [CONSOLE.md](docs/CONSOLE.md)

## 📁 项目结构

```
├── Dockerfile                          # 多阶段 Docker 构建
├── Makefile                            # 根目录构建脚本
├── docs/                               # 技术文档（架构/设计/API/测试/操作指南）
├── shardkv-demo/                       # Web 可视化控制台
│   ├── main.go                         #   入口
│   ├── config.yaml                     #   集群拓扑配置
│   ├── cluster/                        #   集群管理（生命周期、混沌猴子）
│   └── web/                            #   HTTP 服务 + 前端 SPA
└── src/                                # 核心源码
    ├── raft/                           #   Raft 共识算法
    ├── kvraft/                         #   基于 Raft 的单集群 KV 服务（配置仓库）
    ├── shardkv/                        #   分片 KV 存储（控制器 + 分片组）
    ├── kvsrv/                          #   单节点 KV 服务（对比测试用）
    ├── kvtest/                         #   线性一致性检验器
    ├── tester/                         #   分布式测试框架
    └── main/                           #   可执行程序入口
```

## 📚 技术文档

| 文档                                      | 内容                    |
|-----------------------------------------|-----------------------|
| [ARCHITECTURE.md](docs/ARCHITECTURE.md) | 系统架构、分层设计、模块职责        |
| [DESIGN.md](docs/DESIGN.md)             | 各模块详细设计决策             |
| [API.md](docs/API.md)                   | 模块接口定义、RPC 接口定义、错误码语义 |
| [TESTING.md](docs/TESTING.md)           | 73 个测试的详细说明与覆盖分析      |
| [WALKTHROUGH.md](docs/WALKTHROUGH.md)   | 控制面板分步操作指南            |
| [CONSOLE.md](docs/CONSOLE.md)           | 控制台速查手册               |

## 🙏 致谢

本项目基于 [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/) 课程实验框架开发。

## 🔗 相关资源

- [Raft 论文](https://raft.github.io/raft.pdf)
- [MIT 6.5840 Distributed Systems](https://pdos.csail.mit.edu/6.824/)
- [Porcupine 线性一致性检验器](https://github.com/anishathalye/porcupine)
- [最初使用仓库](https://github.com/AOisUsed/mitDistroSys25) - 包含 Raft, KVRaft, MapReduce 等模块的开发历史，后整体迁移至此

## 📬 联系方式
- GitHub: [AOisUsed](https://github.com/AOisUsed)
- Email: ao.gao@qq.com
