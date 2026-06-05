# 接口文档

## Raft 共识模块接口

| 接口 | 说明 |
|------|------|
| `Start(command) → (index, term, isLeader)` | 尝试对命令进行集群共识 |
| `GetState() → (term, isLeader)` | 获取当前任期和角色 |
| `Snapshot(index, snapshot)` | 向 Raft 传递快照，触发日志压缩 |
| `PersistBytes() → size` | 获取日志和 Raft 状态占用字节数 |

## RSM 层接口

| 接口 | 说明 |
|------|------|
| `Submit(request) → (rpcErr, result)` | 提交请求并等待共识和执行结果 |

## 服务层接口

### StateMachine 接口 (RSM使用)

| 接口                        | 说明             |
|---------------------------|----------------|
| `DoOp(request) → result`  | 操作状态机，并返回操作结果  |
| `Snapshot() → snapshotData` | 对状态机快照，并返回快照数据 |
| `Restore(snapshotData)` | 将状态机变成快照记录的状态  |


### 键值读写 RPC 接口

#### Get

| 方向 | 字段 | 类型 | 说明 |
|------|------|------|------|
| **请求** | Key | string | 要查询的键 |
| **回复** | Value | string | 键对应的值 |
| | Version | uint64 | 键当前版本号 |
| | Err | rpcErr | OK / ErrNoKey |

#### Put

| 方向 | 字段 | 类型 | 说明 |
|------|------|------|------|
| **请求** | ClientId | uint64 | 客户端唯一标识 |
| | RequestId | uint64 | 请求序列号（单调递增） |
| | Key | string | 要写入的键 |
| | Value | string | 要写入的值 |
| | Version | uint64 | 客户端声明的预期版本号 |
| **回复** | Err | rpcErr | OK / ErrNoKey / ErrVersion |

### 分片迁移 RPC 接口

#### FreezeShard

冻结旧组上的分片，同时获取状态数据以安装到新组。

| 方向 | 字段 | 类型 | 说明 |
|------|------|------|------|
| **请求** | Shard | int | 分片号 |
| | Num | int | 配置号 |
| **回复** | State | []byte | 被冻结的分片数据 |
| | Num | int | 配置号 |
| | Err | rpcErr | OK / ErrIllegalOperation |

#### InstallShard

将分片数据安装到新组。

| 方向 | 字段 | 类型 | 说明 |
|------|------|------|------|
| **请求** | Shard | int | 分片号 |
| | State | []byte | 分片数据（键值 + 去重元数据） |
| | Num | int | 配置号 |
| **回复** | Err | rpcErr | OK / ErrIllegalOperation |

#### DeleteShard

删除旧组上的分片副本。

| 方向 | 字段 | 类型 | 说明 |
|------|------|------|------|
| **请求** | Shard | int | 分片号 |
| | Num | int | 配置号 |
| **回复** | Err | rpcErr | OK / ErrIllegalOperation |

## 分片配置仓库接口
通过 RPC 实现，此处以 `方法调用` 形式呈现

| 接口 | 说明 |
|------|------|
| `Get(key) → (value, version, rpcErr)` | 读取配置 |
| `Put(key, value, version) → rpcErr` | 带版本检查的写入 |

## 错误码语义

### 键值操作错误码

| 错误码            | 含义     | 说明                                 |
|----------------|--------|------------------------------------|
| OK             | 操作成功   | 读写正常完成                             |
| ErrNoKey       | 键不存在   | Get 时键不存在，或 Put 时键不存在且 version ≠ 0 |
| ErrVersion     | 版本不匹配  | Put 时提供的版本号与当前版本不一致                |
| ErrWrongGroup  | 分片不在本组 | 分片不在当前组内，已迁移到其他组                   |                           |
| ErrWrongLeader | 非领导者   | 当前节点不是 Raft 领导者                    |


### 分片迁移错误码

| 错误码 | 含义 | 说明                 |
|--------|------|--------------------|
| OK | 操作成功 | 正常完成               |
| ErrIllegalOperation | 非法操作 | 分片状态跳转异常           |
| ErrWrongLeader | 非领导者 | 当前节点不是 Raft 领导者    |
| ErrWrongGroup | 分片不在本组 | 配置已变更，分片迁移到其他组     |
| ErrRetryExhausted | 重试耗尽 | 组内所有节点轮询尝试过一定次数仍失败 |

### 错误码传播路径

#### PUT 操作
![PUT RPC Errors](docs/images/put_rpcErrs.svg)

#### GET 操作
![GET RPC Errors](docs/images/get_rpcErrs.svg)

#### 分片迁移 操作
![SHARD MIGRATION RPC Errors](docs/images/shardmigration_rpcErrs.svg)


## Raft 内部 RPC

### RequestVote

| 方向 | 字段 | 类型 | 说明                   |
|------|------|------|----------------------|
| **请求** | Term | int | 候选者任期                |
| | CandidateId | int | 候选者节点 ID             |
| | LastLogIndex | int | 候选者最后一条日志索引          |
| | LastLogTerm | int | 候选者最后一条日志任期          |
| **回复** | Term | int | 接收方当前任期，用于候选者更新自己的任期 |
| | VoteGranted | bool | 是否同意投票               |

### AppendEntries

| 方向 | 字段 | 类型          | 说明 |
|------|------|-------------|------|
| **请求** | Term | int         | 领导者任期 |
| | LeaderId | int         | 领导者节点 ID |
| | PrevLogIndex | int         | 新日志前一条日志的索引 |
| | PrevLogTerm | int         | 新日志前一条日志的任期 |
| | Entries | [ ]LogEntry | 待追加的日志条目 |
| | LeaderCommit | int         | 领导者的已提交索引 |
| **回复** | Success | bool        | 是否接受日志追加 |
| | Term | int         | 接收方当前任期 |
| | ConflictTerm | int         | 冲突位置日志的任期 |
| | ConflictIndex | int         | 冲突位置日志索引 |

#### 日志条目 LogEntry

| 字段      | 类型  | 说明         |
|---------|-----|------------|
| Term    | int | 日志条目追加对应任期 |
| Command | any  | 日志中存放的操作   |

### InstallSnapshot

| 方向 | 字段 | 类型      | 说明 |
|------|------|---------|------|
| **请求** | Term | int     | 领导者任期 |
| | LeaderId | int     | 领导者节点 ID |
| | LastIncludedIndex | int     | 快照包含的最大日志索引 |
| | LastIncludedTerm | int     | 快照包含的最大日志任期 |
| | Data | [ ]byte | 快照数据 |
| **回复** | Term | int     | 接收方当前任期 |


## RSM ApplyMsg 结构

| 类型 | 字段 | 类型      | 说明 |
|------|------|---------|------|
| **日志命令** | CommandValid | bool    | 是否是日志命令 |
| | Command | any     | 命令内容 |
| | CommandIndex | int     | 命令在 Raft 日志中的索引 |
| **快照** | SnapshotValid | bool    | 是否是快照 |
| | Snapshot | [ ]byte | 快照数据 |
| | SnapshotTerm | int     | 快照最后一条日志的任期 |
| | SnapshotIndex | int     | 快照最后一条日志的索引 |
