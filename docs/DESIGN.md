# 详细设计

## 项目定位与设计目标

本系统是一个**高可用、强一致、可水平扩展的分布式分片键值存储**，核心由自实现的 Raft 共识算法驱动。设计目标：

- **强一致性**：对外表现线性一致，所有操作如同在单机按顺序执行。
- **高可用**：每组多副本，容忍少数节点宕机，多数派在线即可继续服务。
- **水平扩展**：数据按哈希分片，多 Raft 组间解耦，扩容通过分片迁移重新分布。
- **可观测与可操控**：快照压缩日志、在线分片迁移、故障恢复，并辅以 Web 控制台实时观测与故障注入演示。

> 本文档聚焦各模块的详细设计；系统架构总览见 [ARCHITECTURE.md](ARCHITECTURE.md)，功能导览见 [WALKTHROUGH.md](WALKTHROUGH.md)。

#todo 加设计文档纵览（说明顺序，与 architecture 的关系）

## Raft 共识算法实现
具体实现：[raft.go](../src/raft/raft.go)

> 📖 **阅读建议：** 推荐按 "先建立直觉、再查细节、自由跳过" 的方式阅读。
> - 先读「Raft 速览」建立整体印象；
> - 「核心数据结构」和各种表格不必详读，作为参考后续查阅即可；
> - 机制小节（选举 / 复制 / 提交 / 快照 / 恢复）等可顺序读，也可跳读；可跳过不关心的细节。

### Raft 速览

> 💡 **提示：** 此小节是对 Raft 的最简介绍，理论可参考 [Raft 论文中文版](RAFT_PAPER_ZH.md)，或访问 [Raft 官网](https://raft.github.io)（包含一个可交互演示）

Raft 解决一个问题：在分布式系统中，如何保证服务的高可用性。做法是设立冗余节点副本，这些节点行为完全一致，对外表现像同一台单机，如此可保证在一些节点宕机时，其他节点仍可提供服务。因此，问题转化为：多个节点如何对命令的操作顺序达成一致，即使面临网络延迟、分区和节点故障。

核心思路：**赋予服务器节点角色和相应的行为模式 —— 在所有节点选出一个领导者，所有客户端请求都经由领导者转送给其他节点 (追随者) 确认；待大多数节点都确认后，领导者就执行请求，并通知其他节点也执行请求；最后领导者将请求结果返回该给客户端。** 

下面用一次 `Get` 请求的共识流程，以帮助建立直观印象（图中以 3 节点集群为例，多数派 = 2）：

```mermaid
sequenceDiagram
    participant C as 客户端
    participant L as 领导者
    participant F1 as 追随者 1
    participant F2 as 追随者 2

    C->>L: ① Get 请求
    L->>L: ② 以日志条目形式追加到本地 (本地存储请求)
    par 并发复制日志给追随者
        L->>F1: ③ 日志条目
        F1-->>L: ④ 确认
    and 
        L->>F2: ③ 日志条目
        F2-->>L: ④ 确认
    end

    Note over L: 收到多数派（含自己）确认
    L->>L: ⑤ 标记日志已提交（请求获得了集群共识）
    L->>L: ⑥ 应用到状态机（执行请求）
    L-->>C: ⑦ 返回请求结果

    note over C,F2:提交进度由领导者通过下一次「心跳 / 日志追加 RPC」顺带发送给追随者 —— 以下事件可发生在任一时刻
    par 并发发送
        L->>F1: 携带当前「提交进度」的 日志追加RPC
        F1->>F1: 根据「提交进度」，应用请求操作到状态机 (执行请求)
    and 
        L->>F2: 携带当前「提交进度」的 日志追加RPC
        F2->>F2: 根据「提交进度」，应用请求操作到状态机 (执行请求)
    end
```

围绕核心思路，Raft 工程实现至少需要如下模块：
- **领导者维持**（保证唯一有效领导）：
  - [**心跳**](#心跳维持)：领导者通过周期性广播维持领导权，阻止新选举。
  - [**选举**](#选举触发)：节点察觉领导者失效后，发起选举以选出新领导者。
- **请求共识**（保证全节点操作一致）：
  1. [**日志复制**](#日志复制)：领导者将客户端请求按顺序复制给所有节点。
  2. [**日志提交**](#共识推进)：领导者依据多数派确认，推进提交进度(即共识进度)并同步给跟随者。
  3. [**日志应用**](#状态应用)：所有节点将已提交日志顺序应用到状态机。

此外，日志无限制增长会造成存储压力，因此需要[**日志压缩**](#日志压缩)机制: 记录截止到某条日志时状态机的快照，并删除其此前的日志以释放存储。

后续小节会分别展开这些主题并详细说明。

### 核心数据结构

Raft 节点的核心状态包括角色、任期、日志和投票信息等字段，并通过 `RequestVote`、`AppendEntries` 和 `InstallSnapshot` 三种 RPC 实现节点间通信（详见 [Raft 内部 RPC](API.md#raft-内部-rpc)）。

**持久化状态**（每次修改需持久化，否则崩溃重启后会丢失，产生数据一致性问题或集群脑裂）：

| 字段                  | 类型           | 说明                                                                                                         |
|---------------------|--------------|------------------------------------------------------------------------------------------------------------|
| `CurrentTerm`       | `int`        | 节点当前已知的最新任期，防止同一任期重复投票                                                                                     |
| `VotedFor`          | `int`        | 当前任期已投票的候选者编号，-1 表示未投票                                                                                     |
| `Log[]`             | `[]LogEntry` | 日志数组，每个条目包含 `Term` 和 `Command`                                                                             |
| `LastIncludedIndex` | `int`        | 快照中包含的最大日志索引（日志压缩后日志数组的起始偏移量）                                                                              |
| `LastIncludedTerm`  | `int`        | `LastIncludedIndex`处日志的`Term` (注：本实现中在日志数组中保留一个 dummy entry 作为`LastIncludedIndex`处的日志，所以此信息通过推导获得，不单独存储字段） |
| `SnapshotData`      | `[]byte`     | 快照数据（日志压缩时保存的状态机状态）                                                                                        |

**易失状态**（可恢复，无需持久化）：

| 角色    | 字段             | 类型      | 说明                                   |
|-------|----------------|---------|--------------------------------------|
| 所有节点  | `commitIndex`  | `int`   | 已提交（即已获得共识）的最大日志索引                   |
|       | `lastApplied`  | `int`   | 已应用到状态机的最大日志索引                       |
|       | `state`        | `enum`  | 节点当前角色：Follower / Candidate / Leader |
| 领导者节点 | `nextIndex[]`  | `[]int` | 下一次要发送给各追随者的日志索引                     |
|       | `matchIndex[]` | `[]int` | 已知已复制到各追随者的最高的日志索引                   |

**功能性字段**（无需持久化）：

| 字段                  | 类型                | 说明                                                                                                    |
|---------------------|-------------------|-------------------------------------------------------------------------------------------------------|
| `applyCh`           | `chan ApplyMsg`   | Raft对日志共识后通过此 channel 将命令传递给上层 RSM 以应用在状态机上                                                           |
| `lastHeardTime`     | `time.Time`       | 收到的最近一次有效 RPC 时间，用于选举超时判定                                                                             |
| `logAppendedCh`     | `chan struct{}`   | 容量为 1，用于通知`replicationDispatcher`协程：本地 Raft 有日志追加                                                     |
| `applyReadyCh`      | `chan struct{}`   | 容量为 1，用于通知`applier`协程：有新的日志 / 快照可应用到状态机上                                                              |
| `replicateReadyChs` | `[]chan struct{}` | 各 channel 容量均为 1，用于通知`replicationWorker`协程：需要向 Follower 发送`AppendEntries RPC` 或 `InstallSnapshot RPC` |
| `snapshotPending`   | `bool`            | 标记是否有快照需要应用到状态机上：在同时有快照和日志的情况下，`applier`将优先应用快照                                                       |

### 并发架构

根据 [Raft 速览](#raft-速览) 中所述模块划分，本系统将共识机制拆解为多个常驻协程协同实现。

Raft 节点启动后创建 5 类常驻协程，协程间通过 channel 非阻塞式异步通信。各协程职责如下：

| 所属模块  | 协程                    | 数量      | 职责                                 | 触发方式                     |
|-------|-----------------------|---------|------------------------------------|--------------------------|
| 领导者维持 | ticker                | 1       | 选举超时检测，并发起选举                       | 定时自动触发 (300ms-600ms间随机数) |
| 领导者维持 | sendHeartbeat         | 1       | 定期广播心跳维持领导权                        | 定时自动触发 (每100ms)          |
| 命令共识  | replicationDispatcher | 1       | 在追随者日志落后情况下唤醒对应 replicationWorker  | 收到`logAppendedCh` 信号     |
| 命令共识  | replicationWorker     | 节点数 - 1 | 向对应的追随者发送日志或快照                     | 收到`replicateReadyCh` 信号  |
| 命令共识  | applier               | 1       | 将已提交日志 / 快照发送到`applyCh`, 驱动状态机应用操作 | 收到`applyReadyCh` 信号      |

#### 时间驱动 + 事件驱动混合并发模型

![Raft Goroutines](images/raft_goroutines.svg)

- 选举超时、心跳机制各自独立运行，以实现 **领导者维持**：
    - 领导者节点上，`sendHeartbeat` 每 100ms 广播一次心跳以维持领导权
  - 非领导者节点上，`ticker` 每隔 300-600ms 间随机时长检查是否期间收到领导者心跳，未收到就重新选举
- 其余协程则按照「日志复制→日志提交→日志应用」的流程协同实现 **请求共识**：
  1. 领导者节点上层收到客户端请求后包装为日志，追加到本地，并唤醒`replicateDispatcher` 分发日志给追随者
  2. `replicateDispatcher` 被唤醒后，观察所有追随者节点日志的状况，如果节点日志落后于自身，则并发唤醒对应的 `replicateWorker` 以 RPC 形式发送日志/快照给落后节点
  3. RPC 返回时，观察日志是否获得多数派确认。如果是，则说明日志条目获得了集群共识，通知 `applier` 将日志中的命令投递给状态机

> 🌟 **设计决策**：
> - **协程生命周期管理**：常驻协程生命周期和节点进程绑定，与节点角色无关，降低管理复杂度且不易泄漏。而由于共识相关协程采用事件驱动，`replicationDispatcher`与`replicationWorker`等与「领导者」角色相关的协程会在追随者节点上长期阻塞、无 CPU 占用，常驻并不产生额外性能开销。
> - **集中调度 + 独立 worker 协程**：由 `replicationDispatcher` 统一接收信号、判定哪些追随者落后，再精准唤醒对应 worker，可以集中决策、分散执行；针对每个追随者使用独立的 `replicationWorker`，可按各节点进度并发发送 RPC，落后节点不会阻塞其他节点的复制进度。
> - **单一 applier 协程**：状态应用必须严格按日志顺序串行执行，并发应用会破坏线性一致性，因此使用单一协程执行。
> - **单一粗粒度锁**：所有共享状态共用一把互斥锁，未细分锁，可以降低死锁风险，代价是可能降低并发度。临界区中大部分操作 (状态读写，少量计算，发送信号) 都极快，只有唯一的慢操作「状态持久化」—— 须在锁内持久化 [Raft 状态](#核心数据结构) ，在日志过长的情况下耗时显著；但由于本系统实现了快照机制，可以控制日志长度上限，进而限制持久化耗时。因此相比细粒度锁，使用粗粒度锁性能损失极小，综合来说是更好的选择。

在此模型之上，协程间信号传递采用了一套统一的通信模式，下文展开说明。

#### 协程间异步通信

协程间通过 channel 异步通信，表达 「有任务待完成」 语义：

```go
    // channel 定义：
    ch := make(chan struct{}, 1) // 最多积压一个信号
```

消息生产者：

```go
    // 生产者：非阻塞式发送信号，满则丢弃
    select{
    case ch <- struct{}{}:
    default:
    }
```

消息消费者：

```go
    // 消费者：持续监听信号，并依据当前 Raft 状态执行操作
    for {
        <- ch
        doWork() // 执行具体(耗时)操作，不同消费者执行内容各异
    }
```

以下以时序图来解释通信机制和实现的效果：

```mermaid
sequenceDiagram
participant a as 生产者
participant b as Channel
participant c as 消费者


a->>b:发送信号 1
b->>c:唤醒 (消费信号 1)

activate c
c->>c:执行操作A ...

a->>b:发送信号 2
b->>b:缓存信号 2


a->>b:发送信号 3
b->>b:channel 已满，丢弃信号 3

a->>b:发送信号 4
b->>b:channel 已满，丢弃信号 4

deactivate c
b->>c:唤醒 (消费信号 2)
activate c
c->>c:执行操作B ...
deactivate c
```

图中信号2、3、4 被压缩为一个信号，因为这些信号传递的语义是相同的 —— 「有任务待完成」；消费者完成当前任务后被唤醒，自行决定行为。

> 🌟 **设计决策**：
> 
> 此设计未使用经典「生产者-消费者」模型中以 channel 来传递具体任务，主要考虑有两点：
> 1. **任务有强实效性**。设想一种场景：上游以领导者角色在本地追加了日志并通知下游；一段时间后，下游协程被唤醒，此时角色已不是领导者。若按照任务内容执行，给其他节点发送日志，将会被拒绝，产生不必要的网络通信。
> 2. **具体任务可以通过 Raft 状态机推导获得，使用 channel 来传递是冗余，且可能产生不一致**。以日志 RPC 为例，`nextIndex[]` 字段记录了下一次应该发送的日志的索引，发送日志的 worker 访问该字段即可推导出接下来应该发送的日志块。
> 
> 因此，本实现的并发哲学仍是 **共享内存为主、channel 为辅**：协程之间通过 channel 只传递「有任务待完成」的触发信号；任务内容则由协程被唤醒时 Raft 的状态来决定。

以上搭建了共识机制的并发骨架：5 类常驻协程通过 channel 信号协同推进「日志复制 → 日志提交 → 日志应用」的共识流程。下文将按此流程顺序逐一展开各机制的实现细节，每一节对应上述某类协程的具体行为：

#todo: 后文完成后添加 link
- [**领导选举与维持**](#领导选举与维持)：`ticker` 与 `sendHeartbeat` 如何维持领导权、超时触发选举；
- [**日志复制**](#日志复制)：`replicationDispatcher` / `replicationWorker` 如何向追随者同步日志；
- [**共识推进与状态应用**](#共识推进与状态应用)：如何判定一条命令获得共识(committed)，并交给 `applier` 应用；
- **快照安装** / **故障恢复**：日志压缩与节点重启后的状态重建。

### 领导选举与维持

此模块负责在集群启动或者领导者失效时，选举出一个新的领导者；领导者上台后维持自身领导权。

#### 选举触发

常驻的 `ticker` 协程，会定时检查是否需要触发选举：

```pseudocode
// 触发选举 (ticker)

ticker(){
    循环执行 {
        electionTimeout = 300-600 ms // 超时时间
        if 当前为follower/candidate 且 
        lastHeardTime-当前时间 > electionTimeout { 
            startElection()
        }
        休眠 electionTimeout
    }
}
```

`ticker` 醒来发现没有收到领导者 RPC 就会触发选举。

- **随机超时范围 (300-600ms)**：避免多个节点同时检测到超时，发起选举，并平票导致无法迅速选出领导者。
- **超时判定依据 `lastHeardTime`**：节点收到**有效**的 RPC 时或发起选举时更新此时间戳，因此只有长时间未与有效领导者通信的节点才会触发选举。

> 🌟 **设计决策**：
>
> 超时上下限的设置与心跳间隔、网络状况密切相关。这里设为3-6倍心跳间隔，最低只可容忍连续2-3次心跳丢失。此设置的前提假设是**网络状况较好，故障多由机器宕机产生**——使用低超时以追求更快的故障恢复。
> 
> 反之，如果**网络状况较差，机器宕机不是故障关键**，则应该设置较长的超时时间以避免网络震荡产生的领导权频繁变更，进而导致无法提供服务——代价是故障恢复更慢。

#### 发起选举

当 `ticker` 检测到超时后，调用 `startElection()` 发起选举。首先增加任期，转为候选人，为自己投票，并更新 `lastHeardTime` 以防止选举触发器立即再次触发选举。接下来给其他节点发送 [RequestVote RPC](API.md#requestvote) 获取选票；获得超过半数节点的投票则当选为领导者，并初始化每个跟随者的 `nextIndex` 和 `matchIndex`.

```pseudocode
// 发起选举 (startElection)

startElection() {  
    CurrentTerm++, state = candidate, VotedFor = me, lastHeardTime = 当前时间  
    persist()  
    构造 args = RequestVoteArgs{  
        Term: CurrentTerm,  
        CandidateId: me,  
        LastLogIndex: lastLogIndex(),  
        LastLogTerm: lastLogTerm()  
    }  
    voteCount = 1  
    对除自己外所有节点，并发执行 {  
        reply = sendRequestVote(args)  
        if 发送失败 { return }  
        if reply.Term > CurrentTerm { 转为follower, 更新lastHeardTime, persist(); return }  
        if reply.VoteGranted && state == candidate && 任期未变 {  
            voteCount++  
            if voteCount > N/2 { state = leader; nextIndex[] 都重置为 lastLogIndex() + 1; matchIndex[] 都重置为 0 }  
        }  
    }  
}
```

#### 应对投票请求

节点收到 `RequestVote RPC` 后，则比较候选人的日志长度和任期来决定是否要投票：
只有候选人的日志至少与接收方一样新（即候选人的最后一条日志任期更大，或者任期相同但索引更大或相等）时才会同意投票。这一机制确保当选者的日志包含所有已提交的日志条目。

```pseudocode
// 投票请求处理 (RequestVote)

RequestVote(args, reply) {
    // 1. 任期判定：收到更高任期则立即降级
    if args.Term > CurrentTerm { 转为follower }

    // 2. 投票三条件（任期足够 && 此任期内未投票 && 候选者日志不旧于自己）
    if args.Term >= CurrentTerm
       && (VotedFor == -1 || VotedFor == args.CandidateId)
       && (args.LastLogTerm > lastLogTerm()
           || (args.LastLogTerm == lastLogTerm() && args.LastLogIndex >= lastLogIndex())) {
        reply.VoteGranted = true
        lastHeardTime = 当前时间
        VotedFor = args.CandidateId
        state = follower
    } else {
        reply.Term = CurrentTerm
        reply.VoteGranted = false
    }
    persist()
}
```

投票三条件含义：

| 条件                                              | 说明                     |
|-------------------------------------------------|------------------------|
| `args.Term >= CurrentTerm`                      | 不能给任期更低的候选者投票          |
| `VotedFor == -1 或 VotedFor == args.CandidateId` | 同一任期内只投一票，防止脑裂         |
| 候选者日志不旧于自己                                      | 日志最新的节点才能当选，保证已提交日志不丢失 |

其中，日志新旧通过比较两个节点的**最后一条日志**来判定：
1. 先比较 `Term`：任期更大的日志为新
2. `Term` 相同时比较 `Index`：`Index` 更大的日志为新

#### 心跳维持

领导者当选后需定期发送心跳以维持领导权，防止追随者超时触发新一轮选举：

```pseudocode
// 心跳 (sendHeartbeat)

sendHeartbeat() {
    循环执行 {
        if 当前是leader {
            对除自己外所有节点并发执行 replicateToFollower() 
        }
        休眠 100ms
    }
}
```

其中，`replicateToFollower()` 内部会根据追随者节点日志的情况发送不同的 RPC，但都可起到维持节点领导权的作用。方法内部见下方 [日志复制](#日志复制) 小节。

### 日志复制

领导者将日志分发给所有追随者，作为共识的基础。

#### 整体流程

以三节点 Raft 为例 (此刻节点 1、2 是追随者)，以下是日志复制的流程:

```mermaid
sequenceDiagram
    participant RSM as Raft 上层
    participant D as 协程 replicationDispatcher
    participant W1 as 协程 replicationWorker 1
    participant W2 as 协程 replicationWorker 2
    RSM->>D: Start(command) 追加日志
    D-->>RSM: 立即返回（异步，不阻塞）
    par 并发同步日志
	    D->>W1: 通知 follower 1 日志待同步
        W1->>W1: synchroniseFollower(1)
    and 
	    D->>W2: 通知 follower 2 日志待同步
        W2->>W2: synchroniseFollower(2)
    end
```

**第一阶段**：上层调用 Raft 的 [Start (command)](API.md#raft-共识模块接口) 在「领导者」节点本地追加日志

```pseudocode
// 第一阶段：Start() 本地日志追加 

Start(command) {  
    if 节点是leader { 
        Log.append({Term: CurrentTerm, Command: command})
        persist()
        notify(logAppendedCh) // 通知 replicationDispatcher 分发日志
        return lastLogIndex, CurrentTerm, true  
    }else { // 非领导者节点不会接受日志
	    return -1, -1, false
    }
}
```

`Start(command)` 是上层向 Raft 发送共识请求的入口。领导者先把命令追加到自己的日志并持久化，再通知分发协程，然后**立即返回**——复制与提交在后台异步完成。返回三个结果，分别是「此命令如果成功共识对应的索引， 任期」和「是否接受共识请求」。对非领导者的请求会拒绝，因为只有领导者可发起共识。

**第二阶段**：将「追加日志」的消息传递到链路末端，触发节点日志同步

```pseudocode
// 第二阶段：replicationDispatcher 分发信号；replicationWorker 接受信号，调用 synchroniseFollower 方法

replicationDispatcher() {
循环等待 logAppendedCh 信号 {
        if 不再是leader { continue }
        遍历所有节点 {
            if nextIndex[i] <= lastLogIndex() {  // 若该节点日志落后
                notify(replicateReadyChs[i]) // 通知对应 replicationWorker 复制
            }
        }
    }
}

replicationWorker(i) {  
    循环等待 replicateReadyCh[i] 信号{
	    收到信号后执行 synchroniseFollower(i) 
    }  
}
```

这一阶段把「有日志追加」这一信号，异步地投递到每个追随者的专属工作协程。首先交给分发协程 `replicationDispatcher` ，依次判断各个追随者日志是否落后；如果日志落后，则通知负责该追随者的工作协程 `replicationWorker`.

**第三阶段**：通过 RPC 同步「追随者」日志到与「领导者」一致

```pseudocode
// 第三阶段：synchroniseFollower 循环，同步节点日志

synchroniseFollower(i) {
    needReplicate = nextIndex[i] <= lastLogIndex() && 此节点当前是 Leader // 检查节点是否需要同步
    while needReplicate {
        rpcOK = replicateToFollower(i) 
        if !rpcOK { // 如果 RPC 失败
            backoff  // 退避 (指数级) 
        }
        检查并更新 needReplicate // 完成后继续检查是否还需推进
    }
}

// 发送日志或快照 
replicateToFollower(i){
	if nextIndex[i] > LastIncludedIndex {
		发送 AppendEntries RPC
	}else {
		发送 InstallSnapshot RPC
	}
}
```

这是`replicationWorker` 针对单个追随者的日志同步，通过 `synchroniseFollower()` 方法循环调用 `replicateToFollower()` 实现。在每轮循环的开始，先检查自己是否有领导权、目标节点是否日志落后。如果是，则循环发送 RPC 直到目标节点日志与自己完全同步或自身失去领导权。

`replicateToFollower(i)` 根据情况在 `AppendEntries` 与 `InstallSnapshot` 两种 RPC 间择一发送，二者细节分别见 [批量复制](#批量复制) 与 [快照安装](#快照安装)。

> 🌟 **设计决策**：`synchroniseFollower` 在 `replicateToFollower(i)` 的 RPC 调用失败后不立即重试，而采用指数退避（以 100ms 为基准、2s 为上限，RPC 成功则回正退避时间），可以防止因目标节点宕机而大量重试，造成网络拥塞。

#### 批量复制

领导者通过 [AppendEntries RPC](API.md#appendentries) 来进行批量日志复制。

领导者为每个追随者记录两个字段：`nextIndex`（下一次要发送的日志起点）与 `matchIndex`（已确认匹配的日志终点）。每次发送 `AppendEntries` 都以 `nextIndex` 为起始位置；当某次复制成功后，`matchIndex` **单调递增地**推进到本次发送日志的终点。 始终满足 `nextIndex > matchIndex`. 

`AppendEntries RPC` 中附带字段 `PrevLogIndex`, `PrevLogTerm`, 记录了待发送日志前一条的信息。追随者收到 RPC 后利用此信息检查，只有本地日志中 `PrevLogIndex` 处的 `Term` 与`PrevLogTerm`一致，才接受这条 RPC 中的全部日志，如下图所示：

![接受日志复制](images/appendEntries_accept.svg)

Raft 的 [**日志匹配性质**](RAFT_PAPER_ZH.md#5-raft-共识算法) 保证这一点：如果跟随者与领导者在同一位置日志条目的 `Term` 匹配，则此日志及此前的所有日志都匹配。

#### 冲突与修复

但日志复制并非都能成功。领导者可能会因崩溃而更替，新领导者当选时，并不保证所有追随者的日志与它完全一致。因此，某次 `AppendEntries RPC` 到达时，追随者在 `PrevLogIndex` 处可能**没有日志**，或**这条日志的 Term 与领导者不同**，即「日志冲突」。

修复冲突的方式是：找到追随者与领导者**最后一条匹配日志**的位置，把追随者该位置之后的日志整体覆盖为领导者的。关键是「如何找到**最后一条匹配日志**的位置」。最朴素的做法是，每次 RPC 回退一条日志，直到发现匹配点。本实现则使用优化：通过让追随者在 RPC 回复中携带额外信息(`ConflictTerm` / `ConflictIndex`), 使领导者能一次最多跳过一个任期内所有日志，以加快消除冲突。

首先来看冲突类型。在收到一条 `AppendEntries RPC`时，追随者在 `PrevLogIndex` 处的日志有三种典型状态，对应**三类冲突**，如下图所示：

![日志复制冲突](images/appendEntries_conflicts.svg)

- **冲突 1 · 有日志但任期不匹配**：追随者有 12 号日志，但 `Term` 与领导者不同。典型场景——旧领导者（任期 5）曾向它复制大量日志，其中仅 9、10 号最终获得集群共识；新领导者上任后，该节点 12 号日志遂与领导者冲突。
- **冲突 2 · 无日志（日志过短）**：追随者最高只有 9 号日志，连 `PrevLogIndex` 都未达到。典型场景——该节点曾被网络分区，直到此刻才恢复，因而整体落后。
- **冲突 3 · 无日志（已被压缩）**：`PrevLogIndex` 处日志已被快照截断，无法追溯。典型场景——这条 `AppendEntries` 因网络延迟未及时送达，领导者超时重发并成功推进共识，随后追随者拍摄了含 1–14 号日志的快照；当延迟的那条 RPC 终于到达时，12 号日志已不存在。

针对三类冲突，追随者有不同回复；领导者则根据回复修改 `nextIndex`, 以调整下一次 `AppendEntries RPC` 中发送日志的起始位置，如下表所示：

| 冲突类型    | 追随者回复 `ConflictTerm` | 追随者回复 `ConflictIndex`    | 领导者设置 `nextIndex`                                                |
|---------|----------------------|--------------------------|------------------------------------------------------------------|
| 日志任期不匹配 | `PrevLogIndex` 处日志任期 | `ConflictTerm` 内第一条日志的索引 | 如果本地有`ConflictTerm` 任期内日志，设为该任期最后一条日志的后一个位置；否则设为 `ConflictIndex` |
| 日志过短    | -1                   | `LastLogIndex` + 1       | `ConflictIndex`                                                  |
| 日志已被压缩  | -1                   | `LastIncludedIndex` + 1  | `ConflictIndex`                                                  |

接下来以例说明。

**冲突 1：**

![日志复制冲突1](images/appendEntries_conflict_1.svg)

上图按时间顺序展示了情况 1 的处理方法，可与上表 **「日志任期不匹配」** 一行对照阅读：
1. **发送**：领导者发出 `AppendEntries`（`PrevLogIndex=12, PrevLogTerm=6`）。追随者在 12 号处的日志任期为 5，与声明不符，拒绝。
2. **回复**：追随者找到 12 号日志所属任期（Term=5）的**第一条**索引，将其信息 `ConflictTerm=5, ConflictIndex=9` 连同 `Success=false` 返回——语义是 "冲突日志属于任期 5，请在该任期内找到匹配日志"。
3. **回退**：领导者据此回复将 `nextIndex` 回退到 11，即日志分歧起点。

**冲突 2：**

![日志复制冲突2](images/appendEntries_conflict_2.svg)

上图按时间顺序展示了情况 2 的处理方法，可与上表 **「日志过短」** 一行对照阅读：
1. **发送**：领导者发出 `AppendEntries`（`PrevLogIndex=12, PrevLogTerm=6`）。追随者最高只有 9 号日志，未达到 `PrevLogIndex=12` ，拒绝。
2. **回复**：追随者发现自己日志过短，于是将 `ConflictTerm=-1, ConflictIndex=10`（`10` 即 `LastLogIndex+1`）连同 `Success=false` 返回——语义是 "该位置无日志，请从我最后一条日志（9 号）之后开始发送"。
3. **回退**：领导者据此回复将 `nextIndex` 回退到 10。

**冲突 3：**

![日志复制冲突3](images/appendEntries_conflict_3.svg)

上图按时间顺序展示了情况 3 的处理方法，可与上表 **「日志已被压缩」** 一行对照阅读：
1. **发送**：领导者发出 `AppendEntries`（`PrevLogIndex=12, PrevLogTerm=6`）。追随者中 `PrevLogIndex=12` 已被压缩，无法判断此处 `Term` 是否匹配，拒绝。
2. **回复**：追随者发现日志已被压缩，于是将 `ConflictTerm=-1, ConflictIndex=15`（`15` 即 `LastIncludedIndex+1`）连同 `Success=false` 返回——语义是 "该位置日志被压缩无法判断，请从我快照之后开始发送"。
3. **回退**：领导者据此回复将 `nextIndex` 推后到 15。

冲突 2、3 实质都是领导者声明的 `PrevLogIndex` 处日志在追随者中不存在，无法确定 `Term` 是否匹配。因此跟随者需要通过合理的回复引导领导者将不确定型冲突转化为确定情况。

同时，日志冲突可能需要多轮 RPC 才能解决，冲突类型也可能发生变化，如下图所示：

![日志复制冲突转化](images/appendEntries_conflict_transition.svg)


最后，由于日志压缩，冲突可能无法使用 `AppendEntries` 解决，如下图所示:

![日志复制日志缺失](images/appendEntries_entries_short.svg)

此时转为发送 `InstallSnapshot`，详见下一节中 [快照安装](#快照安装) 。

### 日志压缩与快照安装

#### 日志压缩
随着请求不断增加，Raft 日志会持续增长，产生两个问题：
1. 日志占用大量存储空间
2. 节点故障重启后，要从第一条日志开始重放，恢复状态机的时间过长

因此，系统使用快照机制，记录并存储某时刻状态机的状态，然后删除此状态机逻辑上已包含的操作日志，即「日志压缩」。

日志压缩的时机由 Raft 上层的 RSM 决定，通过调用 `Snapshot(index, snapshot)`方法实现，如下图所示：

![拍摄快照](images/snapshotting.svg)

首先，RSM 将应用层的状态机记录为快照，再把快照内容 `snapshot` 和此快照中包含的最大日志索引`index` 作为结果返回给 Raft. Raft 则持久化快照数据，将日志数组截断至 `index` 处，再更新 `index` 作为新的 `LastIncludedIndex`.

具体触发时机和条件见 [RSM](#rsm) 小节
#### 快照安装

领导者在需要给追随者发送日志时可能由于日志压缩，失去了这些日志，因此转为以 [`InstallSnapshot RPC`](API.md#installsnapshot)发送快照（注：本实现不使用论文中快照分块的方法，而是一次发送全量快照，流程有所简化），如下图所示：

![安装快照](images/installSnapshot.svg)









### 共识推进与状态应用

#### 共识推进

当一条日志被复制到半数以上的跟随者上后，即获得了集群共识，各节点以 `commitIndex` 来标记已知最大共识进度。

不同角色推进 `commitIndex` 的方式不同：

**领导者**：为每个追随者维护 `matchIndex`，追踪各节点的日志复制进度。每次 `AppendEntries RPC` 成功后，检查 `commitIndex`是否有推进的机会。

从后向前扫描日志，找到满足以下三个条件的最大索引 `N`：
  1. `N` > 当前 `commitIndex`
  2. `Log[N].Term == CurrentTerm`（仅提交当前任期的日志，防止提交覆盖已获得共识的旧任期条目）
  3. 超过半数节点上 `matchIndex >= N`（多数派已复制）

若找到，则更新 `commitIndex = N`. 

**追随者**：每次收到 `AppendEntries RPC` 时读取字段 `LeaderCommit`，若大于自身 `commitIndex`，则尽最大可能推进。收到 InstallSnapshot 时同样检查快照的 LastIncludedIndex。

#### 状态应用

由独立的 applier 协程串行完成。该协程收到 commitIndex 更新的通知后，从 lastApplied + 1 开始，顺序将日志中的命令通过 applyCh 通道发送给状态机层执行，直到处理完所有已提交命令。

**提交与应用分离的好处**：两阶段异步运行，若状态机层因卡顿无法及时应用请求，下层的 Raft 共识层仍能继续推进日志共识，互不影响。

### 故障恢复

需要持久化的内容分为两类：一是保证系统正确性的数据（CurrentTerm、VotedFor、Log），二是保证节点崩溃后快速恢复的数据（LastIncludedIndex、SnapshotData）。

节点启动时调用 readPersist() 从持久器中读取这些状态并恢复，使节点能够继续参与集群，无需从头开始同步。

### 延伸阅读

- **Raft 论文中文版**（[RAFT_PAPER_ZH.md](RAFT_PAPER_ZH.md)）：想看形式化定义、安全性证明与完整机制再读，尤其是 [第五章](RAFT_PAPER_ZH.md#5-raft-共识算法)。本文档的速览已覆盖阅读本设计所需的最小知识。
- **可视化 Raft**（https://raft.github.io）：交互式动画，直观感受选举与日志复制过程。

---

## 复制状态机中间层（RSM）

参考实现：[rsm.go](../src/kvraft/rsm/rsm.go)

### 职责与位置

RSM 位于 [Raft 共识层](API.md#raft-共识模块接口)与[服务层](API.md#实现-statemachine-接口-rsm-使用)之间，承担关键的中介职责：

1. 将服务层的业务请求封装为 `Op{Me, Id, Req}` 结构，提交到 Raft 共识层
2. 等待共识结果，将已共识的命令交付服务层执行，结果返回给调用方
3. 检测 Raft 日志空间占用，超阈值时触发日志压缩
4. 在领导权变化时向调用方返回正确的错误码

RSM 将服务层抽象为 StateMachine 接口（DoOp / Snapshot / Restore），使 RSM 无需关心具体的业务逻辑。

### 请求提交

所有业务请求都通过 Submit(req) 进入，流程如下：

1. 为请求生成单调递增的 ID，封装为 `Op{Me, Id, Req}`
2. 调用 `Raft.GetState()` 检查当前节点是否仍是领导者，若否则直接返回 `ErrWrongLeader`
3. 调用 `Raft.Start(op)` 将 Op 提交到共识层，获得 commandId（Op 在日志中的索引）
4. 以 commandId 为键创建容量为 1 的结果通道，注册到 `resultByCommandId` 映射表中
5. 进入等待阶段

**临界区问题**：结果通道的注册必须在持有锁的情况下完成，否则可能出现 readApply 协程已执行完该日志条目、试图通知等待者时，通道尚未创建的竞态条件。

### 等待结果与特殊情况处理

Submit() 进入等待后，采用三路等待循环覆盖可能发生的不同情况：

| 等待条件    | 触发方式                  | 处理                                        |
| ------- | --------------------- | ----------------------------------------- |
| 领导权变化   | 每 100ms 检查 GetState() | 不再是领导者 → 返回 ErrWrongLeader                |
| 共识结果返回  | 结果通道收到 applyResult    | 检查 opId 匹配 → 返回结果；不匹配 → 返回 ErrWrongLeader |
| Raft 终止 | Raft 节点关闭信号           | 立即退出，返回 ErrWrongLeader                    |

**保守返回 ErrWrongLeader 的合理性**：当旧领导者发现领导权丢失时，它无法区分两种情况：①日志尚未复制到多数节点，必然无法提交；②日志已复制到多数节点，但旧领导者尚未观察到共识。旧领导者只能保守地返回 ErrWrongLeader，由客户端重试和新领导者的去重机制修复。

### 快照触发与状态恢复

- **快照触发**：每次 RSM 从 applyCh 读取到已共识的命令时，检查 Raft 当前日志大小，超过 `maxraftstate` 阈值后调用 `Snapshot(commandId, snapshot)` 触发日志压缩
- **状态恢复**：RSM 启动后调用 StateMachine.Restore() 从快照中恢复故障前的状态

---

## 服务层

参考实现：[server.go](../src/shardkv/shardgrp/server.go)（含键值服务与分片迁移）

服务层实现 [StateMachine 接口](API.md#实现-statemachine-接口-rsm-使用)（DoOp / Snapshot / Restore），作为 RSM 的底层存储引擎。

### 版本化键值读写

详见 [键值读写 RPC](API.md#键值读写-rpc-接口) 中的请求与回复字段定义。核心设计决策：

- **Get 语义**：直接查询键值映射，键存在返回值与版本号，不存在返回 ErrNoKey
- **Put 语义**（CAS 条件更新）：
  - 键已存在 + 版本匹配 → 更新值，自增版本号，返回 OK
  - 键已存在 + 版本不匹配 → 返回 ErrVersion
  - 键不存在 + version=0 → 创建条目，版本=1，返回 OK
  - 键不存在 + version≠0 → 返回 ErrNoKey
- 客户端先通过 Get 获取当前版本号，再携带该版本号执行 Put，保证"写入基于最新的已观测状态"

### 客户端写操作去重机制

在多副本集群中，客户端超时重试可能导致重复执行。服务端维护 `lastPutByClientId` 映射表，记录每个客户端最近一次已执行的写请求及回复。

**去重流程**：处理 Put 时，若请求的 RequestId 不大于已记录的值，直接返回缓存的回复；否则正常执行并更新记录。

**设计约束与权衡**：

- 只保存"最近一次"写请求，因此要求**同一客户端的写请求必须串行**：客户端必须在收到前一个写请求的回复后，才能自增 RequestId 并发送下一个
- 若同一客户端并发发送多个写请求，后到达的请求会覆盖先到达的去重信息，导致重复执行。若要支持同一客户端并发写，需记录所有未确认写请求的去重信息，而非仅保留最近一条

### 键值服务快照与恢复

服务层配合 RSM 完成日志压缩与崩溃恢复，实现 Snapshot() 和 Restore() 两个方法。

- **Snapshot 策略**：先加锁对必要数据进行深拷贝，然后释放锁，将拷贝数据编码为字节数组。深拷贝而非直接序列化的原因在于编码过程较耗时，若在锁内进行会显著降低系统吞吐量。快照包含两类数据：键值映射和去重表（去重表必须在快照中，否则节点重启后无法识别客户端重试，可能重复执行旧写操作）。
- **Restore 场景**：①节点启动时从持久器读取快照恢复状态；②追随者日志落后过多，收到 InstallSnapshot RPC 后更新状态机。

### 分片迁移

#### 分片状态机

系统为每个分片维护三种状态，状态转换遵循三阶段迁移协议：

```
Absent ──Install──→ Serving ──Freeze──→ Frozen ──Delete──→ Absent
  ↑                                                        │
  └────────────────────────────────────────────────────────┘
```

| 状态      | 含义      | 可接受操作    |
| ------- | ------- | -------- |
| Absent  | 分片不在此组  | 无        |
| Serving | 正常服务    | Get, Put |
| Frozen  | 已冻结等待迁出 | Get（只读）  |

Frozen 状态下分片不接受任何写操作，但读操作仍可执行（多次读取同一键不会返回不同值）。

#### 三阶段迁移协议

详见 [分片迁移 RPC 接口](API.md#分片迁移-rpc-接口)。迁移顺序保证了旧组先停止接受写入，再由新组接管状态，最后清理旧副本，避免迁移期间出现状态分叉。

#### 分片数据打包与合并

分片从旧组迁移到新组时，需要传输两类信息：

1. **键值数据**：分片的业务数据，直接打包
2. **客户端去重记录**：仅传输与目标分片相关的去重信息。系统通过 `shardClients[shid]` 集合记录曾在指定分片上进行过写操作的客户端编号，迁移时遍历此集合筛选去重记录

**冗余传输权衡**：筛选策略可能携带部分与当前分片无关的去重记录（因客户端串行写入，只有最后一次写操作的记录有去重价值，其余是陈旧的），但选择此冗余策略避免了维护"客户端-分片精确定位"的复杂索引结构。在分片数较多时，冗余数据会累积，但不会影响系统正确性。

新组收到数据后的合并策略：

- 键值数据：直接覆盖本地对应分片（前提是新组本地无该分片数据）
- 去重记录：以"保留更大 ReqId"的原则合并至本地，防止旧记录覆盖新记录；同时将客户端编号加入本地 `shardClients[shid]`

#### 配置号幂等保护

仅依赖状态转换不足以保证迁移正确性。网络乱序或老旧请求重发可能扰乱迁移流程。系统引入配置号（Num）机制：

- 每个分片维护 `cfgNumByShid` 记录已知最高配置号
- 每次配置变更自增配置号，所有迁移请求携带此版本号
- 服务端根据请求配置号与本地配置号的比较结果分类处理

**请求分类哲学**：

| 类别   | 判定                   | 处理                         |
| ---- | -------------------- | -------------------------- |
| 新请求  | 配置号等于当前状态 + 推进值      | 正常执行，返回 OK                 |
| 旧请求  | 配置号小于等于当前状态对应值       | 幂等执行，返回 OK                 |
| 非法请求 | 配置号大于当前状态 + 推进值，或跳状态 | 不执行，返回 ErrIllegalOperation |

整体哲学：唯一能按规则将当前状态推动至下一状态的请求是新请求；所有能将分片带入当前状态或其前驱状态的请求是旧请求；其他均是非法请求。

---

## 分片配置与分片控制器

参考实现：[shardcfg/config.go](../src/shardkv/shardcfg/config.go), [shardctrler/](../src/shardkv/shardctrler/)

### 分片配置结构

系统采用 `ShardConfig` 结构表示分片配置，提供以下核心方法：

- **键到分片映射**：通过 FNV-1a 哈希将键映射到分片（32 位哈希值对分片总数取模），此映射关系在整个系统中保持一致
- **负载均衡**：分组加入或离开后，首先将无归属分片分配给负载最轻的分组，然后持续从负载最重的分组向最轻的分组迁移，直到各组分片数最大差值不超过 1
- **序列化/反序列化**：支持配置信息的网络传输与存储

### 配置仓库与双配置机制

配置仓库是一个单机的键值存储服务器，提供带版本号的 Get/Put 操作（写入语义与[键值读写](API.md#键值读写-rpc-接口)中 CAS 语义一致）。配置仓库作为**全局配置信息的唯一来源**，保证一致性。

双配置机制在配置仓库中以两个独立键存储：

- **currentConfig**：当前集群中已生效的配置
- **nextConfig**：计划迁移的目标配置，作为迁移意图的声明

迁移未完成时 currentConfig 落后于 nextConfig，二者不等即可用于检测未完成迁移。

### 配置变更流程

配置变更由分片控制器执行，分为三步：

1. **查询 currentConfig**：若比当前将要变更的配置新，退出流程
2. **CAS 写入 nextConfig**（多控制器竞争）：读取 nextConfig 当前值（配置号 + 键版本号），比较配置号大小，版本号保证 CAS 写入正确性——多个控制器同时竞争时只有一个能成功，其他因版本号不匹配而失败
3. **执行分片迁移**：按三阶段协议迁移涉及的分片，成功后更新 currentConfig 为与 nextConfig 一致

**故障恢复**：控制器在迁移中可能崩溃，导致系统处于"中间状态"。每个控制器初始化时检测 `currentConfig ≠ nextConfig`，若不等则优先重做未完成的迁移，完成后才发起自己的新配置变更。由于服务层对迁移操作做了幂等处理，中断在任何阶段均可安全恢复。

---

## 两级客户端架构

参考实现：[shardgrp/client.go](../src/shardkv/shardgrp/client.go)（组内调度员）

### 组客户端（ShardGroup Clerk）

与单个 Raft 分组直接通信的底层客户端，负责将请求路由到组内领导者节点。

- 缓存上次成功通信的领导者编号，优先向其发送请求
- 收到 ErrWrongLeader 或 RPC 超时时，按顺序尝试组内其他节点
- 遍历所有节点仍未成功时，退避等待后重试，最多尝试 `N + 1` 次（N 为组内节点数）

**快速失败策略的设计理由**：分片迁移后，已迁出所有分片的组会关闭节点停止服务。底层组内调度员需要快速反馈错误给上层分片调度员进行跨组重路由，而非对已停止服务的组进行无谓的多轮尝试。退避等待时间与 Raft 心跳频率一致（100ms），在组内正在选举的情况下也保留了一定的等待时间。

### 分片客户端（ShardKV Clerk）

用户直接操作的客户端，负责计算分片、缓存配置、跨组路由。

**配置缓存与单次刷新**：

- 本地缓存分片配置，用户请求时通过哈希计算分片，查询缓存确定目标组
- 收到 ErrWrongGroup 或 ErrRetryExhausted 时触发配置刷新（错误驱动刷新）
- 采用 **single-flight 模式**：多个并发请求同时触发刷新时，只有一个发起查询，其余等待并复用结果，避免对配置仓库的并发查询压力

采用"缓存 + 按需刷新"的原因：配置变更频率远低于数据读写请求，若每次请求都查询最新配置会增加不必要的延迟。

**去重的位置设计**：去重机制放在外层的分片调度员而非底层的组内调度员，因为去重是**跨分片语义**。当键因分片迁移移动到新组时，只有外层分片调度员能感知这一变化，使用同一序列号向新组重试请求。底层组内调度员无法跨组行为，无法分辨一个请求是全新还是重试。

---

## 正确性验证

分布式系统的价值取决于其正确性是否可证。本项目通过三层手段保证并验证正确性：

1. **线性一致性检验器（kvtest）**：基于模型检查，将客户端并发操作历史回放，与单副本顺序语义比对，确认系统对外表现线性一致（参考 [Porcupine](https://github.com/anishathalye/porcupine) 思路）。
2. **仿真故障注入（labrpc Network）**：支持丢包、乱序、延迟、网络分区，且故障模型比真实机房网络更恶劣，使共识逻辑在确定性环境中经受超额考验。
3. **73 个测试用例（TESTING.md）**：覆盖选举、日志复制、快照、配置变更、分片迁移、客户端去重等关键路径。
4. **可视化演示（shardkv-demo）**：Web 控制台实时展示节点状态与分片分布，支持 kill 节点、网络分区、混沌猴子，用于交互式观察系统行为。

## 已知限制与边界

为聚焦共识与分片核心逻辑，本项目在以下方面做了有意简化（面试中可坦诚说明边界）：

- **传输层**：使用课程框架 labrpc（进程内 RPC + 故障注入），非生产级 gRPC；生产部署需替换传输层并引入真实磁盘持久化（`FilePersister` 已实现 crash-safe 落盘与崩溃恢复测试，待接入部署路径）。
- **副本配置**：单分片组固定 3 副本，未实现动态扩缩副本数。
- **部署形态**：当前以单机构建 / 容器内多进程方式运行，未做跨机 / 跨数据中心部署。
- **配置仓库**：为单 Raft 组，存在理论单点；因配置变更频率极低，实践中可接受。
