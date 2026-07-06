# 测试覆盖说明

本系统共 **73 个自动化测试用例**，覆盖 Raft 共识算法、KVRaft 线性一致 KV 服务、ShardKV 分片键值存储三个模块。

**测试框架：** MIT 6.5840 自动化测试框架，可系统性模拟领导者切换、节点崩溃、网络分区、快照恢复和分片迁移等典型分布式故障场景。

**线性一致性验证：** 集成 [Porcupine](https://github.com/anishathalye/porcupine) 线性一致性检查器，自动校验并发操作历史。

## 运行测试

```bash
cd src

# 运行 Raft 测试
make raft

# 运行 KVRaft 测试
make kvraft

# 运行ShardKV测试
make shardkv

# 运行单个或多个(部分)名称匹配的测试用例，如
make RUN="-run 3A" raft  # 运行 raft 测试套组中所有带有“3A”的测试
```
**注意**： 当测试异常中断时，测试框架启动的 daemon 子进程和 UNIX socket 文件可能不会被正常清理。运行以下命令清理：
```bash
# 一键清理所有残留 daemon 进程和 socket 文件
cd ../src
make clean
```

## Raft 共识算法（28 个测试）

### Part 3A — 领导者选举（3 个）

| 编号 | 测试名称                  | 测试功能                   | 验证要点                                                          |
|----|-----------------------|------------------------|---------------------------------------------------------------|
| 1  | TestInitialElection3A | 初始选举：3 节点能否正常选出 Leader | Leader 选举、Term 单调递增                                           |
| 2  | TestReElection3A      | 网络分区后的重新选举             | Leader 故障感知、新 Leader 选举、旧 Leader 回归后降为 Follower、无多数派时无 Leader |
| 3  | TestManyElections3A   | 7 节点下的多次选举             | 大规模集群中随机断连 3 节点后剩余 4 节点仍能选出 Leader                            |

### Part 3B — 日志共识（10 个）

| 编号 | 测试名称                   | 测试功能                         | 验证要点                                    |
|----|------------------------|------------------------------|-----------------------------------------|
| 4  | TestBasicAgree3B       | 基本日志共识                       | Leader 提交流程、日志复制、多数派提交                  |
| 5  | TestRPCBytes3B         | RPC 字节数检查                    | 每条日志条目仅发送到每个节点一次，无冗余传输                  |
| 6  | TestFollowerFailure3B  | Follower 逐级故障                | 断连一个 Follower 后仍可提交、断连两个 Follower 后无法提交 |
| 7  | TestLeaderFailure3B    | Leader 故障                    | Leader 断连后新 Leader 产生、日志在无 Leader 时无法提交 |
| 8  | TestFailAgree3B        | Follower 断连后重连               | Follower 重连后日志同步、历史共识不丢失                |
| 9  | TestFailNoAgree3B      | 多数派断连时无法达成共识                 | 5 节点中断连 3 个 Follower 后无法提交、恢复后重新达成共识    |
| 10 | TestConcurrentStarts3B | 并发提交                         | 多个客户端同时调用 Start() 时所有命令均能被提交            |
| 11 | TestRejoin3B           | 分区 Leader 重连                 | 旧 Leader 离线期间生成的未提交日志，重连后需回滚            |
| 12 | TestBackup3B           | Leader 快速回退覆盖不一致 Follower 日志 | 日志冲突时 Leader 通过 PrevLogIndex 快速回退       |
| 13 | TestCount3B            | 自定义测试计数                      | 验证测试框架的计数准确性                            |

### Part 3C — 持久化与节点动态变化（8 个）

| 编号 | 测试名称                    | 测试功能                       | 验证要点                                     |
|----|-------------------------|----------------------------|------------------------------------------|
| 14 | TestPersist13C          | 基本持久化                      | Kill 全部节点后重启，日志不丢失，连续提交                  |
| 15 | TestPersist23C          | 5 节点多轮 Kill/Restart 持久化    | 多轮交替 kill/restart 不同节点组合后日志持续提交          |
| 16 | TestPersist33C          | 分区 Leader + Follower 崩溃后重启 | 分区后 Leader 和一个 Follower 同时崩溃，剩余节点重选后日志正确 |
| 17 | TestFigure83C           | Figure 8 场景（可靠网络）          | Leader 频繁崩溃时，旧 Leader 未提交日志被覆盖，已提交日志不丢失  |
| 18 | TestUnreliableAgree3C   | 不可靠网络下的共识                  | 丢包环境中并发提交，所有已提交命令值一致                     |
| 19 | TestFigure8Unreliable3C | Figure 8（不可靠网络 + 乱序）       | 丢包 + 乱序环境下旧 Leader 日志被正确覆盖               |
| 20 | TestReliableChurn3C     | 随机 Kill/Restart + 断连（可靠）   | 节点动态变化中并发客户端提交的值最终保留在日志中                 |
| 21 | TestUnreliableChurn3C   | 随机 Kill/Restart + 断连（不可靠）  | 丢包 + 节点变动中并发客户端提交的值最终保留在日志中              |

### Part 3D — 快照与日志压缩（7 个）

| 编号 | 测试名称                            | 测试功能          | 验证要点                                     |
|----|---------------------------------|---------------|------------------------------------------|
| 22 | TestSnapshotBasic3D             | 基本快照          | Raft 日志压缩后状态机快照正确                        |
| 23 | TestSnapshotInstall3D           | 断连场景下安装快照     | 落后 Follower 重连后通过 InstallSnapshot RPC 追赶 |
| 24 | TestSnapshotInstallUnreliable3D | 不可靠网络下安装快照    | 丢包场景下快照安装仍能正确完成                          |
| 25 | TestSnapshotInstallCrash3D      | 崩溃场景下安装快照     | Follower 崩溃重启后通过 InstallSnapshot 追赶      |
| 26 | TestSnapshotInstallUnCrash3D    | 不可靠 + 崩溃下安装快照 | 丢包 + 崩溃双重压力下快照安装成功                       |
| 27 | TestSnapshotAllCrash3D          | 全集群崩溃重启       | 所有节点同时崩溃后重启，日志索引不丢失不倒退                   |
| 28 | TestSnapshotInit3D              | 崩溃后快照初始化      | 快照持久化后重启，状态机正确恢复，后续写入不丢失                 |

## KVRaft 线性一致 KV 服务（22 个测试）

### Part 4B — 基础 KV 服务（13 个）

| 编号 | 测试名称                                         | 测试功能               | 验证要点                  |
|----|----------------------------------------------|--------------------|-----------------------|
| 1  | TestBasic4B                                  | 基本操作测试             | 单客户端 Put/Get 正确性      |
| 2  | TestSpeed4B                                  | 操作速度测试             | Put 操作速度 > 3 ops/心跳间隔 |
| 3  | TestConcurrent4B                             | 多并发客户端             | 5 个客户端并发操作满足线性一致性     |
| 4  | TestUnreliable4B                             | 不可靠网络              | 丢包环境下 5 个客户端满足线性一致性   |
| 5  | TestOnePartition4B                           | 少数派分区测试            | 少数派中的请求不应完成，分区恢复后继续   |
| 6  | TestManyPartitionsOneClient4B                | 多分区单客户端            | 单客户端在网络分区下满足线性一致性     |
| 7  | TestManyPartitionsManyClients4B              | 多分区多客户端            | 多客户端在网络分区下满足线性一致性     |
| 8  | TestPersistOneClient4B                       | 持久化单客户端            | 节点崩溃重启后单客户端操作正确       |
| 9  | TestPersistConcurrent4B                      | 持久化多客户端            | 节点崩溃重启后多客户端操作正确       |
| 10 | TestPersistConcurrentUnreliable4B            | 持久化+不可靠网络          | 崩溃+丢包双重压力下操作正确        |
| 11 | TestPersistPartition4B                       | 持久化+分区             | 崩溃+分区场景下操作正确          |
| 12 | TestPersistPartitionUnreliable4B             | 持久化+分区+不可靠         | 三重压力下操作正确             |
| 13 | TestPersistPartitionUnreliableLinearizable4B | 持久化+分区+不可靠+线性一致性检查 | 最复杂场景下仍满足线性一致性        |

### Part 4C — 快照与恢复（9 个）

| 编号 | 测试名称                                                           | 测试功能         | 验证要点                            |
|----|----------------------------------------------------------------|--------------|---------------------------------|
| 14 | TestSnapshotRPC4C                                              | 快照 RPC 基础    | Raft 日志压缩后快照可用                  |
| 15 | TestSnapshotSize4C                                             | 快照大小检查       | 日志压缩后日志大小不超过阈值 (8×maxraftstate) |
| 16 | TestSpeed4C                                                    | 操作速度测试（快照场景） | 启用快照时操作速度达标                     |
| 17 | TestSnapshotRecover4C                                          | 快照恢复         | 节点从快照恢复后操作正确                    |
| 18 | TestSnapshotRecoverManyClients4C                               | 快照恢复多客户端     | 多客户端在快照恢复场景下操作正确                |
| 19 | TestSnapshotUnreliable4C                                       | 不可靠网络下快照     | 丢包环境中快照和操作均正确                   |
| 20 | TestSnapshotUnreliableRecover4C                                | 不可靠+快照恢复     | 丢包+崩溃恢复后操作正确                    |
| 21 | TestSnapshotUnreliableRecoverConcurrentPartition4C             | 不可靠+快照恢复+分区  | 三重压力下操作正确                       |
| 22 | TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C | 最大压力+线性一致性检查 | 最复杂场景下仍满足线性一致性                  |

## ShardKV 分片键值存储（23 个测试）

### Part 5A — 基本服务与分片调度（14 个）

| 编号 | 测试名称                                | 测试功能                 | 验证要点                                  |
|----|-------------------------------------|----------------------|---------------------------------------|
| 1  | TestInitQuery5A                     | 控制器初始化和查询            | ShardCtrler 的 InitConfig 和 Query 接口正确 |
| 2  | TestStaticOneShardGroup5A           | 单分片组静态服务             | 单组下 Put/Get 正常、Leader 切换后数据不丢失        |
| 3  | TestJoinBasic5A                     | 新组加入与分片迁移            | Join 后新组获得分片、旧组拒绝已迁移分片的请求             |
| 4  | TestDeleteBasic5A                   | 分片迁出后旧组清理            | Delete 后旧组持久化数据释放（快照大小减少）             |
| 5  | TestJoinLeaveBasic5A                | 基本的 Join/Leave 混合操作  | 组加入→分片迁移→组离开→数据重新分布→组重新加入             |
| 6  | TestManyJoinLeaveReliable5A         | 多组 Join/Leave（可靠网络）  | 8 个组同时加入和离开，数据完整性不受影响                 |
| 7  | TestManyJoinLeaveUnreliable5A       | 多组 Join/Leave（不可靠网络） | 丢包环境下分片迁移和读写均正确                       |
| 8  | TestShutdown5A                      | 全集群关停恢复              | 所有节点关停后重启，数据恢复正确                      |
| 9  | TestProgressShutdown5A              | 组关停时不影响未迁移键          | 仅关停某些组时，其他组上的数据读写不受影响                 |
| 10 | TestProgressJoin5A                  | Join 过程中不迁移分片的读取性能   | 稳定分片在 Join 过程中能持续快速响应                 |
| 11 | TestOneConcurrentClerkReliable5A    | 单并发客户端（可靠网络）         | 单个客户端并发 Put/Get 满足线性一致性               |
| 12 | TestManyConcurrentClerkReliable5A   | 多并发客户端（可靠网络）         | 10 个客户端并发 Put/Get 满足线性一致性             |
| 13 | TestOneConcurrentClerkUnreliable5A  | 单并发客户端（不可靠网络）        | 丢包环境下单个客户端满足线性一致性                     |
| 14 | TestManyConcurrentClerkUnreliable5A | 多并发客户端（不可靠网络）        | 丢包环境下 10 个客户端满足线性一致性                  |

### Part 5B — 迁移阻塞与控制器恢复（2 个）

| 编号 | 测试名称                | 测试功能               | 验证要点                      |
|----|---------------------|--------------------|---------------------------|
| 15 | TestJoinLeave5B     | 组关停时 Join/Leave 阻塞 | 目标组不可用时 Join 不能完成、恢复后继续执行 |
| 16 | TestRecoverCtrler5B | 分区控制器恢复            | 控制器在网络分区中丢失权限后，新控制器接管完成迁移 |

### Part 5C — 控制器容错与线性一致性（7 个）

| 编号 | 测试名称                                     | 测试功能              | 验证要点                             |
|----|------------------------------------------|-------------------|----------------------------------|
| 17 | TestConcurrentReliable5C                 | 多控制器并发竞争（可靠网络）    | 多个控制器同时执行 Join/Leave，配置变更正确      |
| 18 | TestAcquireLockConcurrentUnreliable5C    | 多控制器并发竞争（不可靠网络）   | 丢包环境下配置变更仍然正确                    |
| 19 | TestPartitionControllerJoin5C            | Join 中控制器分区       | 控制器在 Join 过程中被分区，新控制器接管完成，旧控制器失效 |
| 20 | TestPartitionRecoveryReliableNoClerk5C   | 控制器分区恢复（可靠、无客户端）  | 多轮分区恢复后配置和分片正确                   |
| 21 | TestPartitionRecoveryUnreliableNoClerk5C | 控制器分区恢复（不可靠、无客户端） | 丢包 + 分区恢复后配置和分片正确                |
| 22 | TestPartitionRecoveryReliableClerks5C    | 控制器分区恢复（可靠、有客户端）  | 分区恢复过程中客户端并发读写满足线性一致性            |
| 23 | TestPartitionRecoveryUnreliableClerks5C  | 控制器分区恢复（不可靠、有客户端） | 丢包 + 分区恢复 + 并发读写，满足线性一致性         |

## 覆盖情况总结

| 模块              | 测试数量   | 覆盖场景                       |
|-----------------|--------|----------------------------|
| Raft 3A (选举)    | 3      | 初始选举、分区重选、大规模选举            |
| Raft 3B (日志)    | 10     | 基本共识、并发提交、节点故障、日志回退        |
| Raft 3C (持久化)   | 8      | 崩溃恢复、Figure 8、不可靠网络、动态节点   |
| Raft 3D (快照)    | 7      | 快照基本、InstallSnapshot、崩溃+丢包 |
| KVRaft 4B (基础)  | 13     | 单/多客户端、分区、持久化、不可靠网络        |
| KVRaft 4C (快照)  | 9      | 快照阈值、恢复、分区+丢包+崩溃综合         |
| ShardKV 5A (基本) | 14     | Join/Leave、多组并发、集群关停、线性一致性 |
| ShardKV 5B (迁移) | 2      | 迁移阻塞、控制器恢复                 |
| ShardKV 5C (容错) | 7      | 多控制器竞争、分区恢复、综合压力           |
| **总计**          | **73** |                            |
