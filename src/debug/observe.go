package debug

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// 观测日志 (Observe Logs)
// 开关方式（运行时动态切换）：
//   Web Demo：前端 toggle 调用 /api/observe 接口设置对应场景开关
//
// 输出目标（本文件无终端输出，全部走内存）：
//   - RingBuffer: 写入内存环形缓冲区，供前端 /api/observe/logs 增量轮询
//   - SSE:        web 包通过 SetObserveLogCallback 注册回调，实时推送到前端
//
// 转发回调（ObserveForwardFn）：所有 ObserveXxxPrintf 的唯一出口。
//   - daemon 子进程（跑 raft / shardkv 的 server 逻辑，以及 server 当 client
//     访问别组时的 client 逻辑）：仅在 OBSERVE_FORWARD 环境变量为 "true"
//     （即 demo 运行）时通过 SetObserveForward 注册转发回调，将日志经 RPC 转发给主进程；
//   - 主进程（demo，跑 shardkv 的 client / Clnt 逻辑）：在
//     shardkv-demo/main.go 启动时也注册一个转发回调，直接指向本进程的
//     ObservePushTagged（按 toggle 写本地环形缓冲区 + 推送 SSE）。
//   两条路径最终都汇聚到主进程的 ObservePushTagged。
//
// 日志样式：
//   Style="":    正常 — 整行用 tag 主题色
//   Style="fault":故障/容错 — tag 保持主题色，正文显示红色
// ============================================================

// --- 原子开关（仅主进程有效） ---

var (
	observeElection    atomic.Bool
	observeMigration   atomic.Bool
	observeKVSubmit    atomic.Bool
	observeSnapshot    atomic.Bool
	observeReplication atomic.Bool
)

// Set/Get 函数
func SetObserveElection(v bool)    { observeElection.Store(v) }
func GetObserveElection() bool     { return observeElection.Load() }
func SetObserveMigration(v bool)   { observeMigration.Store(v) }
func GetObserveMigration() bool    { return observeMigration.Load() }
func SetObserveKVSubmit(v bool)    { observeKVSubmit.Store(v) }
func GetObserveKVSubmit() bool     { return observeKVSubmit.Load() }
func SetObserveSnapshot(v bool)    { observeSnapshot.Store(v) }
func GetObserveSnapshot() bool     { return observeSnapshot.Load() }
func SetObserveReplication(v bool) { observeReplication.Store(v) }
func GetObserveReplication() bool  { return observeReplication.Load() }

// --- 场景标签 ---
const (
	TagElection    = "Election"
	TagMigration   = "Migration"
	TagKVSubmit    = "KVSubmit"
	TagSnapshot    = "Snapshot"
	TagReplication = "Replication"
)

// --- 环形缓冲区 ---

type ObserveLine struct {
	Tag       string `json:"tag"`
	Text      string `json:"text"`
	Id        int64  `json:"id"`              // 单调递增的日志ID，用于前端游标增量获取
	UnixMilli int64  `json:"unixMilli"`       // 日志产生时的服务器时间戳（毫秒）
	Style     string `json:"style,omitempty"` // "fault"=红色正文，空=默认
}

const observeBufSize = 500

var (
	observeBuf [observeBufSize]ObserveLine
	observeIdx atomic.Int64 // 写入位置（单调递增）
	observeMu  sync.Mutex   // 保护快照读取一致性
)

// observeWrite 直接写入环形缓冲区（无终端输出，供 RPC handler 使用避免重复）
func observeWrite(tag, text, style string) {
	idx := observeIdx.Add(1) - 1
	unixMilli := time.Now().UnixMilli()
	observeBuf[idx%observeBufSize] = ObserveLine{Tag: tag, Text: text, Id: idx, UnixMilli: unixMilli, Style: style}

	// 实时推送到 SSE（非阻塞，如果回调未注册则跳过）
	if fn := getObserveLogCallback(); fn != nil {
		fn(tag, text, idx, unixMilli, style)
	}
}

// ObservePushTagged 检查 tag 对应 toggle，开启时写入环形缓冲区（无终端输出）
func ObservePushTagged(tag, text, style string) {
	switch tag {
	case TagElection:
		if observeElection.Load() {
			observeWrite(tag, text, style)
		}
	case TagMigration:
		if observeMigration.Load() {
			observeWrite(tag, text, style)
		}
	case TagKVSubmit:
		if observeKVSubmit.Load() {
			observeWrite(tag, text, style)
		}
	case TagSnapshot:
		if observeSnapshot.Load() {
			observeWrite(tag, text, style)
		}
	case TagReplication:
		if observeReplication.Load() {
			observeWrite(tag, text, style)
		}
	default:
		observeWrite(tag, text, style)
	}
}

// --- 异步处理 RPC ---
// 主进程侧缓冲：PostObserveLog handler 只做 O(1) 入队即返回，重活由 observeSinkDrain 串行处理，
// 避免拖慢 daemon 共享的 ds.rpcc。到达率已被 daemon 侧 fwdCh 限速。

const observeSinkSize = 1000

type observeItem struct {
	tag   string
	text  string
	style string
}

var (
	observeSinkCh   = make(chan observeItem, observeSinkSize)
	observeSinkOnce sync.Once
)

// observeSinkDrain 是 observeSinkCh 的唯一消费者：持续取出日志并交给
// ObservePushTagged 按 toggle 写入环形缓冲区 + 推送 SSE。由主进程启动时
func observeSinkDrain() {
	for it := range observeSinkCh {
		ObservePushTagged(it.tag, it.text, it.style)
	}
}

// StartObserveSink 启动 observeSinkCh 的消费者 goroutine（幂等，重复调用安全）。
// 在主进程初始化时显式调用一次（shardkv-demo/main.go
func StartObserveSink() {
	observeSinkOnce.Do(func() {
		go observeSinkDrain()
	})
}

// EnqueueObserve 将一条观测日志放入有界缓冲，立即返回（不阻塞 RPC handler）。
// 缓冲满则丢弃,消费由 StartObserveSink 启动的 observeSinkDrain 负责。
func EnqueueObserve(tag, text, style string) {
	select {
	case observeSinkCh <- observeItem{tag: tag, text: text, style: style}:
	default:
	}
}

// GetObserveLinesSince 获取所有 Id >= sinceId 的观测日志（最多 bufSize 条）
// 返回结果按时间正序排列。sinceId=0 时返回最近 observeBufSize 条。
// 返回当前最新的 observeIdx 作为 nextId，供前端做游标增量获取。
func GetObserveLinesSince(sinceId int64) (lines []ObserveLine, nextId int64) {
	observeMu.Lock()
	defer observeMu.Unlock()

	current := observeIdx.Load()
	if current == 0 {
		return nil, 0
	}

	// 计算扫描范围：最多 observeBufSize 条
	start := current - int64(observeBufSize)
	if start < 0 {
		start = 0
	}

	// 从最大的 Id 往回扫描，收集 Id >= sinceId 的条目
	count := int(current - start)
	all := make([]ObserveLine, 0, count)
	for i := start; i < current; i++ {
		line := observeBuf[i%observeBufSize]
		if line.Id >= sinceId {
			all = append(all, line)
		}
	}
	return all, current
}

// --- SSE 推送回调（Web Demo 实时日志） ---
//
// observeWrite 写入环形缓冲区时，同时调用此回调将日志实时推送到 SSE 流。
// 由 web 包在启动时通过 SetObserveLogCallback 注册。

type ObserveLogCallbackFn func(tag, text string, id int64, unixMilli int64, style string)

var observeLogCallback atomic.Value // stores ObserveLogCallbackFn

// SetObserveLogCallback 设置实时日志推送回调。
// web 包在 SSEBroker 初始化后调用此函数注册推送逻辑。
func SetObserveLogCallback(fn ObserveLogCallbackFn) {
	observeLogCallback.Store(fn)
}

func getObserveLogCallback() ObserveLogCallbackFn {
	fn, _ := observeLogCallback.Load().(ObserveLogCallbackFn)
	return fn
}

// --- 转发回调（子进程模式） ---

// ObserveForwardFn 是子进程向主进程转发观测日志的回调函数类型
type ObserveForwardFn func(tag, text, style string)

var observeForward atomic.Value // stores ObserveForwardFn

// SetObserveForward 设置转发回调。子进程中设置此回调后，
// ObserveXxxPrintf 将转发日志到主进程，不检查 toggle。
func SetObserveForward(fn ObserveForwardFn) {
	observeForward.Store(fn)
}

func getObserveForward() ObserveForwardFn {
	fn, _ := observeForward.Load().(ObserveForwardFn)
	return fn
}

// --- 打印函数 ---
//
// 统一出口：唯一分支是「是否存在转发回调 fn」。
//   - fn != nil（demo 下 daemon 与主进程都已注册）：
//       日志经 fn 送出——daemon 走 RPC 转发、主进程走本地 ObservePushTagged，
//       最终都汇聚到主进程的 ObservePushTagged 按 toggle 落盘 + 推送 SSE。
//   - fn == nil（go test，两进程都不注册）：
//       函数直接返回。

func ObserveElectionPrintf(format string, a ...interface{}) {
	if fn := getObserveForward(); fn != nil {
		fn(TagElection, fmt.Sprintf(format, a...), "")
	}
}

func ObserveMigrationPrintf(format string, a ...interface{}) {
	if fn := getObserveForward(); fn != nil {
		fn(TagMigration, fmt.Sprintf(format, a...), "")
	}
}

// ObserveMigrationFTPrintf 分片迁移过程中的故障日志（红色正文）
func ObserveMigrationFTPrintf(format string, a ...interface{}) {
	if fn := getObserveForward(); fn != nil {
		fn(TagMigration, fmt.Sprintf(format, a...), "fault")
	}
}

func ObserveKVSubmitPrintf(format string, a ...interface{}) {
	if fn := getObserveForward(); fn != nil {
		fn(TagKVSubmit, fmt.Sprintf(format, a...), "")
	}
}

// ObserveKVSubmitFTPrintf KV提交过程中的故障日志（红色正文）
func ObserveKVSubmitFTPrintf(format string, a ...interface{}) {
	if fn := getObserveForward(); fn != nil {
		fn(TagKVSubmit, fmt.Sprintf(format, a...), "fault")
	}
}

func ObserveSnapshotPrintf(format string, a ...interface{}) {
	if fn := getObserveForward(); fn != nil {
		fn(TagSnapshot, fmt.Sprintf(format, a...), "")
	}
}

func ObserveReplicationPrintf(format string, a ...interface{}) {
	if fn := getObserveForward(); fn != nil {
		fn(TagReplication, fmt.Sprintf(format, a...), "")
	}
}

// --- Leader 身份变更通知（子进程模式）---
//
// Raft 层调用 ReportLeaderChange(serverIndex, isLeader)，
// daemon 层通过 SetLeaderChangeForward 注册转发函数，
// 将 (gid, sid, isLeader) 通过 sockrpc 发送到主进程。

type LeaderChangeFn func(serverIndex int, isLeader bool)

var reportLeaderChange atomic.Value // stores LeaderChangeFn

// SetLeaderChangeForward 设置 leader 身份变更的转发回调。
// 在 daemon 子进程的 InitDaemon 中调用，将通知转发到主进程。
func SetLeaderChangeForward(fn LeaderChangeFn) {
	reportLeaderChange.Store(fn)
}

// ReportLeaderChange 供 Raft 层调用，将本节点 leader 身份变更事件通知主进程。
// 使用方式：在 raft.go 中检测到 leader 身份变化时，调用：
//
//	debug.ReportLeaderChange(rf.me, true/false)
//
// 在 daemon 子进程中，通知通过 SetLeaderChangeForward 注册的转发函数发送到主进程。
// 在主进程（测试环境）中，此函数为空操作（无转发回调）。
func ReportLeaderChange(serverIndex int, isLeader bool) {
	if fn, ok := reportLeaderChange.Load().(LeaderChangeFn); ok {
		fn(serverIndex, isLeader)
	}
}
