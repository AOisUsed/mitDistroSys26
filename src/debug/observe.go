package debug

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ============================================================
// 观测日志 (Observe Logs)
// 按 walkthrough 观测场景分类，与底层模块日志(D3/D4/D5)互相独立。
// 每个场景只有几个关键日志点，避免噪音干扰。
//
// 使用方式（运行时动态切换）：
//   1. Web Demo：通过前端 toggle 开关，调用 /api/observe 接口
//   2. 环境变量：启动时设置 OBSERVE_ELECTION=true 等（仅初始化生效）
//   3. 代码调用：直接调用 debug.SetObserveElection(true)
//
// 输出目标：
//   - Terminal: log.Printf 输出到控制台
//   - RingBuffer: 存入内存环形缓冲区，供 Web Demo 前端轮询
//
// 跨进程支持：
//   服务器 daemon（子进程）通过 RPC 将观测日志转发到主进程的环形缓冲区。
//   子进程不检查 toggle 状态，始终输出终端日志 + 转发；
//   主进程 RPC handler 根据 toggle 决定是否写入环形缓冲区。
//
// 日志样式：
//   Style="":    正常 — 整行用 tag 主题色
//   Style="fault":故障 — tag 保持主题色，正文显示红色
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

// 从环境变量初始化（init 时调用）
func InitObserveFromEnv() {
	// 只在环境变量显式为 "true" 时开启
	// 不需要 import os, 留待 main 中调用
}

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

// observePush 格式化后写入环形缓冲区（正常样式）
func observePush(tag, format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	observeWrite(tag, text, "")
}

// observePushFault 格式化后写入环形缓冲区（故障红色样式）
func observePushFault(tag, format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	observeWrite(tag, text, "fault")
}

// ObservePushTagged 检查 tag 对应 toggle，开启时写入环形缓冲区（无终端输出）
// 供主进程 TesterRPC.PostObserveLog handler 调用，避免子进程转发时的重复日志
func ObservePushTagged(tag, text string) {
	switch tag {
	case TagElection:
		if observeElection.Load() {
			observeWrite(tag, text, "")
		}
	case TagMigration:
		if observeMigration.Load() {
			observeWrite(tag, text, "")
		}
	case TagKVSubmit:
		if observeKVSubmit.Load() {
			observeWrite(tag, text, "")
		}
	case TagSnapshot:
		if observeSnapshot.Load() {
			observeWrite(tag, text, "")
		}
	case TagReplication:
		if observeReplication.Load() {
			observeWrite(tag, text, "")
		}
	default:
		observeWrite(tag, text, "")
	}
}

// GetObserveLines 获取最近 N 条观测日志（最多 bufSize 条）
// 返回结果按时间正序排列（最早的在前）
func GetObserveLines(n int) []ObserveLine {
	if n <= 0 || n > observeBufSize {
		n = observeBufSize
	}

	observeMu.Lock()
	defer observeMu.Unlock()

	current := observeIdx.Load()
	if current == 0 {
		return nil
	}

	start := current - int64(n)
	if start < 0 {
		start = 0
	}

	count := int(current - start)
	lines := make([]ObserveLine, 0, count)
	for i := start; i < current; i++ {
		lines = append(lines, observeBuf[i%observeBufSize])
	}
	return lines
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
type ObserveForwardFn func(tag, text string)

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
// 两种模式：
//   - 主进程模式（无转发回调）：检查 toggle，开启时写入环形缓冲区 + 终端
//   - 子进程模式（有转发回调）：输出到终端 + 转发到主进程，toggle 由主进程控制

func ObserveElectionPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagElection, text) // 子进程模式：转发到主进程
		return
	}
	// 主进程模式
	if observeElection.Load() {
		observePush(TagElection, format, a...)
	}
}

func ObserveMigrationPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagMigration, text)
		return
	}
	if observeMigration.Load() {
		observePush(TagMigration, format, a...)
	}
}

// ObserveMigrationFaultPrintf 分片迁移过程中的故障日志（红色正文）
func ObserveMigrationFaultPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagMigration, text)
		return
	}
	if observeMigration.Load() {
		observePushFault(TagMigration, format, a...)
	}
}

func ObserveKVRequestPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagKVSubmit, text)
		return
	}
	if observeKVSubmit.Load() {
		observePush(TagKVSubmit, format, a...)
	}
}

// ObserveKVRequestFaultPrintf KV请求过程中的故障日志（红色正文）
func ObserveKVRequestFaultPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagKVSubmit, text)
		return
	}
	if observeKVSubmit.Load() {
		observePushFault(TagKVSubmit, format, a...)
	}
}

func ObserveSnapshotPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagSnapshot, text)
		return
	}
	if observeSnapshot.Load() {
		observePush(TagSnapshot, format, a...)
	}
}

func ObserveReplicationPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagReplication, text)
		return
	}
	if observeReplication.Load() {
		observePush(TagReplication, format, a...)
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
