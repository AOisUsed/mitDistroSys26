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
//   1. Web Demo：通过前端 4 个 toggle 开关，调用 /api/observe 接口
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
// ============================================================

// --- 原子开关（仅主进程有效） ---

var (
	observeElection      atomic.Bool
	observeMigration     atomic.Bool
	observeKVSubmit      atomic.Bool
	observeFaultRecovery atomic.Bool
)

// Set/Get 函数
func SetObserveElection(v bool)      { observeElection.Store(v) }
func GetObserveElection() bool       { return observeElection.Load() }
func SetObserveMigration(v bool)     { observeMigration.Store(v) }
func GetObserveMigration() bool      { return observeMigration.Load() }
func SetObserveKVSubmit(v bool)      { observeKVSubmit.Store(v) }
func GetObserveKVSubmit() bool       { return observeKVSubmit.Load() }
func SetObserveFaultRecovery(v bool) { observeFaultRecovery.Store(v) }
func GetObserveFaultRecovery() bool  { return observeFaultRecovery.Load() }

// 从环境变量初始化（init 时调用）
func InitObserveFromEnv() {
	// 只在环境变量显式为 "true" 时开启
	// 不需要 import os, 留待 main 中调用
}

// --- 场景标签 ---
const (
	TagElection  = "Election"
	TagMigration = "Migration"
	TagKVSubmit  = "KVSubmit"
	TagFault     = "Fault"
)

// --- 环形缓冲区 ---

type ObserveLine struct {
	Tag       string `json:"tag"`
	Text      string `json:"text"`
	Id        int64  `json:"id"`        // 单调递增的日志ID，用于前端游标增量获取
	UnixMilli int64  `json:"unixMilli"` // 日志产生时的服务器时间戳（毫秒）
}

const observeBufSize = 500

var (
	observeBuf [observeBufSize]ObserveLine
	observeIdx atomic.Int64 // 写入位置（单调递增）
	observeMu  sync.Mutex   // 保护快照读取一致性
)

// observeWrite 直接写入环形缓冲区（无终端输出，供 RPC handler 使用避免重复）
func observeWrite(tag, text string) {
	idx := observeIdx.Add(1) - 1
	observeBuf[idx%observeBufSize] = ObserveLine{Tag: tag, Text: text, Id: idx, UnixMilli: time.Now().UnixMilli()}
}

// observePush 格式化后写入环形缓冲区
func observePush(tag, format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	observeWrite(tag, text)
}

// ObservePushTagged 检查 tag 对应 toggle，开启时写入环形缓冲区（无终端输出）
// 供主进程 TesterRPC.PostObserveLog handler 调用，避免子进程转发时的重复日志
func ObservePushTagged(tag, text string) {
	switch tag {
	case TagElection:
		if observeElection.Load() {
			observeWrite(tag, text)
		}
	case TagMigration:
		if observeMigration.Load() {
			observeWrite(tag, text)
		}
	case TagKVSubmit:
		if observeKVSubmit.Load() {
			observeWrite(tag, text)
		}
	case TagFault:
		if observeFaultRecovery.Load() {
			observeWrite(tag, text)
		}
	default:
		observeWrite(tag, text)
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

func ObserveFaultPrintf(format string, a ...interface{}) {
	text := fmt.Sprintf(format, a...)
	if fn := getObserveForward(); fn != nil {
		fn(TagFault, text)
		return
	}
	if observeFaultRecovery.Load() {
		observePush(TagFault, format, a...)
	}
}
