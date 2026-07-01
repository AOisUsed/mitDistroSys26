package cluster

import (
	"kvstore/tester"
	"log"
	"math/rand"
	"sync"
	"time"
)

// ========== 混沌猴子（Chaos Monkey）==========

// ChaosState 表示一个组当前的混沌状态
type ChaosState struct {
	Active  bool   `json:"active"`
	GID     int    `json:"gid"`
	Summary string `json:"summary"`
}

// ChaosMonkey 在后台随机 kill/restart 指定组的节点
// 保证同一时刻存活节点 ≥ quorum（3 节点组保活 ≥ 2）
type ChaosMonkey struct {
	cm             *ClusterManager
	gid            tester.Tgid
	stop           chan struct{}
	pendingRestart map[int]struct{} // 正在等待自动重启的节点（避免重复操作）
	pmu            sync.Mutex
	onChange       func(action string, idx int) // SSE 推送回调
}

// run 是 ChaosMonkey 的主循环
func (m *ChaosMonkey) run() {
	for {
		select {
		case <-m.stop:
			return
		case <-time.After(time.Duration(1000+rand.Intn(2000)) * time.Millisecond):
			m.tick()
		}
	}
}

// tick 执行一次混沌操作：只在 quorum 满足时 kill 一个存活节点，延迟自动重启。
func (m *ChaosMonkey) tick() {
	sg := m.cm.infra.Group(m.gid)
	if sg == nil {
		return
	}
	connected := sg.GetConnected()
	n := sg.N()

	quorum := n/2 + 1

	m.pmu.Lock()
	pending := m.pendingRestart
	if pending == nil {
		pending = make(map[int]struct{})
		m.pendingRestart = pending
	}
	// 清理已恢复的 pending 记录（节点已连接且不在 pending 中 → 实际已恢复）
	for idx := range pending {
		if idx < len(connected) && connected[idx] {
			delete(pending, idx)
		}
	}
	m.pmu.Unlock()

	// 选出没有正在等待重启的存活节点
	eligible := make([]int, 0)
	for i := 0; i < n; i++ {
		if i < len(connected) && connected[i] {
			// 不在 pending 中才可杀（有 pending=即将自动重启，不需要再杀）
			if _, ok := pending[i]; !ok {
				eligible = append(eligible, i)
			}
		}
	}

	// 如果 eligible > quorum，随机杀一个（保证存活 ≥ quorum + 1 才杀）
	if len(eligible) > quorum {
		idx := eligible[rand.Intn(len(eligible))]
		m.pmu.Lock()
		m.pendingRestart[idx] = struct{}{}
		m.pmu.Unlock()
		go m.killNode(idx, sg)
		return
	}
}

func (m *ChaosMonkey) killNode(idx int, sg *tester.ServerGrp) {
	log.Printf("[ChaosMonkey] Kill: GID %d 节点 %d (%s)", m.gid, idx, sg.SrvName(idx))
	sg.ShutdownServer(idx)
	if m.onChange != nil {
		m.onChange("kill", idx)
	}

	// 2~4 秒后自动重启
	delay := 2 + rand.Intn(3)
	select {
	case <-m.stop:
		return
	case <-time.After(time.Duration(delay) * time.Second):
		m.restartNode(idx, sg)
	}
}

func (m *ChaosMonkey) restartNode(idx int, sg *tester.ServerGrp) {
	err := sg.StartServer(idx)
	if err != nil {
		log.Printf("[ChaosMonkey] Restart GID %d 节点 %d 失败: %v — 5 秒后重试", m.gid, idx, err)
		select {
		case <-m.stop:
			return
		case <-time.After(5 * time.Second):
			m.restartNode(idx, sg)
		}
		return
	}
	sg.ConnectOne(idx)

	// 重新应用分区状态——ConnectOne 可能破坏已建立的隔离
	m.cm.reapplyIsolation(m.gid)

	log.Printf("[ChaosMonkey] Restart: GID %d 节点 %d (%s) 已恢复", m.gid, idx, sg.SrvName(idx))
	if m.onChange != nil {
		m.onChange("restart", idx)
	}

	// 清理 pending 记录
	m.pmu.Lock()
	delete(m.pendingRestart, idx)
	m.pmu.Unlock()
}
