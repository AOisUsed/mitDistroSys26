package cluster

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardctrler"
	"6.5840/tester1"
)

// ClusterManager 管理集群生命周期
type ClusterManager struct {
	mu           sync.Mutex
	cfg          *tester.Config
	ctl          *shardctrler.ShardCtrler
	ck           kvtest.IKVClerk
	clnt         *tester.Clnt // controller's client
	maxRaftState int
	nextGid      tester.Tgid
	groups       map[tester.Tgid]bool         // tracked groups
	isolated     map[tester.Tgid]map[int]bool // 被网络隔离的节点
	chaosMonkeys []*ChaosMonkey               // 活跃的混沌猴子
}

// NewClusterManager 创建一个新的集群管理器
func NewClusterManager() *ClusterManager {
	cm := &ClusterManager{
		nextGid:      shardcfg.Gid1 + 1, // Gid1(1) 已被 Init 使用
		groups:       make(map[tester.Tgid]bool),
		isolated:     make(map[tester.Tgid]map[int]bool),
		maxRaftState: -1,
	}
	return cm
}

// getArgs returns common args for group creation
func (cm *ClusterManager) getArgs() []string {
	if cm.maxRaftState > 0 {
		return []string{fmt.Sprintf("--max-raft-state=%d", cm.maxRaftState)}
	}
	return []string{}
}

// Init 初始化集群，包括 config store 和初始 shard group
func (cm *ClusterManager) Init() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// 创建 tester Config（使用 kvsrv1d 作为 config store）
	cm.cfg = tester.MakeDemoConfig("kvsrv1d", []string{})

	// 创建 shard controller
	cm.clnt = cm.cfg.MakeClient()
	cm.ctl = shardctrler.MakeShardCtrler(cm.clnt)

	// 初始化 config store 的配置
	scfg := shardcfg.MakeShardConfig()
	args := cm.getArgs()
	cm.cfg.MakeGroupStart("shardgrp1d", args, shardcfg.Gid1, 3)
	scfg.JoinBalance(map[tester.Tgid][]string{
		shardcfg.Gid1: cm.cfg.Group(shardcfg.Gid1).SrvNames(),
	})
	cm.ctl.InitConfig(scfg)
	cm.groups[shardcfg.Gid1] = true

	// 创建 shardkv clerk
	cm.ck = shardkv.MakeClerk(cm.clnt, cm.ctl)

	log.Printf("[Cluster] 初始化完成: Gid1=%v 拥有全部 %d 个 shard",
		cm.cfg.Group(shardcfg.Gid1).SrvNames(), shardcfg.NShards)
	return nil
}

// Config 返回底层 Config
func (cm *ClusterManager) Config() *tester.Config {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.cfg
}

// Ctl 返回 ShardCtrler
func (cm *ClusterManager) Ctl() *shardctrler.ShardCtrler {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.ctl
}

// Clerk 返回 shardkv Clerk（kvtest.IKVClerk 类型）
func (cm *ClusterManager) Clerk() kvtest.IKVClerk {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.ck
}

// newGid 生成下一个可用的 GID
func (cm *ClusterManager) newGid() tester.Tgid {
	gid := cm.nextGid
	cm.nextGid++
	return gid
}

// Group 返回指定 GID 的 ServerGrp
func (cm *ClusterManager) Group(gid tester.Tgid) *tester.ServerGrp {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.cfg.Group(gid)
}

// Status 返回完整集群状态
func (cm *ClusterManager) Status() *ClusterState {
	// Query() 是 RPC 调用，在不可靠网络下可能延迟，
	// 必须在锁外执行，否则所有需要 cm.mu 的操作（恢复、隔离等）会被阻塞。
	cfg := cm.ctl.Query()
	hasPending := cm.ctl.HasPendingMigration()
	nextCfg := cm.ctl.QueryNext()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	state := &ClusterState{
		Config:              cfg,
		HasPendingMigration: hasPending,
		PendingConfigNum:    nextCfg.Num,
	}
	// 只展示 cm.groups 中追踪的组（排除 configStore GRP0，也不展示"幽灵组"）
	for gid := range cm.groups {
		sg := cm.cfg.Group(gid)
		if sg == nil {
			continue
		}
		gs := GroupState{
			GID:      gid,
			SrvNames: sg.SrvNames(),
		}
		// Count shards assigned to this group
		for _, g := range state.Config.Shards {
			if g == gid {
				gs.NShards++
			}
		}
		// Server states
		gs.Servers = make([]ServerState, sg.N())
		connected := sg.GetConnected()
		isoMap := cm.isolated[gid]
		for i := 0; i < sg.N(); i++ {
			isAlive := i < len(connected) && connected[i]
			isIsolated := isoMap != nil && isoMap[i]
			gs.Servers[i] = ServerState{
				Index:      i,
				Name:       sg.SrvName(i),
				IsAlive:    isAlive,
				IsIsolated: isIsolated,
			}
		}
		state.Groups = append(state.Groups, gs)
	}
	return state
}

// Stop 关闭所有服务器
func (cm *ClusterManager) Stop() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	log.Printf("[Cluster] 正在关闭所有组...")
	for gid := range cm.groups {
		sg := cm.cfg.Group(gid)
		if sg != nil {
			sg.Shutdown()
		}
	}
	// Cleanup the config store group (GRP0)
	grp0 := cm.cfg.Group(tester.GRP0)
	if grp0 != nil {
		grp0.Shutdown()
	}
	log.Printf("[Cluster] 所有组已关闭")
}

// NewClerk 创建一个新的独立 Clerk（用于 CAS 并发竞赛演示）
func (cm *ClusterManager) NewClerk() kvtest.IKVClerk {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return shardkv.MakeClerk(cm.clnt, cm.ctl)
}

// Put 写入键值（带乐观锁版本号）
func (cm *ClusterManager) Put(key, value string, version rpc.Tversion) rpc.Err {
	return cm.ck.Put(key, value, version)
}

// Get 读取键
func (cm *ClusterManager) Get(key string) (string, rpc.Tversion, rpc.Err) {
	return cm.ck.Get(key)
}

// KillServer 停掉指定组中的某个节点
func (cm *ClusterManager) KillServer(gid tester.Tgid, srv int) {
	sg := cm.cfg.Group(gid)
	if sg == nil {
		log.Printf("[Cluster] KillServer: 组 %d 不存在", gid)
		return
	}
	if srv < 0 || srv >= sg.N() {
		log.Printf("[Cluster] KillServer: 节点索引 %d 超出范围 [0, %d)", srv, sg.N())
		return
	}
	sg.ShutdownServer(srv)
	log.Printf("[Cluster] 已停掉节点 %s", sg.SrvName(srv))
}

// IsolateNode 通过 Partition 隔离指定节点（进程保持运行，仅隔离网络）
// 注意：每次调用会重置组的整个分区状态——之前被隔离的节点会恢复到 rest 组。
// 只操作当前还活着的节点，跳过已下线节点，避免意外重连已下线的节点。
func (cm *ClusterManager) IsolateNode(gid tester.Tgid, srv int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sg := cm.cfg.Group(gid)
	if sg == nil {
		return
	}
	if srv < 0 || srv >= sg.N() {
		return
	}
	connected := sg.GetConnected()
	rest := make([]int, 0)
	for i := 0; i < sg.N(); i++ {
		if i != srv && i < len(connected) && connected[i] {
			rest = append(rest, i)
		}
	}
	sg.Partition(rest, []int{srv})
	// 清除该组所有旧隔离记录，只标记当前被隔离的节点
	cm.isolated[gid] = map[int]bool{srv: true}
	log.Printf("[Cluster] 已隔离节点 %s (Partition: rest=%v, isolated=[%d])", sg.SrvName(srv), rest, srv)
}

// RecoverNode 恢复单个节点的网络（只重连该节点到组内所有存活节点，不影响已下线节点）
func (cm *ClusterManager) RecoverNode(gid tester.Tgid, srv int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sg := cm.cfg.Group(gid)
	if sg == nil {
		return
	}
	if srv < 0 || srv >= sg.N() {
		return
	}

	// 找出所有存活且未被隔离的节点
	connected := sg.GetConnected()
	alive := make([]int, 0)
	for i := 0; i < sg.N(); i++ {
		if i != srv && i < len(connected) && connected[i] {
			alive = append(alive, i)
		}
	}
	// 把该节点连接到所有存活节点
	allAlive := append(alive, srv)
	sg.Partition(allAlive, []int{})
	// 清除该节点在隔离记录中的标记
	if m, ok := cm.isolated[gid]; ok {
		delete(m, srv)
		if len(m) == 0 {
			delete(cm.isolated, gid)
		}
	}
	log.Printf("[Cluster] 已恢复节点 %s 的网络连接（连接到 %v）", sg.SrvName(srv), alive)
}

// RecoverGroup 恢复指定组的全部网络连接（取消所有分区），但只操作存活节点，跳过已下线节点
func (cm *ClusterManager) RecoverGroup(gid tester.Tgid) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sg := cm.cfg.Group(gid)
	if sg == nil {
		return
	}
	connected := sg.GetConnected()
	alive := make([]int, 0)
	for i := 0; i < sg.N(); i++ {
		if i < len(connected) && connected[i] {
			alive = append(alive, i)
		}
	}
	sg.Partition(alive, []int{})
	// 清除指定组的隔离记录
	delete(cm.isolated, gid)
	log.Printf("[Cluster] 已恢复组 %d 的网络连接（仅操作 %d 个存活节点）", gid, len(alive))
}

// RecoverAllGroups 恢复所有组的网络连接
func (cm *ClusterManager) RecoverAllGroups() {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for gid := range cm.groups {
		sg := cm.cfg.Group(gid)
		if sg == nil {
			continue
		}
		n := sg.N()
		all := make([]int, n)
		for i := 0; i < n; i++ {
			all[i] = i
		}
		sg.Partition(all, []int{})
	}
	cm.isolated = make(map[tester.Tgid]map[int]bool) // 清除所有隔离记录
	log.Printf("[Cluster] 已恢复全部组的网络连接")
}

// StartServer 启动指定组中的某个节点，带健康检查
func (cm *ClusterManager) StartServer(gid tester.Tgid, srv int) error {
	sg := cm.cfg.Group(gid)
	if sg == nil {
		return fmt.Errorf("组 %d 不存在", gid)
	}
	if srv < 0 || srv >= sg.N() {
		return fmt.Errorf("节点索引 %d 超出范围 [0, %d)", srv, sg.N())
	}
	err := sg.StartServer(srv)
	if err != nil {
		return fmt.Errorf("启动节点 %s 失败: %v", sg.SrvName(srv), err)
	}
	sg.ConnectOne(srv)

	// 健康检查：等待最多 2 秒确认节点已连接
	for i := 0; i < 20; i++ {
		connected := sg.GetConnected()
		if srv < len(connected) && connected[srv] {
			log.Printf("[Cluster] 已启动节点 %s（健康）", sg.SrvName(srv))
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("启动节点 %s 超时: 节点未连入网络", sg.SrvName(srv))
}

// ensureServerLive 检查指定组是否有足够的存活节点完成 RPC 操作
// 返回错误时附带可读原因，调用方应据此拒绝操作
func (cm *ClusterManager) ensureServerLive(gid tester.Tgid) error {
	sg := cm.cfg.Group(gid)
	if sg == nil {
		return fmt.Errorf("组 %d 不存在", gid)
	}
	connected := sg.GetConnected()
	nAlive := 0
	deadServers := []int{}
	for i := 0; i < sg.N(); i++ {
		if i < len(connected) && connected[i] {
			nAlive++
		} else {
			deadServers = append(deadServers, i)
		}
	}
	// 计算 quorum：N/2 + 1
	quorum := sg.N()/2 + 1
	if nAlive >= quorum {
		return nil // 已有足够节点存活
	}
	return fmt.Errorf("组 %d 仅有 %d/%d 个节点存活，需要 quorum=%d 才能完成 shard 迁移。"+
		"请先使用 Start 操作恢复至少 %d 个节点（当前已死亡节点: %v）",
		gid, nAlive, sg.N(), quorum, quorum-nAlive, deadServers)
}

// KillGroup 停掉整个组
func (cm *ClusterManager) KillGroup(gid tester.Tgid) {
	sg := cm.cfg.Group(gid)
	if sg == nil {
		return
	}
	sg.Shutdown()
	log.Printf("[Cluster] 已停掉组 %d", gid)
}

// StartGroup 启动整个组
func (cm *ClusterManager) StartGroup(gid tester.Tgid) error {
	sg := cm.cfg.Group(gid)
	if sg == nil {
		return fmt.Errorf("组 %d 不存在", gid)
	}
	sg.StartServers()
	log.Printf("[Cluster] 已启动组 %d", gid)
	return nil
}

// tryResolvePendingMigration 检查未完成的 shard 迁移并尝试恢复
// 必须在锁外调用（会做 RPC），返回 true 表示已解决或无 pending
func (cm *ClusterManager) tryResolvePendingMigration() bool {
	hasPending := cm.ctl.HasPendingMigration()
	if hasPending {
		log.Printf("[Cluster] 检测到未完成的 shard 迁移，调用 InitController 恢复...")
		cm.ctl.InitController()
		// 重新检查
		hasPending = cm.ctl.HasPendingMigration()
		if hasPending {
			log.Printf("[Cluster] InitController 无法完成 pending 迁移，可能因节点不可用")
			return false
		}
	}
	return true
}

// JoinGroup 加入新组
func (cm *ClusterManager) JoinGroup(gid tester.Tgid) (bool, string) {
	// ---- 阶段 0：先处理任何 pending migration（锁外执行 RPC）----
	if !cm.tryResolvePendingMigration() {
		msg := fmt.Sprintf("组 %d 加入失败：存在无法恢复的未完成迁移，请检查节点状态后重试", gid)
		log.Printf("[Cluster] JoinGroup: %s", msg)
		return false, msg
	}

	// ---- 阶段 1：加锁执行创建操作 ----
	cm.mu.Lock()

	args := cm.getArgs()
	cm.cfg.MakeGroupStart("shardgrp1d", args, gid, 3)
	srvs := cm.cfg.Group(gid).SrvNames()

	cfg := cm.ctl.Query()
	newcfg := cfg.Copy()
	if ok := newcfg.JoinBalance(map[tester.Tgid][]string{gid: srvs}); !ok {
		sg := cm.cfg.Group(gid)
		if sg != nil {
			sg.Shutdown()
		}
		delete(cm.groups, gid)
		cm.cfg.ExitGroup(gid)
		cm.mu.Unlock()
		log.Printf("[Cluster] JoinGroup: 组 %d 加入失败（重复）", gid)
		return false, fmt.Sprintf("组 %d 已存在", gid)
	}

	// 检查已有组是否有足够存活节点完成 shard 迁移
	for existingGid := range cm.groups {
		if existingGid == gid {
			continue
		}
		sg := cm.cfg.Group(existingGid)
		if sg == nil {
			continue
		}
		connected := sg.GetConnected()
		nAlive := 0
		for i := 0; i < sg.N(); i++ {
			if i < len(connected) && connected[i] {
				nAlive++
			}
		}
		quorum := sg.N()/2 + 1
		if nAlive < quorum {
			sg = cm.cfg.Group(gid)
			if sg != nil {
				sg.Shutdown()
			}
			delete(cm.groups, gid)
			cm.cfg.ExitGroup(gid)
			cm.mu.Unlock()
			msg := fmt.Sprintf("组 %d 无法加入：组 %d 仅有 %d/%d 节点存活（需要 quorum=%d），shard 迁移无法完成。请先恢复故障组", gid, existingGid, nAlive, sg.N(), quorum)
			log.Printf("[Cluster] JoinGroup: %s", msg)
			return false, msg
		}
	}

	cm.groups[gid] = true
	cm.ctl.ChangeConfigTo(newcfg)
	cm.mu.Unlock()

	// ---- 阶段 2：解锁，轮询等待迁移完成 ----
	const maxWait = 100 // 100ms * 100 = 10s
	for i := 0; i < maxWait; i++ {
		cur := cm.ctl.Query()
		if cur.Num >= newcfg.Num {
			if _, ok := cur.Groups[gid]; ok {
				log.Printf("[Cluster] 组 %d 已加入集群, servers=%v (config #%d)", gid, srvs, cur.Num)
				return true, ""
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// ---- 阶段 3：超时，完全清理新组（不留幽灵组）----
	cm.mu.Lock()
	cur := cm.ctl.Query()
	log.Printf("[Cluster] JoinGroup: 组 %d 加入超时（当前 config #%d，期望 #%d），迁移可能因节点故障失败", gid, cur.Num, newcfg.Num)

	// 完全清理：shutdown 组进程、从 tester.Config 中删除
	sg := cm.cfg.Group(gid)
	if sg != nil {
		sg.Shutdown()
	}
	delete(cm.groups, gid)
	cm.cfg.ExitGroup(gid)

	failInfo := ""
	for existingGid := range cm.groups {
		sg := cm.cfg.Group(existingGid)
		if sg == nil {
			continue
		}
		connected := sg.GetConnected()
		nAlive := 0
		for i := 0; i < sg.N(); i++ {
			if i < len(connected) && connected[i] {
				nAlive++
			}
		}
		quorum := sg.N()/2 + 1
		if nAlive < quorum {
			failInfo += fmt.Sprintf("组 %d 存活 %d/%d（需要 %d）；", existingGid, nAlive, sg.N(), quorum)
		}
	}
	cm.mu.Unlock()

	if failInfo != "" {
		failInfo = "可能原因：" + failInfo
	}
	msg := fmt.Sprintf("组 %d 加入超时：shard 迁移未在 10 秒内完成。%s", gid, failInfo)
	return false, msg
}

// LeaveGroup 移除组，返回 (是否成功, 错误信息)
func (cm *ClusterManager) LeaveGroup(gid tester.Tgid) (bool, string) {
	// ---- 先检查组是否存在（锁内）----
	cm.mu.Lock()
	cfg := cm.ctl.Query()
	_, inController := cfg.Groups[gid]
	sg := cm.cfg.Group(gid)
	_, _ = cm.groups[gid]
	cm.mu.Unlock()

	// 检查组是否存在：可能在 cm.groups 中或在 tester.Config 中
	if sg == nil {
		log.Printf("[Cluster] LeaveGroup: 组 %d 不存在于任何地方", gid)
		return false, fmt.Sprintf("组 %d 不存在", gid)
	}

	// 如果组在 tester.Config 中但不在 controller 中（Join 超时残留），直接清理本地
	if sg != nil && !inController {
		cm.mu.Lock()
		sg.Shutdown()
		delete(cm.groups, gid)
		cm.cfg.ExitGroup(gid)
		cm.mu.Unlock()
		log.Printf("[Cluster] 组 %d 已清理（有进程但不在集群配置中）", gid)
		return true, ""
	}

	// ---- 阶段 0：先做 pending migration 恢复（锁外 RPC）----
	// 不管是谁的 pending，先让 controller 完成它
	if !cm.tryResolvePendingMigration() {
		msg := fmt.Sprintf("组 %d 离开失败：存在无法恢复的未完成迁移，请检查节点状态后重试", gid)
		log.Printf("[Cluster] LeaveGroup: %s", msg)
		return false, msg
	}

	// ---- 阶段 1：加锁执行检查和新配置生成 ----
	cm.mu.Lock()

	// 再次检查组是否存在（在锁内重新确认）
	sg = cm.cfg.Group(gid)
	if sg == nil {
		cm.mu.Unlock()
		msg := fmt.Sprintf("组 %d 不存在", gid)
		log.Printf("[Cluster] LeaveGroup: %s", msg)
		return false, msg
	}

	// 再次检查是否在 controller 中（pending 恢复后可能已变化）
	cfg = cm.ctl.Query()
	if _, exists := cfg.Groups[gid]; !exists {
		// 不在 controller config 中，直接清理
		sg.Shutdown()
		delete(cm.groups, gid)
		cm.cfg.ExitGroup(gid)
		cm.mu.Unlock()
		log.Printf("[Cluster] 组 %d 已清理（有进程但不在集群配置中）", gid)
		return true, ""
	}

	// 检查是否有其他组可以接管 shard
	if len(cfg.Groups) <= 1 {
		cm.mu.Unlock()
		msg := fmt.Sprintf("不能移除最后一个组 %d，集群中至少需要保留一个组", gid)
		log.Printf("[Cluster] LeaveGroup: %s", msg)
		return false, msg
	}

	// 在迁移前确保组内至少 quorum 节点存活
	if err := cm.ensureServerLive(gid); err != nil {
		cm.mu.Unlock()
		msg := fmt.Sprintf("LeaveGroup 失败: %v", err)
		log.Printf("[Cluster] LeaveGroup: %s", msg)
		return false, msg
	}

	// 生成新配置
	cfg = cm.ctl.Query()
	newcfg := cfg.Copy()
	if ok := newcfg.LeaveBalance([]tester.Tgid{gid}); !ok {
		cm.mu.Unlock()
		msg := fmt.Sprintf("组 %d 移除失败（LeaveBalance 拒绝）", gid)
		log.Printf("[Cluster] LeaveGroup: %s", msg)
		return false, msg
	}
	cm.mu.Unlock()

	// ---- 阶段 2：解锁，轮询等待 shard 迁移完成 ----
	const maxRetries = 30 // 最多等 30 秒（每次 1s）
	changed := false
	for retry := 0; retry < maxRetries; retry++ {
		cm.ctl.ChangeConfigTo(newcfg)

		for poll := 0; poll < 20; poll++ {
			cur := cm.ctl.Query()
			if cur.Num >= newcfg.Num {
				changed = true
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if changed {
			log.Printf("[Cluster] 组 %d 的 shard 迁移已完成 (config=%d)", gid, newcfg.Num)
			break
		}
		log.Printf("[Cluster] 组 %d Leave: shard 迁移尚未完成，重试 ChangeConfigTo (#%d)", gid, retry+1)
		time.Sleep(1 * time.Second)
	}

	if !changed {
		msg := fmt.Sprintf("组 %d 的 shard 迁移失败，请检查网络和集群状态后重试", gid)
		log.Printf("[Cluster] LeaveGroup: %s", msg)
		return false, msg
	}

	// ---- 阶段 3：重新加锁，确认组真的没有分片了，再清理 ----
	cm.mu.Lock()

	// 验证组已没有分片
	cfg = cm.ctl.Query()
	for _, g := range cfg.Shards {
		if g == gid {
			cm.mu.Unlock()
			msg := fmt.Sprintf("组 %d 迁移后仍有分片分配（config #%d），拒绝关闭进程", gid, cfg.Num)
			log.Printf("[Cluster] LeaveGroup: %s", msg)
			return false, msg
		}
	}

	sg = cm.cfg.Group(gid)
	if sg != nil {
		sg.Shutdown()
	}
	delete(cm.groups, gid)
	cm.cfg.ExitGroup(gid)
	cm.mu.Unlock()

	log.Printf("[Cluster] 组 %d 已离开集群（已确认无分片）", gid)
	return true, ""
}

// QueryConfig 查询当前 shard 配置
func (cm *ClusterManager) QueryConfig() *shardcfg.ShardConfig {
	return cm.ctl.Query()
}

// InitController 初始化控制器（用于恢复）
func (cm *ClusterManager) InitController() {
	cm.ctl.InitController()
}

// SetReliable 设置网络是否可靠（false 时随机延迟 + 10% 丢请求/回复）
func (cm *ClusterManager) SetReliable(yes bool) {
	cm.cfg.SetReliable(yes)
	log.Printf("[Cluster] 网络可靠性: %v", yes)
}

// IsReliable 返回当前网络是否可靠
func (cm *ClusterManager) IsReliable() bool {
	return cm.cfg.IsReliable()
}

// SetLongReordering 设置是否长延迟重排序（67% 回复延迟 200ms~2.2s）
func (cm *ClusterManager) SetLongReordering(yes bool) {
	cm.cfg.SetLongReordering(yes)
	log.Printf("[Cluster] 长延迟重排序: %v", yes)
}

// ConnectAll 恢复全部网络连接
func (cm *ClusterManager) ConnectAll() {
	cm.clnt.ConnectAll()
}

// Partition 网络分区
func (cm *ClusterManager) Partition(gid tester.Tgid, p1, p2 []int) {
	sg := cm.cfg.Group(gid)
	if sg != nil {
		sg.Partition(p1, p2)
	}
}

// NewGid 返回一个新的 GID
func (cm *ClusterManager) NewGid() tester.Tgid {
	return cm.newGid()
}

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
}

// StartChaos 为指定组启动混沌猴子
func (cm *ClusterManager) StartChaos(gid tester.Tgid) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sg := cm.cfg.Group(gid)
	if sg == nil {
		return fmt.Errorf("组 %d 不存在", gid)
	}

	// 如果已启动，直接返回
	for _, m := range cm.chaosMonkeys {
		if m.gid == gid {
			return nil
		}
	}

	m := &ChaosMonkey{
		cm:   cm,
		gid:  gid,
		stop: make(chan struct{}),
	}
	cm.chaosMonkeys = append(cm.chaosMonkeys, m)
	go m.run()
	log.Printf("[ChaosMonkey] 组 %d 混沌启动", gid)
	return nil
}

// StopChaos 停止指定组的混沌猴子
func (cm *ClusterManager) StopChaos(gid tester.Tgid) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	for i, m := range cm.chaosMonkeys {
		if m.gid == gid {
			close(m.stop)
			cm.chaosMonkeys = append(cm.chaosMonkeys[:i], cm.chaosMonkeys[i+1:]...)
			log.Printf("[ChaosMonkey] 组 %d 混沌停止", gid)
			return
		}
	}
}

// ChaosStatus 返回所有组的混沌状态
func (cm *ClusterManager) ChaosStatus() []ChaosState {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	states := make([]ChaosState, 0, len(cm.groups))
	active := make(map[tester.Tgid]bool)
	for _, m := range cm.chaosMonkeys {
		active[m.gid] = true
	}
	for gid := range cm.groups {
		sg := cm.cfg.Group(gid)
		if sg == nil {
			continue
		}
		connected := sg.GetConnected()
		alive := 0
		for i := 0; i < sg.N(); i++ {
			if i < len(connected) && connected[i] {
				alive++
			}
		}
		state := ChaosState{
			Active: active[gid],
			GID:    int(gid),
		}
		if active[gid] {
			state.Summary = fmt.Sprintf("活跃 (存活 %d/%d)", alive, sg.N())
		} else {
			state.Summary = fmt.Sprintf("停止 (存活 %d/%d)", alive, sg.N())
		}
		states = append(states, state)
	}
	return states
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

// tick 执行一次混沌操作
// 原则：只杀不救 — tick() 只在安全时可杀（alive > quorum），从不主动 restart
//
//	后续自动恢复由 killNode() 的延迟 goroutine 负责
func (m *ChaosMonkey) tick() {
	sg := m.cm.cfg.Group(m.gid)
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
	log.Printf("[ChaosMonkey] Restart: GID %d 节点 %d (%s) 已恢复", m.gid, idx, sg.SrvName(idx))

	// 清理 pending 记录
	m.pmu.Lock()
	delete(m.pendingRestart, idx)
	m.pmu.Unlock()
}

// 初始化随机种子
func init() {
	rand.Seed(time.Now().UnixNano())
}
