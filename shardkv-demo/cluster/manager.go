package cluster

import (
	"fmt"
	"log"
	"shardkv-demo/config"
	"strings"
	"sync"
	"time"

	"kvstore/kvraft"
	"kvstore/kvsrv/rpcapi"
	"kvstore/kvtest"
	"kvstore/shardkv"
	"kvstore/shardkv/shardcfg"
	"kvstore/shardkv/shardctrler"
	"kvstore/tester"
)

const (
	staleConfigTimeout = 5 * time.Second // 超过此时间未更新 config 视为缓存
	clerkPoolSize      = 500
)

// ClusterManager 管理集群生命周期
type ClusterManager struct {
	// —— 不可变（new 时确定，之后只读）——
	dcfg         config.DemoConfig // 启动参数
	maxRaftState int               // raft 日志压缩阈值

	// —— 核心依赖（Init 时注入，之后只读）——
	infra *tester.Config           // 运行时基础设施（进程/网络）
	ctl   *shardctrler.ShardCtrler // 分片控制器
	ck    kvtest.IKVClerk          // shardkv 客户端
	clnt  *tester.Clnt             // controller 的 RPC 客户端

	// —— 同步 ——
	mu sync.Mutex // 保护以下所有可变字段

	// —— 集群成员状态 ——
	nextGid tester.Tgid          // 下一个可分配的 GID
	groups  map[tester.Tgid]bool // 活跃的组

	// —— 故障注入状态 ——
	isolated     map[tester.Tgid]map[int]bool // 网络隔离节点
	chaosMonkeys []*ChaosMonkey               // 活跃混沌猴子

	// —— Leader 感知 ——
	leaders               map[tester.Tgid]map[int]bool
	leaderChangeListeners []func(gid tester.Tgid, sid int, isLeader bool)

	// —— Config 缓存（poller 写入，Status 读取）——
	cachedConfig     *shardcfg.ShardConfig
	cachedNextConfig *shardcfg.ShardConfig
	lastConfigOk     time.Time

	// —— 生命周期 ——
	done     chan struct{} // 通知后台 goroutine 退出
	stopOnce sync.Once

	// —— Clerk 池（懒初始化） ——
	clerkPool *ClerkPool
	poolOnce  sync.Once
}

// NewClusterManager 创建一个新的集群管理器，使用给定配置
func NewClusterManager(dcfg config.DemoConfig) *ClusterManager {
	cm := &ClusterManager{
		dcfg:         dcfg,
		nextGid:      shardcfg.Gid1 + 1, // Gid1(1) 已被 Init 使用
		groups:       make(map[tester.Tgid]bool),
		isolated:     make(map[tester.Tgid]map[int]bool),
		leaders:      make(map[tester.Tgid]map[int]bool),
		maxRaftState: dcfg.MaxRaftState,
		done:         make(chan struct{}),
	}
	return cm
}

// RegisterLeaderChangeListener 注册 Leader 变更通知回调
// 回调在 cm.mu 锁外被调用，可直接执行耗时操作
func (cm *ClusterManager) RegisterLeaderChangeListener(fn func(gid tester.Tgid, sid int, isLeader bool)) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.leaderChangeListeners = append(cm.leaderChangeListeners, fn)
}

// serverArgs 返回分片组创建需要的参数（目前只使用 maxraftstate）
func (cm *ClusterManager) serverArgs() []string {
	if cm.maxRaftState > 0 {
		return []string{fmt.Sprintf("--max-raft-state=%d", cm.maxRaftState)}
	}
	return []string{}
}

// Init 初始化集群，包括 config store（kvraft Raft 组）和初始 shard group
// 使用 cm.dcfg 中配置的组数和节点数
//
// 初始化流程：
//  1. 启动所有配置的 shard group 进程
//  2. configStore 写入初始配置：所有 shard → Gid1（与真实数据位置一致）
//  3. 如果有多个组，将 Gid1 上 shard 重新分配，迁移到各组分担
func (cm *ClusterManager) Init() error {
	// 从配置中读取参数
	nsrv := cm.dcfg.Nsrv
	if nsrv <= 0 {
		nsrv = 3
	}
	reliable := cm.dcfg.Reliable

	// 创建 tester Config，使用 kvraft（nsrv 节点 Raft 组）作为 config store
	cm.infra = tester.MakeDemoConfig("kvraft", []string{}, nsrv)

	// 注册回调
	cm.infra.SetLeaderChangeListener(func(gid, sid int, isLeader bool) {
		cm.mu.Lock()
		leaders, ok := cm.leaders[tester.Tgid(gid)]
		if !ok {
			leaders = make(map[int]bool)
			cm.leaders[tester.Tgid(gid)] = leaders
		}
		if isLeader {
			leaders[sid] = true
		} else {
			delete(leaders, sid)
		}
		// 复制 listener 列表，在锁外调用（避免死锁）
		listeners := make([]func(gid tester.Tgid, sid int, isLeader bool), len(cm.leaderChangeListeners))
		copy(listeners, cm.leaderChangeListeners)
		cm.mu.Unlock()

		for _, fn := range listeners {
			fn(tester.Tgid(gid), sid, isLeader)
		}
	})

	cm.infra.SetReliable(reliable)

	// 创建 shard controller
	cm.clnt = cm.infra.MakeClient()
	cm.ctl = shardctrler.MakeShardCtrler(cm.clnt)

	// 用 kvraft clerk 替换 ShardCtrler 的 configStore（连接 GRP0 的 nsrv 节点 Raft 组）
	grp0Srvs := cm.infra.Group(tester.GRP0).SrvNames()
	kvraftCk := kvraft.MakeClerk(cm.clnt, grp0Srvs)
	cm.ctl.SetConfigStore(kvraftCk)

	// 构建初始组映射
	initGroups := cm.dcfg.Groups
	if len(initGroups) == 0 {
		initGroups = []config.GroupConfig{{Gid: int(shardcfg.Gid1), Servers: nsrv}}
	}

	log.Printf("[Cluster] Init: 正在启动组 ...")
	// 1. 并发启动所有配置的 shard group 进程
	args := cm.serverArgs()
	servers := make(map[tester.Tgid][]string)
	var wg sync.WaitGroup
	for _, g := range initGroups {
		wg.Add(1)
		go func(g config.GroupConfig) {
			defer wg.Done()
			gid := tester.Tgid(g.Gid)
			srvN := g.Servers
			if srvN <= 0 {
				srvN = nsrv
			}
			cm.infra.MakeGroupStart("shardgrp", args, gid, srvN)
			cm.mu.Lock()
			servers[gid] = cm.infra.Group(gid).SrvNames()
			cm.groups[gid] = true
			cm.mu.Unlock()
		}(g)
	}
	wg.Wait()

	// 2. 初始配置：所有 shard → Gid1（与真实数据位置一致）
	//    先 Join 添加 Gid1 到 Groups，再 Rebalance 将所有 shard 分配给 Gid1。
	scfg := shardcfg.MakeShardConfig()
	scfg.Join(map[tester.Tgid][]string{shardcfg.Gid1: servers[shardcfg.Gid1]})
	scfg.Rebalance() // 将全部 12 个 shard 分配给唯一的组 Gid1
	cm.ctl.InitConfig(scfg)

	// GRP0 加入管理
	cm.groups[tester.GRP0] = true

	// 3. 如果有多个组，执行 ChangeConfigTo 实际迁移 shard
	//    必须在锁外执行（涉及 RPC）

	serversToJoin := make(map[tester.Tgid][]string)
	for _, g := range initGroups {
		gid := tester.Tgid(g.Gid)
		if gid == shardcfg.Gid1 {
			continue
		}
		serversToJoin[gid] = servers[gid]
	}
	cm.initJoinGroup(serversToJoin)

	// 创建 shardkv clerk（在所有迁移完成后，才能正确路由到各组的 shard）
	cm.ck = shardkv.MakeClerk(cm.clnt, cm.ctl)

	// 构建日志描述
	gidDescs := make([]string, 0, len(initGroups))
	for _, g := range initGroups {
		gidDescs = append(gidDescs, fmt.Sprintf("Gid%d=%v", g.Gid, servers[tester.Tgid(g.Gid)]))
	}
	log.Printf("[Cluster] Init: 初始化完成")

	// 启动后台 config 轮询（替代每次调用开 goroutine → 消除 goroutine 泄漏）
	cm.startConfigPoller()

	// 初始化  Cfg / NextCfg Cache
	if err := cm.initConfigCache(); err != nil {
		return fmt.Errorf("初始化集群失败: %w", err)
	}

	return nil

}

// Group 返回指定 GID 的 ServerGrp
func (cm *ClusterManager) Group(gid tester.Tgid) *tester.ServerGrp {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.infra.Group(gid)
}

// updateCachedCfg 更新缓存的Config和更新时间, 只有配置号不减小才生效
func (cm *ClusterManager) updateCachedCfg(newcfg *shardcfg.ShardConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.cachedConfig.Num <= newcfg.Num {
		cm.cachedConfig = newcfg
		cm.lastConfigOk = time.Now()
	}
}

// updateCachedCfg 更新缓存的NextConfig, 只有配置号不减小才生效
func (cm *ClusterManager) updateCachedNextCfg(newcfg *shardcfg.ShardConfig) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.cachedNextConfig.Num <= newcfg.Num {
		cm.cachedNextConfig = newcfg
	}
}

const pollInterval = 2 * time.Second

// startConfigPoller 开启轮询 config 和 nextConfig
func (cm *ClusterManager) startConfigPoller() {
	// 轮询 Query() 写入 cachedConfig；收到 done 后立即退出
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-cm.done:
				return
			case <-ticker.C:
				cfg := cm.ctl.Query()
				if cfg != nil {
					cm.updateCachedCfg(cfg)
				}
			}
		}
	}()
	// 轮询 QueryNext() 写入 cachedNextConfig；收到 done 后立即退出
	go func() {
		ticker := time.NewTicker(pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-cm.done:
				return
			case <-ticker.C:
				cfg := cm.ctl.QueryNext()
				if cfg != nil {
					cm.updateCachedNextCfg(cfg)
				}
			}
		}
	}()
}

// initConfigCache 在初始化阶段同步预填充 current 和 next 配置缓存。
// 这样 NewClusterManager() 返回后，前端立即轮询 Status() 也不会读到 nil，
// 避免 startConfigPoller() 的 ticker 首次触发（2 秒后）前的空窗期。
func (cm *ClusterManager) initConfigCache() error {
	cfg := cm.ctl.Query()
	if cfg == nil {
		return fmt.Errorf("config store 未返回 currentConfig")
	}
	nextCfg := cm.ctl.QueryNext()
	if nextCfg == nil {
		return fmt.Errorf("config store 未返回 nextConfig")
	}
	cm.mu.Lock()
	cm.cachedConfig = cfg
	cm.lastConfigOk = time.Now()
	cm.cachedNextConfig = nextCfg
	cm.mu.Unlock()
	return nil
}

// hasPendingMigration 返回是否在分片迁移中（需要在读锁内使用）
func (cm *ClusterManager) hasPendingMigration() bool {
	// 仅依赖缓存（非阻塞）
	if cm.cachedConfig == nil || cm.cachedNextConfig == nil {
		return false
	}
	return cm.cachedConfig.Num != cm.cachedNextConfig.Num
}

// Status 返回完整集群状态。
// 使用非阻塞查询：configStore（kvraft）无响应时使用缓存，但节点存活信息（GetConnected）始终是实时准确的。
func (cm *ClusterManager) Status() *ClusterState {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cfg := cm.cachedConfig
	nextCfg := cm.cachedNextConfig
	hasPending := cm.hasPendingMigration()

	// 判断 config 是否来自缓存： 为空或 lastConfigOk 超过阈值（太久未更新）
	configCached := false
	if cfg == nil {
		// configStore 完全无响应且无缓存，退化为仅显示节点状态
		cfg = shardcfg.MakeShardConfig()
	} else if time.Since(cm.lastConfigOk) > staleConfigTimeout {
		configCached = true
	}

	state := &ClusterState{
		Config:              cfg,
		HasPendingMigration: hasPending,
		PendingConfigNum:    nextCfg.Num,
		ConfigCached:        configCached,
	}
	// 展示 cm.groups 中追踪的所有组（包括 configStore GRP0）
	for gid := range cm.groups {
		sg := cm.infra.Group(gid)
		if sg == nil {
			continue
		}
		gs := GroupState{
			GID:      gid,
			SrvNames: sg.SrvNames(),
		}
		// 计算组中的分片数
		for _, g := range state.Config.Shards {
			if g == gid {
				gs.NShards++
			}
		}
		// Server states — 实时从 GetConnected() 获取，不受 configStore 影响
		gs.Servers = make([]ServerState, sg.N())
		connected := sg.GetConnected()
		isoMap := cm.isolated[gid]
		// 获取该组最新的 leader 信息（事件驱动，由 leaderChangeCb 实时更新）
		leaders := cm.leaders[gid]
		for i := 0; i < sg.N(); i++ {
			isAlive := i < len(connected) && connected[i]
			isIsolated := isoMap != nil && isoMap[i]
			isLeader := leaders != nil && leaders[i] // 事件驱动的 leader 缓存
			gs.Servers[i] = ServerState{
				Index:      i,
				Name:       sg.SrvName(i),
				IsLeader:   isLeader,
				IsAlive:    isAlive,
				IsIsolated: isIsolated,
			}
		}
		state.Groups = append(state.Groups, gs)
	}
	return state
}

// Stop 关闭所有服务器、停止混沌猴子，并通知后台 goroutine 退出
func (cm *ClusterManager) Stop() {
	// 先通知后台 goroutine 退出，不持锁，避免 poller 因等待 cm.mu 而延迟感知 done
	cm.stopOnce.Do(func() {
		close(cm.done)
	})

	cm.mu.Lock()
	defer cm.mu.Unlock()

	for _, m := range cm.chaosMonkeys {
		close(m.stop)
	}
	cm.chaosMonkeys = nil

	log.Printf("[Cluster] 正在关闭所有组...")
	for gid := range cm.groups {
		sg := cm.infra.Group(gid)
		if sg != nil {
			sg.Shutdown()
		}
	}
	log.Printf("[Cluster] 所有组已关闭")

	// 清理本地未追踪但 tester 中仍存活的残留组
	for _, gid := range cm.infra.Groups.Gids() {
		if cm.groups[gid] {
			continue
		}
		log.Printf("[Cluster] Stop: 清理残留组 %d", gid)
		cm.infra.ExitGroup(gid)
	}
}

// NewClerk 创建一个新的独立 Clerk（用于 CAS 并发竞赛演示）
func (cm *ClusterManager) NewClerk() kvtest.IKVClerk {
	return shardkv.MakeClerk(cm.clnt, cm.ctl)
}

// GetClerkPool 返回全局 Clerk 池 - 懒初始化
// 批量写入等高并发场景通过 Borrow/Return 复用 Clerk，
// 避免每次创建临时对象并减少 configStore 的 Query() 并发压力
func (cm *ClusterManager) GetClerkPool() *ClerkPool {
	cm.poolOnce.Do(func() {
		cm.clerkPool = NewClerkPool(cm, clerkPoolSize)
	})
	return cm.clerkPool
}

// Put 写入键值（带乐观锁版本号）
func (cm *ClusterManager) Put(key, value string, version rpcapi.Tversion) rpcapi.Err {
	return cm.ck.Put(key, value, version)
}

// Get 读取键
func (cm *ClusterManager) Get(key string) (string, rpcapi.Tversion, rpcapi.Err) {
	return cm.ck.Get(key)
}

// KillServer 停掉指定组中的某个节点
func (cm *ClusterManager) KillServer(gid tester.Tgid, srv int) {
	sg := cm.infra.Group(gid)
	if sg == nil {
		log.Printf("[Cluster] KillServer: 组 %d 不存在", gid)
		return
	}
	if srv < 0 || srv >= sg.N() {
		log.Printf("[Cluster] KillServer: 节点索引 %d 超出范围 [0, %d)", srv, sg.N())
		return
	}
	sg.ShutdownServer(srv)
	log.Printf("[Cluster] 已停止节点 %s", sg.SrvName(srv))
}

// IsolateNode 通过 Partition 隔离指定节点（进程保持运行，仅隔离网络）
// 注意：每次调用会重置组的整个分区状态——之前被隔离的节点会恢复到 rest 组。
// 只操作当前还活着的节点，跳过已下线节点，避免意外重连已下线的节点。
func (cm *ClusterManager) IsolateNode(gid tester.Tgid, srv int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sg := cm.infra.Group(gid)
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

	sg := cm.infra.Group(gid)
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

// StartServer 启动指定组中的某个节点，带健康检查
func (cm *ClusterManager) StartServer(gid tester.Tgid, srv int) error {
	sg := cm.infra.Group(gid)
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

// KillGroup 停掉整个组
func (cm *ClusterManager) KillGroup(gid tester.Tgid) {
	sg := cm.infra.Group(gid)
	if sg == nil {
		return
	}
	sg.Shutdown()
	log.Printf("[Cluster] 已停掉组 %d", gid)
}

// StartGroup 启动整个组
func (cm *ClusterManager) StartGroup(gid tester.Tgid) error {
	sg := cm.infra.Group(gid)
	if sg == nil {
		return fmt.Errorf("组 %d 不存在", gid)
	}
	sg.StartServers()
	log.Printf("[Cluster] 已启动组 %d", gid)
	return nil
}

// initJoinGroup 在进程已启动的情况下，将指定组通过 ChangeConfigTo 加入集群（含实际 shard 迁移）
func (cm *ClusterManager) initJoinGroup(servers map[tester.Tgid][]string) {
	if len(servers) == 0 {
		return
	}
	cfg := cm.ctl.Query()
	newcfg := cfg.Copy()
	newcfg.JoinBalance(servers)

	// 构建组 ID 列表用于日志
	gids := make([]string, 0, len(servers))
	for gid := range servers {
		gids = append(gids, fmt.Sprintf("%d", gid))
	}
	log.Printf("[Cluster] initJoinGroup: 正在将组 %s 加入集群（迁移分片）...", strings.Join(gids, ","))
	cm.ctl.ChangeConfigTo(newcfg)

	// 构建详细的组服务器信息用于日志（多行格式）
	log.Printf("[Cluster] initJoinGroup: 已加入 %d 个组:", len(servers))
	for gid, srvs := range servers {
		log.Printf("          G%d: {%s}", gid, strings.Join(srvs, ", "))
	}
}

// JoinGroup 加入新组
func (cm *ClusterManager) JoinGroup(gid tester.Tgid) (bool, string) {
	// 1. 锁内检查，避免重复创建进程
	cm.mu.Lock()
	if cm.groups[gid] {
		cm.mu.Unlock()
		log.Printf("[Cluster] JoinGroup: 组 %d 加入失败（已存在）", gid)
		return false, fmt.Sprintf("组 %d 已存在", gid)
	}
	cm.mu.Unlock()

	// 2. 锁外启动子进程
	nsrv := cm.dcfg.Nsrv
	if nsrv <= 0 {
		nsrv = 3
	}
	args := cm.serverArgs()
	cm.infra.MakeGroupStart("shardgrp", args, gid, nsrv)

	// 3. 加锁注册元数据（二次检查处理并发）
	cm.mu.Lock()
	if cm.groups[gid] {
		cm.mu.Unlock()
		// 并发场景下另一调用者已抢先注册，回滚本调用创建的进程
		sg := cm.infra.Group(gid)
		if sg != nil {
			sg.Shutdown()
		}
		cm.infra.ExitGroup(gid)
		log.Printf("[Cluster] JoinGroup: 组 %d 加入失败（并发重复）", gid)
		return false, fmt.Sprintf("组 %d 已存在", gid)
	}
	srvs := cm.infra.Group(gid).SrvNames() // 记录名字后面日志输出用
	cm.groups[gid] = true
	cm.mu.Unlock()

	// 4. 循环执行：恢复未完成迁移（如果有）+ 分片配置变更（分片迁移）直到成功
	for {
		cm.ctl.InitController()
		newcfg := cm.ctl.Query()
		if ok := newcfg.JoinBalance(map[tester.Tgid][]string{gid: srvs}); !ok {
			log.Printf("[Cluster] JoinGroup: 组 %d 加入失败（已存在）", gid)
			return false, fmt.Sprintf("组 %d 已存在", gid)
		}
		if cm.ctl.ChangeConfigTo(newcfg) {
			// 5. 迁移完成后，更新缓存
			cm.updateCachedCfg(newcfg)
			cm.updateCachedNextCfg(newcfg)
			log.Printf("[Cluster] 组 %d 已加入集群 (config #%d)", gid, newcfg.Num)
			return true, ""
		}
	}
}

// LeaveGroup 移除组
func (cm *ClusterManager) LeaveGroup(gid tester.Tgid) (bool, string) {
	// 1. 检查进程是否存在
	cm.mu.Lock()
	sg := cm.infra.Group(gid)
	if sg == nil {
		delete(cm.groups, gid)
		cm.mu.Unlock()
		log.Printf("[Cluster] LeaveGroup: 组 %d 已离开", gid)
		return true, ""
	}
	cm.mu.Unlock()

	// 2. 检查是否正在 JoinGroup（本地有记录但 config store 配置中没有）
	//    此时 JoinGroup 的 ChangeConfigTo 可能正在进行，不应中断
	if cm.groups[gid] {
		cfg := cm.ctl.Query()
		if _, ok := cfg.Groups[gid]; !ok {
			return false, fmt.Sprintf("组 %d 正在加入集群中，请勿移除", gid)
		}
	}

	// 3. 循环执行：恢复未完成迁移（如果有）+ 分片配置变更（分片迁移）直到成功
	for {
		cm.ctl.InitController()
		newcfg := cm.ctl.Query()

		if _, ok := newcfg.Groups[gid]; !ok {
			// 别的请求已经移除了这个组，幂等返回
			cm.mu.Lock()
			delete(cm.groups, gid)
			cm.mu.Unlock()
			log.Printf("[Cluster] LeaveGroup: 组 %d 已不存在于配置中", gid)
			return true, ""
		}
		if len(newcfg.Groups) <= 1 {
			return false, fmt.Sprintf("不能移除最后一个组 %d，集群中至少需要保留一个组", gid)
		}

		if ok := newcfg.LeaveBalance([]tester.Tgid{gid}); !ok {
			return false, fmt.Sprintf("组 %d 移除失败（LeaveBalance 拒绝）", gid)
		}

		if cm.ctl.ChangeConfigTo(newcfg) {
			// 更新缓存
			cm.updateCachedCfg(newcfg)
			cm.updateCachedNextCfg(newcfg)
			log.Printf("[Cluster] 组 %d 已离开集群", gid)
			break
		}
	}

	// 4. 清理本地记录和进程
	cm.mu.Lock()
	delete(cm.groups, gid)
	cm.infra.ExitGroup(gid)
	cm.mu.Unlock()
	// 5. 停止混沌猴子
	cm.StopChaos(gid)
	return true, ""
}

// SetReliable 设置网络是否可靠（false 时随机延迟 + 可配置丢包率）
func (cm *ClusterManager) SetReliable(yes bool) {
	cm.infra.SetReliable(yes)
	log.Printf("[Cluster] 网络可靠性: %v", yes)
}

// IsReliable 返回当前网络是否可靠
func (cm *ClusterManager) IsReliable() bool {
	return cm.infra.IsReliable()
}

// SetDropRate 设置丢包率 (0-1000, 0=使用默认常量)
func (cm *ClusterManager) SetDropRate(rate int) {
	cm.infra.SetDropRate(rate)
	log.Printf("[Cluster] 丢包率: %d/1000 (%d%%)", rate, rate/10)
}

// GetDropRate 返回当前丢包率
func (cm *ClusterManager) GetDropRate() int {
	return cm.infra.GetDropRate()
}

// SetShortDelayMs 设置不可靠网络下的最小延迟 (ms)
func (cm *ClusterManager) SetShortDelayMs(ms int) {
	cm.infra.SetShortDelayMs(ms)
	log.Printf("[Cluster] 最小延迟: %dms", ms)
}

// GetShortDelayMs 返回当前最小延迟
func (cm *ClusterManager) GetShortDelayMs() int {
	return cm.infra.GetShortDelayMs()
}

// SetLongDelayMs 设置 disconnected 状态下的最大延迟 (ms)
func (cm *ClusterManager) SetLongDelayMs(ms int) {
	cm.infra.SetLongDelayMs(ms)
	log.Printf("[Cluster] 最大延迟: %dms", ms)
}

// GetLongDelayMs 返回当前最大延迟
func (cm *ClusterManager) GetLongDelayMs() int {
	return cm.infra.GetLongDelayMs()
}

// SetLongDelays 设置断线时是否等待长超时（true 时使用 longDelayMs，false 时 0~100ms）
func (cm *ClusterManager) SetLongDelays(yes bool) {
	cm.infra.SetLongDelays(yes)
	log.Printf("[Cluster] 长延迟模式: %v", yes)
}

// IsLongDelays 返回当前是否使用长超时模式
func (cm *ClusterManager) IsLongDelays() bool {
	return cm.infra.IsLongDelays()
}

// SetLongReordering 设置是否长延迟重排序（67% 回复延迟 200ms~2.2s）
func (cm *ClusterManager) SetLongReordering(yes bool) {
	cm.infra.SetLongReordering(yes)
	log.Printf("[Cluster] 长延迟重排序: %v", yes)
}

// IsLongReordering 返回当前是否开启回复乱序
func (cm *ClusterManager) IsLongReordering() bool {
	return cm.infra.IsLongReordering()
}

// NewGid 返回一个新的 GID
func (cm *ClusterManager) NewGid() tester.Tgid {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	// 跳过已被占用的 GID（防止与配置中初始组ID冲突）
	for cm.groups[cm.nextGid] {
		cm.nextGid++
	}
	gid := cm.nextGid
	cm.nextGid++
	return gid
}

// StartChaos 为指定组启动混沌猴子
func (cm *ClusterManager) StartChaos(gid tester.Tgid) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	sg := cm.infra.Group(gid)
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
		sg := cm.infra.Group(gid)
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
