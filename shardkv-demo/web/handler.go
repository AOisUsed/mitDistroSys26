package web

import (
	"encoding/json"
	"fmt"
	"kvstore/debug"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"kvstore/kvsrv/rpcapi"
	"kvstore/rpc"
	"kvstore/shardkv/shardcfg"
	"kvstore/tester"
	"shardkv-demo/cluster"
)

type Handler struct {
	cm        *cluster.ClusterManager
	staticDir string // 静态文件目录（shardkv-demo 目录路径）
}

func NewHandler(cm *cluster.ClusterManager) *Handler {
	return &Handler{cm: cm}
}

// SetStaticDir 设置静态文件目录路径（用于定位 index.html）
// 必须在 RegisterRoutes 之前调用
func (h *Handler) SetStaticDir(dir string) {
	h.staticDir = dir
}

// --- 根 / 静态文件 ---

func (h *Handler) HandleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	// 使用绝对路径定位 index.html（因为工作目录可能在 src/ 下）
	indexPath := h.staticDir + "/web/static/index.html"
	http.ServeFile(w, r, indexPath)
}

// --- API: 集群状态 (带超时保护) ---

func (h *Handler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state := h.cm.Status()
	writeJSON(w, state)
}

func (h *Handler) HandleStatusTree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state := h.cm.Status()

	// Build tree structure
	type ShardNode struct {
		Shard int         `json:"shard"`
		GID   tester.Tgid `json:"gid"`
	}
	type GroupNode struct {
		GID     tester.Tgid           `json:"gid"`
		Servers []cluster.ServerState `json:"servers"`
		NShards int                   `json:"nShards"`
	}
	tree := struct {
		ConfigNum           shardcfg.Tnum `json:"configNum"`
		HasPendingMigration bool          `json:"hasPendingMigration"`
		PendingConfigNum    shardcfg.Tnum `json:"pendingConfigNum"`
		ConfigCached        bool          `json:"configCached"`
		Groups              []GroupNode   `json:"groups"`
		Shards              []ShardNode   `json:"shards"`
	}{
		ConfigNum:           state.Config.Num,
		HasPendingMigration: state.HasPendingMigration,
		PendingConfigNum:    state.PendingConfigNum,
		ConfigCached:        state.ConfigCached,
	}

	for _, gs := range state.Groups {
		tree.Groups = append(tree.Groups, GroupNode{
			GID:     gs.GID,
			Servers: gs.Servers,
			NShards: gs.NShards,
		})
	}
	for i, gid := range state.Config.Shards {
		tree.Shards = append(tree.Shards, ShardNode{Shard: i, GID: gid})
	}
	writeJSON(w, tree)
}

// ErrTimeoutStr 定义超时字符串常量，与 rpcapi.Err 同类型
const ErrTimeoutStr rpcapi.Err = "ErrTimeout"

// --- API: KV 操作 (带超时保护) ---

func (h *Handler) HandlePut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	shard := shardcfg.Key2Shard(req.Key)

	// 超时保护的 Get：在 goroutine 中执行，12s 超时
	type getResult struct {
		val     string
		version rpcapi.Tversion
		err     rpcapi.Err
	}
	getCh := make(chan getResult, 1)
	go func() {
		v, ver, e := h.cm.Get(req.Key)
		getCh <- getResult{v, ver, e}
	}()

	var version rpcapi.Tversion
	select {
	case gres := <-getCh:
		if gres.err != rpcapi.OK && gres.err != rpcapi.ErrNoKey {
			log.Printf("[Put] key=%q S%d Get 失败: %s", req.Key, shard, gres.err)
			writeJSON(w, map[string]any{
				"success": false, "key": req.Key, "value": req.Value, "shard": int(shard),
				"err": string(gres.err),
			})
			return
		}
		version = gres.version
	case <-time.After(12 * time.Second):
		log.Printf("[Put] key=%q S%d Get 超时: 集群无响应 (Raft 可能无 leader)", req.Key, shard)

		writeJSON(w, map[string]any{
			"success": false, "key": req.Key, "value": req.Value, "shard": int(shard),
			"err": "ErrTimeout", "message": "集群无响应（可能是 Raft 组无 leader）",
		})
		return
	}

	// 超时保护的 Put
	putCh := make(chan rpcapi.Err, 1)
	go func() {
		putCh <- h.cm.Put(req.Key, req.Value, version)
	}()

	var putErr rpcapi.Err
	select {
	case putErr = <-putCh:
	case <-time.After(12 * time.Second):
		log.Printf("[Put] key=%q S%d 超时: PUT 无响应", req.Key, shard)
		writeJSON(w, map[string]any{
			"success": false, "key": req.Key, "value": req.Value, "shard": int(shard),
			"err": "ErrTimeout", "message": "PUT 超时（集群无响应）",
		})
		return
	}

	resp := struct {
		Success bool   `json:"success"`
		Err     string `json:"err,omitempty"`
		Key     string `json:"key"`
		Value   string `json:"value"`
		Shard   int    `json:"shard"`
		ReqVer  int    `json:"reqVer"`
	}{
		Key:    req.Key,
		Value:  req.Value,
		Shard:  int(shard),
		ReqVer: int(version),
	}
	if putErr == rpcapi.OK {
		resp.Success = true
		log.Printf("[Put] key=%q value=%q S%d reqVer=%d OK", req.Key, req.Value, shard, version)
	} else {
		resp.Err = string(putErr)
		log.Printf("[Put] key=%q value=%q S%d reqVer=%d %s", req.Key, req.Value, shard, version, putErr)
	}
	writeJSON(w, resp)
}

func (h *Handler) HandleGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/api/kv/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	// 超时保护的 Get
	type getResult struct {
		val     string
		version rpcapi.Tversion
		err     rpcapi.Err
	}
	getCh := make(chan getResult, 1)
	go func() {
		v, ver, e := h.cm.Get(key)
		getCh <- getResult{v, ver, e}
	}()

	var value string
	var version rpcapi.Tversion
	var getErr rpcapi.Err
	select {
	case gres := <-getCh:
		value, version, getErr = gres.val, gres.version, gres.err
	case <-time.After(12 * time.Second):
		log.Printf("[Get] key=%q 超时: 集群无响应 (Raft 可能无 leader)", key)

		writeJSON(w, map[string]any{
			"success": false, "key": key,
			"err": "ErrTimeout", "message": "集群无响应（可能是 Raft 组无 leader）",
		})
		return
	}

	resp := struct {
		Success bool            `json:"success"`
		Key     string          `json:"key"`
		Value   string          `json:"value,omitempty"`
		Version rpcapi.Tversion `json:"version,omitempty"`
		Err     string          `json:"err,omitempty"`
	}{
		Key: key,
	}
	if getErr == rpcapi.OK {
		resp.Success = true
		resp.Value = value
		resp.Version = version
		log.Printf("[Get] key=%q value=%q version=%d OK", key, value, version)
	} else if getErr == rpcapi.ErrNoKey {
		resp.Err = "ErrNoKey"
		log.Printf("[Get] key=%q ErrNoKey", key)
	} else {
		resp.Err = string(getErr)
		log.Printf("[Get] key=%q err=%s", key, getErr)
	}
	writeJSON(w, resp)
}

// --- API: Node 操作 ---

type nodeOpRequest struct {
	GID int `json:"gid"`
	Srv int `json:"srv"`
}

func (h *Handler) HandleKillNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req nodeOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	srvName := fmt.Sprintf("server-%d-%d", req.GID, req.Srv)
	h.cm.KillServer(tester.Tgid(req.GID), req.Srv)

	// 检查该组还有多少节点存活
	sg := h.cm.Group(tester.Tgid(req.GID))
	aliveCount := 0
	totalNodes := 0
	if sg != nil {
		connected := sg.GetConnected()
		totalNodes = sg.N()
		for _, c := range connected {
			if c {
				aliveCount++
			}
		}
	}
	log.Printf("[KillNode] Group %d Server %d (%s) — 该组存活: %d/%d", req.GID, req.Srv, srvName, aliveCount, totalNodes)
	writeJSON(w, map[string]any{
		"success":    true,
		"action":     "kill",
		"gid":        req.GID,
		"srv":        req.Srv,
		"srvName":    srvName,
		"aliveGroup": aliveCount,
		"totalGroup": totalNodes,
	})
}

func (h *Handler) HandleStartNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req nodeOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	err := h.cm.StartServer(tester.Tgid(req.GID), req.Srv)
	resp := map[string]any{"success": err == nil, "action": "start", "gid": req.GID, "srv": req.Srv}
	if err != nil {
		resp["error"] = err.Error()
	}
	log.Printf("[StartNode] group=%d server=%d err=%v", req.GID, req.Srv, err)
	writeJSON(w, resp)
}

// HandleIsolateNode 隔离指定节点的网络（进程保持运行，仅隔离网络连接）
func (h *Handler) HandleIsolateNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req nodeOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.IsolateNode(tester.Tgid(req.GID), req.Srv)
	log.Printf("[IsolateNode] group=%d server=%d (network partition isolated)", req.GID, req.Srv)
	writeJSON(w, map[string]any{"success": true, "action": "isolate", "gid": req.GID, "srv": req.Srv})
}

// HandleRecoverNode 恢复单个节点的网络连接（只影响该节点，不影响已下线节点）
func (h *Handler) HandleRecoverNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		GID int `json:"gid"`
		Srv int `json:"srv"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.RecoverNode(tester.Tgid(req.GID), req.Srv)
	log.Printf("[RecoverNode] group=%d server=%d 网络已恢复", req.GID, req.Srv)
	writeJSON(w, map[string]any{"success": true, "action": "recover-node", "gid": req.GID, "srv": req.Srv})
}

// HandleRecoverGroup 恢复指定组的全部网络连接（取消所有节点分区，但只操作存活节点）
func (h *Handler) HandleRecoverGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		GID int `json:"gid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.RecoverGroup(tester.Tgid(req.GID))
	log.Printf("[RecoverGroup] group=%d 网络已恢复（仅操作存活节点）", req.GID)
	writeJSON(w, map[string]any{"success": true, "action": "recover-group", "gid": req.GID})
}

// HandleRecoverAllGroups 恢复所有组的网络连接
func (h *Handler) HandleRecoverAllGroups(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.cm.RecoverAllGroups()
	log.Printf("[RecoverAllGroups] 所有组网络已恢复")
	writeJSON(w, map[string]any{"success": true, "action": "recover-all"})
}

// --- API: Group 操作 ---

type groupOpRequest struct {
	GID int `json:"gid"`
}

func (h *Handler) HandleKillGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.KillGroup(tester.Tgid(req.GID))
	log.Printf("[KillGroup] group=%d", req.GID)
	writeJSON(w, map[string]any{"success": true, "action": "kill-group", "gid": req.GID})
}

func (h *Handler) HandleStartGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	err := h.cm.StartGroup(tester.Tgid(req.GID))
	resp := map[string]any{"success": err == nil, "action": "start-group", "gid": req.GID}
	if err != nil {
		resp["error"] = err.Error()
	}
	log.Printf("[StartGroup] group=%d err=%v", req.GID, err)
	writeJSON(w, resp)
}

// --- API: Join/Leave ---

func (h *Handler) HandleJoinGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	gid := h.cm.NewGid()
	ok, msg := h.cm.JoinGroup(gid)
	resp := map[string]any{"success": ok, "action": "join", "gid": int(gid)}
	if ok {
		if msg != "" {
			resp["message"] = msg
		}
		log.Printf("[JoinGroup] new group %d joined%s", gid, func() string {
			if msg != "" {
				return fmt.Sprintf(" (%s)", msg)
			}
			return ""
		}())
	} else {
		resp["error"] = msg
		resp["message"] = msg
		log.Printf("[JoinGroup] failed to join new group: %s", msg)
	}
	writeJSON(w, resp)
}

func (h *Handler) HandleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	ok, errMsg := h.cm.LeaveGroup(tester.Tgid(req.GID))
	resp := map[string]any{"success": ok, "action": "leave", "gid": req.GID}
	if ok {
		log.Printf("[LeaveGroup] group %d left", req.GID)
	} else {
		resp["error"] = errMsg
		resp["message"] = errMsg // 便于前端直接展示
		log.Printf("[LeaveGroup] failed to leave group %d: %s", req.GID, errMsg)
	}
	writeJSON(w, resp)
}

// --- API: Config ---

func (h *Handler) HandleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cfg := h.cm.QueryConfig()
	writeJSON(w, cfg)
}

// --- API: Network ---

func (h *Handler) HandleReliable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Method == http.MethodGet {
		// GET: 查询当前网络状态
		writeJSON(w, map[string]any{
			"reliable":       h.cm.IsReliable(),
			"longReordering": false, // labrpc 没暴露 IsLongReordering
		})
		return
	}
	// POST: 设置网络可靠性
	var req struct {
		Reliable *bool `json:"reliable"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Reliable != nil {
		h.cm.SetReliable(*req.Reliable)
		if !*req.Reliable {
			h.cm.SetLongReordering(false) // 关掉长延迟重排序
		}
	}
	writeJSON(w, map[string]any{"success": true, "action": "reliable", "reliable": h.cm.IsReliable()})
}

func (h *Handler) HandleNetParams(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, map[string]any{
		"dropRate":     rpc.GetDropRate(),
		"shortDelayMs": rpc.GetShortDelay(),
		"longDelayMs":  rpc.GetLongDelay(),
	})
}

func (h *Handler) HandleConnectAll(w http.ResponseWriter, r *http.Request) {

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.cm.ConnectAll()
	log.Printf("[ConnectAll] all connections restored")
	writeJSON(w, map[string]any{"success": true, "action": "connect-all"})
}

type partitionRequest struct {
	GID int   `json:"gid"`
	P1  []int `json:"p1"`
	P2  []int `json:"p2"`
}

func (h *Handler) HandlePartition(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req partitionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.Partition(tester.Tgid(req.GID), req.P1, req.P2)
	log.Printf("[Partition] group=%d p1=%v p2=%v", req.GID, req.P1, req.P2)
	writeJSON(w, map[string]any{"success": true, "action": "partition", "gid": req.GID, "p1": req.P1, "p2": req.P2})
}

// --- API: Chaos Monkey ---

func (h *Handler) HandleChaosStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		GID int `json:"gid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	err := h.cm.StartChaos(tester.Tgid(req.GID))
	if err != nil {
		writeJSON(w, map[string]any{"success": false, "error": err.Error()})
		return
	}
	log.Printf("[Chaos] GID %d 混沌已启动", req.GID)
	writeJSON(w, map[string]any{"success": true, "action": "chaos-start", "gid": req.GID})
}

func (h *Handler) HandleChaosStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		GID int `json:"gid"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.StopChaos(tester.Tgid(req.GID))
	log.Printf("[Chaos] GID %d 混沌已停止", req.GID)
	writeJSON(w, map[string]any{"success": true, "action": "chaos-stop", "gid": req.GID})
}

func (h *Handler) HandleChaosStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	states := h.cm.ChaosStatus()
	writeJSON(w, map[string]any{"success": true, "states": states})
}

// HandleInitController 手动触发 InitController 恢复 pending 迁移
func (h *Handler) HandleInitController(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	log.Printf("[InitController] 手动触发 InitController 恢复...")
	h.cm.InitController()
	writeJSON(w, map[string]any{"success": true, "message": "InitController 已触发"})
}

// --- API: CAS Put（单次 CAS 写入，前端并发调用，每条实时可见） ---
// 前端先 Get 当前版本号，然后并发发起 N 个独立的 POST 到 /api/put-cas，
// 每个请求独立经历网络延迟/丢包，前端收到响应后立即输出日志。

func (h *Handler) HandlePutCas(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Key     string          `json:"key"`
		Value   string          `json:"value"`
		Version rpcapi.Tversion `json:"version"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	shard := shardcfg.Key2Shard(req.Key)

	// 超时保护的 Put：在 goroutine 中执行，12s 超时

	putCh := make(chan rpcapi.Err, 1)
	go func() {
		putCh <- h.cm.Put(req.Key, req.Value, req.Version)
	}()

	var putErr rpcapi.Err
	select {
	case putErr = <-putCh:
	case <-time.After(12 * time.Second):
		log.Printf("[Put] key=%q S%d CAS 超时: PUT 无响应 (Raft 可能无 leader)", req.Key, shard)

		writeJSON(w, map[string]any{
			"success": false, "key": req.Key, "value": req.Value, "shard": int(shard), "reqVer": int(req.Version),
			"err": "ErrTimeout", "message": "PUT 超时（可能是 Raft 组无 leader）",
		})
		return
	}

	if putErr == rpcapi.OK {
		log.Printf("[Put] key=%q value=%q S%d reqVer=%d OK", req.Key, req.Value, shard, req.Version)
		writeJSON(w, map[string]any{"success": true, "key": req.Key, "value": req.Value, "shard": int(shard), "reqVer": int(req.Version)})
	} else {
		log.Printf("[Put] key=%q value=%q S%d reqVer=%d %s", req.Key, req.Value, shard, req.Version, putErr)
		writeJSON(w, map[string]any{"success": false, "key": req.Key, "value": req.Value, "shard": int(shard), "reqVer": int(req.Version), "err": string(putErr)})
	}
}

// HandleCasGetVersion 获取当前 key 的版本号，用于 CAS 竞赛
func (h *Handler) HandleCasGetVersion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key query param required", http.StatusBadRequest)
		return
	}
	_, curVer, getErr := h.cm.Get(key)
	if getErr == rpcapi.OK || getErr == rpcapi.ErrNoKey {
		writeJSON(w, map[string]any{"success": true, "key": key, "version": int(curVer)})
	} else {
		writeJSON(w, map[string]any{"success": false, "err": string(getErr)})
	}
}

// --- API: 观测日志开关 ---

func (h *Handler) HandleObserve(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		// GET: 查询所有观测开关状态
		writeJSON(w, map[string]any{
			"election":  debug.GetObserveElection(),
			"migration": debug.GetObserveMigration(),
			"kvsubmit":  debug.GetObserveKVSubmit(),
			"fault":     debug.GetObserveFaultRecovery(),
		})
	case http.MethodPost:
		// POST: 设置某个观测开关
		var req struct {
			Scene string `json:"scene"` // election / migration / kvsubmit / fault
			On    bool   `json:"on"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		switch req.Scene {
		case "election":
			debug.SetObserveElection(req.On)
		case "migration":
			debug.SetObserveMigration(req.On)
		case "kvsubmit":
			debug.SetObserveKVSubmit(req.On)
		case "fault":
			debug.SetObserveFaultRecovery(req.On)
		default:
			http.Error(w, "unknown scene: "+req.Scene, http.StatusBadRequest)
			return
		}
		log.Printf("[Observe] scene=%q on=%v", req.Scene, req.On)
		writeJSON(w, map[string]any{"success": true, "scene": req.Scene, "on": req.On})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleObserveLogs 获取观测日志，支持 ?since=xxx 游标增量获取
// 返回 {lines, nextId}，nextId 是当前最新的 observeIdx，
// 前端下次轮询时传入 since=nextId 即可只取新日志
func (h *Handler) HandleObserveLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 解析 since 参数（默认 0）
	sinceStr := r.URL.Query().Get("since")
	var since int64
	if sinceStr != "" {
		if parsed, err := strconv.ParseInt(sinceStr, 10, 64); err == nil {
			since = parsed
		}
	}

	lines, nextId := debug.GetObserveLinesSince(since)
	if lines == nil {
		writeJSON(w, map[string]any{"lines": []debug.ObserveLine{}, "nextId": nextId})
		return
	}
	writeJSON(w, map[string]any{"lines": lines, "nextId": nextId})
}

// --- Helpers ---

func writeJSON(w http.ResponseWriter, v any) {

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

// RegisterRoutes 注册所有路由
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", h.HandleIndex)
	mux.HandleFunc("/api/status", h.HandleStatus)
	mux.HandleFunc("/api/status/tree", h.HandleStatusTree)
	mux.HandleFunc("/api/config", h.HandleConfig)

	// KV — single handler dispatches by method
	mux.HandleFunc("/api/kv/", h.HandleKV)

	// Node operations
	mux.HandleFunc("/api/node/kill", h.HandleKillNode)
	mux.HandleFunc("/api/node/start", h.HandleStartNode)
	mux.HandleFunc("/api/node/isolate", h.HandleIsolateNode)
	mux.HandleFunc("/api/node/recover-node", h.HandleRecoverNode)
	mux.HandleFunc("/api/node/recover-group", h.HandleRecoverGroup)

	// Group operations
	mux.HandleFunc("/api/group/kill", h.HandleKillGroup)
	mux.HandleFunc("/api/group/start", h.HandleStartGroup)
	mux.HandleFunc("/api/group/join", h.HandleJoinGroup)
	mux.HandleFunc("/api/group/leave", h.HandleLeaveGroup)

	// Network
	mux.HandleFunc("/api/network/connect-all", h.HandleConnectAll)
	mux.HandleFunc("/api/network/recover-all", h.HandleRecoverAllGroups)
	mux.HandleFunc("/api/network/partition", h.HandlePartition)
	mux.HandleFunc("/api/network/params", h.HandleNetParams)
	mux.HandleFunc("/api/network/reliable", h.HandleReliable)

	// CAS Put（独立单次，前端并发调用实时显示）
	mux.HandleFunc("/api/put-cas", h.HandlePutCas)
	mux.HandleFunc("/api/cas-get-version", h.HandleCasGetVersion)

	// Chaos Monkey
	mux.HandleFunc("/api/chaos/start", h.HandleChaosStart)
	mux.HandleFunc("/api/chaos/stop", h.HandleChaosStop)
	mux.HandleFunc("/api/chaos/status", h.HandleChaosStatus)

	// InitController (手动恢复 pending 迁移)
	mux.HandleFunc("/api/init-controller", h.HandleInitController)

	// 观测日志
	mux.HandleFunc("/api/observe", h.HandleObserve)
	mux.HandleFunc("/api/observe/logs", h.HandleObserveLogs)
}

// HandleKV dispatches KV requests by HTTP method
func (h *Handler) HandleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.HandleGet(w, r)
	case http.MethodPut:
		h.HandlePut(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
