package web

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"shardkv-demo/cluster"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
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

	type statusResult struct {
		state *cluster.ClusterState
		err   rpc.Err
	}
	ch := make(chan statusResult, 1)
	go func() {
		ch <- statusResult{state: h.cm.Status()}
	}()

	var state *cluster.ClusterState
	select {
	case r := <-ch:
		state = r.state
	case <-time.After(5 * time.Second):
		log.Printf("[Status] 超时: configStore 无响应（可能网络不可靠）")
		writeJSON(w, map[string]any{
			"err": "ErrTimeout", "message": "查询集群状态超时（网络不可靠或 configStore 无响应）",
		})
		return
	}
	writeJSON(w, state)
}

func (h *Handler) HandleStatusTree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	type treeResult struct {
		state *cluster.ClusterState
		err   rpc.Err
	}
	ch := make(chan treeResult, 1)
	go func() {
		ch <- treeResult{state: h.cm.Status()}
	}()

	var state *cluster.ClusterState
	select {
	case r := <-ch:
		state = r.state
	case <-time.After(5 * time.Second):
		log.Printf("[StatusTree] 超时: configStore 无响应")
		writeJSON(w, map[string]any{
			"err": "ErrTimeout", "message": "查询集群拓扑超时",
		})
		return
	}

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
		ConfigNum shardcfg.Tnum `json:"configNum"`
		Groups    []GroupNode   `json:"groups"`
		Shards    []ShardNode   `json:"shards"`
	}{
		ConfigNum: state.Config.Num,
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

// ErrTimeoutStr 定义超时字符串常量，与 rpc.Err 同类型
const ErrTimeoutStr rpc.Err = "ErrTimeout"

// timeoutGet 对返回 (string, Tversion, Err) 的函数做超时保护
func timeoutGet(fn func() (string, rpc.Tversion, rpc.Err), timeout time.Duration) (string, rpc.Tversion, rpc.Err) {
	type getRes struct {
		val     string
		version rpc.Tversion
		err     rpc.Err
	}
	ch := make(chan getRes, 1)
	go func() {
		v, ver, e := fn()
		ch <- getRes{v, ver, e}
	}()
	select {
	case r := <-ch:
		return r.val, r.version, r.err
	case <-time.After(timeout):
		return "", 0, ErrTimeoutStr
	}
}

// timeoutPut 对返回 rpc.Err 的函数做超时保护
func timeoutPut(fn func() rpc.Err, timeout time.Duration) rpc.Err {
	ch := make(chan rpc.Err, 1)
	go func() {
		ch <- fn()
	}()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		return ErrTimeoutStr
	}
}

// timeoutAny 对任意返回 rpc.Err 的只读查询做超时保护
func timeoutQuery[T any](fn func() (T, rpc.Err), timeout time.Duration) (T, rpc.Err) {
	type qRes struct {
		val T
		err rpc.Err
	}
	ch := make(chan qRes, 1)
	go func() {
		v, e := fn()
		ch <- qRes{v, e}
	}()
	select {
	case r := <-ch:
		return r.val, r.err
	case <-time.After(timeout):
		var zero T
		return zero, ErrTimeoutStr
	}
}

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

	// 超时保护的 Get：在 goroutine 中执行，5s 超时
	type getResult struct {
		val     string
		version rpc.Tversion
		err     rpc.Err
	}
	getCh := make(chan getResult, 1)
	go func() {
		v, ver, e := h.cm.Get(req.Key)
		getCh <- getResult{v, ver, e}
	}()

	var version rpc.Tversion
	select {
	case gres := <-getCh:
		if gres.err != rpc.OK && gres.err != rpc.ErrNoKey {
			log.Printf("[Put] key=%q S%d Get 失败: %s", req.Key, shard, gres.err)
			writeJSON(w, map[string]any{
				"success": false, "key": req.Key, "value": req.Value, "shard": int(shard),
				"err": string(gres.err),
			})
			return
		}
		version = gres.version
	case <-time.After(5 * time.Second):
		log.Printf("[Put] key=%q S%d Get 超时: 集群无响应 (Raft 可能无 leader)", req.Key, shard)
		writeJSON(w, map[string]any{
			"success": false, "key": req.Key, "value": req.Value, "shard": int(shard),
			"err": "ErrTimeout", "message": "集群无响应（可能是 Raft 组无 leader）",
		})
		return
	}

	// 超时保护的 Put
	putCh := make(chan rpc.Err, 1)
	go func() {
		putCh <- h.cm.Put(req.Key, req.Value, version)
	}()

	var putErr rpc.Err
	select {
	case putErr = <-putCh:
	case <-time.After(5 * time.Second):
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
		Version int    `json:"version,omitempty"`
	}{
		Key:     req.Key,
		Value:   req.Value,
		Shard:   int(shard),
		Version: int(version + 1),
	}
	if putErr == rpc.OK {
		resp.Success = true
		log.Printf("[Put] key=%q value=%q S%d (ver %d→%d) OK", req.Key, req.Value, shard, version, version+1)
	} else {
		resp.Err = string(putErr)
		log.Printf("[Put] key=%q value=%q S%d err=%s", req.Key, req.Value, shard, putErr)
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
		version rpc.Tversion
		err     rpc.Err
	}
	getCh := make(chan getResult, 1)
	go func() {
		v, ver, e := h.cm.Get(key)
		getCh <- getResult{v, ver, e}
	}()

	var value string
	var version rpc.Tversion
	var getErr rpc.Err
	select {
	case gres := <-getCh:
		value, version, getErr = gres.val, gres.version, gres.err
	case <-time.After(5 * time.Second):
		log.Printf("[Get] key=%q 超时: 集群无响应 (Raft 可能无 leader)", key)
		writeJSON(w, map[string]any{
			"success": false, "key": key,
			"err": "ErrTimeout", "message": "集群无响应（可能是 Raft 组无 leader）",
		})
		return
	}

	resp := struct {
		Success bool         `json:"success"`
		Key     string       `json:"key"`
		Value   string       `json:"value,omitempty"`
		Version rpc.Tversion `json:"version,omitempty"`
		Err     string       `json:"err,omitempty"`
	}{
		Key: key,
	}
	if getErr == rpc.OK {
		resp.Success = true
		resp.Value = value
		resp.Version = version
		log.Printf("[Get] key=%q value=%q version=%d OK", key, value, version)
	} else if getErr == rpc.ErrNoKey {
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

// HandleRecoverGroup 恢复指定组的全部网络连接（取消所有节点分区）
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
	log.Printf("[RecoverGroup] group=%d 网络已恢复", req.GID)
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
	ok, errMsg := h.cm.JoinGroup(gid)
	resp := map[string]any{"success": ok, "action": "join", "gid": int(gid)}
	if ok {
		log.Printf("[JoinGroup] new group %d joined", gid)
	} else {
		resp["error"] = errMsg
		resp["message"] = errMsg
		log.Printf("[JoinGroup] failed to join new group: %s", errMsg)
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

	type cfgResult struct {
		cfg *shardcfg.ShardConfig
	}
	ch := make(chan cfgResult, 1)
	go func() {
		ch <- cfgResult{cfg: h.cm.QueryConfig()}
	}()

	var cfg *shardcfg.ShardConfig
	select {
	case r := <-ch:
		cfg = r.cfg
	case <-time.After(5 * time.Second):
		log.Printf("[Config] 超时: configStore 无响应")
		writeJSON(w, map[string]any{
			"err": "ErrTimeout", "message": "查询配置超时（网络不可靠或 configStore 无响应）",
		})
		return
	}
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
	mux.HandleFunc("/api/network/reliable", h.HandleReliable)

	// Chaos Monkey
	mux.HandleFunc("/api/chaos/start", h.HandleChaosStart)
	mux.HandleFunc("/api/chaos/stop", h.HandleChaosStop)
	mux.HandleFunc("/api/chaos/status", h.HandleChaosStatus)
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

// parseGIDAndSrv parses "gid,srv" from a query string
func parseGIDAndSrv(s string) (int, int, error) {
	parts := strings.Split(s, ",")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("invalid format: expected gid,srv")
	}
	gid, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid gid: %v", err)
	}
	srv, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid srv: %v", err)
	}
	return gid, srv, nil
}
