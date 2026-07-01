package web

import (
	"encoding/json"
	"fmt"
	"kvstore/debug"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"kvstore/kvsrv/rpcapi"
	"kvstore/shardkv/shardcfg"
	"kvstore/tester"
	"shardkv-demo/cluster"
)

type Handler struct {
	cm        *cluster.ClusterManager
	staticDir string // 静态文件目录（shardkv-demo 目录路径）
	sseBroker *SSEBroker
	taskSeq   atomic.Int64 // 异步任务序列号
}

func NewHandler(cm *cluster.ClusterManager) *Handler {
	h := &Handler{cm: cm, sseBroker: NewSSEBroker()}

	// 注册观测日志 SSE 推送回调
	debug.SetObserveLogCallback(func(tag, text string, id int64, unixMilli int64) {
		h.PublishObserveLog(tag, text, id, unixMilli)
	})

	// 注册 ChaosMonkey 节点变更 SSE 推送回调
	cm.OnChaosEvent = func(gid tester.Tgid, action string, idx int) {
		h.PublishClusterChange(int(gid), action, idx)
	}

	return h
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
		PoolSize            int           `json:"poolSize"`
		Groups              []GroupNode   `json:"groups"`
		Shards              []ShardNode   `json:"shards"`
	}{
		ConfigNum:           state.Config.Num,
		HasPendingMigration: state.HasPendingMigration,
		PendingConfigNum:    state.PendingConfigNum,
		ConfigCached:        state.ConfigCached,
		PoolSize:            h.cm.ClerkPoolSize(),
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

	taskID := fmt.Sprintf("put-%s-%d", req.Key, h.taskSeq.Add(1))

	// 立即返回 taskId，异步执行
	writeJSON(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "put",
		"key":    req.Key,
	})

	go func() {
		shard := shardcfg.Key2Shard(req.Key)

		// Get 当前版本
		_, ver, e := h.cm.Get(req.Key)
		if e != rpcapi.OK && e != rpcapi.ErrNoKey {
			log.Printf("[Put] key=%q S%d Get 失败 (task=%s): %s", req.Key, shard, taskID, e)
			h.PublishTaskDone(TaskDoneEvent{
				TaskID:  taskID,
				Success: false,
				Action:  "put",
				Error:   string(e),
			})
			return
		}
		version := ver

		// Put
		putErr := h.cm.Put(req.Key, req.Value, version)

		if putErr == rpcapi.OK {
			log.Printf("[Put] key=%q value=%q S%d version=%d OK (task=%s)", req.Key, req.Value, shard, version, taskID)
			payload, _ := json.Marshal(map[string]any{
				"key":    req.Key,
				"value":  req.Value,
				"shard":  int(shard),
				"reqVer": int(version),
			})
			h.PublishTaskDone(TaskDoneEvent{
				TaskID:  taskID,
				Success: true,
				Action:  "put",
				Data:    payload,
			})
		} else {
			log.Printf("[Put] key=%q value=%q S%d version=%d 失败 (task=%s, err=%s)", req.Key, req.Value, shard, version, taskID, putErr)
			h.PublishTaskDone(TaskDoneEvent{
				TaskID:  taskID,
				Success: false,
				Action:  "put",
				Error:   string(putErr),
			})
		}
	}()
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

	taskID := fmt.Sprintf("get-%s-%d", key, h.taskSeq.Add(1))

	// 立即返回 taskId，异步执行
	writeJSON(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "get",
		"key":    key,
	})

	go func() {
		v, ver, e := h.cm.Get(key)
		if e == rpcapi.OK {
			log.Printf("[Get] key=%q value=%q version=%d OK (task=%s)", key, v, ver, taskID)
			payload, _ := json.Marshal(map[string]any{
				"key":     key,
				"value":   v,
				"version": int(ver),
			})
			h.PublishTaskDone(TaskDoneEvent{
				TaskID:  taskID,
				Success: true,
				Action:  "get",
				Data:    payload,
			})
		} else if e == rpcapi.ErrNoKey {
			log.Printf("[Get] key=%q 失败 (task=%s, err=ErrNoKey)", key, taskID)
			h.PublishTaskDone(TaskDoneEvent{
				TaskID:  taskID,
				Success: false,
				Action:  "get",
				Error:   "ErrNoKey",
			})
		} else {
			log.Printf("[Get] key=%q 失败 (task=%s, err=%s)", key, taskID, e)
			h.PublishTaskDone(TaskDoneEvent{
				TaskID:  taskID,
				Success: false,
				Action:  "get",
				Error:   string(e),
			})
		}
	}()
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
	log.Printf("[KillNode] 组 %d Server-%d (%s) — 该组存活: %d/%d", req.GID, req.Srv, srvName, aliveCount, totalNodes)
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
	log.Printf("[StartNode] 组 %d server-%d err=%v", req.GID, req.Srv, err)
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
	log.Printf("[IsolateNode] 组 %d 节点 %d 已隔离", req.GID, req.Srv)
	writeJSON(w, map[string]any{"success": true, "action": "isolate", "gid": req.GID, "srv": req.Srv})
}

// HandleRecoverNode 恢复单个节点的网络连接（只影响该节点，不影响已下线节点）
func (h *Handler) HandleRecoverNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req nodeOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.RecoverNode(tester.Tgid(req.GID), req.Srv)
	log.Printf("[RecoverNode] 组 %d 节点 %d 网络已恢复", req.GID, req.Srv)
	writeJSON(w, map[string]any{"success": true, "action": "recover-node", "gid": req.GID, "srv": req.Srv})
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
	log.Printf("[KillGroup] 组 %d 已停止", req.GID)
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
	log.Printf("[StartGroup] 组 %d err=%v", req.GID, err)
	writeJSON(w, resp)
}

// --- API: Join/Leave ---

func (h *Handler) HandleJoinGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	gid := h.cm.NewGid()
	taskID := fmt.Sprintf("join-%d-%d", int(gid), h.taskSeq.Add(1))

	// 立即返回 taskId，异步执行 JoinGroup
	writeJSON(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "join",
		"gid":    int(gid),
	})

	go func() {
		ok, msg := h.cm.JoinGroup(gid)
		ev := TaskDoneEvent{
			TaskID:  taskID,
			Success: ok,
			Action:  "join",
			GID:     int(gid),
		}
		if ok {
			ev.Message = msg
			log.Printf("[JoinGroup] 新组 %d 已加入 (task=%s)", gid, taskID)
		} else {
			ev.Error = msg
			log.Printf("[JoinGroup] 加入新组失败 (task=%s): %s", taskID, msg)
		}
		h.PublishTaskDone(ev)
	}()
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
	if req.GID == 0 {
		log.Printf("[LeaveGroup] 组 0 (配置仓库) 是系统核心组件，不能离开集群")
		writeJSON(w, map[string]any{"success": false, "action": "leave", "gid": 0, "error": "ConfigStore (组 0) 是系统配置仓库，不能离开集群", "message": "配置仓库 (组 0) 是系统核心组件，不允许离开集群"})
		return
	}
	gid := req.GID
	taskID := fmt.Sprintf("leave-%d-%d", gid, h.taskSeq.Add(1))

	// 立即返回 taskId，异步执行 LeaveGroup
	writeJSON(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "leave",
		"gid":    gid,
	})

	go func() {
		ok, errMsg := h.cm.LeaveGroup(tester.Tgid(gid))
		ev := TaskDoneEvent{
			TaskID:  taskID,
			Success: ok,
			Action:  "leave",
			GID:     gid,
		}
		if ok {
			log.Printf("[LeaveGroup] 组 %d 已离开 (task=%s)", gid, taskID)
		} else {
			ev.Error = errMsg
			log.Printf("[LeaveGroup] 组 %d 离开失败 (task=%s): %s", gid, taskID, errMsg)
		}
		h.PublishTaskDone(ev)
	}()
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
			"longReordering": h.cm.IsLongReordering(),
			"longDelays":     h.cm.IsLongDelays(),
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
			// 开启不可靠网络：同时启用长延迟模式（使断线超时滑块生效）
			h.cm.SetLongDelays(true)
		} else {
			// 关闭不可靠网络：恢复短超时模式
			h.cm.SetLongDelays(false)
			h.cm.SetLongReordering(false)
		}
	}
	writeJSON(w, map[string]any{"success": true, "action": "reliable", "reliable": h.cm.IsReliable()})
}

func (h *Handler) HandleNetParams(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		// GET: 查询当前网络参数（从 Network 获取运行时可调值）
		writeJSON(w, map[string]any{
			"dropRate":     h.cm.GetDropRate(),
			"shortDelayMs": h.cm.GetShortDelayMs(),
			"longDelayMs":  h.cm.GetLongDelayMs(),
		})
		return
	}
	if r.Method == http.MethodPost {
		// POST: 设置网络参数
		var req struct {
			DropRate     *int `json:"dropRate"`
			ShortDelayMs *int `json:"shortDelayMs"`
			LongDelayMs  *int `json:"longDelayMs"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON", http.StatusBadRequest)
			return
		}
		if req.DropRate != nil {
			h.cm.SetDropRate(*req.DropRate)
		}
		if req.ShortDelayMs != nil {
			h.cm.SetShortDelayMs(*req.ShortDelayMs)
		}
		if req.LongDelayMs != nil {
			h.cm.SetLongDelayMs(*req.LongDelayMs)
		}
		writeJSON(w, map[string]any{
			"success":      true,
			"dropRate":     h.cm.GetDropRate(),
			"shortDelayMs": h.cm.GetShortDelayMs(),
			"longDelayMs":  h.cm.GetLongDelayMs(),
		})
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (h *Handler) HandleLongReordering(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, map[string]any{"longReordering": h.cm.IsLongReordering()})
		return
	}
	var req struct {
		On *bool `json:"on"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.On != nil {
		h.cm.SetLongReordering(*req.On)
		if *req.On {
			// 开启回复乱序时自动开启不可靠网络
			h.cm.SetReliable(false)
			h.cm.SetLongDelays(true)
		}
	}
	log.Printf("[LongReordering] 开启=%v", h.cm.IsLongReordering())
	writeJSON(w, map[string]any{"success": true, "longReordering": h.cm.IsLongReordering()})
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

// --- API: 批量随机写入 ---

var randomSeed = uint64(time.Now().UnixNano())

// batchChars 是与前端 randomChars 一致的字符集
const batchChars = "abcdefghijklmnopqrstuvwxyz0123456789"

func batchRandomKey() string {
	b := make([]byte, 8)
	for i := range b {
		randomSeed = randomSeed*6364136223846793005 + 1442695040888963407
		b[i] = batchChars[int((randomSeed>>33)%uint64(len(batchChars)))]
	}
	return string(b)
}

func (h *Handler) HandleBatchPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Count int  `json:"count"` // key 数量
		Shard *int `json:"shard"` // nil 表示任意分片，0-11 指定分片
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Count <= 0 || req.Count > 10000 {
		http.Error(w, "count must be 1~10000", http.StatusBadRequest)
		return
	}

	shardInfo := "任意分片"
	if req.Shard != nil {
		shardInfo = fmt.Sprintf("分片 %d", *req.Shard)
	}
	taskID := fmt.Sprintf("batch-%d", h.taskSeq.Add(1))

	// 立即返回 taskId，异步执行
	writeJSON(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "batch-put",
		"count":  req.Count,
	})

	go func() {
		log.Printf("[BatchPut] 开始批量写入 (task=%s): count=%d shard=%s", taskID, req.Count, shardInfo)
		start := time.Now()

		// 生成 key-value pair
		type kvPair struct {
			key   string
			value string
		}
		pairs := make([]kvPair, 0, req.Count)
		valBuf := make([]byte, 3)

		for len(pairs) < req.Count {
			key := batchRandomKey()
			shard := shardcfg.Key2Shard(key)
			if req.Shard != nil && int(shard) != *req.Shard {
				continue
			}
			for i := range valBuf {
				randomSeed = randomSeed*6364136223846793005 + 1442695040888963407
				valBuf[i] = batchChars[int((randomSeed>>33)%uint64(len(batchChars)))]
			}
			pairs = append(pairs, kvPair{key: key, value: string(valBuf)})
		}

		log.Printf("[BatchPut] 已生成 %d 个 key-value pair (task=%s)", len(pairs), taskID)

		type opResult struct {
			idx int
			err rpcapi.Err
		}
		resultCh := make(chan opResult, len(pairs))
		pool := h.cm.GetClerkPool()

		for i, pair := range pairs {
			go func(idx int, k, v string) {
				ck := pool.Borrow()
				defer pool.Return(ck)
				_, ver, getErr := ck.Get(k)
				if getErr != rpcapi.OK && getErr != rpcapi.ErrNoKey {
					resultCh <- opResult{idx, getErr}
					return
				}
				putErr := ck.Put(k, v, ver)
				resultCh <- opResult{idx, putErr}
			}(i, pair.key, pair.value)
		}

		successCount := 0
		failCount := 0
		for i := 0; i < len(pairs); i++ {
			res := <-resultCh
			if res.err == rpcapi.OK {
				successCount++
			} else {
				failCount++
			}
		}
		elapsed := time.Since(start).Seconds()
		log.Printf("[BatchPut] 完成 (task=%s): 成功=%d 失败=%d 用时=%.2fs", taskID, successCount, failCount, elapsed)

		payload, _ := json.Marshal(map[string]any{
			"successCount": successCount,
			"failCount":    failCount,
			"elapsed":      fmt.Sprintf("%.2fs", elapsed),
		})
		h.PublishTaskDone(TaskDoneEvent{
			TaskID:  taskID,
			Success: true,
			Action:  "batch-put",
			Data:    payload,
		})
	}()
}

// --- API: CAS 竞赛（后端并发）---

func casRandomValue() string {
	b := make([]byte, 3)
	for i := range b {
		randomSeed = randomSeed*6364136223846793005 + 1442695040888963407
		b[i] = batchChars[int((randomSeed>>33)%uint64(len(batchChars)))]
	}
	return string(b)
}

// HandleCasRace 后端并发 CAS 竞赛（异步 + SSE 推送）。
// POST /api/kv/cas-race  body: {key, nClient}
func (h *Handler) HandleCasRace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Key     string `json:"key"`
		NClient int    `json:"nClient"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}
	if req.NClient < 1 || req.NClient > h.cm.ClerkPoolSize() {
		http.Error(w, fmt.Sprintf("nClient must be 1~%d", h.cm.ClerkPoolSize()), http.StatusBadRequest)
		return
	}

	taskID := fmt.Sprintf("cas-%d", h.taskSeq.Add(1))

	// 立即返回 taskId，异步执行
	writeJSON(w, map[string]any{
		"taskId":  taskID,
		"async":   true,
		"action":  "cas-race",
		"key":     req.Key,
		"nClient": req.NClient,
	})

	go func() {
		shard := shardcfg.Key2Shard(req.Key)

		// 1. 获取基线版本号
		_, curVer, getErr := h.cm.Get(req.Key)
		if getErr != rpcapi.OK && getErr != rpcapi.ErrNoKey {
			h.PublishTaskDone(TaskDoneEvent{
				TaskID:  taskID,
				Success: false,
				Action:  "cas-race",
				Error:   string(getErr),
			})
			return
		}

		log.Printf("[CasRace] 开始 (task=%s): key=%q S%d nClient=%d version=%d", taskID, req.Key, shard, req.NClient, curVer)
		start := time.Now()

		// 2. 预生成每个客户端的随机 value
		values := make([]string, req.NClient)
		for i := range values {
			values[i] = casRandomValue()
		}

		// 3. 并发启动 N 个 goroutine 执行 CAS Put
		type raceResult struct {
			err rpcapi.Err
		}
		resultCh := make(chan raceResult, req.NClient)
		pool := h.cm.GetClerkPool()

		for i := 0; i < req.NClient; i++ {
			go func(val string) {
				ck := pool.Borrow()
				defer pool.Return(ck)
				putErr := ck.Put(req.Key, val, curVer)
				resultCh <- raceResult{putErr}
			}(values[i])
		}

		// 4. 收集结果
		successCount := 0
		versionErrCount := 0
		for i := 0; i < req.NClient; i++ {
			res := <-resultCh
			if res.err == rpcapi.OK {
				successCount++
			} else {
				versionErrCount++
			}
		}

		elapsed := time.Since(start).Seconds()

		// 5. 获取最终值
		finalValue, _, _ := h.cm.Get(req.Key)

		log.Printf("[CasRace] 完成 (task=%s): 成功=%d 冲突=%d 用时=%.2fs 最终=%q", taskID, successCount, versionErrCount, elapsed, finalValue)

		payload, _ := json.Marshal(map[string]any{
			"key":             req.Key,
			"version":         int(curVer),
			"nClient":         req.NClient,
			"successCount":    successCount,
			"versionErrCount": versionErrCount,
			"finalValue":      finalValue,
			"elapsed":         fmt.Sprintf("%.2fs", elapsed),
		})
		h.PublishTaskDone(TaskDoneEvent{
			TaskID:  taskID,
			Success: true,
			Action:  "cas-race",
			Data:    payload,
		})
	}()
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
	mux.HandleFunc("/api/status/tree", h.HandleStatusTree)

	// KV — single handler dispatches by method
	mux.HandleFunc("/api/kv/", h.HandleKV)

	// Node operations
	mux.HandleFunc("/api/node/kill", h.HandleKillNode)
	mux.HandleFunc("/api/node/start", h.HandleStartNode)
	mux.HandleFunc("/api/node/isolate", h.HandleIsolateNode)
	mux.HandleFunc("/api/node/recover-node", h.HandleRecoverNode)

	// Group operations
	mux.HandleFunc("/api/group/kill", h.HandleKillGroup)
	mux.HandleFunc("/api/group/start", h.HandleStartGroup)
	mux.HandleFunc("/api/group/join", h.HandleJoinGroup)
	mux.HandleFunc("/api/group/leave", h.HandleLeaveGroup)

	// Network
	mux.HandleFunc("/api/network/params", h.HandleNetParams)
	mux.HandleFunc("/api/network/reliable", h.HandleReliable)
	mux.HandleFunc("/api/network/long-reordering", h.HandleLongReordering)

	// Chaos Monkey
	mux.HandleFunc("/api/chaos/start", h.HandleChaosStart)
	mux.HandleFunc("/api/chaos/stop", h.HandleChaosStop)
	mux.HandleFunc("/api/chaos/status", h.HandleChaosStatus)

	// SSE 事件流
	mux.HandleFunc("/api/events", h.HandleSSE)

	// 批量写入
	mux.HandleFunc("/api/kv/batch-put", h.HandleBatchPut)

	// CAS 竞赛
	mux.HandleFunc("/api/kv/cas-race", h.HandleCasRace)

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
