package web

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"kvstore/kvsrv/rpcapi"
	"kvstore/shardkv/shardcfg"
)

// ========== Helpers ==========

const batchChars = "abcdefghijklmnopqrstuvwxyz0123456789"

// batchRandomKey 生成 8 位随机 key
func batchRandomKey() string {
	b := make([]byte, 8)
	for i := range b {
		b[i] = batchChars[rand.IntN(len(batchChars))]
	}
	return string(b)
}

// casRandomValue 生成 3 位随机 value
func casRandomValue() string {
	b := make([]byte, 3)
	for i := range b {
		b[i] = batchChars[rand.IntN(len(batchChars))]
	}
	return string(b)
}

// ========== KV 操作 ==========

// HandleKV 按 HTTP 方法分发 KV 请求。
func (h *Handler) HandleKV(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleGet(w, r)
	case http.MethodPut:
		h.handlePut(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *Handler) handlePut(w http.ResponseWriter, r *http.Request) {
	var req putRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	taskID := fmt.Sprintf("put-%s-%d", req.Key, h.taskSeq.Add(1))

	h.startTask(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "put",
		"key":    req.Key,
	}, func() TaskDoneEvent {
		shard := shardcfg.Key2Shard(req.Key)
		_, ver, e := h.cm.Get(req.Key)
		if e != rpcapi.OK && e != rpcapi.ErrNoKey {
			log.Printf("[Put] key=%q S%d Get 失败 (task=%s): %s", req.Key, shard, taskID, e)
			return TaskDoneEvent{TaskID: taskID, Success: false, Action: "put", Error: string(e)}
		}
		version := ver

		putErr := h.cm.Put(req.Key, req.Value, version)
		if putErr == rpcapi.OK {
			log.Printf("[Put] key=%q value=%q S%d version=%d OK (task=%s)", req.Key, req.Value, shard, version, taskID)
			payload, _ := json.Marshal(map[string]any{
				"key":    req.Key,
				"value":  req.Value,
				"shard":  int(shard),
				"reqVer": int(version),
			})
			return TaskDoneEvent{TaskID: taskID, Success: true, Action: "put", Data: payload}
		}
		log.Printf("[Put] key=%q value=%q S%d version=%d 失败 (task=%s, err=%s)", req.Key, req.Value, shard, version, taskID, putErr)
		return TaskDoneEvent{TaskID: taskID, Success: false, Action: "put", Error: string(putErr)}
	})
}

func (h *Handler) handleGet(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/api/kv/")
	if key == "" {
		http.Error(w, "key is required", http.StatusBadRequest)
		return
	}

	taskID := fmt.Sprintf("get-%s-%d", key, h.taskSeq.Add(1))
	h.startTask(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "get",
		"key":    key,
	}, func() TaskDoneEvent {
		v, ver, e := h.cm.Get(key)
		if e == rpcapi.OK {
			log.Printf("[Get] key=%q value=%q version=%d OK (task=%s)", key, v, ver, taskID)
			payload, _ := json.Marshal(map[string]any{
				"key":     key,
				"value":   v,
				"version": int(ver),
			})
			return TaskDoneEvent{TaskID: taskID, Success: true, Action: "get", Data: payload}
		} else if e == rpcapi.ErrNoKey {
			log.Printf("[Get] key=%q 失败 (task=%s, err=ErrNoKey)", key, taskID)
			return TaskDoneEvent{TaskID: taskID, Success: false, Action: "get", Error: "ErrNoKey"}
		}
		log.Printf("[Get] key=%q 失败 (task=%s, err=%s)", key, taskID, e)
		return TaskDoneEvent{TaskID: taskID, Success: false, Action: "get", Error: string(e)}
	})
}

// ========== 批量随机写入 ==========

// HandleBatchPut 批量随机写入（POST /api/kv/batch-put，异步 + SSE 推送）。
func (h *Handler) HandleBatchPut(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req batchPutRequest
	if !decodeJSON(w, r, &req) {
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

	h.startTask(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "batch-put",
		"count":  req.Count,
	}, func() TaskDoneEvent {
		log.Printf("[BatchPut] 开始批量写入 (task=%s): count=%d shard=%s", taskID, req.Count, shardInfo)
		start := time.Now()

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
				valBuf[i] = batchChars[rand.IntN(len(batchChars))]
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
		return TaskDoneEvent{
			TaskID:  taskID,
			Success: true,
			Action:  "batch-put",
			Data:    payload,
		}
	})
}

// ========== CAS 竞赛 ==========

// HandleCasRace 后端并发 CAS 竞赛（POST /api/kv/cas-race，异步 + SSE 推送）。
func (h *Handler) HandleCasRace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req casRaceRequest
	if !decodeJSON(w, r, &req) {
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

	h.startTask(w, map[string]any{
		"taskId":  taskID,
		"async":   true,
		"action":  "cas-race",
		"key":     req.Key,
		"nClient": req.NClient,
	}, func() TaskDoneEvent {
		shard := shardcfg.Key2Shard(req.Key)

		_, curVer, getErr := h.cm.Get(req.Key)
		if getErr != rpcapi.OK && getErr != rpcapi.ErrNoKey {
			return TaskDoneEvent{
				TaskID:  taskID,
				Success: false,
				Action:  "cas-race",
				Error:   string(getErr),
			}
		}

		log.Printf("[CasRace] 开始 (task=%s): key=%q S%d nClient=%d version=%d", taskID, req.Key, shard, req.NClient, curVer)
		start := time.Now()

		values := make([]string, req.NClient)
		for i := range values {
			values[i] = casRandomValue()
		}

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
		return TaskDoneEvent{
			TaskID:  taskID,
			Success: true,
			Action:  "cas-race",
			Data:    payload,
		}
	})
}
