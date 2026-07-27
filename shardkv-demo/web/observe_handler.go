package web

import (
	"log"
	"net/http"
	"strconv"

	"kvstore/debug"
)

// HandleObserve 查询/设置观测日志开关（GET/POST /api/observe）。
func (h *Handler) HandleObserve(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, ObserveStatus{
			Election:    debug.GetObserveElection(),
			Migration:   debug.GetObserveMigration(),
			KVSubmit:    debug.GetObserveKVSubmit(),
			Snapshot:    debug.GetObserveSnapshot(),
			Replication: debug.GetObserveReplication(),
		})
	case http.MethodPost:
		var req observeSetRequest
		if !decodeJSON(w, r, &req) {
			return
		}
		switch req.Scene {
		case "election":
			debug.SetObserveElection(req.On)
		case "migration":
			debug.SetObserveMigration(req.On)
		case "kvsubmit":
			debug.SetObserveKVSubmit(req.On)
		case "snapshot":
			debug.SetObserveSnapshot(req.On)
		case "replication":
			debug.SetObserveReplication(req.On)
		default:
			http.Error(w, "unknown scene: "+req.Scene, http.StatusBadRequest)
			return
		}
		log.Printf("[Observe] scene=%q on=%v", req.Scene, req.On)
		writeJSON(w, ObserveSetResult{Success: true, Scene: req.Scene, On: req.On})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// HandleObserveLogs 获取观测日志（GET /api/observe/logs?since=xxx 增量获取）。
func (h *Handler) HandleObserveLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sinceStr := r.URL.Query().Get("since")
	var since int64
	if sinceStr != "" {
		if parsed, err := strconv.ParseInt(sinceStr, 10, 64); err == nil {
			since = parsed
		}
	}

	lines, nextId := debug.GetObserveLinesSince(since)
	if lines == nil {
		writeJSON(w, ObserveLogsResult{Lines: []debug.ObserveLine{}, NextID: nextId})
		return
	}
	writeJSON(w, ObserveLogsResult{Lines: lines, NextID: nextId})
}
