package web

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"

	"kvstore/debug"
)

// HandleObserve 查询/设置观测日志开关（GET/POST /api/observe）。
func (h *Handler) HandleObserve(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, map[string]any{
			"election":    debug.GetObserveElection(),
			"migration":   debug.GetObserveMigration(),
			"kvsubmit":    debug.GetObserveKVSubmit(),
			"snapshot":    debug.GetObserveSnapshot(),
			"replication": debug.GetObserveReplication(),
		})
	case http.MethodPost:
		var req observeSetRequest
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
		case "snapshot":
			debug.SetObserveSnapshot(req.On)
		case "replication":
			debug.SetObserveReplication(req.On)
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
		writeJSON(w, map[string]any{"lines": []debug.ObserveLine{}, "nextId": nextId})
		return
	}
	writeJSON(w, map[string]any{"lines": lines, "nextId": nextId})
}
