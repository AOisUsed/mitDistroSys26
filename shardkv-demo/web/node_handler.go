package web

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"kvstore/tester"
)

// HandleKillNode 停止指定节点（POST /api/node/kill）。
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

// HandleStartNode 启动指定节点（POST /api/node/start）。
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

// HandleIsolateNode 隔离指定节点的网络（POST /api/node/isolate）。
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

// HandleRecoverNode 恢复单个节点的网络连接（POST /api/node/recover-node）。
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
