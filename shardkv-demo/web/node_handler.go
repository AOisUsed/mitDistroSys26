package web

import (
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
	if !decodeJSON(w, r, &req) {
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
	writeJSON(w, NodeKillResult{
		Success:    true,
		Action:     "kill",
		GID:        req.GID,
		Srv:        req.Srv,
		SRVName:    srvName,
		AliveGroup: aliveCount,
		TotalGroup: totalNodes,
	})
}

// HandleStartNode 启动指定节点（POST /api/node/start）。
func (h *Handler) HandleStartNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req nodeOpRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	err := h.cm.StartServer(tester.Tgid(req.GID), req.Srv)
	result := NodeActionResult{Success: err == nil, Action: "start", GID: req.GID, Srv: req.Srv}
	if err != nil {
		result.Error = err.Error()
	}
	log.Printf("[StartNode] 组 %d server-%d err=%v", req.GID, req.Srv, err)
	writeJSON(w, result)
}

// HandleIsolateNode 隔离指定节点的网络（POST /api/node/isolate）。
func (h *Handler) HandleIsolateNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req nodeOpRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	h.cm.IsolateNode(tester.Tgid(req.GID), req.Srv)
	log.Printf("[IsolateNode] 组 %d 节点 %d 已隔离", req.GID, req.Srv)
	writeJSON(w, NodeActionResult{Success: true, Action: "isolate", GID: req.GID, Srv: req.Srv})
}

// HandleRecoverNode 恢复单个节点的网络连接（POST /api/node/recover-node）。
func (h *Handler) HandleRecoverNode(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req nodeOpRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	h.cm.RecoverNode(tester.Tgid(req.GID), req.Srv)
	log.Printf("[RecoverNode] 组 %d 节点 %d 网络已恢复", req.GID, req.Srv)
	writeJSON(w, NodeActionResult{Success: true, Action: "recover-node", GID: req.GID, Srv: req.Srv})
}
