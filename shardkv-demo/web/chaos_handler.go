package web

import (
	"log"
	"net/http"

	"kvstore/tester"
)

// HandleChaosStart 启动指定组的混沌猴子（POST /api/chaos/start）。
func (h *Handler) HandleChaosStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req chaosRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	err := h.cm.StartChaos(tester.Tgid(req.GID))
	if err != nil {
		writeJSON(w, ChaosActionResult{Success: false, Error: err.Error()})
		return
	}
	log.Printf("[Chaos] GID %d 混沌已启动", req.GID)
	writeJSON(w, ChaosActionResult{Success: true, Action: "chaos-start", GID: req.GID})
}

// HandleChaosStop 停止指定组的混沌猴子（POST /api/chaos/stop）。
func (h *Handler) HandleChaosStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req chaosRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	h.cm.StopChaos(tester.Tgid(req.GID))
	log.Printf("[Chaos] GID %d 混沌已停止", req.GID)
	writeJSON(w, ChaosActionResult{Success: true, Action: "chaos-stop", GID: req.GID})
}

// HandleChaosStatus 查询所有混沌猴子状态（GET /api/chaos/status）。
func (h *Handler) HandleChaosStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	states := h.cm.ChaosStatus()
	writeJSON(w, ChaosStatusResult{Success: true, States: states})
}
