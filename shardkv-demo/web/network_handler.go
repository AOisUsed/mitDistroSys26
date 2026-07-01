package web

import (
	"encoding/json"
	"log"
	"net/http"
)

// HandleReliable 查询/设置网络可靠性（GET/POST /api/network/reliable）。
func (h *Handler) HandleReliable(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, map[string]any{
			"reliable":       h.cm.IsReliable(),
			"longReordering": h.cm.IsLongReordering(),
			"longDelays":     h.cm.IsLongDelays(),
		})
		return
	}
	var req reliableRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Reliable != nil {
		h.cm.SetReliable(*req.Reliable)
		if !*req.Reliable {
			h.cm.SetLongDelays(true)
		} else {
			h.cm.SetLongDelays(false)
			h.cm.SetLongReordering(false)
		}
	}
	writeJSON(w, map[string]any{"success": true, "action": "reliable", "reliable": h.cm.IsReliable()})
}

// HandleNetParams 查询/设置网络参数（GET/POST /api/network/params）。
func (h *Handler) HandleNetParams(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, map[string]any{
			"dropRate":     h.cm.GetDropRate(),
			"shortDelayMs": h.cm.GetShortDelayMs(),
			"longDelayMs":  h.cm.GetLongDelayMs(),
		})
		return
	}
	if r.Method == http.MethodPost {
		var req netParamsRequest
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

// HandleLongReordering 查询/设置长延迟 RPC 乱序（GET/POST /api/network/long-reordering）。
func (h *Handler) HandleLongReordering(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Method == http.MethodGet {
		writeJSON(w, map[string]any{"longReordering": h.cm.IsLongReordering()})
		return
	}
	var req longReorderingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.On != nil {
		h.cm.SetLongReordering(*req.On)
		if *req.On {
			h.cm.SetReliable(false)
			h.cm.SetLongDelays(true)
		}
	}
	log.Printf("[LongReordering] 开启=%v", h.cm.IsLongReordering())
	writeJSON(w, map[string]any{"success": true, "longReordering": h.cm.IsLongReordering()})
}
