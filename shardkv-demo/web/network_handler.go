package web

import (
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
		writeJSON(w, NetworkStatus{
			Reliable:       h.cm.IsReliable(),
			LongReordering: h.cm.IsLongReordering(),
			LongDelays:     h.cm.IsLongDelays(),
		})
		return
	}
	var req reliableRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.Reliable != nil {
		h.cm.SetReliable(*req.Reliable)
	}
	writeJSON(w, ReliableSetResult{Success: true, Action: "reliable", Reliable: h.cm.IsReliable()})
}

// HandleNetParams 查询/设置网络参数（GET/POST /api/network/params）。
func (h *Handler) HandleNetParams(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		writeJSON(w, NetParams{
			DropRate:     h.cm.GetDropRate(),
			ShortDelayMs: h.cm.GetShortDelayMs(),
			LongDelayMs:  h.cm.GetLongDelayMs(),
		})
		return
	}
	if r.Method == http.MethodPost {
		var req netParamsRequest
		if !decodeJSON(w, r, &req) {
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
		writeJSON(w, NetParamsSetResult{
			Success:      true,
			DropRate:     h.cm.GetDropRate(),
			ShortDelayMs: h.cm.GetShortDelayMs(),
			LongDelayMs:  h.cm.GetLongDelayMs(),
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
		writeJSON(w, LongReorderingResult{LongReordering: h.cm.IsLongReordering()})
		return
	}
	var req longReorderingRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if req.On != nil {
		h.cm.SetLongReordering(*req.On)
	}
	log.Printf("[LongReordering] 开启=%v", h.cm.IsLongReordering())
	writeJSON(w, LongReorderingResult{Success: true, LongReordering: h.cm.IsLongReordering()})
}
