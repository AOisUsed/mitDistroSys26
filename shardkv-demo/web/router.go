package web

import "net/http"

// RegisterRoutes 注册所有 API 路由。
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/", h.HandleIndex)
	mux.HandleFunc("/api/status/tree", h.HandleStatusTree)

	// KV — 按方法分发
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
