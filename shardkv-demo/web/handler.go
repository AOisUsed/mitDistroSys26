package web

import (
	"encoding/json"
	"net/http"
	"sync/atomic"

	"kvstore/debug"
	"kvstore/tester"

	"shardkv-demo/cluster"
)

// Handler 封装所有 HTTP handler 方法和共享依赖。
type Handler struct {
	cm        *cluster.ClusterManager
	staticDir string
	sseBroker *SSEBroker
	taskSeq   atomic.Int64
}

// NewHandler 创建 Handler 并注册 SSE 推送回调。
func NewHandler(cm *cluster.ClusterManager) *Handler {
	h := &Handler{cm: cm, sseBroker: NewSSEBroker()}

	debug.SetObserveLogCallback(func(tag, text string, id int64, unixMilli int64, style string) {
		h.PublishObserveLog(tag, text, id, unixMilli, style)
	})

	cm.OnChaosEvent = func(gid tester.Tgid, action string, idx int) {
		h.PublishClusterChange(int(gid), action, idx)
	}

	return h
}

// SetStaticDir 设置静态文件目录路径（必须在 RegisterRoutes 之前调用）。
func (h *Handler) SetStaticDir(dir string) {
	h.staticDir = dir
}

// HandleIndex 提供根路径的静态页面。
func (h *Handler) HandleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	indexPath := h.staticDir + "/web/static/index.html"
	http.ServeFile(w, r, indexPath)
}

// writeJSON 将 v 序列化为 JSON 并写入响应。
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}
