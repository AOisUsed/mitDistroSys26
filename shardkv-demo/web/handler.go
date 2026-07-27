package web

import (
	"encoding/json"
	"log"
	"net/http"
	"path"
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
	indexPath := path.Join(h.staticDir, "web", "static", "index.html")
	http.ServeFile(w, r, indexPath)
}

// writeJSON 将 v 序列化为 JSON 并写入响应。
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		log.Printf("[web] writeJSON encode error: %v", err)
	}
}

// decodeJSON 解析请求体为 req；失败时写 400 并返回 false。
// 同时负责关闭请求体
func decodeJSON(w http.ResponseWriter, r *http.Request, req any) bool {
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return false
	}
	return true
}

// startTask 立即向客户端返回异步任务响应，并在后台 goroutine 中执行 work；
// work 返回的 TaskDoneEvent 通过 SSE 推送给前端。goroutine 内带 recover，
func (h *Handler) startTask(w http.ResponseWriter, immediate map[string]any, work func() TaskDoneEvent) {
	writeJSON(w, immediate)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[task] goroutine panic recovered: %v", rec)
				h.PublishTaskDone(TaskDoneEvent{Success: false, Error: "internal error"})
			}
		}()
		h.PublishTaskDone(work())
	}()
}
