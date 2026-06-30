package web

import (
	"encoding/json"
	"net/http"
	"sync"

	"kvstore/tester"
)

// sseEvent 事件包络，支持多种事件类型
type sseEvent struct {
	Type string      // SSE event 行："leader-change" | "task-done"
	Data interface{} // 将被 JSON 序列化后写入 data: 行
}

// SSEBroker 事件总线，支持多个 subscriber
type SSEBroker struct {
	mu          sync.Mutex
	subscribers map[chan sseEvent]struct{}
}

// NewSSEBroker 创建 SSE 事件总线
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		subscribers: make(map[chan sseEvent]struct{}),
	}
}

// Subscribe 注册一个新的 subscriber，返回接收事件的 channel
func (b *SSEBroker) Subscribe() chan sseEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan sseEvent, 128)
	b.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe 取消注册
func (b *SSEBroker) Unsubscribe(ch chan sseEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// publish 广播事件到所有 subscriber（非阻塞，channel 满则丢弃）
func (b *SSEBroker) publish(event sseEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		select {
		case ch <- event:
		default:
			// channel 满，丢弃防止阻塞
		}
	}
}

// ---------- Leader 变更事件 ----------

// LeaderEvent 表示一个 Leader 变更事件。
type LeaderEvent struct {
	GID      int  `json:"gid"`
	Sid      int  `json:"sid"`
	IsLeader bool `json:"isLeader"`
}

// PublishLeaderChange 供外部调用，将 Leader 变更事件发布到 SSE 总线。
func (h *Handler) PublishLeaderChange(gid tester.Tgid, sid int, isLeader bool) {
	h.sseBroker.publish(sseEvent{
		Type: "leader-change",
		Data: LeaderEvent{
			GID:      int(gid),
			Sid:      sid,
			IsLeader: isLeader,
		},
	})
}

// ---------- 异步任务完成事件 ----------

// TaskDoneEvent 异步任务完成时推送给前端的结果。
type TaskDoneEvent struct {
	TaskID  string          `json:"taskId"`
	Success bool            `json:"success"`
	Action  string          `json:"action"` // "put" | "get" | "join" | "leave" | "cas-race" | "batch-put"
	GID     int             `json:"gid,omitempty"`
	Message string          `json:"message,omitempty"`
	Error   string          `json:"error,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"` // action-specific payload
}

// PublishTaskDone 发布异步任务完成事件。
func (h *Handler) PublishTaskDone(ev TaskDoneEvent) {
	h.sseBroker.publish(sseEvent{
		Type: "task-done",
		Data: ev,
	})
}

// ---------- SSE HTTP Handler ----------

// HandleSSE 提供 SSE 端点，支持 leader-change 与 task-done 两种事件。
func (h *Handler) HandleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := h.sseBroker.Subscribe()
	defer h.sseBroker.Unsubscribe(ch)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(ev.Data)
			if err != nil {
				continue
			}
			_, writeErr := w.Write([]byte("event: " + ev.Type + "\ndata: " + string(data) + "\n\n"))
			if writeErr != nil {
				return
			}
			flusher.Flush()
		}
	}
}
