package web

import (
	"encoding/json"
	"net/http"
	"sync"

	"kvstore/tester"
)

// LeaderEvent 表示一个 Leader 变更事件
type LeaderEvent struct {
	GID      int  `json:"gid"`
	Sid      int  `json:"sid"`
	IsLeader bool `json:"isLeader"`
}

// SSEBroker 事件总线，支持多个 subscriber
type SSEBroker struct {
	mu          sync.Mutex
	subscribers map[chan LeaderEvent]struct{}
}

// NewSSEBroker 创建 SSE 事件总线
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		subscribers: make(map[chan LeaderEvent]struct{}),
	}
}

// Subscribe 注册一个新的 subscriber，返回接收事件的 channel
func (b *SSEBroker) Subscribe() chan LeaderEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan LeaderEvent, 64)
	b.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe 取消注册
func (b *SSEBroker) Unsubscribe(ch chan LeaderEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// Publish 广播事件到所有 subscriber（非阻塞，channel 满则丢弃）
func (b *SSEBroker) Publish(event LeaderEvent) {
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

// HandleSSE 提供 SSE 端点
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
			// 客户端断开连接
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(event)
			if err != nil {
				continue
			}
			_, writeErr := w.Write([]byte("event: leader-change\ndata: " + string(data) + "\n\n"))
			if writeErr != nil {
				// 客户端已断开，不做清理（defer 会处理）
				return
			}
			flusher.Flush()
		}
	}
}

// PublishLeaderChange 供外部调用，将 Leader 变更事件发布到 SSE 总线
func (h *Handler) PublishLeaderChange(gid tester.Tgid, sid int, isLeader bool) {
	h.sseBroker.Publish(LeaderEvent{
		GID:      int(gid),
		Sid:      sid,
		IsLeader: isLeader,
	})
}
