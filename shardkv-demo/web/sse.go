package web

import (
	"encoding/json"
	"net/http"
	"sync"

	"kvstore/tester"
)

// sseEvent 事件包络，支持多种事件类型
type sseEvent struct {
	Type string      // SSE event 行："leader-change" | "task-done" | "observe-log"
	Data interface{} // 将被 JSON 序列化后写入 data: 行
}

// isPriorityEvent 判断事件是否不可丢弃。
func isPriorityEvent(t string) bool {
	return t == "task-done" || t == "leader-change" || t == "cluster-change"
}

// subscribePair 每个 subscriber 持有两条 channel：
//
//	priority — task-done / leader-change / cluster-change（阻塞发送，不可丢弃）
//	normal   — observe-log（非阻塞发送，满则丢弃）
type subscribePair struct {
	priority chan sseEvent
	normal   chan sseEvent
}

// SSEBroker 事件总线，支持多个 subscriber
type SSEBroker struct {
	mu          sync.Mutex
	subscribers map[*subscribePair]struct{}
}

// NewSSEBroker 创建 SSE 事件总线
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		subscribers: make(map[*subscribePair]struct{}),
	}
}

// Subscribe 注册一个新的 subscriber，返回双 channel pair
func (b *SSEBroker) Subscribe() *subscribePair {
	b.mu.Lock()
	defer b.mu.Unlock()
	sp := &subscribePair{
		priority: make(chan sseEvent, 100),  // 高优先事件
		normal:   make(chan sseEvent, 1000), // 观测日志，允许丢弃
	}
	b.subscribers[sp] = struct{}{}
	return sp
}

// Unsubscribe 取消注册
func (b *SSEBroker) Unsubscribe(sp *subscribePair) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[sp]; ok {
		delete(b.subscribers, sp)
		close(sp.priority)
		close(sp.normal)
	}
}

// Publish 广播事件到所有 subscriber。
// priority 事件（task-done / leader-change / cluster-change）阻塞发送不可丢弃；
// normal 事件（observe-log）满则丢弃。
func (b *SSEBroker) Publish(event sseEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for sp := range b.subscribers {
		if isPriorityEvent(event.Type) {
			sp.priority <- event
		} else {
			select {
			case sp.normal <- event:
			default:
				// observe-log channel 满则丢弃，防止阻塞 Raft 线程
			}
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
	h.sseBroker.Publish(sseEvent{
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
	h.sseBroker.Publish(sseEvent{
		Type: "task-done",
		Data: ev,
	})
}

// ---------- 集群拓扑变更事件（ChaosMonkey kill/restart） ----------

// ClusterChangeEvent 表示 ChaosMonkey 触发的节点状态变更。
type ClusterChangeEvent struct {
	Action string `json:"action"` // "kill" | "restart"
	GID    int    `json:"gid"`
	Index  int    `json:"index"`
}

// PublishClusterChange 推送 ChaosMonkey 操作事件到 SSE 流。
func (h *Handler) PublishClusterChange(gid int, action string, idx int) {
	h.sseBroker.Publish(sseEvent{
		Type: "cluster-change",
		Data: ClusterChangeEvent{
			Action: action,
			GID:    gid,
			Index:  idx,
		},
	})
}

// ---------- 观测日志实时推送 ----------

// ObserveLogEvent 观测日志实时推送事件。
type ObserveLogEvent struct {
	Tag       string `json:"tag"`
	Text      string `json:"text"`
	Id        int64  `json:"id"`
	UnixMilli int64  `json:"unixMilli"`
}

// PublishObserveLog 推送观测日志到 SSE 流。
func (h *Handler) PublishObserveLog(tag, text string, id int64, unixMilli int64) {
	h.sseBroker.Publish(sseEvent{
		Type: "observe-log",
		Data: ObserveLogEvent{
			Tag:       tag,
			Text:      text,
			Id:        id,
			UnixMilli: unixMilli,
		},
	})
}

// ---------- SSE HTTP Handler ----------

// HandleSSE 提供 SSE 端点，同时监听 priority（task-done/leader-change）和 normal（observe-log）双 channel。
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

	sp := h.sseBroker.Subscribe()
	defer h.sseBroker.Unsubscribe(sp)

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-sp.priority:
			if !ok {
				return
			}
			if !writeSSEEvent(w, flusher, ev) {
				return
			}
		case ev, ok := <-sp.normal:
			if !ok {
				return
			}
			if !writeSSEEvent(w, flusher, ev) {
				return
			}
		}
	}
}

// writeSSEEvent 将一个 sseEvent 序列化并写入 HTTP 响应流。
// 返回 false 表示写入失败（连接断开等）。
func writeSSEEvent(w http.ResponseWriter, flusher http.Flusher, ev sseEvent) bool {
	data, err := json.Marshal(ev.Data)
	if err != nil {
		return true // JSON 序列化失败跳过，继续处理后续事件
	}
	_, writeErr := w.Write([]byte("event: " + ev.Type + "\ndata: " + string(data) + "\n\n"))
	if writeErr != nil {
		return false
	}
	flusher.Flush()
	return true
}
