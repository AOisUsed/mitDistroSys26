package web

import (
	"encoding/json"
	"net/http"
	"sync"

	"kvstore/tester"
)

const (
	taskDoneCacheSize = 50
	priorityChanSize  = 100
	normalChanSize    = 5000
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
	mu             sync.Mutex
	subscribers    map[*subscribePair]struct{}
	recentTaskDone []sseEvent // 缓存最近 N 条 task-done 事件，供新 SSE 连接重放
}

// NewSSEBroker 创建 SSE 事件总线
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		subscribers: make(map[*subscribePair]struct{}),
	}
}

// subscribe 注册一个新的 subscriber，返回双 channel pair
func (b *SSEBroker) subscribe() *subscribePair {
	b.mu.Lock()
	defer b.mu.Unlock()
	sp := &subscribePair{
		priority: make(chan sseEvent, priorityChanSize), // 高优先事件
		normal:   make(chan sseEvent, normalChanSize),   // 观测日志，允许丢弃
	}
	b.subscribers[sp] = struct{}{}
	return sp
}

// unsubscribe 取消注册
func (b *SSEBroker) unsubscribe(sp *subscribePair) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[sp]; ok {
		delete(b.subscribers, sp)
		close(sp.priority)
		close(sp.normal)
	}
}

// publish 广播事件到所有 subscriber。
// priority 事件（task-done / leader-change / cluster-change）阻塞发送不可丢弃；
// normal 事件（observe-log）满则丢弃。
func (b *SSEBroker) publish(event sseEvent) {
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

// PublishTaskDone 发布异步任务完成事件，同时缓存以供 SSE 重连重放。
func (h *Handler) PublishTaskDone(ev TaskDoneEvent) {
	event := sseEvent{
		Type: "task-done",
		Data: ev,
	}
	h.sseBroker.storeTaskDone(event)
	h.sseBroker.publish(event)
}

// storeTaskDone 将 task-done 事件存入环形缓存，新 SSE 连接建立时重放。
func (b *SSEBroker) storeTaskDone(event sseEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.recentTaskDone = append(b.recentTaskDone, event)
	if len(b.recentTaskDone) > taskDoneCacheSize {
		b.recentTaskDone = b.recentTaskDone[len(b.recentTaskDone)-taskDoneCacheSize:]
	}
}

// getRecentTaskDone 返回缓存的 task-done 事件副本，不清除缓存（多连接安全）。
func (b *SSEBroker) getRecentTaskDone() []sseEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.recentTaskDone) == 0 {
		return nil
	}
	copied := make([]sseEvent, len(b.recentTaskDone))
	copy(copied, b.recentTaskDone)
	return copied
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
	h.sseBroker.publish(sseEvent{
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
	h.sseBroker.publish(sseEvent{
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

	sp := h.sseBroker.subscribe()
	defer h.sseBroker.unsubscribe(sp)

	// 新连接建立时，重放缓存的 task-done 事件，防止断开期间事件丢失
	cached := h.sseBroker.getRecentTaskDone()
	for _, ev := range cached {
		if !writeSSEEvent(w, flusher, ev) {
			return
		}
	}

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
