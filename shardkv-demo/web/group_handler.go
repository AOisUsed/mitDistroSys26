package web

import (
	"fmt"
	"log"
	"net/http"

	"kvstore/tester"
)

// HandleKillGroup 停止指定组所有节点（POST /api/group/kill）。
func (h *Handler) HandleKillGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	h.cm.KillGroup(tester.Tgid(req.GID))
	log.Printf("[KillGroup] 组 %d 已停止", req.GID)
	writeJSON(w, GroupActionResult{Success: true, Action: "kill-group", GID: req.GID})
}

// HandleStartGroup 启动指定组所有节点（POST /api/group/start）。
func (h *Handler) HandleStartGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	err := h.cm.StartGroup(tester.Tgid(req.GID))
	result := GroupActionResult{Success: err == nil, Action: "start-group", GID: req.GID}
	if err != nil {
		result.Error = err.Error()
	}
	log.Printf("[StartGroup] 组 %d err=%v", req.GID, err)
	writeJSON(w, result)
}

// HandleJoinGroup 加入新组（POST /api/group/join，异步 + SSE 推送）。
func (h *Handler) HandleJoinGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	gid := h.cm.NewGid()
	taskID := fmt.Sprintf("join-%d-%d", int(gid), h.taskSeq.Add(1))

	h.startTask(w, TaskAccepted{
		TaskID: taskID,
		Async:  true,
		Action: "join",
		GID:    int(gid),
	}, func() TaskDoneEvent {
		ok, msg := h.cm.JoinGroup(gid)
		ev := TaskDoneEvent{TaskID: taskID, Success: ok, Action: "join", GID: int(gid)}
		if ok {
			ev.Message = msg
			log.Printf("[JoinGroup] 新组 %d 已加入 (task=%s)", gid, taskID)
		} else {
			ev.Error = msg
			log.Printf("[JoinGroup] 加入新组失败 (task=%s): %s", taskID, msg)
		}
		return ev
	})
}

// HandleLeaveGroup 移除指定组（POST /api/group/leave，异步 + SSE 推送）。
func (h *Handler) HandleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	gid := req.GID
	taskID := fmt.Sprintf("leave-%d-%d", gid, h.taskSeq.Add(1))

	h.startTask(w, TaskAccepted{
		TaskID: taskID,
		Async:  true,
		Action: "leave",
		GID:    gid,
	}, func() TaskDoneEvent {
		ok, errMsg := h.cm.LeaveGroup(tester.Tgid(gid))
		ev := TaskDoneEvent{TaskID: taskID, Success: ok, Action: "leave", GID: gid}
		if ok {
			log.Printf("[LeaveGroup] 组 %d 已离开 (task=%s)", gid, taskID)
		} else {
			ev.Error = errMsg
			log.Printf("[LeaveGroup] 组 %d 离开失败 (task=%s): %s", gid, taskID, errMsg)
		}
		return ev
	})
}
