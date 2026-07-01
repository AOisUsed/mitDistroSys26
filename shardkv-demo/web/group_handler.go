package web

import (
	"encoding/json"
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	h.cm.KillGroup(tester.Tgid(req.GID))
	log.Printf("[KillGroup] 组 %d 已停止", req.GID)
	writeJSON(w, map[string]any{"success": true, "action": "kill-group", "gid": req.GID})
}

// HandleStartGroup 启动指定组所有节点（POST /api/group/start）。
func (h *Handler) HandleStartGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	err := h.cm.StartGroup(tester.Tgid(req.GID))
	resp := map[string]any{"success": err == nil, "action": "start-group", "gid": req.GID}
	if err != nil {
		resp["error"] = err.Error()
	}
	log.Printf("[StartGroup] 组 %d err=%v", req.GID, err)
	writeJSON(w, resp)
}

// HandleJoinGroup 加入新组（POST /api/group/join，异步 + SSE 推送）。
func (h *Handler) HandleJoinGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	gid := h.cm.NewGid()
	taskID := fmt.Sprintf("join-%d-%d", int(gid), h.taskSeq.Add(1))

	writeJSON(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "join",
		"gid":    int(gid),
	})

	go func() {
		ok, msg := h.cm.JoinGroup(gid)
		ev := TaskDoneEvent{
			TaskID:  taskID,
			Success: ok,
			Action:  "join",
			GID:     int(gid),
		}
		if ok {
			ev.Message = msg
			log.Printf("[JoinGroup] 新组 %d 已加入 (task=%s)", gid, taskID)
		} else {
			ev.Error = msg
			log.Printf("[JoinGroup] 加入新组失败 (task=%s): %s", taskID, msg)
		}
		h.PublishTaskDone(ev)
	}()
}

// HandleLeaveGroup 移除指定组（POST /api/group/leave，异步 + SSE 推送）。
func (h *Handler) HandleLeaveGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req groupOpRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return
	}
	if req.GID == 0 {
		log.Printf("[LeaveGroup] 组 0 (配置仓库) 是系统核心组件，不能离开集群")
		writeJSON(w, map[string]any{
			"success": false, "action": "leave", "gid": 0,
			"error":   "ConfigStore (组 0) 是系统配置仓库，不能离开集群",
			"message": "配置仓库 (组 0) 是系统核心组件，不能离开集群",
		})
		return
	}
	gid := req.GID
	taskID := fmt.Sprintf("leave-%d-%d", gid, h.taskSeq.Add(1))

	writeJSON(w, map[string]any{
		"taskId": taskID,
		"async":  true,
		"action": "leave",
		"gid":    gid,
	})

	go func() {
		ok, errMsg := h.cm.LeaveGroup(tester.Tgid(gid))
		ev := TaskDoneEvent{
			TaskID:  taskID,
			Success: ok,
			Action:  "leave",
			GID:     gid,
		}
		if ok {
			log.Printf("[LeaveGroup] 组 %d 已离开 (task=%s)", gid, taskID)
		} else {
			ev.Error = errMsg
			log.Printf("[LeaveGroup] 组 %d 离开失败 (task=%s): %s", gid, taskID, errMsg)
		}
		h.PublishTaskDone(ev)
	}()
}
