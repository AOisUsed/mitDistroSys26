package web

import (
	"kvstore/debug"
	"kvstore/tester"

	"shardkv-demo/cluster"
)

// ========== 请求 DTO ==========

// nodeOpRequest 节点级操作请求（Kill/Start/Isolate/Recover）
type nodeOpRequest struct {
	GID int `json:"gid"`
	Srv int `json:"srv"`
}

// groupOpRequest 组级操作请求（Kill/Start/Leave）
type groupOpRequest struct {
	GID int `json:"gid"`
}

// putRequest PUT 请求
type putRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// casRaceRequest CAS 竞赛请求
type casRaceRequest struct {
	Key     string `json:"key"`
	NClient int    `json:"nClient"`
}

// batchPutRequest 批量写入请求
type batchPutRequest struct {
	Count int  `json:"count"`
	Shard *int `json:"shard"` // nil = 任意分片
}

// reliableRequest 网络可靠性设置请求
type reliableRequest struct {
	Reliable *bool `json:"reliable"`
}

// netParamsRequest 网络参数设置请求
type netParamsRequest struct {
	DropRate     *int `json:"dropRate"`
	ShortDelayMs *int `json:"shortDelayMs"`
	LongDelayMs  *int `json:"longDelayMs"`
}

// longReorderingRequest 长延迟乱序设置请求
type longReorderingRequest struct {
	On *bool `json:"on"`
}

// chaosRequest 混沌猴子请求
type chaosRequest struct {
	GID int `json:"gid"`
}

// observeSetRequest 观测日志开关设置请求
type observeSetRequest struct {
	Scene string `json:"scene"`
	On    bool   `json:"on"`
}

// ========== 响应 / 状态树 DTO ==========

// ShardNode 分片分布节点
type ShardNode struct {
	Shard int         `json:"shard"`
	GID   tester.Tgid `json:"gid"`
}

// GroupNode 组状态节点
type GroupNode struct {
	GID     tester.Tgid           `json:"gid"`
	Servers []cluster.ServerState `json:"servers"`
	NShards int                   `json:"nShards"`
}

// StatusTree 集群状态树（/api/status/tree 响应）
type StatusTree struct {
	ConfigNum           int         `json:"configNum"`
	HasPendingMigration bool        `json:"hasPendingMigration"`
	PendingConfigNum    int         `json:"pendingConfigNum"`
	ConfigCached        bool        `json:"configCached"`
	PoolSize            int         `json:"poolSize"`
	Groups              []GroupNode `json:"groups"`
	Shards              []ShardNode `json:"shards"`
}

// ========== 异步任务即时响应 ==========

// TaskAccepted 异步任务被接受时的即时响应：先返回 taskId，结果经 SSE 推送。
// Key/GID/Count/NClient 为各端点的可选字段，使用 omitempty。
type TaskAccepted struct {
	TaskID  string `json:"taskId"`
	Async   bool   `json:"async"`
	Action  string `json:"action"`
	Key     string `json:"key,omitempty"`
	GID     int    `json:"gid,omitempty"`
	Count   int    `json:"count,omitempty"`
	NClient int    `json:"nClient,omitempty"`
}

// ========== 同步操作响应 ==========

// GroupActionResult 组级操作（kill/start）的同步响应。
type GroupActionResult struct {
	Success bool   `json:"success"`
	Action  string `json:"action"`
	GID     int    `json:"gid"`
	Error   string `json:"error,omitempty"`
}

// NodeActionResult 节点操作（start/isolate/recover）的同步响应。
type NodeActionResult struct {
	Success bool   `json:"success"`
	Action  string `json:"action"`
	GID     int    `json:"gid"`
	Srv     int    `json:"srv"`
	Error   string `json:"error,omitempty"`
}

// NodeKillResult 节点 Kill 操作的同步响应（附带该组存活统计）。
type NodeKillResult struct {
	Success    bool   `json:"success"`
	Action     string `json:"action"`
	GID        int    `json:"gid"`
	Srv        int    `json:"srv"`
	SRVName    string `json:"srvName"`
	AliveGroup int    `json:"aliveGroup"`
	TotalGroup int    `json:"totalGroup"`
}

// ChaosActionResult 混沌猴子操作的同步响应。
// 错误分支不带 action/gid（零值由 omitempty 省略）。
type ChaosActionResult struct {
	Success bool   `json:"success"`
	Action  string `json:"action,omitempty"`
	GID     int    `json:"gid,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ChaosStatusResult 混沌猴子状态查询响应。
type ChaosStatusResult struct {
	Success bool                 `json:"success"`
	States  []cluster.ChaosState `json:"states"`
}

// NetworkStatus 网络可靠性/乱序查询响应（GET /api/network/reliable）。
type NetworkStatus struct {
	Reliable       bool `json:"reliable"`
	LongReordering bool `json:"longReordering"`
	LongDelays     bool `json:"longDelays"`
}

// ReliableSetResult 设置网络可靠性响应（POST /api/network/reliable）。
type ReliableSetResult struct {
	Success  bool   `json:"success"`
	Action   string `json:"action"`
	Reliable bool   `json:"reliable"`
}

// NetParams 网络参数查询/设置响应（/api/network/params）。
type NetParams struct {
	DropRate     int `json:"dropRate"`
	ShortDelayMs int `json:"shortDelayMs"`
	LongDelayMs  int `json:"longDelayMs"`
}

// NetParamsSetResult 设置网络参数响应（POST /api/network/params）。
type NetParamsSetResult struct {
	Success      bool `json:"success"`
	DropRate     int  `json:"dropRate"`
	ShortDelayMs int  `json:"shortDelayMs"`
	LongDelayMs  int  `json:"longDelayMs"`
}

// LongReorderingResult 长延迟乱序查询/设置响应。
// GET 时不带 success 字段（零值由 omitempty 省略）。
type LongReorderingResult struct {
	Success        bool `json:"success,omitempty"`
	LongReordering bool `json:"longReordering"`
}

// ObserveStatus 观测开关查询响应（GET /api/observe）。
type ObserveStatus struct {
	Election    bool `json:"election"`
	Migration   bool `json:"migration"`
	KVSubmit    bool `json:"kvsubmit"`
	Snapshot    bool `json:"snapshot"`
	Replication bool `json:"replication"`
}

// ObserveSetResult 设置观测开关响应（POST /api/observe）。
type ObserveSetResult struct {
	Success bool   `json:"success"`
	Scene   string `json:"scene"`
	On      bool   `json:"on"`
}

// ObserveLogsResult 观测日志增量拉取响应。
type ObserveLogsResult struct {
	Lines  []debug.ObserveLine `json:"lines"`
	NextID int64               `json:"nextId"`
}

// ========== 异步任务结果载荷（TaskDoneEvent.Data） ==========

// PutResult PUT 任务的结果载荷。
type PutResult struct {
	Key    string `json:"key"`
	Value  string `json:"value"`
	Shard  int    `json:"shard"`
	ReqVer int    `json:"reqVer"`
}

// GetResult GET 任务的结果载荷。
type GetResult struct {
	Key     string `json:"key"`
	Value   string `json:"value"`
	Version int    `json:"version"`
}

// BatchPutResult 批量写入任务的结果载荷。
type BatchPutResult struct {
	SuccessCount int    `json:"successCount"`
	FailCount    int    `json:"failCount"`
	Elapsed      string `json:"elapsed"`
}

// CasRaceResult CAS 竞赛任务的结果载荷。
type CasRaceResult struct {
	Key             string `json:"key"`
	Version         int    `json:"version"`
	NClient         int    `json:"nClient"`
	SuccessCount    int    `json:"successCount"`
	VersionErrCount int    `json:"versionErrCount"`
	FinalValue      string `json:"finalValue"`
	Elapsed         string `json:"elapsed"`
}
