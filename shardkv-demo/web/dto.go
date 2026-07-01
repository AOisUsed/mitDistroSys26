package web

import (
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
