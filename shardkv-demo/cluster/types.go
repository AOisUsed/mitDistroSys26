package cluster

import (
	"kvstore/shardkv/shardcfg"
	"kvstore/tester"
)

// ClusterState 集群当前状态
type ClusterState struct {
	Groups              []GroupState          `json:"groups"`
	Config              *shardcfg.ShardConfig `json:"config"`
	HasPendingMigration bool                  `json:"hasPendingMigration"`
	PendingConfigNum    shardcfg.Tnum         `json:"pendingConfigNum"`
	ConfigCached        bool                  `json:"configCached"` // 当 configStore 无响应时，Config 来自缓存
}

// GroupState 单个组的状态
type GroupState struct {
	GID      tester.Tgid   `json:"gid"`
	Servers  []ServerState `json:"servers"`
	NShards  int           `json:"nShards"`
	SrvNames []string      `json:"srvNames"`
}

// ServerState 单个节点状态
type ServerState struct {
	Index      int    `json:"index"`
	Name       string `json:"name"`
	IsLeader   bool   `json:"isLeader"`
	IsAlive    bool   `json:"isAlive"`
	IsIsolated bool   `json:"isIsolated"`
}
