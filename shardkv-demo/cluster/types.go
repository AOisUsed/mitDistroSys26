package cluster

import (
	"6.5840/shardkv1/shardcfg"
	"6.5840/tester1"
)

// ClusterState 集群当前状态
type ClusterState struct {
	Groups []GroupState          `json:"groups"`
	Config *shardcfg.ShardConfig `json:"config"`
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
