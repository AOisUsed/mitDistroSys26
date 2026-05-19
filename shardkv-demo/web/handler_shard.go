package web

import (
	"encoding/json"
	"log"
	"net/http"

	"shardkv-demo/cluster"
)

// ShardHandler 处理 shard 相关的查询
type ShardHandler struct {
	cm *cluster.ClusterManager
}

func NewShardHandler(cm *cluster.ClusterManager) *ShardHandler {
	return &ShardHandler{cm: cm}
}

// HandleShardDetail 返回每个 shard 的详细信息
func (sh *ShardHandler) HandleShardDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state := sh.cm.Status()

	type ShardInfo struct {
		ShardID int                   `json:"shardId"`
		GroupID int                   `json:"groupId"`
		Servers []cluster.ServerState `json:"servers,omitempty"`
	}
	type GroupInfo struct {
		GroupID  int                   `json:"groupId"`
		NShards  int                   `json:"nShards"`
		Servers  []cluster.ServerState `json:"servers"`
		ShardIDs []int                 `json:"shardIds"`
	}

	// Build group-to-shards mapping
	groupShards := make(map[int][]int)
	for i, gid := range state.Config.Shards {
		g := int(gid)
		groupShards[g] = append(groupShards[g], i)
	}

	// Build response
	type Response struct {
		ConfigNum int         `json:"configNum"`
		Groups    []GroupInfo `json:"groups"`
		Shards    []ShardInfo `json:"shards"`
	}
	resp := Response{
		ConfigNum: int(state.Config.Num),
	}

	// Groups view
	for _, gs := range state.Groups {
		gid := int(gs.GID)
		gi := GroupInfo{
			GroupID:  gid,
			NShards:  gs.NShards,
			Servers:  gs.Servers,
			ShardIDs: groupShards[gid],
		}
		resp.Groups = append(resp.Groups, gi)
	}

	// Shards view
	for i, gid := range state.Config.Shards {
		g := int(gid)
		si := ShardInfo{
			ShardID: i,
			GroupID: g,
		}
		// Find servers for this group
		for _, gs := range state.Groups {
			if int(gs.GID) == g {
				si.Servers = gs.Servers
				break
			}
		}
		resp.Shards = append(resp.Shards, si)
	}

	writeJSON(w, resp)
}

// RegisterRoutes 注册 shard 相关的路由
func (sh *ShardHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/shard/detail", sh.HandleShardDetail)
}

// ensure the handler compiles
var _ = log.Printf
var _ = json.Marshal
