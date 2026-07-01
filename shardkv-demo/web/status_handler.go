package web

import (
	"net/http"
)

// HandleStatusTree 返回集群完整状态树（GET /api/status/tree）。
func (h *Handler) HandleStatusTree(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	state := h.cm.Status()

	tree := StatusTree{
		ConfigNum:           int(state.Config.Num),
		HasPendingMigration: state.HasPendingMigration,
		PendingConfigNum:    int(state.PendingConfigNum),
		ConfigCached:        state.ConfigCached,
		PoolSize:            h.cm.ClerkPoolSize(),
	}

	for _, gs := range state.Groups {
		tree.Groups = append(tree.Groups, GroupNode{
			GID:     gs.GID,
			Servers: gs.Servers,
			NShards: gs.NShards,
		})
	}
	for i, gid := range state.Config.Shards {
		tree.Shards = append(tree.Shards, ShardNode{Shard: i, GID: gid})
	}
	writeJSON(w, tree)
}
