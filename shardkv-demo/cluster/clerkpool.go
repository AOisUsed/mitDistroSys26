package cluster

import (
	"kvstore/kvtest"
)

// ClerkPool 使用 buffered channel 实现的 Clerk 池。
//
// 设计要点：
//   - 池大小固定（默认 100），创建时预填充 Clerk，不做 config 预热。
//   - Borrow() 在池空时阻塞，天然限制并发度（背压）。
//   - Return() 将 Clerk 放回池尾，供下次复用。
//   - 每个 Clerk 拥有独立的 clerkId，服务端按 clientId 分别去重，互不干扰。
//   - 复用 Clerk 可避免每次批量写入创建大量临时对象，同时复用 cachedConfig，
//     减少 configStore（Raft 集群）的 Query() 并发压力。
type ClerkPool struct {
	pool chan kvtest.IKVClerk
	cm   *ClusterManager
}

// NewClerkPool 创建一个容量为 size 的 Clerk 池。
// 池在创建时同步填充 size 个 Clerk（不预热 config cache）。
func NewClerkPool(cm *ClusterManager, size int) *ClerkPool {
	p := &ClerkPool{
		pool: make(chan kvtest.IKVClerk, size),
		cm:   cm,
	}
	for i := 0; i < size; i++ {
		p.pool <- cm.NewClerk()
	}
	return p
}

// Borrow 从池中借出一个 Clerk。如果池为空，阻塞直到有 Clerk 归还。
func (p *ClerkPool) Borrow() kvtest.IKVClerk {
	return <-p.pool
}

// Return 将用完的 Clerk 归还到池中。
// 调用者必须确保每次 Borrow 都有对应的 Return（建议用 defer）。
func (p *ClerkPool) Return(ck kvtest.IKVClerk) {
	p.pool <- ck
}
