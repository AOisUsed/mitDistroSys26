package cluster

import (
	"kvstore/kvtest"
)

// ClerkPool Clerk 池
// 使用 Clerk 池可以防止频繁创建/删除 clerk，
// 同时避免新产生 clerk 第一次 KV操作前对 configStore 的查询压力

type ClerkPool struct {
	pool chan kvtest.IKVClerk
	cm   *ClusterManager
}

// NewClerkPool 创建一个容量为 size 的 Clerk 池。
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
func (p *ClerkPool) Return(ck kvtest.IKVClerk) {
	p.pool <- ck
}
