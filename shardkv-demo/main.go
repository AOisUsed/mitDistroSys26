package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"shardkv-demo/cluster"
	"shardkv-demo/web"
)

func main() {
	port := flag.Int("port", 8080, "HTTP server port")
	flag.Parse()

	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.Lshortfile)
	log.Printf("shardkv-demo 正在启动...")

	// 保存 shardkv-demo 目录路径（用于定位静态文件）
	demoDir, err := os.Getwd()
	if err != nil {
		log.Fatalf("获取当前目录失败: %v", err)
	}

	// 必须将工作目录切换到 src/ 下，因为 daemon 进程（kvsrv, shardgrp）
	// 会通过 cwd 中的 "src" 来定位二进制文件路径
	srcDir, err := filepath.Abs("../src")
	if err != nil {
		log.Fatalf("获取 src 目录失败: %v", err)
	}
	if err := os.Chdir(srcDir); err != nil {
		log.Fatalf("切换工作目录到 %s 失败: %v", srcDir, err)
	}
	log.Printf("工作目录切换到: %s", srcDir)

	// 创建集群管理器
	cm := cluster.NewClusterManager()

	// 初始化集群
	if err := cm.Init(); err != nil {
		log.Fatalf("集群初始化失败: %v", err)
	}
	defer cm.Stop()

	// 创建 HTTP handler 并注册路由
	mux := http.NewServeMux()
	h := web.NewHandler(cm)
	h.SetStaticDir(demoDir) // 设置静态文件目录
	h.RegisterRoutes(mux)
	sh := web.NewShardHandler(cm)
	sh.RegisterRoutes(mux)

	// 添加 CORS 中间件
	handler := corsMiddleware(mux)

	// 启动 HTTP 服务
	addr := fmt.Sprintf(":%d", *port)
	log.Printf("HTTP 服务已启动: http://localhost%s", addr)
	log.Printf("打开浏览器访问 http://localhost%s 查看集群控制台", addr)

	// 优雅关闭
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Printf("收到终止信号，正在关闭...")
		cm.Stop()
		os.Exit(0)
	}()

	if err := http.ListenAndServe(addr, handler); err != nil {
		log.Fatalf("HTTP 服务错误: %v", err)
	}
}

// corsMiddleware 添加 CORS 头以支持前端跨域请求
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}
