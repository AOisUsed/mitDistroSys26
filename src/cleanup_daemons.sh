#!/usr/bin/env bash
# cleanup_daemons.sh — 清理残留的 daemon 进程和 UNIX socket 文件
#
# 当测试/demo 异常中断时，daemon 子进程和 socket 文件可能未被正常清理。
# 此脚本查找并杀掉所有残留进程，删除残留的 socket 文件。
set -euo pipefail

# Daemon 二进制名称（与 src/main/ 目录下的文件名一致）
DAEMON_NAMES=("kvraft" "kvsrv" "raft" "shardgrp")

# --- 1. 清理 daemon 进程 ---
echo "==> 查找残留 daemon 进程..."

PIDS=()
for name in "${DAEMON_NAMES[@]}"; do
    # 匹配命令行中包含 "src/main/<name>" 的进程
    while IFS= read -r pid; do
        if [[ -n "$pid" ]]; then
            PIDS+=("$pid")
        fi
    done < <(pgrep -f "src/main/${name}" 2>/dev/null || true)
done

if [[ ${#PIDS[@]} -eq 0 ]]; then
    echo "  没有发现残留 daemon 进程。"
else
    echo "  发现 ${#PIDS[@]} 个残留 daemon 进程: ${PIDS[*]}"
    # 先发送 SIGTERM，等待 2 秒；仍未退出的强制 SIGKILL
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 2
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done
    echo "  已清理。"
fi

# --- 2. 清理 /tmp 下的 UNIX socket 残留文件 ---
echo "==> 清理 UNIX socket 残留文件..."
SOCK_FILES=(/tmp/kvstore-*)
REMOVED=0
for f in "${SOCK_FILES[@]}"; do
    # 跳过 glob 未展开的情况
    [[ -e "$f" ]] || continue
    rm -f "$f"
    REMOVED=$((REMOVED + 1))
done

if [[ $REMOVED -eq 0 ]]; then
    echo "  没有残留 socket 文件。"
else
    echo "  已清理 ${REMOVED} 个 socket 文件。"
fi

echo "==> 完成。"
