package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// GroupConfig 描述一个初始 Shard Group
type GroupConfig struct {
	Gid     int `yaml:"gid"`
	Servers int `yaml:"servers"` // 该组有多少个副本节点
}

// DemoConfig 完整配置，对应 config.yaml
type DemoConfig struct {
	MaxRaftState int  `yaml:"maxRaftState"` // Raft State 超过此字节数时触发快照；0 或负值表示不触发快照
	Nsrv         int  `yaml:"nsrv"`         // 每组副本节点数
	Reliable     bool `yaml:"reliable"`     // 是否可靠网络
	Cluster      struct {
		Groups []GroupConfig `yaml:"groups"` // 初始组列表（嵌套在 cluster 下，兼容旧配置）
	} `yaml:"cluster"`
	Groups []GroupConfig `yaml:"groups"` // 初始组列表（顶层，优先级更高）
}

// DefaultConfig 返回默认配置
func DefaultConfig() DemoConfig {
	var cfg DemoConfig
	cfg.MaxRaftState = 5000
	cfg.Nsrv = 3
	cfg.Reliable = true
	cfg.Groups = []GroupConfig{{Gid: 1, Servers: 3}}
	return cfg
}

// Load 从文件加载配置，文件不存在时返回默认值
func Load(path string) (DemoConfig, error) {
	cfg := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return cfg, fmt.Errorf("读取配置文件 %s 失败: %w", path, err)
	}

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return cfg, fmt.Errorf("解析 YAML 失败: %w", err)
	}

	// 兜底：顶层 groups 为空时尝试 cluster.groups
	if len(cfg.Groups) == 0 && len(cfg.Cluster.Groups) > 0 {
		cfg.Groups = cfg.Cluster.Groups
	}
	// 如果最后 groups 还是空的，用默认值
	if len(cfg.Groups) == 0 {
		cfg.Groups = []GroupConfig{{Gid: 1, Servers: cfg.Nsrv}}
	}
	// 确保每组 servers 字段 > 0，否则用全局 Nsrv
	for i := range cfg.Groups {
		if cfg.Groups[i].Servers <= 0 {
			cfg.Groups[i].Servers = cfg.Nsrv
		}
	}
	if cfg.Nsrv <= 0 {
		cfg.Nsrv = 3
	}

	return cfg, nil
}
