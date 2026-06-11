module shardkv-demo

go 1.22

require (
	gopkg.in/yaml.v3 v3.0.1
	kvstore v0.0.0
)

require github.com/anishathalye/porcupine v1.0.3 // indirect

replace kvstore => ../src
