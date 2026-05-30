# Use `RUN="-run Wc` to run a specific test; for example:
# `$ make RUN="-run TestName" shardkv`

VERB=-v
RACE=-race
VER=$(shell go version | grep -q 'go1.17.[012345]'; echo $$?)

MAINS=kvsrv raft kvraft
MAIN_BUILDS=$(addsuffix -build, $(MAINS))

all: shardkv

$(addsuffix -build, $(MAINS)): %-build : .FORCE check-version
	go build $(RACE) -o main/$* main/$*.go

$(MAINS): % : .FORCE check-version %-build
	cd $@ && go test -timeout 15m $(VERB) $(RACE) $(RUN)

shardkv: .FORCE check-version shardkv-build
	cd shardkv; go test -timeout 15m $(VERB) $(RACE) $(RUN)

shardkv-build: .FORCE check-version
	go build $(RACE) -o main/kvsrvd main/kvsrv.go
	go build $(RACE) -o main/shardgrpd main/shardgrp.go

.PHONY: .FORCE $(MAINS) $(MAIN_BUILDS)

# -race with plug-ins on x86 MacOS 12 with go1.17 before 1.17.6 sometimes crash.
check-version:
ifeq ($(VER), 0)
	@echo '*** Turning off -race since it may not work on a Mac'
	$(eval RACE := $(shell echo ""))
endif
