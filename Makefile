default: help

HOST_GOLANG_VERSION := $(go version | cut -d ' ' -f3 | cut -c 3-)
MODULE := redis-trib
ifneq (,$(wildcard .git/.*))
    COMMIT = $(shell git rev-parse HEAD 2> /dev/null || true)
    VERSION	= $(shell git describe --tags --abbrev=0 2> /dev/null)
else
    COMMIT = "unknown"
    VERSION = "unknown"
endif

## Make bin for redis-trib.
bin:
	go build -i -ldflags "-X main.gitCommit=${COMMIT} -X main.version=${VERSION}" -o redis-trib .

## Make static link bin for redis-trib.
static-bin: $(REDIS_LINK)
	go build -i -ldflags "-w -extldflags -static -X main.gitCommit=${COMMIT} -X main.version=${VERSION}" -o redis-trib .

## Get godep go tools.
godep:
	go get github.com/tools/godep
	godep restore

## Get govendor go tools.
govendor:
	go get -u -v github.com/kardianos/govendor
	govendor init
	govendor add +external

## Get vet go tools.
vet:
	go get golang.org/x/tools/cmd/vet

## Validate this go project.
validate:
	script/validate-gofmt
	#go vet ./...

## Run test case for this go project.
test:
	go test -v ./...

## Clean everything (including stray volumes).
clean:
#	find . -name '*.created' -exec rm -f {} +
	-rm -rf var
	-rm -f redis-trib
	-rm -rf ${REDIS_LINK}/

help: # Some kind of magic from https://gist.github.com/rcmachado/af3db315e31383502660
	$(info Available targets)
	@awk '/^[a-zA-Z\-\_0-9]+:/ {                                   \
		nb = sub( /^## /, "", helpMsg );                             \
		if(nb == 0) {                                                \
			helpMsg = $$0;                                             \
			nb = sub( /^[^:]*:.* ## /, "", helpMsg );                  \
		}                                                            \
		if (nb)                                                      \
			printf "\033[1;31m%-" width "s\033[0m %s\n", $$1, helpMsg; \
	}                                                              \
	{ helpMsg = $$0 }'                                             \
	width=$$(grep -o '^[a-zA-Z_0-9]\+:' $(MAKEFILE_LIST) | wc -L)  \
	$(MAKEFILE_LIST)

